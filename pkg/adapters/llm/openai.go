package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/drewv-labs/carrierpigeons/internal/jsonutil"
)

var _ BaseLLM = (*OpenAIAdapter)(nil)

type OpenAIAdapter struct {
	endpoint string
	apiKey   string
	model    string
	tools    ToolExecutor
	client   *http.Client
}

func NewOpenAIAdapter(endpoint, apiKey, model string, tools ToolExecutor) *OpenAIAdapter {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1/chat/completions"
	} else {
		trimmed := strings.TrimRight(endpoint, "/")
		switch {
		case strings.HasSuffix(trimmed, "/chat/completions"):
			endpoint = trimmed
		case strings.HasSuffix(trimmed, "/v1"):
			endpoint = trimmed + "/chat/completions"
		default:
			endpoint = trimmed + "/v1/chat/completions"
		}
	}
	if strings.TrimSpace(model) == "" {
		model = "gpt-4o"
	}
	return &OpenAIAdapter{
		endpoint: endpoint,
		apiKey:   apiKey,
		model:    model,
		tools:    tools,
		client:   defaultLLMHTTPClient(),
	}
}

func (a *OpenAIAdapter) headers() map[string]string {
	if a.apiKey == "" {
		return nil
	}
	return map[string]string{"Authorization": "Bearer " + a.apiKey}
}

func buildOpenAIMessages(systemPrompt string, history []Message) []map[string]any {
	messages := make([]map[string]any, 0, len(history)+1)
	messages = append(messages, map[string]any{"role": "system", "content": systemPrompt})
	for i, m := range history {
		role := strings.ToLower(strings.TrimSpace(m.Role))
		switch role {
		case "tool":
			callID := strings.TrimSpace(m.ToolCallID)
			if callID == "" {
				callID = fmt.Sprintf("call_%d", i)
			}
			messages = append(messages, map[string]any{
				"role":         "tool",
				"tool_call_id": callID,
				"content":      m.Content,
			})
		case "assistant":
			entry := map[string]any{
				"role":    "assistant",
				"content": m.Content,
			}
			if len(m.ToolCalls) > 0 {
				wireCalls := make([]map[string]any, 0, len(m.ToolCalls))
				for idx, tc := range m.ToolCalls {
					callID := strings.TrimSpace(tc.ID)
					if callID == "" {
						callID = fmt.Sprintf("call_%d_%d", i, idx)
					}
					argsStr := "{}"
					if len(tc.Function.Arguments) > 0 {
						if b, err := json.Marshal(tc.Function.Arguments); err == nil {
							argsStr = string(b)
						}
					}
					wireCalls = append(wireCalls, map[string]any{
						"id":   callID,
						"type": "function",
						"function": map[string]any{
							"name":      tc.Function.Name,
							"arguments": argsStr,
						},
					})
				}
				entry["tool_calls"] = wireCalls
			}
			messages = append(messages, entry)
		default:
			if role == "" {
				role = "user"
			}
			messages = append(messages, map[string]any{"role": role, "content": m.Content})
		}
	}
	return messages
}

type openAIWireToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message struct {
			Role      string               `json:"role"`
			Content   string               `json:"content"`
			ToolCalls []openAIWireToolCall `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
}

type openAIResolvedToolCall struct {
	id   string
	name string
	args string
}

func (a *OpenAIAdapter) executeStreamToolTurn(
	ctx context.Context,
	tokenChan chan<- string,
	messages []map[string]any,
	assistantContent string,
	calls []openAIResolvedToolCall,
) ([]map[string]any, bool) {
	assistantCalls := make([]map[string]any, 0, len(calls))
	for _, tc := range calls {
		assistantCalls = append(assistantCalls, map[string]any{
			"id":   tc.id,
			"type": "function",
			"function": map[string]any{
				"name":      tc.name,
				"arguments": tc.args,
			},
		})
	}

	messages = append(messages, map[string]any{
		"role":       "assistant",
		"content":    assistantContent,
		"tool_calls": assistantCalls,
	})

	for _, tc := range calls {
		if !sendToken(ctx, tokenChan, fmt.Sprintf("\n*(Running tool: %s...)*\n", tc.name)) {
			return nil, false
		}
		obs := executeToolCallRaw(ctx, a.tools, tc.name, tc.args)
		messages = append(messages, map[string]any{
			"role":         "tool",
			"tool_call_id": tc.id,
			"content":      obs,
		})
	}
	return messages, true
}

func (a *OpenAIAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	messages := buildOpenAIMessages(systemPrompt, history)
	var finalReply string

	err := runToolLoop(defaultMaxToolTurns, "openai", func(_ int) (bool, error) {
		payload := map[string]any{
			"model":    a.model,
			"messages": messages,
		}
		if a.tools != nil {
			if defs := a.tools.Definitions(); len(defs) > 0 {
				payload["tools"] = defs
			}
		}

		resp, err := postJSON(ctx, a.client, a.endpoint, a.headers(), payload, "OpenAI")
		if err != nil {
			return false, err
		}

		var result openAIChatResponse

		err = json.NewDecoder(io.LimitReader(resp.Body, 4*1024*1024)).Decode(&result)
		resp.Body.Close()
		if err != nil || len(result.Choices) == 0 {
			return false, fmt.Errorf("failed to decode openai response")
		}

		choice := result.Choices[0].Message
		if len(choice.ToolCalls) == 0 || a.tools == nil {
			finalReply = choice.Content
			return true, nil
		}

		messages = append(messages, map[string]any{
			"role":       "assistant",
			"content":    choice.Content,
			"tool_calls": choice.ToolCalls,
		})
		for _, tc := range choice.ToolCalls {
			obs := executeToolCallRaw(ctx, a.tools, tc.Function.Name, tc.Function.Arguments)
			messages = append(messages, map[string]any{
				"role":         "tool",
				"tool_call_id": tc.ID,
				"content":      obs,
			})
		}
		return false, nil
	})
	if err != nil {
		return "", err
	}
	return finalReply, nil
}

func (a *OpenAIAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	tokenChan := make(chan string)

	go func() {
		defer close(tokenChan)
		messages := buildOpenAIMessages(systemPrompt, history)

		if err := runToolLoop(defaultMaxToolTurns, "openai", func(_ int) (bool, error) {
			payload := map[string]any{
				"model":    a.model,
				"messages": messages,
				"stream":   true,
			}
			if a.tools != nil {
				if defs := a.tools.Definitions(); len(defs) > 0 {
					payload["tools"] = defs
				}
			}

			resp, err := postJSON(ctx, a.client, a.endpoint, a.headers(), payload, "OpenAI")
			if err != nil {
				if ctx.Err() == nil {
					sendToken(ctx, tokenChan, fmt.Sprintf("[%v]", err))
				}
				return true, nil
			}

			// Support both SSE (`text/event-stream`) and non-SSE JSON fallback servers seamlessly.
			ct := strings.ToLower(resp.Header.Get("Content-Type"))
			if !strings.Contains(ct, "event-stream") {
				rawBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
				resp.Body.Close()
				var nonStream openAIChatResponse
				if json.Unmarshal(rawBody, &nonStream) == nil && len(nonStream.Choices) > 0 {
					msg := nonStream.Choices[0].Message
					if msg.Content != "" {
						if !sendToken(ctx, tokenChan, msg.Content) {
							return true, nil
						}
					}
					if len(msg.ToolCalls) == 0 || a.tools == nil {
						return true, nil
					}
					calls := make([]openAIResolvedToolCall, 0, len(msg.ToolCalls))
					for idx, tc := range msg.ToolCalls {
						callID := tc.ID
						if callID == "" {
							callID = fmt.Sprintf("call_%d", idx)
						}
						calls = append(calls, openAIResolvedToolCall{
							id:   callID,
							name: tc.Function.Name,
							args: tc.Function.Arguments,
						})
					}
					nextMessages, ok := a.executeStreamToolTurn(ctx, tokenChan, messages, msg.Content, calls)
					if !ok {
						return true, nil
					}
					messages = nextMessages
					return false, nil
				}
				resp.Body = io.NopCloser(bytes.NewReader(rawBody))
			}

			type partialToolCall struct {
				id   string
				name strings.Builder
				args strings.Builder
			}
			partials := make(map[int]*partialToolCall)
			var fullContent strings.Builder
			var capErr error

			capped := newCappedStreamReader(resp.Body, streamBodyLimit)
			scanner := bufio.NewScanner(capped)
			scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if data == "" || data == "[DONE]" {
					continue
				}
				var chunk struct {
					Choices []struct {
						Delta struct {
							Content   string `json:"content"`
							ToolCalls []struct {
								Index    int    `json:"index"`
								ID       string `json:"id"`
								Function struct {
									Name      string `json:"name"`
									Arguments string `json:"arguments"`
								} `json:"function"`
							} `json:"tool_calls"`
						} `json:"delta"`
					} `json:"choices"`
				}
				if json.Unmarshal([]byte(data), &chunk) != nil || len(chunk.Choices) == 0 {
					continue
				}
				delta := chunk.Choices[0].Delta
				if delta.Content != "" {
					if !sendToken(ctx, tokenChan, delta.Content) {
						resp.Body.Close()
						return true, nil
					}
					writeBounded(&fullContent, delta.Content, streamContentLimit)
				}
				for _, tc := range delta.ToolCalls {
					entry, ok := partials[tc.Index]
					if !ok {
						if len(partials) >= maxStreamToolCalls {
							capErr = fmt.Errorf("stream exceeded max tool calls (%d)", maxStreamToolCalls)
							break
						}
						entry = &partialToolCall{}
						partials[tc.Index] = entry
					}
					if tc.ID != "" {
						entry.id = tc.ID
					}
					if tc.Function.Name != "" {
						if !writeBounded(&entry.name, tc.Function.Name, maxStreamToolNameBytes) {
							capErr = fmt.Errorf("stream tool name exceeded limit (%d bytes)", maxStreamToolNameBytes)
							break
						}
					}
					if tc.Function.Arguments != "" {
						if !writeBounded(&entry.args, tc.Function.Arguments, streamToolArgLimit) {
							capErr = fmt.Errorf("stream tool arguments exceeded limit (%d bytes)", streamToolArgLimit)
							break
						}
					}
				}
				if capErr != nil {
					break
				}
			}
			scanErr := scanner.Err()
			resp.Body.Close()
			if capErr != nil && ctx.Err() == nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[OpenAI stream read error: %v]", capErr))
				return true, nil
			}
			if scanErr != nil && ctx.Err() == nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[OpenAI stream read error: %v]", jsonutil.RedactError(scanErr, a.apiKey)))
				return true, nil
			}

			if len(partials) == 0 || a.tools == nil {
				return true, nil
			}

			indices := make([]int, 0, len(partials))
			for idx := range partials {
				indices = append(indices, idx)
			}
			sort.Ints(indices)

			calls := make([]openAIResolvedToolCall, 0, len(indices))
			for _, idx := range indices {
				p := partials[idx]
				callID := p.id
				if callID == "" {
					callID = fmt.Sprintf("call_%d", idx)
				}
				calls = append(calls, openAIResolvedToolCall{
					id:   callID,
					name: p.name.String(),
					args: p.args.String(),
				})
			}

			nextMessages, ok := a.executeStreamToolTurn(ctx, tokenChan, messages, fullContent.String(), calls)
			if !ok {
				return true, nil
			}
			messages = nextMessages
			return false, nil
		}); err != nil && ctx.Err() == nil {
			sendToken(ctx, tokenChan, fmt.Sprintf("\n[OpenAI tool loop error: %v]", err))
		}
	}()

	return tokenChan, nil
}

// LlamaCPPAdapter wraps OpenAIAdapter for a local llama.cpp server.
type LlamaCPPAdapter struct {
	*OpenAIAdapter
}

func NewLlamaCPPAdapter(endpoint, apiKey, model string, tools ToolExecutor) *LlamaCPPAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://127.0.0.1:8080/v1/chat/completions"
	}
	if strings.TrimSpace(apiKey) == "" {
		apiKey = "local-no-key"
	}
	if strings.TrimSpace(model) == "" {
		model = "default"
	}
	return &LlamaCPPAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, apiKey, model, tools),
	}
}

// VLLMAdapter wraps OpenAIAdapter for a local or cluster vLLM server.
type VLLMAdapter struct {
	*OpenAIAdapter
}

func NewVLLMAdapter(endpoint, apiKey, model string, tools ToolExecutor) *VLLMAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://127.0.0.1:8000/v1/chat/completions"
	}
	if strings.TrimSpace(apiKey) == "" {
		apiKey = "vllm-no-key"
	}
	if strings.TrimSpace(model) == "" {
		model = "Qwen/Qwen2.5-Coder-32B-Instruct"
	}
	return &VLLMAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, apiKey, model, tools),
	}
}
