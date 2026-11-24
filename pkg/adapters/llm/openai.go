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
	} else if !strings.HasSuffix(endpoint, "/chat/completions") {
		endpoint = strings.TrimRight(endpoint, "/") + "/v1/chat/completions"
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

		var result struct {
			Choices []struct {
				Message struct {
					Role      string `json:"role"`
					Content   string `json:"content"`
					ToolCalls []struct {
						ID       string `json:"id"`
						Type     string `json:"type"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"message"`
			} `json:"choices"`
		}

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
				var nonStream struct {
					Choices []struct {
						Message struct {
							Content string `json:"content"`
						} `json:"message"`
					} `json:"choices"`
				}
				if json.Unmarshal(rawBody, &nonStream) == nil && len(nonStream.Choices) > 0 {
					sendToken(ctx, tokenChan, nonStream.Choices[0].Message.Content)
					return true, nil
				}
				resp.Body = io.NopCloser(bytes.NewReader(rawBody))
			}

			type partialToolCall struct {
				id   string
				name string
				args strings.Builder
			}
			partials := make(map[int]*partialToolCall)
			var fullContent strings.Builder

			scanner := bufio.NewScanner(resp.Body)
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
					fullContent.WriteString(delta.Content)
				}
				for _, tc := range delta.ToolCalls {
					entry, ok := partials[tc.Index]
					if !ok {
						entry = &partialToolCall{}
						partials[tc.Index] = entry
					}
					if tc.ID != "" {
						entry.id = tc.ID
					}
					if tc.Function.Name != "" {
						entry.name += tc.Function.Name
					}
					if tc.Function.Arguments != "" {
						entry.args.WriteString(tc.Function.Arguments)
					}
				}
			}
			scanErr := scanner.Err()
			resp.Body.Close()
			if scanErr != nil && ctx.Err() == nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[OpenAI stream read error: %v]", scanErr))
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

			assistantCalls := make([]map[string]any, 0, len(indices))
			for _, idx := range indices {
				p := partials[idx]
				callID := p.id
				if callID == "" {
					callID = fmt.Sprintf("call_%d", idx)
				}
				assistantCalls = append(assistantCalls, map[string]any{
					"id":   callID,
					"type": "function",
					"function": map[string]any{
						"name":      p.name,
						"arguments": p.args.String(),
					},
				})
			}

			messages = append(messages, map[string]any{
				"role":       "assistant",
				"content":    fullContent.String(),
				"tool_calls": assistantCalls,
			})

			for _, idx := range indices {
				p := partials[idx]
				callID := p.id
				if callID == "" {
					callID = fmt.Sprintf("call_%d", idx)
				}
				if !sendToken(ctx, tokenChan, fmt.Sprintf("\n*(Running tool: %s...)*\n", p.name)) {
					return true, nil
				}
				obs := executeToolCallRaw(ctx, a.tools, p.name, p.args.String())
				messages = append(messages, map[string]any{
					"role":         "tool",
					"tool_call_id": callID,
					"content":      obs,
				})
			}
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

func NewLlamaCPPAdapter(endpoint string, tools ToolExecutor) *LlamaCPPAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://127.0.0.1:8080/v1/chat/completions"
	}
	return &LlamaCPPAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, "local-no-key", "default", tools),
	}
}

// VLLMAdapter wraps OpenAIAdapter for a local or cluster vLLM server.
type VLLMAdapter struct {
	*OpenAIAdapter
}

func NewVLLMAdapter(endpoint, model string, tools ToolExecutor) *VLLMAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://127.0.0.1:8000/v1/chat/completions"
	}
	if strings.TrimSpace(model) == "" {
		model = "Qwen/Qwen2.5-Coder-32B-Instruct"
	}
	return &VLLMAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, "vllm-no-key", model, tools),
	}
}
