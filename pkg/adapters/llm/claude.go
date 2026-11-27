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

var _ BaseLLM = (*ClaudeAdapter)(nil)

type ClaudeAdapter struct {
	endpoint string
	apiKey   string
	model    string
	tools    ToolExecutor
	client   *http.Client
}

func NewClaudeAdapter(apiKey, model string, tools ToolExecutor) *ClaudeAdapter {
	return NewClaudeAdapterWithEndpoint("https://api.anthropic.com/v1/messages", apiKey, model, tools)
}

func NewClaudeAdapterWithEndpoint(endpoint, apiKey, model string, tools ToolExecutor) *ClaudeAdapter {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = "https://api.anthropic.com/v1/messages"
	}
	if strings.TrimSpace(model) == "" {
		model = "claude-sonnet-4-5"
	}
	return &ClaudeAdapter{
		endpoint: endpoint,
		apiKey:   apiKey,
		model:    model,
		tools:    tools,
		client:   defaultLLMHTTPClient(),
	}
}

func (a *ClaudeAdapter) headers() map[string]string {
	return map[string]string{
		"x-api-key":         a.apiKey,
		"anthropic-version": "2023-06-01",
	}
}

func convertToAnthropicTools(defs []map[string]any) []map[string]any {
	var out []map[string]any
	for _, d := range defs {
		fn, ok := d["function"].(map[string]any)
		if !ok {
			continue
		}
		name, _ := fn["name"].(string)
		if name == "" {
			continue
		}
		desc, _ := fn["description"].(string)
		schema := fn["parameters"]
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, map[string]any{
			"name":         name,
			"description":  desc,
			"input_schema": schema,
		})
	}
	return out
}

func buildAnthropicMessages(history []Message) []map[string]any {
	messages := make([]map[string]any, 0, len(history))
	for i, m := range history {
		role := strings.ToLower(strings.TrimSpace(m.Role))
		if role == "system" {
			continue
		}
		if role == "tool" {
			callID := strings.TrimSpace(m.ToolCallID)
			if callID == "" {
				callID = fmt.Sprintf("toolu_%d", i)
			}
			block := map[string]any{
				"type":        "tool_result",
				"tool_use_id": callID,
				"content":     m.Content,
			}
			// Coalesce consecutive tool_result blocks into one user turn per Anthropic API requirements.
			if len(messages) > 0 && messages[len(messages)-1]["role"] == "user" {
				if prevBlocks, ok := messages[len(messages)-1]["content"].([]map[string]any); ok {
					messages[len(messages)-1]["content"] = append(prevBlocks, block)
					continue
				}
			}
			messages = append(messages, map[string]any{
				"role":    "user",
				"content": []map[string]any{block},
			})
			continue
		}
		if role == "assistant" && len(m.ToolCalls) > 0 {
			blocks := make([]map[string]any, 0, len(m.ToolCalls)+1)
			if m.Content != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": m.Content})
			}
			for idx, tc := range m.ToolCalls {
				callID := strings.TrimSpace(tc.ID)
				if callID == "" {
					callID = fmt.Sprintf("toolu_%d_%d", i, idx)
				}
				input := tc.Function.Arguments
				if input == nil {
					input = map[string]any{}
				}
				blocks = append(blocks, map[string]any{
					"type":  "tool_use",
					"id":    callID,
					"name":  tc.Function.Name,
					"input": input,
				})
			}
			messages = append(messages, map[string]any{
				"role":    "assistant",
				"content": blocks,
			})
			continue
		}
		if role != "assistant" {
			role = "user"
		}
		messages = append(messages, map[string]any{
			"role":    role,
			"content": m.Content,
		})
	}
	return messages
}

type claudeResponse struct {
	StopReason string `json:"stop_reason"`
	Content    []struct {
		Type  string         `json:"type"`
		Text  string         `json:"text,omitempty"`
		ID    string         `json:"id,omitempty"`
		Name  string         `json:"name,omitempty"`
		Input map[string]any `json:"input,omitempty"`
	} `json:"content"`
}

func (a *ClaudeAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	messages := buildAnthropicMessages(history)
	var anthropicTools []map[string]any
	if a.tools != nil {
		anthropicTools = convertToAnthropicTools(a.tools.Definitions())
	}

	var finalReply string
	err := runToolLoop(defaultMaxToolTurns, "claude", func(_ int) (bool, error) {
		payload := map[string]any{
			"model":      a.model,
			"max_tokens": 2048,
			"system":     systemPrompt,
			"messages":   messages,
		}
		if len(anthropicTools) > 0 {
			payload["tools"] = anthropicTools
		}

		resp, err := postJSON(ctx, a.client, a.endpoint, a.headers(), payload, "Claude")
		if err != nil {
			return false, err
		}

		var result claudeResponse
		err = json.NewDecoder(io.LimitReader(resp.Body, 4*1024*1024)).Decode(&result)
		resp.Body.Close()
		if err != nil || len(result.Content) == 0 {
			return false, fmt.Errorf("failed to decode claude response")
		}

		var textParts []string
		var assistantBlocks []map[string]any
		var toolResults []map[string]any

		for _, block := range result.Content {
			switch block.Type {
			case "tool_use":
				input := block.Input
				if input == nil {
					input = map[string]any{}
				}
				assistantBlocks = append(assistantBlocks, map[string]any{
					"type":  "tool_use",
					"id":    block.ID,
					"name":  block.Name,
					"input": input,
				})
				if a.tools != nil {
					obs := a.tools.Execute(ctx, block.Name, input)
					toolResults = append(toolResults, map[string]any{
						"type":        "tool_result",
						"tool_use_id": block.ID,
						"content":     obs,
					})
				}
			default:
				if block.Text != "" {
					textParts = append(textParts, block.Text)
					assistantBlocks = append(assistantBlocks, map[string]any{
						"type": "text",
						"text": block.Text,
					})
				}
			}
		}

		if len(toolResults) == 0 || a.tools == nil {
			finalReply = strings.Join(textParts, "\n")
			return true, nil
		}

		messages = append(messages,
			map[string]any{"role": "assistant", "content": assistantBlocks},
			map[string]any{"role": "user", "content": toolResults},
		)
		return false, nil
	})
	if err != nil {
		return "", err
	}
	return finalReply, nil
}

func (a *ClaudeAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	tokenChan := make(chan string)

	go func() {
		defer close(tokenChan)
		messages := buildAnthropicMessages(history)
		var anthropicTools []map[string]any
		if a.tools != nil {
			anthropicTools = convertToAnthropicTools(a.tools.Definitions())
		}

		if err := runToolLoop(defaultMaxToolTurns, "claude", func(_ int) (bool, error) {
			payload := map[string]any{
				"model":      a.model,
				"max_tokens": 2048,
				"system":     systemPrompt,
				"messages":   messages,
				"stream":     true,
			}
			if len(anthropicTools) > 0 {
				payload["tools"] = anthropicTools
			}

			resp, err := postJSON(ctx, a.client, a.endpoint, a.headers(), payload, "Claude")
			if err != nil {
				if ctx.Err() == nil {
					sendToken(ctx, tokenChan, fmt.Sprintf("[%v]", err))
				}
				return true, nil
			}

			// Support non-SSE JSON fallback seamlessly for proxies/tests.
			ct := strings.ToLower(resp.Header.Get("Content-Type"))
			if !strings.Contains(ct, "event-stream") {
				rawBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
				resp.Body.Close()
				var nonStream claudeResponse
				if json.Unmarshal(rawBody, &nonStream) == nil && len(nonStream.Content) > 0 {
					var assistantBlocks []map[string]any
					var toolResults []map[string]any
					for _, block := range nonStream.Content {
						switch block.Type {
						case "tool_use":
							input := block.Input
							if input == nil {
								input = map[string]any{}
							}
							assistantBlocks = append(assistantBlocks, map[string]any{
								"type":  "tool_use",
								"id":    block.ID,
								"name":  block.Name,
								"input": input,
							})
							if a.tools != nil {
								if !sendToken(ctx, tokenChan, fmt.Sprintf("\n*(Running tool: %s...)*\n", block.Name)) {
									return true, nil
								}
								obs := a.tools.Execute(ctx, block.Name, input)
								toolResults = append(toolResults, map[string]any{
									"type":        "tool_result",
									"tool_use_id": block.ID,
									"content":     obs,
								})
							}
						default:
							if block.Text != "" {
								if !sendToken(ctx, tokenChan, block.Text) {
									return true, nil
								}
								assistantBlocks = append(assistantBlocks, map[string]any{
									"type": "text",
									"text": block.Text,
								})
							}
						}
					}
					if len(toolResults) == 0 || a.tools == nil {
						return true, nil
					}
					messages = append(messages,
						map[string]any{"role": "assistant", "content": assistantBlocks},
						map[string]any{"role": "user", "content": toolResults},
					)
					return false, nil
				}
				resp.Body = io.NopCloser(bytes.NewReader(rawBody))
			}

			type streamBlock struct {
				blockType string
				id        string
				name      string
				text      strings.Builder
				inputJSON strings.Builder
			}
			blocks := make(map[int]*streamBlock)
			totalTextBytes := 0
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
				var ev struct {
					Type         string `json:"type"`
					Index        int    `json:"index"`
					ContentBlock struct {
						Type  string         `json:"type"`
						ID    string         `json:"id"`
						Name  string         `json:"name"`
						Text  string         `json:"text"`
						Input map[string]any `json:"input"`
					} `json:"content_block"`
					Delta struct {
						Type        string `json:"type"`
						Text        string `json:"text"`
						PartialJSON string `json:"partial_json"`
					} `json:"delta"`
				}
				if json.Unmarshal([]byte(data), &ev) != nil {
					continue
				}
				switch ev.Type {
				case "content_block_start":
					if _, exists := blocks[ev.Index]; !exists && len(blocks) >= maxStreamToolCalls {
						capErr = fmt.Errorf("stream exceeded max content blocks (%d)", maxStreamToolCalls)
						break
					}
					b := &streamBlock{
						blockType: ev.ContentBlock.Type,
						id:        ev.ContentBlock.ID,
						name:      ev.ContentBlock.Name,
					}
					if ev.ContentBlock.Text != "" {
						if totalTextBytes+len(ev.ContentBlock.Text) <= streamContentLimit {
							b.text.WriteString(ev.ContentBlock.Text)
							totalTextBytes += len(ev.ContentBlock.Text)
						}
						if !sendToken(ctx, tokenChan, ev.ContentBlock.Text) {
							resp.Body.Close()
							return true, nil
						}
					}
					if len(ev.ContentBlock.Input) > 0 {
						if raw, mErr := json.Marshal(ev.ContentBlock.Input); mErr == nil {
							if !writeBounded(&b.inputJSON, string(raw), streamToolArgLimit) {
								capErr = fmt.Errorf("stream tool input exceeded limit (%d bytes)", streamToolArgLimit)
								break
							}
						}
					}
					blocks[ev.Index] = b
				case "content_block_delta":
					b, ok := blocks[ev.Index]
					if !ok {
						if len(blocks) >= maxStreamToolCalls {
							capErr = fmt.Errorf("stream exceeded max content blocks (%d)", maxStreamToolCalls)
							break
						}
						b = &streamBlock{blockType: "text"}
						blocks[ev.Index] = b
					}
					switch ev.Delta.Type {
					case "text_delta":
						if ev.Delta.Text != "" {
							if totalTextBytes+len(ev.Delta.Text) <= streamContentLimit {
								b.text.WriteString(ev.Delta.Text)
								totalTextBytes += len(ev.Delta.Text)
							}
							if !sendToken(ctx, tokenChan, ev.Delta.Text) {
								resp.Body.Close()
								return true, nil
							}
						}
					case "input_json_delta":
						if ev.Delta.PartialJSON != "" {
							if !writeBounded(&b.inputJSON, ev.Delta.PartialJSON, streamToolArgLimit) {
								capErr = fmt.Errorf("stream tool input exceeded limit (%d bytes)", streamToolArgLimit)
							}
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
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[Claude stream read error: %v]", capErr))
				return true, nil
			}
			if scanErr != nil && ctx.Err() == nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[Claude stream read error: %v]", jsonutil.RedactError(scanErr, a.apiKey)))
				return true, nil
			}

			indices := make([]int, 0, len(blocks))
			for idx := range blocks {
				indices = append(indices, idx)
			}
			sort.Ints(indices)

			var assistantBlocks []map[string]any
			var toolResults []map[string]any
			for _, idx := range indices {
				b := blocks[idx]
				if b.blockType == "tool_use" {
					callID := b.id
					if callID == "" {
						callID = fmt.Sprintf("toolu_%d", idx)
					}
					parsedInput, parseErr := parseToolArgs(b.inputJSON.String())
					wireInput := parsedInput
					if wireInput == nil {
						wireInput = map[string]any{}
					}
					assistantBlocks = append(assistantBlocks, map[string]any{
						"type":  "tool_use",
						"id":    callID,
						"name":  b.name,
						"input": wireInput,
					})
					if a.tools != nil {
						if !sendToken(ctx, tokenChan, fmt.Sprintf("\n*(Running tool: %s...)*\n", b.name)) {
							return true, nil
						}
						var obs string
						if parseErr != nil {
							obs = fmt.Sprintf("Error: invalid JSON arguments for tool %q: %v", b.name, parseErr)
						} else {
							obs = a.tools.Execute(ctx, b.name, parsedInput)
						}
						toolResults = append(toolResults, map[string]any{
							"type":        "tool_result",
							"tool_use_id": callID,
							"content":     obs,
						})
					}
				} else if b.text.Len() > 0 {
					assistantBlocks = append(assistantBlocks, map[string]any{
						"type": "text",
						"text": b.text.String(),
					})
				}
			}

			if len(toolResults) == 0 || a.tools == nil {
				return true, nil
			}
			messages = append(messages,
				map[string]any{"role": "assistant", "content": assistantBlocks},
				map[string]any{"role": "user", "content": toolResults},
			)
			return false, nil
		}); err != nil && ctx.Err() == nil {
			sendToken(ctx, tokenChan, fmt.Sprintf("\n[Claude tool loop error: %v]", err))
		}
	}()

	return tokenChan, nil
}
