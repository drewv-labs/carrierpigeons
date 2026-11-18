package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
		client:   &http.Client{Timeout: 60 * time.Second},
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

func (a *ClaudeAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	messages := make([]map[string]any, 0, len(history))
	for _, m := range history {
		role := strings.ToLower(strings.TrimSpace(m.Role))
		if role == "system" || role == "tool" {
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

	var anthropicTools []map[string]any
	if a.tools != nil {
		anthropicTools = convertToAnthropicTools(a.tools.Definitions())
	}

	for range 6 {
		payload := map[string]any{
			"model":      a.model,
			"max_tokens": 2048,
			"system":     systemPrompt,
			"messages":   messages,
		}
		if len(anthropicTools) > 0 {
			payload["tools"] = anthropicTools
		}

		body, err := json.Marshal(payload)
		if err != nil {
			return "", err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewBuffer(body))
		if err != nil {
			return "", err
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", a.apiKey)
		req.Header.Set("anthropic-version", "2023-06-01")

		resp, err := a.client.Do(req)
		if err != nil {
			return "", err
		}

		if resp.StatusCode >= http.StatusBadRequest {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return "", fmt.Errorf("claude HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
		}

		var result struct {
			StopReason string `json:"stop_reason"`
			Content    []struct {
				Type  string         `json:"type"`
				Text  string         `json:"text,omitempty"`
				ID    string         `json:"id,omitempty"`
				Name  string         `json:"name,omitempty"`
				Input map[string]any `json:"input,omitempty"`
			} `json:"content"`
		}

		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil || len(result.Content) == 0 {
			return "", fmt.Errorf("failed to decode claude response")
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
			return strings.Join(textParts, "\n"), nil
		}

		messages = append(messages,
			map[string]any{"role": "assistant", "content": assistantBlocks},
			map[string]any{"role": "user", "content": toolResults},
		)
	}

	return "", fmt.Errorf("claude tool loop exceeded max turns")
}

func (a *ClaudeAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	ch := make(chan string, 1)
	go func() {
		defer close(ch)
		reply, err := a.Chat(ctx, systemPrompt, history)
		if err != nil {
			if ctx.Err() == nil {
				sendToken(ctx, ch, fmt.Sprintf("[Claude error: %v]", err))
			}
			return
		}
		sendToken(ctx, ch, reply)
	}()
	return ch, nil
}
