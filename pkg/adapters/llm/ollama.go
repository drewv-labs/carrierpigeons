package llm

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

var _ BaseLLM = (*OllamaAdapter)(nil)

type OllamaAdapter struct {
	endpoint string
	model    string
	tools    ToolExecutor
	client   *http.Client
}

func NewOllamaAdapter(endpoint, model string, tools ToolExecutor) *OllamaAdapter {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:11434"
	}
	if strings.TrimSpace(model) == "" {
		model = "qwen2.5-coder"
	}
	return &OllamaAdapter{
		endpoint: endpoint + "/api/chat",
		model:    model,
		tools:    tools,
		client:   defaultLLMHTTPClient(),
	}
}

func (a *OllamaAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	ch, err := a.StreamChat(ctx, systemPrompt, history)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for tok := range ch {
		sb.WriteString(tok)
	}
	out := strings.TrimSpace(sb.String())
	if strings.HasPrefix(out, "[Ollama ") && strings.HasSuffix(out, "]") {
		return "", fmt.Errorf("%s", out)
	}
	return out, nil
}

func (a *OllamaAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	tokenChan := make(chan string)

	go func() {
		defer close(tokenChan)
		messages := make([]Message, 0, len(history)+1)
		messages = append(messages, Message{Role: "system", Content: systemPrompt})
		messages = append(messages, history...)

		_ = runToolLoop(defaultOllamaMaxTurn, "ollama", func(_ int) (bool, error) {
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

			resp, err := postJSON(ctx, a.client, a.endpoint, nil, payload, "Ollama")
			if err != nil {
				if ctx.Err() == nil {
					sendToken(ctx, tokenChan, fmt.Sprintf("[%v]", err))
				}
				return true, nil
			}

			scanner := bufio.NewScanner(resp.Body)
			scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

			var toolCalls []ToolCall
			var fullContent strings.Builder

			for scanner.Scan() {
				var chunk struct {
					Message Message `json:"message"`
				}
				if err := json.Unmarshal(scanner.Bytes(), &chunk); err == nil {
					if chunk.Message.Content != "" {
						if !sendToken(ctx, tokenChan, chunk.Message.Content) {
							resp.Body.Close()
							return true, nil
						}
						fullContent.WriteString(chunk.Message.Content)
					}
					if len(chunk.Message.ToolCalls) > 0 {
						toolCalls = append(toolCalls, chunk.Message.ToolCalls...)
					}
				}
			}
			scanErr := scanner.Err()
			resp.Body.Close()
			if scanErr != nil && ctx.Err() == nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[Ollama stream error: %v]", scanErr))
				return true, nil
			}

			if len(toolCalls) == 0 || a.tools == nil {
				return true, nil
			}

			messages = append(messages, Message{
				Role:      "assistant",
				Content:   fullContent.String(),
				ToolCalls: toolCalls,
			})

			for _, tc := range toolCalls {
				if !sendToken(ctx, tokenChan, fmt.Sprintf("\n*(Running tool: %s...)*\n", tc.Function.Name)) {
					return true, nil
				}
				observation := a.tools.Execute(ctx, tc.Function.Name, tc.Function.Arguments)
				messages = append(messages, Message{
					Role:       "tool",
					ToolCallID: tc.ID,
					Content:    observation,
				})
			}
			return false, nil
		})
	}()

	return tokenChan, nil
}
