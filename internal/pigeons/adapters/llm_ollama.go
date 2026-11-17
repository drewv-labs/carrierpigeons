package adapters

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

var _ BaseLLM = (*OllamaAdapter)(nil)

type OllamaAdapter struct {
	endpoint string
	model    string
	store    db.BaseDB
	client   *http.Client
}

func NewOllamaAdapter(endpoint, model string, store db.BaseDB) *OllamaAdapter {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		endpoint = "http://localhost:11434"
	}
	if strings.TrimSpace(model) == "" {
		model = "qwen2.5-coder"
	}
	return &OllamaAdapter{
		endpoint: endpoint + "/api/chat",
		model:    model,
		store:    store,
		client:   &http.Client{},
	}
}

func (a *OllamaAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	tokenChan := make(chan string)

	go func() {
		defer close(tokenChan)
		messages := make([]Message, 0, len(history)+1)
		messages = append(messages, Message{Role: "system", Content: systemPrompt})
		messages = append(messages, history...)

		for {
			payload := map[string]any{
				"model":    a.model,
				"messages": messages,
				"tools":    PigeonTools,
				"stream":   true,
			}

			body, err := json.Marshal(payload)
			if err != nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("[Ollama encode error: %v]", err))
				return
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewBuffer(body))
			if err != nil {
				sendToken(ctx, tokenChan, fmt.Sprintf("[Ollama request error: %v]", err))
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := a.client.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					sendToken(ctx, tokenChan, fmt.Sprintf("[Ollama connection error: %v]", err))
				}
				return
			}

			if resp.StatusCode >= http.StatusBadRequest {
				errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				resp.Body.Close()
				sendToken(ctx, tokenChan, fmt.Sprintf("[Ollama HTTP %d: %s]", resp.StatusCode, strings.TrimSpace(string(errBody))))
				return
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
							return
						}
						fullContent.WriteString(chunk.Message.Content)
					}
					if len(chunk.Message.ToolCalls) > 0 {
						toolCalls = append(toolCalls, chunk.Message.ToolCalls...)
					}
				}
			}
			resp.Body.Close()

			if len(toolCalls) == 0 {
				return
			}

			messages = append(messages, Message{
				Role:      "assistant",
				Content:   fullContent.String(),
				ToolCalls: toolCalls,
			})

			for _, tc := range toolCalls {
				if !sendToken(ctx, tokenChan, fmt.Sprintf("\n*(Running tool: %s...)*\n", tc.Function.Name)) {
					return
				}

				observation := ExecuteTool(ctx, tc.Function.Name, tc.Function.Arguments, a.store)
				messages = append(messages, Message{
					Role:    "tool",
					Content: observation,
				})
			}
		}
	}()

	return tokenChan, nil
}
