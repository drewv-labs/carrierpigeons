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
	return a.runChat(ctx, systemPrompt, history, nil)
}

func (a *OllamaAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	tokenChan := make(chan string)

	go func() {
		defer close(tokenChan)
		sentAny := false
		emit := func(tok string) bool {
			sentAny = true
			return sendToken(ctx, tokenChan, tok)
		}
		if _, err := a.runChat(ctx, systemPrompt, history, emit); err != nil && ctx.Err() == nil {
			if sentAny {
				sendToken(ctx, tokenChan, fmt.Sprintf("\n[%v]", err))
			} else {
				sendToken(ctx, tokenChan, fmt.Sprintf("[%v]", err))
			}
		}
	}()

	return tokenChan, nil
}

func (a *OllamaAdapter) runChat(
	ctx context.Context,
	systemPrompt string,
	history []Message,
	emit func(string) bool,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	messages := make([]Message, 0, len(history)+1)
	messages = append(messages, Message{Role: "system", Content: systemPrompt})
	messages = append(messages, history...)

	var finalReply string
	stepFailed := false

	err := runToolLoop(defaultOllamaMaxTurn, "ollama", func(_ int) (bool, error) {
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
			stepFailed = true
			return true, err
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
					if emit != nil && !emit(chunk.Message.Content) {
						resp.Body.Close()
						stepFailed = true
						return true, ctx.Err()
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
		if err := ctx.Err(); err != nil {
			stepFailed = true
			return true, err
		}
		if scanErr != nil {
			stepFailed = true
			return true, fmt.Errorf("Ollama stream error: %w", scanErr)
		}

		if len(toolCalls) == 0 || a.tools == nil {
			finalReply = strings.TrimSpace(fullContent.String())
			return true, nil
		}

		messages = append(messages, Message{
			Role:      "assistant",
			Content:   fullContent.String(),
			ToolCalls: toolCalls,
		})

		for _, tc := range toolCalls {
			if emit != nil && !emit(fmt.Sprintf("\n*(Running tool: %s...)*\n", tc.Function.Name)) {
				stepFailed = true
				return true, ctx.Err()
			}
			observation := a.tools.Execute(ctx, tc.Function.Name, tc.Function.Arguments)
			if err := ctx.Err(); err != nil {
				stepFailed = true
				return true, err
			}
			messages = append(messages, Message{
				Role:       "tool",
				ToolCallID: tc.ID,
				Content:    observation,
			})
		}
		return false, nil
	})
	if err != nil {
		if !stepFailed {
			return "", fmt.Errorf("Ollama tool loop error: %w", err)
		}
		return "", err
	}
	return finalReply, nil
}
