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
		client:   &http.Client{Timeout: 60 * time.Second},
	}
}

func (a *OpenAIAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	messages := make([]map[string]any, 0, len(history)+1)
	messages = append(messages, map[string]any{"role": "system", "content": systemPrompt})
	for _, m := range history {
		messages = append(messages, map[string]any{"role": m.Role, "content": m.Content})
	}

	for range 6 {
		payload := map[string]any{
			"model":    a.model,
			"messages": messages,
		}
		if a.tools != nil {
			if defs := a.tools.Definitions(); len(defs) > 0 {
				payload["tools"] = defs
			}
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
		if a.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+a.apiKey)
		}

		resp, err := a.client.Do(req)
		if err != nil {
			return "", err
		}
		if resp.StatusCode >= http.StatusBadRequest {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return "", fmt.Errorf("openai HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
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

		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil || len(result.Choices) == 0 {
			return "", fmt.Errorf("failed to decode openai response")
		}

		choice := result.Choices[0].Message
		if len(choice.ToolCalls) == 0 || a.tools == nil {
			return choice.Content, nil
		}

		messages = append(messages, map[string]any{
			"role":       "assistant",
			"content":    choice.Content,
			"tool_calls": choice.ToolCalls,
		})
		for _, tc := range choice.ToolCalls {
			var parsedArgs map[string]any
			_ = json.Unmarshal([]byte(tc.Function.Arguments), &parsedArgs)
			obs := a.tools.Execute(ctx, tc.Function.Name, parsedArgs)
			messages = append(messages, map[string]any{
				"role":         "tool",
				"tool_call_id": tc.ID,
				"content":      obs,
			})
		}
	}

	return "", fmt.Errorf("openai tool loop exceeded max turns")
}

func (a *OpenAIAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	reply, err := a.Chat(ctx, systemPrompt, history)
	if err != nil {
		return nil, err
	}
	ch := make(chan string, 1)
	ch <- reply
	close(ch)
	return ch, nil
}

// LlamaCPPAdapter wraps OpenAIAdapter for a local llama.cpp server.
type LlamaCPPAdapter struct {
	*OpenAIAdapter
}

func NewLlamaCPPAdapter(endpoint string, tools ToolExecutor) *LlamaCPPAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "http://localhost:8080/v1/chat/completions"
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
		endpoint = "http://localhost:8000/v1/chat/completions"
	}
	if strings.TrimSpace(model) == "" {
		model = "Qwen/Qwen2.5-Coder-32B-Instruct"
	}
	return &VLLMAdapter{
		OpenAIAdapter: NewOpenAIAdapter(endpoint, "vllm-no-key", model, tools),
	}
}
