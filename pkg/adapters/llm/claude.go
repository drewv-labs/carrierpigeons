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
	apiKey string
	model  string
	tools  ToolExecutor
	client *http.Client
}

func NewClaudeAdapter(apiKey, model string, tools ToolExecutor) *ClaudeAdapter {
	if strings.TrimSpace(model) == "" {
		model = "claude-sonnet-4-5"
	}
	return &ClaudeAdapter{
		apiKey: apiKey,
		model:  model,
		tools:  tools,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

func (a *ClaudeAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	payload := map[string]any{
		"model":      a.model,
		"max_tokens": 2048,
		"system":     systemPrompt,
		"messages":   history,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewBuffer(body))
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
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("claude HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || len(result.Content) == 0 {
		return "", fmt.Errorf("failed to decode claude response")
	}

	return result.Content[0].Text, nil
}

func (a *ClaudeAdapter) StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error) {
	reply, err := a.Chat(ctx, systemPrompt, history)
	if err != nil {
		return nil, err
	}
	ch := make(chan string, 1)
	ch <- reply
	close(ch)
	return ch, nil
}
