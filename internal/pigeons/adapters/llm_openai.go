package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

var _ BaseLLM = (*OpenAIAdapter)(nil)

type OpenAIAdapter struct {
	endpoint string
	apiKey   string
	model    string
	store    db.BaseDB
	client   *http.Client
}

func NewOpenAIAdapter(endpoint, apiKey, model string, store db.BaseDB) *OpenAIAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "https://api.openai.com/v1/chat/completions"
	}
	if strings.TrimSpace(model) == "" {
		model = "gpt-4o"
	}
	return &OpenAIAdapter{
		endpoint: endpoint,
		apiKey:   apiKey,
		model:    model,
		store:    store,
		client:   &http.Client{Timeout: 60 * time.Second},
	}
}

func (a *OpenAIAdapter) Chat(ctx context.Context, systemPrompt string, history []Message) (string, error) {
	messages := make([]Message, 0, len(history)+1)
	messages = append(messages, Message{Role: "system", Content: systemPrompt})
	messages = append(messages, history...)

	payload := map[string]any{
		"model":    a.model,
		"messages": messages,
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
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("openai HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
	}

	var result struct {
		Choices []struct {
			Message Message `json:"message"`
		} `json:"choices"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || len(result.Choices) == 0 {
		return "", fmt.Errorf("failed to decode openai response")
	}

	return result.Choices[0].Message.Content, nil
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
