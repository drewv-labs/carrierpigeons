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

const (
	defaultMaxToolTurns  = 6
	defaultOllamaMaxTurn = 8
	maxErrorBodyBytes    = 4096
)

// ToolCallFunction represents a tool invocation emitted by an LLM.
type ToolCallFunction struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// ToolCall wraps a function call inside an assistant message.
type ToolCall struct {
	ID       string           `json:"id,omitempty"`
	Type     string           `json:"type,omitempty"`
	Function ToolCallFunction `json:"function"`
}

// Message represents a chat message in an OpenAI/Ollama/Anthropic conversation turn.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolExecutor decouples LLM inference providers from tool execution & security boundaries.
type ToolExecutor interface {
	Definitions() []map[string]any
	Execute(ctx context.Context, name string, args map[string]any) string
}

// BaseLLM defines the whitelabelable inference adapter contract for PigeonCare and PigeonMath.
type BaseLLM interface {
	Chat(ctx context.Context, systemPrompt string, history []Message) (string, error)
	StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error)
}

// Config configures provider selection and optional tool execution binding.
type Config struct {
	Provider string
	Model    string
	Endpoint string
	APIKey   string
	Tools    ToolExecutor
}

func defaultLLMHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
}

func sendToken(ctx context.Context, ch chan<- string, token string) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- token:
		return true
	}
}

// postJSON marshals payload, sends an HTTP POST with headers, and rejects non-2xx responses with bounded error bodies.
func postJSON(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	headers map[string]string,
	payload any,
	provider string,
) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%s encode error: %w", provider, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s request error: %w", provider, err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s connection error: %w", provider, err)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		resp.Body.Close()
		return nil, fmt.Errorf("%s HTTP %d: %s", provider, resp.StatusCode, strings.TrimSpace(string(errBody)))
	}
	return resp, nil
}

// parseToolArgs decodes raw JSON tool arguments into a map, rejecting malformed JSON instead of silently yielding nil.
func parseToolArgs(raw string) (map[string]any, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return map[string]any{}, nil
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(trimmed), &parsed); err != nil {
		return nil, err
	}
	if parsed == nil {
		return map[string]any{}, nil
	}
	return parsed, nil
}

// executeToolCallRaw parses JSON arguments and executes the tool, returning an explicit error string back to the LLM on invalid JSON.
func executeToolCallRaw(ctx context.Context, tools ToolExecutor, name, rawArgs string) string {
	parsedArgs, err := parseToolArgs(rawArgs)
	if err != nil {
		return fmt.Sprintf("Error: invalid JSON arguments for tool %q: %v", name, err)
	}
	return tools.Execute(ctx, name, parsedArgs)
}

// runToolLoop executes up to maxTurns rounds until step signals done or returns an error.
func runToolLoop(maxTurns int, provider string, step func(turn int) (done bool, err error)) error {
	for turn := range maxTurns {
		done, err := step(turn)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return fmt.Errorf("%s tool loop exceeded max turns (%d)", provider, maxTurns)
}

// New constructs the configured LLM inference backend.
func New(cfg Config) BaseLLM {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "ollama":
		return NewOllamaAdapter(cfg.Endpoint, cfg.Model, cfg.Tools)
	case "claude", "anthropic":
		return NewClaudeAdapterWithEndpoint(cfg.Endpoint, cfg.APIKey, cfg.Model, cfg.Tools)
	case "openai":
		return NewOpenAIAdapter(cfg.Endpoint, cfg.APIKey, cfg.Model, cfg.Tools)
	case "llamacpp", "llama.cpp":
		return NewLlamaCPPAdapter(cfg.Endpoint, cfg.Tools)
	case "vllm":
		return NewVLLMAdapter(cfg.Endpoint, cfg.Model, cfg.Tools)
	default:
		model := strings.TrimSpace(cfg.Model)
		if model == "" {
			model = "qwen2.5-coder"
		}
		endpoint := strings.TrimSpace(cfg.Endpoint)
		if endpoint == "" {
			endpoint = "http://localhost:11434"
		}
		return NewOllamaAdapter(endpoint, model, cfg.Tools)
	}
}
