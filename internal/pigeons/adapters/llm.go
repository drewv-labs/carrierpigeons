// File: internal/pigeons/adapters/llm.go
package adapters

import (
	"context"
	"strings"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

// Tool schemas mapped to the OpenAI/Ollama specification.
type ToolCallFunction struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type ToolCall struct {
	Function ToolCallFunction `json:"function"`
}

type Message struct {
	Role      string     `json:"role"`
	Content   string     `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

// BaseLLM (aliased as LLMAdapter for backward compatibility) defines the LLM mounting point.
type BaseLLM interface {
	// StreamChat returns a channel that yields tokens as they are generated.
	StreamChat(ctx context.Context, systemPrompt string, history []Message) (<-chan string, error)
}

type LLMAdapter = BaseLLM

type LLMConfig struct {
	Provider string
	Model    string
	Endpoint string
	APIKey   string
	Store    db.BaseDB
}

func sendToken(ctx context.Context, ch chan<- string, token string) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- token:
		return true
	}
}

// NewLLMAdapter routes the configuration to the configured inference backend.
func NewLLMAdapter(cfg LLMConfig) BaseLLM {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "ollama":
		return NewOllamaAdapter(cfg.Endpoint, cfg.Model, cfg.Store)
	case "claude", "anthropic":
		return NewClaudeAdapter(cfg.APIKey, cfg.Model, cfg.Store)
	case "openai":
		return NewOpenAIAdapter(cfg.Endpoint, cfg.APIKey, cfg.Model, cfg.Store)
	case "llamacpp", "llama.cpp":
		return NewLlamaCPPAdapter(cfg.Endpoint, cfg.Store)
	case "vllm":
		return NewVLLMAdapter(cfg.Endpoint, cfg.Model, cfg.Store)
	default:
		model := cfg.Model
		if model == "" {
			model = "qwen3.6:35b-a3b-q8_0"
		}
		endpoint := cfg.Endpoint
		if endpoint == "" {
			endpoint = "http://localhost:11434"
		}
		return NewOllamaAdapter(endpoint, model, cfg.Store)
	}
}
