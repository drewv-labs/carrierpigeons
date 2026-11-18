package llm

import (
	"context"
	"strings"
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
	Role      string     `json:"role"`
	Content   string     `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
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

func sendToken(ctx context.Context, ch chan<- string, token string) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- token:
		return true
	}
}

// New constructs the configured LLM inference backend.
func New(cfg Config) BaseLLM {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "ollama":
		return NewOllamaAdapter(cfg.Endpoint, cfg.Model, cfg.Tools)
	case "claude", "anthropic":
		return NewClaudeAdapter(cfg.APIKey, cfg.Model, cfg.Tools)
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
