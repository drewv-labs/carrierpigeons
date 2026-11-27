package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/jsonutil"
)

const (
	defaultMaxToolTurns    = 6
	defaultOllamaMaxTurn   = 8
	maxErrorBodyBytes      = 4096
	maxStreamBodyBytes     = 16 << 20 // 16 MiB total response stream cap per turn
	maxStreamContentBytes  = 4 << 20  // 4 MiB cap for accumulated assistant text per turn
	maxStreamToolArgBytes  = 1 << 20  // 1 MiB cap for accumulated tool arguments per tool call
	maxStreamToolNameBytes = 256      // 256 B cap for streamed tool name
	maxStreamToolCalls     = 64       // max distinct tool calls accumulated per turn
)

var (
	streamBodyLimit    int64 = maxStreamBodyBytes
	streamContentLimit       = maxStreamContentBytes
	streamToolArgLimit       = maxStreamToolArgBytes

	errStreamLimitExceeded = errors.New("stream response exceeded size limit")
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
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
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

type cappedStreamReader struct {
	lr        *io.LimitedReader
	truncated bool
}

func newCappedStreamReader(r io.Reader, limit int64) *cappedStreamReader {
	if limit <= 0 {
		limit = maxStreamBodyBytes
	}
	return &cappedStreamReader{
		lr: &io.LimitedReader{R: r, N: limit + 1},
	}
}

func (c *cappedStreamReader) Read(p []byte) (int, error) {
	if c.truncated || c.lr.N <= 0 {
		c.truncated = true
		return 0, errStreamLimitExceeded
	}
	if c.lr.N == 1 {
		var probe [1]byte
		n, err := c.lr.R.Read(probe[:])
		if n > 0 {
			c.lr.N = 0
			c.truncated = true
			return 0, errStreamLimitExceeded
		}
		if err != nil {
			return 0, err
		}
		return 0, nil
	}
	maxRead := c.lr.N - 1
	if int64(len(p)) > maxRead {
		p = p[:maxRead]
	}
	n, err := c.lr.R.Read(p)
	c.lr.N -= int64(n)
	return n, err
}

func writeBounded(sb *strings.Builder, s string, limit int) bool {
	if limit <= 0 || sb.Len()+len(s) <= limit {
		sb.WriteString(s)
		return true
	}
	return false
}

func authSecretsFromHeaders(headers map[string]string) []string {
	if len(headers) == 0 {
		return nil
	}
	var secrets []string
	for k, v := range headers {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if strings.EqualFold(k, "authorization") {
			secrets = append(secrets, trimmed)
			if len(trimmed) > len("bearer ") && strings.EqualFold(trimmed[:len("bearer ")], "bearer ") {
				if tok := strings.TrimSpace(trimmed[len("bearer "):]); tok != "" {
					secrets = append(secrets, tok)
				}
			}
		} else if strings.EqualFold(k, "x-api-key") {
			secrets = append(secrets, trimmed)
		}
	}
	return secrets
}

// postJSON marshals payload, sends an HTTP POST with headers, and rejects non-2xx responses with bounded, redacted error bodies.
func postJSON(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	headers map[string]string,
	payload any,
	provider string,
) (*http.Response, error) {
	secrets := authSecretsFromHeaders(headers)
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%s encode error: %w", provider, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%s request error: %w", provider, jsonutil.RedactError(err, secrets...))
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s connection error: %w", provider, jsonutil.RedactError(err, secrets...))
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		resp.Body.Close()
		msg := jsonutil.RedactCredentials(strings.TrimSpace(string(errBody)), secrets...)
		if msg == "" && resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest {
			msg = "redirect blocked"
		}
		return nil, fmt.Errorf("%s HTTP %d: %s", provider, resp.StatusCode, msg)
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
		return NewLlamaCPPAdapter(cfg.Endpoint, cfg.APIKey, cfg.Model, cfg.Tools)
	case "vllm":
		return NewVLLMAdapter(cfg.Endpoint, cfg.APIKey, cfg.Model, cfg.Tools)
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
