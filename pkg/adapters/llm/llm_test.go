package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type stubToolExecutor struct{}

func (s *stubToolExecutor) Definitions() []map[string]any {
	return []map[string]any{
		{
			"type": "function",
			"function": map[string]any{
				"name":        "get_node_telemetry",
				"description": "Fetch node telemetry",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"node_id": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
}

func (s *stubToolExecutor) Execute(_ context.Context, name string, args map[string]any) string {
	nodeID, _ := args["node_id"].(string)
	return name + ":" + nodeID + " load_1m=0.42"
}

func TestProviderRoutingAndOllamaToolLoop(t *testing.T) {
	for _, provider := range []string{"ollama", "claude", "openai", "llamacpp", "vllm", "default"} {
		adapter := New(Config{Provider: provider})
		if adapter == nil {
			t.Fatalf("New(%q) returned nil", provider)
		}
	}

	var turns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := turns.Add(1)
		enc := json.NewEncoder(w)
		if n == 1 {
			_ = enc.Encode(map[string]any{
				"message": Message{
					Role: "assistant",
					ToolCalls: []ToolCall{
						{Function: ToolCallFunction{
							Name:      "get_node_telemetry",
							Arguments: map[string]any{"node_id": "rv64-compute-01"},
						}},
					},
				},
			})
			return
		}
		_ = enc.Encode(map[string]any{
			"message": Message{Role: "assistant", Content: "Node load is healthy."},
		})
	}))
	defer srv.Close()

	adapter := NewOllamaAdapter(srv.URL, "qwen2.5-coder", &stubToolExecutor{})
	ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "status?"}})
	if err != nil {
		t.Fatalf("StreamChat error: %v", err)
	}

	var out strings.Builder
	for tok := range ch {
		out.WriteString(tok)
	}
	if !strings.Contains(out.String(), "Running tool: get_node_telemetry") || !strings.Contains(out.String(), "Node load is healthy.") {
		t.Fatalf("unexpected streamed output: %q", out.String())
	}
}

func TestClaudeToolUseLoop(t *testing.T) {
	var turns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := turns.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			_, _ = fmt.Fprint(w, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"toolu_01","name":"get_node_telemetry","input":{"node_id":"rv64-compute-01"}}]}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Claude verified load_1m=0.42."}]}`)
	}))
	defer srv.Close()

	adapter := NewClaudeAdapterWithEndpoint(srv.URL, "test-key", "claude-sonnet-4-5", &stubToolExecutor{})
	reply, err := adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "check node"}})
	if err != nil {
		t.Fatalf("Claude Chat failed: %v", err)
	}
	if !strings.Contains(reply, "Claude verified load_1m=0.42.") {
		t.Fatalf("unexpected Claude reply: %q", reply)
	}
}

func TestOpenAISSEStreamingAndToolLoop(t *testing.T) {
	var turns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := turns.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_node_telemetry","arguments":"{\"node_id\":"}}]}}]}`)
			_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"rv64-compute-01\"}"}}]}}]}`)
			_, _ = fmt.Fprintln(w, `data: [DONE]`)
			return
		}
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"OpenAI SSE "}}]}`)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"healthy."}}]}`)
		_, _ = fmt.Fprintln(w, `data: [DONE]`)
	}))
	defer srv.Close()

	adapter := NewOpenAIAdapter(srv.URL+"/v1/chat/completions", "test-key", "gpt-4o", &stubToolExecutor{})
	ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "status?"}})
	if err != nil {
		t.Fatalf("OpenAI StreamChat error: %v", err)
	}
	var out strings.Builder
	for tok := range ch {
		out.WriteString(tok)
	}
	if !strings.Contains(out.String(), "Running tool: get_node_telemetry") || !strings.Contains(out.String(), "OpenAI SSE healthy.") {
		t.Fatalf("unexpected OpenAI SSE output: %q", out.String())
	}
}
