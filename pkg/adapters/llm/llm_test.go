package llm

import (
	"context"
	"encoding/json"
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
				"name": "get_node_telemetry",
			},
		},
	}
}

func (s *stubToolExecutor) Execute(_ context.Context, name string, args map[string]any) string {
	return name + ":" + args["node_id"].(string) + " load_1m=0.42"
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
