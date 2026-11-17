package adapters

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

type mockDB struct {
	rows []map[string]any
}

func (m *mockDB) EnsureSchema(context.Context) error                  { return nil }
func (m *mockDB) InsertEvent(context.Context, *core.CTDPayload) error { return nil }
func (m *mockDB) UpsertArtifact(context.Context, core.Artifact) error { return nil }
func (m *mockDB) InsertTrace(context.Context, core.TraceEdge) error   { return nil }
func (m *mockDB) GetClusterStatus(context.Context, time.Duration) ([]db.NodeSummary, error) {
	return nil, nil
}
func (m *mockDB) GetLineageDAG(context.Context, string, int) (*db.DAG, error) { return nil, nil }
func (m *mockDB) FetchForUnload(context.Context, db.UnloadFilter) ([]core.CTDPayload, error) {
	return nil, nil
}
func (m *mockDB) RegisterView(context.Context, string, string) error { return nil }
func (m *mockDB) ExecRaw(context.Context, string, ...any) error      { return nil }
func (m *mockDB) QueryMaps(context.Context, string, ...any) ([]map[string]any, error) {
	return m.rows, nil
}
func (m *mockDB) Close() {}

func TestExecuteToolAndAdapterRouting(t *testing.T) {
	for _, provider := range []string{"ollama", "claude", "openai", "llamacpp", "vllm", "default"} {
		llm := NewLLMAdapter(LLMConfig{Provider: provider})
		if llm == nil {
			t.Fatalf("NewLLMAdapter(%q) returned nil", provider)
		}
	}

	if out := ExecuteTool(context.Background(), "get_node_telemetry", map[string]any{"node_id": "n1"}, nil); !strings.Contains(out, "not connected") {
		t.Fatalf("expected nil store error, got: %s", out)
	}

	store := &mockDB{
		rows: []map[string]any{
			{
				"component":       "host-os",
				"metrics":         map[string]any{"load_1m": 0.75},
				"event_timestamp": time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC),
			},
		},
	}

	res := ExecuteTool(context.Background(), "get_node_telemetry", map[string]any{"node_id": "edge-rv64-01"}, store)
	if !strings.Contains(res, "host-os") || !strings.Contains(res, `"load_1m":0.75`) {
		t.Fatalf("unexpected tool output: %s", res)
	}
}

func TestOllamaStreamChatWithToolCall(t *testing.T) {
	var turns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := turns.Add(1)
		enc := json.NewEncoder(w)
		if n == 1 {
			_ = enc.Encode(map[string]any{
				"message": Message{
					Role: "assistant",
					ToolCalls: []ToolCall{
						{Function: ToolCallFunction{
							Name:      "get_node_telemetry",
							Arguments: map[string]any{"node_id": "edge-arm64-01"},
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

	store := &mockDB{
		rows: []map[string]any{
			{
				"component":       "host-os",
				"metrics":         map[string]any{"load_1m": 0.42},
				"event_timestamp": time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC),
			},
		},
	}

	adapter := NewOllamaAdapter(srv.URL, "qwen2.5-coder", store)
	ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "status?"}})
	if err != nil {
		t.Fatalf("StreamChat error: %v", err)
	}

	var collected strings.Builder
	for tok := range ch {
		collected.WriteString(tok)
	}
	if !strings.Contains(collected.String(), "Running tool: get_node_telemetry") || !strings.Contains(collected.String(), "Node load is healthy.") {
		t.Fatalf("unexpected streamed output: %q", collected.String())
	}
}
