package llm

import (
	"context"
	"encoding/json"
	"errors"
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

func TestOpenAINonSSEJSONToolLoopInStreamChat(t *testing.T) {
	t.Run("explicit_id_and_content", func(t *testing.T) {
		var turns atomic.Int32
		var secondReq struct {
			Messages []map[string]any `json:"messages"`
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := turns.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if n == 1 {
				_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"Checking node...","tool_calls":[{"id":"call_non_sse_1","type":"function","function":{"name":"get_node_telemetry","arguments":"{\"node_id\":\"rv64-compute-01\"}"}}]}}]}`)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&secondReq); err != nil {
				t.Errorf("failed to decode second request body: %v", err)
			}
			_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"Non-SSE OpenAI healthy."}}]}`)
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
		got := out.String()
		if !strings.Contains(got, "Checking node...") ||
			!strings.Contains(got, "Running tool: get_node_telemetry") ||
			!strings.Contains(got, "Non-SSE OpenAI healthy.") {
			t.Fatalf("unexpected non-SSE OpenAI output: %q", got)
		}
		if turns.Load() != 2 {
			t.Fatalf("expected 2 turns, got %d", turns.Load())
		}
		if len(secondReq.Messages) != 4 {
			t.Fatalf("expected 4 messages in follow-up turn, got %d: %+v", len(secondReq.Messages), secondReq.Messages)
		}
		assistantMsg := secondReq.Messages[2]
		if assistantMsg["role"] != "assistant" || assistantMsg["content"] != "Checking node..." {
			t.Fatalf("unexpected assistant message in history: %+v", assistantMsg)
		}
		rawCalls, ok := assistantMsg["tool_calls"].([]any)
		if !ok || len(rawCalls) != 1 {
			t.Fatalf("expected 1 assistant tool_call, got %+v", assistantMsg["tool_calls"])
		}
		callMap, _ := rawCalls[0].(map[string]any)
		fnMap, _ := callMap["function"].(map[string]any)
		if callMap["id"] != "call_non_sse_1" || fnMap["name"] != "get_node_telemetry" || fnMap["arguments"] != `{"node_id":"rv64-compute-01"}` {
			t.Fatalf("unexpected tool_call payload forwarded to turn 2: %+v", callMap)
		}
		toolMsg := secondReq.Messages[3]
		if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "call_non_sse_1" || toolMsg["content"] != "get_node_telemetry:rv64-compute-01 load_1m=0.42" {
			t.Fatalf("unexpected tool observation message in turn 2: %+v", toolMsg)
		}
	})

	t.Run("missing_id_synthesizes_indexed_id", func(t *testing.T) {
		var turns atomic.Int32
		var secondReq struct {
			Messages []map[string]any `json:"messages"`
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := turns.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if n == 1 {
				_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"type":"function","function":{"name":"get_node_telemetry","arguments":"{\"node_id\":\"rv64-compute-02\"}"}}]}}]}`)
				return
			}
			_ = json.NewDecoder(r.Body).Decode(&secondReq)
			_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"Done."}}]}`)
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
		if !strings.Contains(out.String(), "Running tool: get_node_telemetry") || !strings.Contains(out.String(), "Done.") {
			t.Fatalf("unexpected output: %q", out.String())
		}
		if len(secondReq.Messages) != 4 || secondReq.Messages[3]["tool_call_id"] != "call_0" {
			t.Fatalf("expected synthesized call_0 tool_call_id, got %+v", secondReq.Messages)
		}
	})
}

func TestOpenAIInvalidToolJSONAndHistoryPreservation(t *testing.T) {
	// parseToolArgs & executeToolCallRaw must return explicit error string on invalid JSON instead of calling tool with nil args.
	called := false
	exec := &recordingToolExecutor{onExec: func() { called = true }}
	res := executeToolCallRaw(context.Background(), exec, "get_node_telemetry", "{not-valid-json")
	if called {
		t.Fatal("expected tool NOT to be invoked when JSON arguments are malformed")
	}
	if !strings.Contains(res, "invalid JSON arguments") {
		t.Fatalf("expected explicit JSON parse error string, got %q", res)
	}

	// Verify multi-turn history preserves tool_calls and tool_result turns for both OpenAI and Claude.
	hist := []Message{
		{Role: "user", Content: "check rv64"},
		{
			Role: "assistant",
			ToolCalls: []ToolCall{{
				ID: "call_42",
				Function: ToolCallFunction{
					Name:      "get_node_telemetry",
					Arguments: map[string]any{"node_id": "rv64-compute-01"},
				},
			}},
		},
		{Role: "tool", ToolCallID: "call_42", Content: "load_1m=0.25"},
	}

	oaiMsgs := buildOpenAIMessages("sys", hist)
	if len(oaiMsgs) != 4 || oaiMsgs[3]["role"] != "tool" || oaiMsgs[3]["tool_call_id"] != "call_42" {
		t.Fatalf("expected OpenAI history to preserve tool turn, got %+v", oaiMsgs)
	}
	if oaiMsgs[2]["tool_calls"] == nil {
		t.Fatalf("expected OpenAI history to preserve assistant tool_calls, got %+v", oaiMsgs[2])
	}

	claudeMsgs := buildAnthropicMessages(hist)
	if len(claudeMsgs) != 3 || claudeMsgs[1]["role"] != "assistant" || claudeMsgs[2]["role"] != "user" {
		t.Fatalf("expected Anthropic history to preserve assistant tool_use & user tool_result turns, got %+v", claudeMsgs)
	}
}

type recordingToolExecutor struct {
	onExec func()
}

func (r *recordingToolExecutor) Definitions() []map[string]any { return nil }
func (r *recordingToolExecutor) Execute(_ context.Context, _ string, _ map[string]any) string {
	if r.onExec != nil {
		r.onExec()
	}
	return "ok"
}

func TestClaudeSSEStreamingAndToolLoop(t *testing.T) {
	var turns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := turns.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			_, _ = fmt.Fprintln(w, `event: content_block_start`)
			_, _ = fmt.Fprintln(w, `data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_sse_1","name":"get_node_telemetry"}}`)
			_, _ = fmt.Fprintln(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"node_id\":\"rv64-compute-01\"}"}}`)
			_, _ = fmt.Fprintln(w, `data: {"type":"message_stop"}`)
			return
		}
		_, _ = fmt.Fprintln(w, `data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		_, _ = fmt.Fprintln(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Claude SSE "}}`)
		_, _ = fmt.Fprintln(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"verified."}}`)
		_, _ = fmt.Fprintln(w, `data: {"type":"message_stop"}`)
	}))
	defer srv.Close()

	adapter := NewClaudeAdapterWithEndpoint(srv.URL, "test-key", "claude-sonnet-4-5", &stubToolExecutor{})
	ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "check"}})
	if err != nil {
		t.Fatalf("Claude StreamChat error: %v", err)
	}
	var out strings.Builder
	for tok := range ch {
		out.WriteString(tok)
	}
	if !strings.Contains(out.String(), "Running tool: get_node_telemetry") || !strings.Contains(out.String(), "Claude SSE verified.") {
		t.Fatalf("unexpected Claude SSE output: %q", out.String())
	}
}

func TestStreamChatEmitsTerminalErrorOnMaxToolTurns(t *testing.T) {
	t.Run("claude", func(t *testing.T) {
		var turns atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n := turns.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_%d\",\"name\":\"get_node_telemetry\"}}\n", n)
			_, _ = fmt.Fprintln(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"node_id\":\"rv64-compute-01\"}"}}`)
			_, _ = fmt.Fprintln(w, `data: {"type":"message_stop"}`)
		}))
		defer srv.Close()

		adapter := NewClaudeAdapterWithEndpoint(srv.URL, "test-key", "claude-sonnet-4-5", &stubToolExecutor{})
		ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "loop"}})
		if err != nil {
			t.Fatalf("Claude StreamChat error: %v", err)
		}
		var out strings.Builder
		for tok := range ch {
			out.WriteString(tok)
		}
		if turns.Load() != defaultMaxToolTurns {
			t.Fatalf("expected %d turns, got %d", defaultMaxToolTurns, turns.Load())
		}
		if !strings.Contains(out.String(), "[Claude tool loop error:") || !strings.Contains(out.String(), "exceeded max turns") {
			t.Fatalf("expected terminal Claude tool loop error token, got %q", out.String())
		}
	})

	t.Run("openai", func(t *testing.T) {
		var turns atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n := turns.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_%d\",\"function\":{\"name\":\"get_node_telemetry\",\"arguments\":\"{\\\"node_id\\\":\\\"rv64-compute-01\\\"}\"}}]}}]}\n", n)
			_, _ = fmt.Fprintln(w, `data: [DONE]`)
		}))
		defer srv.Close()

		adapter := NewOpenAIAdapter(srv.URL+"/v1/chat/completions", "test-key", "gpt-4o", &stubToolExecutor{})
		ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "loop"}})
		if err != nil {
			t.Fatalf("OpenAI StreamChat error: %v", err)
		}
		var out strings.Builder
		for tok := range ch {
			out.WriteString(tok)
		}
		if turns.Load() != defaultMaxToolTurns {
			t.Fatalf("expected %d turns, got %d", defaultMaxToolTurns, turns.Load())
		}
		if !strings.Contains(out.String(), "[OpenAI tool loop error:") || !strings.Contains(out.String(), "exceeded max turns") {
			t.Fatalf("expected terminal OpenAI tool loop error token, got %q", out.String())
		}
	})

	t.Run("ollama", func(t *testing.T) {
		var turns atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			turns.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
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
		}))
		defer srv.Close()

		adapter := NewOllamaAdapter(srv.URL, "qwen2.5-coder", &stubToolExecutor{})
		ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "loop"}})
		if err != nil {
			t.Fatalf("Ollama StreamChat error: %v", err)
		}
		var out strings.Builder
		for tok := range ch {
			out.WriteString(tok)
		}
		if turns.Load() != defaultOllamaMaxTurn {
			t.Fatalf("expected %d turns, got %d", defaultOllamaMaxTurn, turns.Load())
		}
		if !strings.Contains(out.String(), "[Ollama tool loop error:") || !strings.Contains(out.String(), "exceeded max turns") {
			t.Fatalf("expected terminal Ollama tool loop error token, got %q", out.String())
		}
	})
}

func TestChatRejectsOversizedResponseBody(t *testing.T) {
	oversizedText := strings.Repeat("a", 4*1024*1024+1024)

	t.Run("claude", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"stop_reason":"end_turn","content":[{"type":"text","text":"%s"}]}`, oversizedText)
		}))
		defer srv.Close()

		adapter := NewClaudeAdapterWithEndpoint(srv.URL, "test-key", "claude-sonnet-4-5", nil)
		_, err := adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "hi"}})
		if err == nil || !strings.Contains(err.Error(), "failed to decode claude response") {
			t.Fatalf("expected decode failure on oversized Claude response, got %v", err)
		}
	})

	t.Run("openai", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":"%s"}}]}`, oversizedText)
		}))
		defer srv.Close()

		adapter := NewOpenAIAdapter(srv.URL+"/v1/chat/completions", "test-key", "gpt-4o", nil)
		_, err := adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "hi"}})
		if err == nil || !strings.Contains(err.Error(), "failed to decode openai response") {
			t.Fatalf("expected decode failure on oversized OpenAI response, got %v", err)
		}
	})
}

func TestOllamaChatPropagatesToolLoopAndCancelErrors(t *testing.T) {
	t.Run("max_turns_exceeded", func(t *testing.T) {
		var turns atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			turns.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message": Message{
					Role:    "assistant",
					Content: "Checking telemetry...",
					ToolCalls: []ToolCall{
						{Function: ToolCallFunction{
							Name:      "get_node_telemetry",
							Arguments: map[string]any{"node_id": "rv64-compute-01"},
						}},
					},
				},
			})
		}))
		defer srv.Close()

		adapter := NewOllamaAdapter(srv.URL, "qwen2.5-coder", &stubToolExecutor{})
		reply, err := adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "loop"}})
		if err == nil || !strings.Contains(err.Error(), "exceeded max turns") {
			t.Fatalf("expected max turns error from OllamaAdapter.Chat, got reply=%q err=%v", reply, err)
		}
		if turns.Load() != defaultOllamaMaxTurn {
			t.Fatalf("expected %d turns, got %d", defaultOllamaMaxTurn, turns.Load())
		}
	})

	t.Run("mid_stream_context_cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		chunkSent := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = fmt.Fprintln(w, `{"message":{"role":"assistant","content":"CERTIFIED: "}}`)
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
			close(chunkSent)
			<-r.Context().Done()
		}))
		defer srv.Close()

		go func() {
			<-chunkSent
			cancel()
		}()

		adapter := NewOllamaAdapter(srv.URL, "qwen2.5-coder", nil)
		reply, err := adapter.Chat(ctx, "sys", []Message{{Role: "user", Content: "verify"}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled on mid-stream cancellation, got reply=%q err=%v", reply, err)
		}
	})
}
