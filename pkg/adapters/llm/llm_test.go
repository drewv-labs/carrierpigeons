package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
			_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_node_","arguments":"{\"node_id\":"}}]}}]}`)
			_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"telemetry","arguments":"\"rv64-compute-01\"}"}}]}}]}`)
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

func TestClaudeCustomEndpointRoutingViaNew(t *testing.T) {
	for _, provider := range []string{"claude", "anthropic", " Claude "} {
		t.Run(strings.TrimSpace(provider), func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if r.Header.Get("x-api-key") != "custom-key" {
					t.Errorf("expected x-api-key=custom-key, got %q", r.Header.Get("x-api-key"))
				}
				if r.Header.Get("anthropic-version") != "2023-06-01" {
					t.Errorf("expected anthropic-version=2023-06-01, got %q", r.Header.Get("anthropic-version"))
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"stop_reason":"end_turn","content":[{"type":"text","text":"custom-endpoint-ok"}]}`)
			}))
			defer srv.Close()

			adapter := New(Config{
				Provider: provider,
				Endpoint: srv.URL,
				APIKey:   "custom-key",
				Model:    "claude-sonnet-4-5",
			})
			reply, err := adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "ping"}})
			if err != nil {
				t.Fatalf("Chat failed: %v", err)
			}
			if reply != "custom-endpoint-ok" {
				t.Fatalf("unexpected reply: %q", reply)
			}

			ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "ping"}})
			if err != nil {
				t.Fatalf("StreamChat failed: %v", err)
			}
			var streamed strings.Builder
			for tok := range ch {
				streamed.WriteString(tok)
			}
			if streamed.String() != "custom-endpoint-ok" {
				t.Fatalf("unexpected streamed reply: %q", streamed.String())
			}
			if hits.Load() != 2 {
				t.Fatalf("expected 2 requests to custom endpoint, got %d", hits.Load())
			}
		})
	}

	defaultAdapter, ok := New(Config{Provider: "claude", Endpoint: "   ", APIKey: "k"}).(*ClaudeAdapter)
	if !ok {
		t.Fatal("expected *ClaudeAdapter for provider=claude")
	}
	if defaultAdapter.endpoint != "https://api.anthropic.com/v1/messages" {
		t.Fatalf("expected blank endpoint to fall back to default Anthropic URL, got %q", defaultAdapter.endpoint)
	}
}

func TestDefaultLLMHTTPClientBlocksRedirectsAndPreservesTransportDefaults(t *testing.T) {
	client := defaultLLMHTTPClient()
	tr, ok := client.Transport.(*http.Transport)
	if !ok || tr == nil {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if tr == http.DefaultTransport {
		t.Fatal("expected cloned transport, not shared http.DefaultTransport")
	}
	if tr.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("expected ResponseHeaderTimeout=30s, got %v", tr.ResponseHeaderTimeout)
	}
	if tr.Proxy == nil {
		t.Fatal("expected cloned http.DefaultTransport to retain Proxy function")
	}

	var sinkHits atomic.Int32
	exfilSink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sinkHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer exfilSink.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, exfilSink.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	claude := NewClaudeAdapterWithEndpoint(redirector.URL, "claude-secret-key", "claude-sonnet-4-5", nil)
	if _, err := claude.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "hi"}}); err == nil {
		t.Fatal("expected Claude Chat to reject 307 redirect")
	} else if !strings.Contains(err.Error(), "HTTP 307") || strings.Contains(err.Error(), "claude-secret-key") {
		t.Fatalf("unexpected redirect error: %v", err)
	}

	openai := NewOpenAIAdapter(redirector.URL+"/v1/chat/completions", "openai-secret-key", "gpt-4o", nil)
	if _, err := openai.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "hi"}}); err == nil {
		t.Fatal("expected OpenAI Chat to reject 307 redirect")
	} else if !strings.Contains(err.Error(), "HTTP 307") || strings.Contains(err.Error(), "openai-secret-key") {
		t.Fatalf("unexpected redirect error: %v", err)
	}

	if sinkHits.Load() != 0 {
		t.Fatalf("redirect target received %d hits; expected 0", sinkHits.Load())
	}
}

func TestPostJSONRedactsURLErrorsAndAuthHeaders(t *testing.T) {
	// 1. Connection/transport *url.Error must redact userinfo and query secrets while preserving *url.Error.
	closedSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := closedSrv.URL
	closedSrv.Close()

	u, err := url.Parse(closedURL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	u.User = url.UserPassword("svc", "url_secret_pw")
	u.RawQuery = "api_key=query_secret_tok&mode=fast"

	_, err = postJSON(
		context.Background(),
		defaultLLMHTTPClient(),
		u.String(),
		map[string]string{
			"Authorization": "Bearer bearer_secret_val",
			"x-api-key":     "xapi_secret_val",
		},
		map[string]string{"hello": "world"},
		"OpenAI",
	)
	if err == nil {
		t.Fatal("expected connection error against closed server")
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		t.Fatalf("expected *url.Error to remain unwrappable, got %T: %v", err, err)
	}
	for _, secret := range []string{"url_secret_pw", "query_secret_tok", "bearer_secret_val", "xapi_secret_val"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("postJSON connection error leaked secret %q: %v", secret, err)
		}
	}

	// 2. Non-2xx upstream response echoing auth headers or raw tokens must be scrubbed.
	errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprintf(w, "rejected auth=%s key=%s raw_tok=%s",
			r.Header.Get("Authorization"),
			r.Header.Get("x-api-key"),
			strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "),
		)
	}))
	defer errSrv.Close()

	_, err = postJSON(
		context.Background(),
		defaultLLMHTTPClient(),
		errSrv.URL,
		map[string]string{
			"Authorization": "Bearer live_bearer_secret_987",
			"x-api-key":     "live_anthropic_secret_654",
		},
		map[string]string{"hello": "world"},
		"Claude",
	)
	if err == nil {
		t.Fatal("expected HTTP 401 error")
	}
	for _, secret := range []string{"live_bearer_secret_987", "live_anthropic_secret_654"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("postJSON HTTP error body leaked secret %q: %v", secret, err)
		}
	}
}

func TestStreamChatEnforcesBodyAndBuilderCaps(t *testing.T) {
	if maxStreamBodyBytes != 16<<20 {
		t.Fatalf("expected default maxStreamBodyBytes=16MiB, got %d", maxStreamBodyBytes)
	}

	origBody := streamBodyLimit
	origContent := streamContentLimit
	origToolArg := streamToolArgLimit
	t.Cleanup(func() {
		streamBodyLimit = origBody
		streamContentLimit = origContent
		streamToolArgLimit = origToolArg
	})

	t.Run("claude_body_limit", func(t *testing.T) {
		streamBodyLimit = 256
		streamContentLimit = origContent
		streamToolArgLimit = origToolArg

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for i := 0; i < 20; i++ {
				_, _ = fmt.Fprintln(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"0123456789abcdef"}}`)
			}
		}))
		defer srv.Close()

		adapter := NewClaudeAdapterWithEndpoint(srv.URL, "k", "claude-sonnet-4-5", nil)
		ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "go"}})
		if err != nil {
			t.Fatalf("StreamChat: %v", err)
		}
		var out strings.Builder
		for tok := range ch {
			out.WriteString(tok)
		}
		if !strings.Contains(out.String(), "exceeded size limit") {
			t.Fatalf("expected Claude body size limit error, got %q", out.String())
		}
	})

	t.Run("claude_tool_arg_limit_skips_execution", func(t *testing.T) {
		streamBodyLimit = origBody
		streamContentLimit = origContent
		streamToolArgLimit = 32

		called := false
		exec := &recordingToolExecutor{onExec: func() { called = true }}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintln(w, `data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"get_node_telemetry"}}`)
			_, _ = fmt.Fprintln(w, `data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"node_id\":\"0123456789012345678901234567890123456789\"}"}}`)
		}))
		defer srv.Close()

		adapter := NewClaudeAdapterWithEndpoint(srv.URL, "k", "claude-sonnet-4-5", exec)
		ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "go"}})
		if err != nil {
			t.Fatalf("StreamChat: %v", err)
		}
		var out strings.Builder
		for tok := range ch {
			out.WriteString(tok)
		}
		if called {
			t.Fatal("expected tool NOT to be executed when streamed input_json_delta exceeded limit")
		}
		if !strings.Contains(out.String(), "tool input exceeded limit") {
			t.Fatalf("expected Claude tool input cap error, got %q", out.String())
		}
	})

	t.Run("openai_body_and_tool_arg_limit", func(t *testing.T) {
		streamBodyLimit = 256
		streamContentLimit = origContent
		streamToolArgLimit = origToolArg

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for i := 0; i < 20; i++ {
				_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"0123456789abcdef"}}]}`)
			}
		}))
		defer srv.Close()

		adapter := NewOpenAIAdapter(srv.URL+"/v1", "k", "gpt-4o", nil)
		ch, err := adapter.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "go"}})
		if err != nil {
			t.Fatalf("StreamChat: %v", err)
		}
		var out strings.Builder
		for tok := range ch {
			out.WriteString(tok)
		}
		if !strings.Contains(out.String(), "exceeded size limit") {
			t.Fatalf("expected OpenAI body size limit error, got %q", out.String())
		}

		streamBodyLimit = origBody
		streamToolArgLimit = 24
		called := false
		exec := &recordingToolExecutor{onExec: func() { called = true }}
		toolSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"get_node_telemetry","arguments":"{\"node_id\":\"01234567890123456789012345\"}"}}]}}]}`)
		}))
		defer toolSrv.Close()

		adapter2 := NewOpenAIAdapter(toolSrv.URL+"/v1", "k", "gpt-4o", exec)
		ch2, err := adapter2.StreamChat(context.Background(), "sys", []Message{{Role: "user", Content: "go"}})
		if err != nil {
			t.Fatalf("StreamChat: %v", err)
		}
		var out2 strings.Builder
		for tok := range ch2 {
			out2.WriteString(tok)
		}
		if called {
			t.Fatal("expected tool NOT to be executed when OpenAI streamed args exceeded limit")
		}
		if !strings.Contains(out2.String(), "tool arguments exceeded limit") {
			t.Fatalf("expected OpenAI tool args cap error, got %q", out2.String())
		}
	})

	t.Run("ollama_body_and_content_limit", func(t *testing.T) {
		streamBodyLimit = 180
		streamContentLimit = origContent
		streamToolArgLimit = origToolArg

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			for i := 0; i < 15; i++ {
				_, _ = fmt.Fprintln(w, `{"message":{"role":"assistant","content":"0123456789abcdef"}}`)
			}
		}))
		defer srv.Close()

		adapter := NewOllamaAdapter(srv.URL, "qwen2.5-coder", nil)
		_, err := adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "go"}})
		if err == nil || !strings.Contains(err.Error(), "exceeded size limit") {
			t.Fatalf("expected Ollama body limit error, got %v", err)
		}

		streamBodyLimit = origBody
		streamContentLimit = 20
		_, err = adapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "go"}})
		if err == nil || !strings.Contains(err.Error(), "exceeded max content size") {
			t.Fatalf("expected Ollama content limit error, got %v", err)
		}
	})
}

func TestOpenAIEndpointNormalizationAndVLLMLlamaCPPConfig(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "https://api.openai.com/v1/chat/completions"},
		{"http://127.0.0.1:8000", "http://127.0.0.1:8000/v1/chat/completions"},
		{"http://127.0.0.1:8000/", "http://127.0.0.1:8000/v1/chat/completions"},
		{"http://127.0.0.1:8000/v1", "http://127.0.0.1:8000/v1/chat/completions"},
		{"http://127.0.0.1:8000/v1/", "http://127.0.0.1:8000/v1/chat/completions"},
		{"http://127.0.0.1:8000/v1/chat/completions", "http://127.0.0.1:8000/v1/chat/completions"},
		{"http://127.0.0.1:8000/v1/chat/completions/", "http://127.0.0.1:8000/v1/chat/completions"},
	}
	for _, tc := range cases {
		a := NewOpenAIAdapter(tc.in, "k", "m", nil)
		if a.endpoint != tc.want {
			t.Fatalf("NewOpenAIAdapter(%q).endpoint = %q, want %q", tc.in, a.endpoint, tc.want)
		}
	}

	// Verify vLLM and llama.cpp pass through APIKey & Model and normalize /v1 endpoints properly.
	for _, tc := range []struct {
		provider     string
		apiKey       string
		model        string
		defaultKey   string
		defaultModel string
	}{
		{"vllm", "vllm-prod-key", "custom/vllm-model", "vllm-no-key", "Qwen/Qwen2.5-Coder-32B-Instruct"},
		{"llamacpp", "llama-prod-key", "custom-gguf", "local-no-key", "default"},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			var gotPath, gotAuth, gotModel string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotAuth = r.Header.Get("Authorization")
				var reqBody struct {
					Model string `json:"model"`
				}
				_ = json.NewDecoder(r.Body).Decode(&reqBody)
				gotModel = reqBody.Model
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
			}))
			defer srv.Close()

			a := New(Config{
				Provider: tc.provider,
				Endpoint: srv.URL + "/v1",
				APIKey:   tc.apiKey,
				Model:    tc.model,
			})
			reply, err := a.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "ping"}})
			if err != nil || reply != "ok" {
				t.Fatalf("%s Chat failed: reply=%q err=%v", tc.provider, reply, err)
			}
			if gotPath != "/v1/chat/completions" {
				t.Fatalf("%s hit path %q, want /v1/chat/completions", tc.provider, gotPath)
			}
			if gotAuth != "Bearer "+tc.apiKey {
				t.Fatalf("%s sent Authorization %q, want Bearer %s", tc.provider, gotAuth, tc.apiKey)
			}
			if gotModel != tc.model {
				t.Fatalf("%s sent model %q, want %q", tc.provider, gotModel, tc.model)
			}

			// Fallback defaults when APIKey and Model are empty.
			defAdapter := New(Config{Provider: tc.provider, Endpoint: srv.URL + "/v1"})
			if _, err := defAdapter.Chat(context.Background(), "sys", []Message{{Role: "user", Content: "ping"}}); err != nil {
				t.Fatalf("%s default Chat failed: %v", tc.provider, err)
			}
			if gotAuth != "Bearer "+tc.defaultKey || gotModel != tc.defaultModel {
				t.Fatalf("%s default auth/model = (%q, %q), want (Bearer %s, %s)",
					tc.provider, gotAuth, gotModel, tc.defaultKey, tc.defaultModel)
			}
		})
	}
}
