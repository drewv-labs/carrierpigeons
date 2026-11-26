package mcpserver

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/mark3labs/mcp-go/mcp"
)

type stubAgentSafeDB struct{}

func (stubAgentSafeDB) GetMeshIdentity(context.Context) (*core.MeshIdentity, error) {
	return &core.MeshIdentity{MeshName: "rv64-lab", CoopNodeID: "rv64-coop-01"}, nil
}
func (stubAgentSafeDB) GetClusterStatus(context.Context, time.Duration) ([]db.NodeSummary, error) {
	return []db.NodeSummary{{NodeID: "edge-01", Status: "ONLINE"}}, nil
}
func (stubAgentSafeDB) ResolveNodes(context.Context, core.RunnerSelector) ([]string, error) {
	return []string{"edge-01"}, nil
}
func (stubAgentSafeDB) GetLineageDAG(context.Context, string, int) (*db.DAG, error) {
	return &db.DAG{}, nil
}
func (stubAgentSafeDB) GetLedgerActivity(context.Context, int) ([]db.LedgerActivity, error) {
	return nil, nil
}
func (stubAgentSafeDB) QueryReadOnlyMaps(context.Context, string, ...any) ([]map[string]any, error) {
	return nil, nil
}
func (stubAgentSafeDB) ExplainReadOnly(context.Context, string) (string, error) {
	return `[{"Plan":{}}]`, nil
}
func (stubAgentSafeDB) EnsureMathSchema(context.Context) error { return nil }
func (stubAgentSafeDB) SavePMVDraft(_ context.Context, r db.PMVRecord) (*db.PMVRecord, error) {
	cp := r
	return &cp, nil
}
func (stubAgentSafeDB) UpdatePMVState(context.Context, db.PMVRecord) error { return nil }
func (stubAgentSafeDB) GetPMV(context.Context, string) (*db.PMVRecord, error) {
	return &db.PMVRecord{}, nil
}
func (stubAgentSafeDB) ListPMVs(context.Context) ([]db.PMVRecord, error) { return nil, nil }
func (stubAgentSafeDB) MaterializePMV(context.Context, string, string) error {
	return nil
}
func (stubAgentSafeDB) DropPMV(context.Context, string) error { return nil }
func (stubAgentSafeDB) Close()                                {}

type configurableAgentSafeDB struct {
	stubAgentSafeDB
	calls atomic.Int32

	getMeshIdentityFn   func(context.Context) (*core.MeshIdentity, error)
	getClusterStatusFn  func(context.Context, time.Duration) ([]db.NodeSummary, error)
	queryReadOnlyMapsFn func(context.Context, string, ...any) ([]map[string]any, error)
	getLineageDAGFn     func(context.Context, string, int) (*db.DAG, error)
	getLedgerActivityFn func(context.Context, int) ([]db.LedgerActivity, error)
	listPMVsFn          func(context.Context) ([]db.PMVRecord, error)
	ensureMathSchemaFn  func(context.Context) error
	explainReadOnlyFn   func(context.Context, string) (string, error)
	materializePMVFn    func(context.Context, string, string) error
}

func (c *configurableAgentSafeDB) GetMeshIdentity(ctx context.Context) (*core.MeshIdentity, error) {
	c.calls.Add(1)
	if c.getMeshIdentityFn != nil {
		return c.getMeshIdentityFn(ctx)
	}
	return c.stubAgentSafeDB.GetMeshIdentity(ctx)
}

func (c *configurableAgentSafeDB) GetClusterStatus(ctx context.Context, d time.Duration) ([]db.NodeSummary, error) {
	c.calls.Add(1)
	if c.getClusterStatusFn != nil {
		return c.getClusterStatusFn(ctx, d)
	}
	return c.stubAgentSafeDB.GetClusterStatus(ctx, d)
}

func (c *configurableAgentSafeDB) QueryReadOnlyMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	c.calls.Add(1)
	if c.queryReadOnlyMapsFn != nil {
		return c.queryReadOnlyMapsFn(ctx, sql, args...)
	}
	return c.stubAgentSafeDB.QueryReadOnlyMaps(ctx, sql, args...)
}

func (c *configurableAgentSafeDB) GetLineageDAG(ctx context.Context, id string, depth int) (*db.DAG, error) {
	c.calls.Add(1)
	if c.getLineageDAGFn != nil {
		return c.getLineageDAGFn(ctx, id, depth)
	}
	return c.stubAgentSafeDB.GetLineageDAG(ctx, id, depth)
}

func (c *configurableAgentSafeDB) GetLedgerActivity(ctx context.Context, limit int) ([]db.LedgerActivity, error) {
	c.calls.Add(1)
	if c.getLedgerActivityFn != nil {
		return c.getLedgerActivityFn(ctx, limit)
	}
	return c.stubAgentSafeDB.GetLedgerActivity(ctx, limit)
}

func (c *configurableAgentSafeDB) ListPMVs(ctx context.Context) ([]db.PMVRecord, error) {
	c.calls.Add(1)
	if c.listPMVsFn != nil {
		return c.listPMVsFn(ctx)
	}
	return c.stubAgentSafeDB.ListPMVs(ctx)
}

func (c *configurableAgentSafeDB) EnsureMathSchema(ctx context.Context) error {
	c.calls.Add(1)
	if c.ensureMathSchemaFn != nil {
		return c.ensureMathSchemaFn(ctx)
	}
	return c.stubAgentSafeDB.EnsureMathSchema(ctx)
}

func (c *configurableAgentSafeDB) ExplainReadOnly(ctx context.Context, sql string) (string, error) {
	if c.explainReadOnlyFn != nil {
		return c.explainReadOnlyFn(ctx, sql)
	}
	return c.stubAgentSafeDB.ExplainReadOnly(ctx, sql)
}

func (c *configurableAgentSafeDB) MaterializePMV(ctx context.Context, name, sql string) error {
	if c.materializePMVFn != nil {
		return c.materializePMVFn(ctx, name, sql)
	}
	return c.stubAgentSafeDB.MaterializePMV(ctx, name, sql)
}

type stubOptimizerLLM struct {
	chatFn func(ctx context.Context, systemPrompt string, history []llm.Message) (string, error)
}

func (s *stubOptimizerLLM) Chat(ctx context.Context, systemPrompt string, history []llm.Message) (string, error) {
	if s.chatFn != nil {
		return s.chatFn(ctx, systemPrompt, history)
	}
	return "CERTIFIED: ok", nil
}

func (s *stubOptimizerLLM) StreamChat(context.Context, string, []llm.Message) (<-chan string, error) {
	ch := make(chan string)
	close(ch)
	return ch, nil
}

func makeToolReq(args map[string]any) mcp.CallToolRequest {
	return mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: args,
		},
	}
}

func extractResultText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatalf("expected non-empty CallToolResult content, got %+v", res)
	}
	tc, ok := mcp.AsTextContent(res.Content[0])
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	return tc.Text
}

func TestMCPHTTPAuthAndDNSRebindingProtection(t *testing.T) {
	srv := New(Options{Store: stubAgentSafeDB{}})

	// Non-loopback ServeHTTPWithOptions without token must fail closed immediately.
	if err := ServeHTTPWithOptions(srv, "0.0.0.0:18089", HTTPOptions{}); err == nil {
		t.Fatal("expected ServeHTTPWithOptions on 0.0.0.0 without AuthToken to fail closed")
	}

	// Loopback handler without token enforces Host and Origin checks against DNS rebinding.
	loopbackHandler := NewHTTPHandler(srv, HTTPOptions{})

	evilHostReq := httptest.NewRequest(http.MethodPost, "http://evil.attacker.invalid/mcp", strings.NewReader(`{}`))
	evilHostReq.Host = "evil.attacker.invalid:8089"
	rec := httptest.NewRecorder()
	loopbackHandler.ServeHTTP(rec, evilHostReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for DNS-rebound Host header, got %d", rec.Code)
	}

	evilOriginReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
	evilOriginReq.Host = "127.0.0.1:8089"
	evilOriginReq.Header.Set("Origin", "https://evil.example.com")
	rec = httptest.NewRecorder()
	loopbackHandler.ServeHTTP(rec, evilOriginReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for cross-origin browser request, got %d", rec.Code)
	}

	// Handler with AuthToken enforces constant-time Bearer authentication.
	authedHandler := NewHTTPHandler(srv, HTTPOptions{AuthToken: "super-secret-mcp-token"})

	unauthReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
	rec = httptest.NewRecorder()
	authedHandler.ServeHTTP(rec, unauthReq)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized when missing Bearer token, got %d", rec.Code)
	}

	wrongReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
	wrongReq.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	authedHandler.ServeHTTP(rec, wrongReq)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for invalid Bearer token, got %d", rec.Code)
	}

	initBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`
	validReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(initBody))
	validReq.Header.Set("Authorization", "Bearer super-secret-mcp-token")
	validReq.Header.Set("Content-Type", "application/json")
	validReq.Header.Set("Accept", "application/json, text/event-stream")
	rec = httptest.NewRecorder()
	authedHandler.ServeHTTP(rec, validReq)
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("expected authenticated MCP request to pass auth middleware, got %d", rec.Code)
	}

	// EnforceLoopbackHost rejects non-loopback Host even when Bearer auth token is present.
	loopbackAuthed := NewHTTPHandler(srv, HTTPOptions{
		AuthToken:           "super-secret-mcp-token",
		EnforceLoopbackHost: true,
	})
	rebindReq := httptest.NewRequest(http.MethodPost, "http://evil.attacker.invalid/mcp", strings.NewReader(initBody))
	rebindReq.Host = "evil.attacker.invalid:8089"
	rebindReq.Header.Set("Authorization", "Bearer super-secret-mcp-token")
	rec = httptest.NewRecorder()
	loopbackAuthed.ServeHTTP(rec, rebindReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for non-loopback Host when EnforceLoopbackHost=true, got %d", rec.Code)
	}

	// AllowUnauthenticatedNonLoopback permits non-loopback Host/Origin requests when explicitly opted in.
	nonLoopbackOptIn := NewHTTPHandler(srv, HTTPOptions{
		AllowUnauthenticatedNonLoopback: true,
	})
	lanReq := httptest.NewRequest(http.MethodPost, "http://telemetry-node.local:8089/mcp", strings.NewReader(initBody))
	lanReq.Host = "telemetry-node.local:8089"
	lanReq.Header.Set("Origin", "https://dashboard.example.com")
	lanReq.Header.Set("Content-Type", "application/json")
	lanReq.Header.Set("Accept", "application/json, text/event-stream")
	rec = httptest.NewRecorder()
	nonLoopbackOptIn.ServeHTTP(rec, lanReq)
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("expected AllowUnauthenticatedNonLoopback=true to allow non-loopback Host/Origin, got %d", rec.Code)
	}

	// EnforceLoopbackHost still wins over AllowUnauthenticatedNonLoopback when bound to loopback.
	loopbackForced := NewHTTPHandler(srv, HTTPOptions{
		EnforceLoopbackHost:             true,
		AllowUnauthenticatedNonLoopback: true,
	})
	forcedReq := httptest.NewRequest(http.MethodPost, "http://telemetry-node.local:8089/mcp", strings.NewReader(initBody))
	forcedReq.Host = "telemetry-node.local:8089"
	rec = httptest.NewRecorder()
	loopbackForced.ServeHTTP(rec, forcedReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected EnforceLoopbackHost=true to reject non-loopback Host even with AllowUnauthenticatedNonLoopback=true, got %d", rec.Code)
	}

	// Default MCP server registers get_mesh_identity and omits create_pigeonmath_view unless AllowWriteViews is enabled.
	if srv.GetTool("get_mesh_identity") == nil {
		t.Fatal("expected get_mesh_identity tool to be registered")
	}
	if srv.GetTool("create_pigeonmath_view") != nil {
		t.Fatal("expected create_pigeonmath_view to be omitted when AllowWriteViews is false")
	}
	writeSrv := New(ServerOptions{Store: stubAgentSafeDB{}, AllowWriteViews: true})
	if writeSrv.GetTool("create_pigeonmath_view") == nil {
		t.Fatal("expected create_pigeonmath_view to be registered when AllowWriteViews is true")
	}
}

func TestMCPToolHandlerTimeouts(t *testing.T) {
	checkDeadline := func(ctx context.Context, minD, maxD time.Duration) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatalf("expected bounded context deadline, got none")
		}
		rem := time.Until(dl)
		if rem <= minD || rem > maxD {
			t.Fatalf("expected context deadline in (%s, %s], got %s", minD, maxD, rem)
		}
	}

	spy := &configurableAgentSafeDB{
		getMeshIdentityFn: func(ctx context.Context) (*core.MeshIdentity, error) {
			checkDeadline(ctx, 8*time.Second, 10*time.Second)
			return &core.MeshIdentity{MeshName: "mesh-a", CoopNodeID: "coop-01"}, nil
		},
		getClusterStatusFn: func(ctx context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			checkDeadline(ctx, 8*time.Second, 10*time.Second)
			return []db.NodeSummary{{NodeID: "edge-01", Status: "ONLINE", IsHealthy: true}}, nil
		},
		queryReadOnlyMapsFn: func(ctx context.Context, _ string, _ ...any) ([]map[string]any, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("expected deadline on QueryReadOnlyMaps")
			}
			return nil, nil
		},
		getLineageDAGFn: func(ctx context.Context, id string, _ int) (*db.DAG, error) {
			checkDeadline(ctx, 8*time.Second, 10*time.Second)
			return &db.DAG{RootID: id}, nil
		},
		getLedgerActivityFn: func(ctx context.Context, _ int) ([]db.LedgerActivity, error) {
			checkDeadline(ctx, 8*time.Second, 10*time.Second)
			return nil, nil
		},
		listPMVsFn: func(ctx context.Context) ([]db.PMVRecord, error) {
			checkDeadline(ctx, 8*time.Second, 10*time.Second)
			return nil, nil
		},
		ensureMathSchemaFn: func(ctx context.Context) error {
			checkDeadline(ctx, 55*time.Second, 60*time.Second)
			return nil
		},
	}

	srv := New(Options{Store: spy, AllowWriteViews: true})
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{name: "get_mesh_identity"},
		{name: "get_cluster_status"},
		{name: "get_node_telemetry", args: map[string]any{"node_id": "edge-01"}},
		{name: "check_offline_nodes"},
		{name: "query_ctd_readonly", args: map[string]any{"sql": "SELECT 1"}},
		{name: "get_lineage_dag", args: map[string]any{"artifact_id": "dataset:raw-001"}},
		{name: "get_ledger_activity"},
		{name: "list_pigeonmath_views"},
		{name: "create_pigeonmath_view", args: map[string]any{"view_name": "cooling_headroom", "sql": "SELECT 1"}},
	} {
		tool := srv.GetTool(tc.name)
		if tool == nil {
			t.Fatalf("missing tool %s", tc.name)
		}
		res, err := tool.Handler(context.Background(), makeToolReq(tc.args))
		if err != nil {
			t.Fatalf("%s returned unexpected Go error: %v", tc.name, err)
		}
		if res.IsError {
			t.Fatalf("%s unexpectedly failed: %s", tc.name, extractResultText(t, res))
		}
	}

	// Verify short bounded timeouts actually cancel blocking DB calls promptly.
	blockingDB := &configurableAgentSafeDB{
		getClusterStatusFn: func(ctx context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		ensureMathSchemaFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	shortSrv := newWithTimeouts(Options{Store: blockingDB, AllowWriteViews: true}, 25*time.Millisecond, 35*time.Millisecond)

	res, err := shortSrv.GetTool("get_cluster_status").Handler(context.Background(), makeToolReq(nil))
	if err != nil || !res.IsError {
		t.Fatalf("expected get_cluster_status timeout error, got res=%+v err=%v", res, err)
	}
	if txt := extractResultText(t, res); !strings.Contains(txt, "timed out") {
		t.Fatalf("expected 'timed out' error message, got %q", txt)
	}

	res, err = shortSrv.GetTool("create_pigeonmath_view").Handler(context.Background(), makeToolReq(map[string]any{
		"view_name": "slow_view",
		"sql":       "SELECT 1",
	}))
	if err != nil || !res.IsError {
		t.Fatalf("expected create_pigeonmath_view timeout error, got res=%+v err=%v", res, err)
	}
	if txt := extractResultText(t, res); !strings.Contains(txt, "timed out") {
		t.Fatalf("expected 'timed out' error message for PMV, got %q", txt)
	}

	// Nil store fails cleanly without panicking.
	nilSrv := New(Options{AllowWriteViews: true})
	res, err = nilSrv.GetTool("get_cluster_status").Handler(context.Background(), makeToolReq(nil))
	if err != nil || !res.IsError {
		t.Fatalf("expected error on nil store, got res=%+v err=%v", res, err)
	}
}

func TestMCPToolOutputTruncation256KiB(t *testing.T) {
	// Include multi-byte UTF-8 runes ('🚀' = 4 bytes, 'α' = 2 bytes) so truncation boundary lands mid-rune.
	largeUnit := strings.Repeat("telemetry-α-🚀-", 200)
	var rows []map[string]any
	for i := 0; i < 95; i++ {
		rows = append(rows, map[string]any{
			"idx":             i,
			"payload":         largeUnit,
			"pillar":          "observer",
			"component":       "host-os",
			"status":          "active",
			"event_timestamp": time.Now().UTC(),
			"metrics":         map[string]any{"blob": largeUnit},
		})
	}

	var offlineNodes []db.NodeSummary
	for i := 0; i < 200; i++ {
		offlineNodes = append(offlineNodes, db.NodeSummary{
			NodeID:    "node-" + strings.Repeat("x", 1500),
			Arch:      "riscv64",
			Status:    "OFFLINE",
			Component: "host-os",
			LastSeen:  time.Now().Add(-10 * time.Minute),
			IsHealthy: false,
		})
	}

	store := &configurableAgentSafeDB{
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return rows, nil
		},
		getClusterStatusFn: func(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			return offlineNodes, nil
		},
	}

	srv := New(Options{Store: store})
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{name: "query_ctd_readonly", args: map[string]any{"sql": "SELECT payload FROM telemetric_ledger"}},
		{name: "get_cluster_status"},
		{name: "check_offline_nodes"},
		{name: "get_node_telemetry", args: map[string]any{"node_id": "edge-01"}},
	} {
		res, err := srv.GetTool(tc.name).Handler(context.Background(), makeToolReq(tc.args))
		if err != nil || res.IsError {
			t.Fatalf("%s failed: res=%+v err=%v", tc.name, res, err)
		}
		txt := extractResultText(t, res)
		if len(txt) > maxMCPOutputBytes {
			t.Fatalf("%s exceeded 256 KiB cap: got %d bytes", tc.name, len(txt))
		}
		if !strings.HasSuffix(txt, mcpTruncationMarker) {
			t.Fatalf("%s missing truncation marker suffix; len=%d", tc.name, len(txt))
		}
		if !utf8.ValidString(txt) {
			t.Fatalf("%s truncated output produced invalid UTF-8", tc.name)
		}
	}
}

func TestMCPToolNonFiniteFloatSanitizationAndMarshalErrors(t *testing.T) {
	nanPtr := new(float64)
	*nanPtr = math.NaN()

	sharedMetrics := map[string]any{
		"temp_c":       math.NaN(),
		"pwm_ratio":    math.Inf(1),
		"delta":        math.Inf(-1),
		"float32_nan":  float32(math.NaN()),
		"ptr_nan":      nanPtr,
		"series":       []float64{12.5, math.NaN(), math.Inf(1)},
		"nested":       map[string]any{"inner_inf": math.Inf(-1), "ok": 42.5},
		"json_num_nan": json.Number("NaN"),
	}

	store := &configurableAgentSafeDB{
		getClusterStatusFn: func(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			return []db.NodeSummary{{
				NodeID:        "rv64-compute-01",
				Status:        "active",
				LastSeen:      time.Now().UTC(),
				IsHealthy:     true,
				LatestMetrics: sharedMetrics,
			}}, nil
		},
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return []map[string]any{{
				"node_id":         "rv64-compute-01",
				"pillar":          "observer",
				"component":       "host-os",
				"status":          "active",
				"event_timestamp": time.Now().UTC(),
				"metrics":         sharedMetrics,
			}}, nil
		},
		listPMVsFn: func(_ context.Context) ([]db.PMVRecord, error) {
			return []db.PMVRecord{{
				ViewName:         "pmv_test",
				Status:           "certified",
				ValidationReport: map[string]any{"score": math.NaN(), "ok": 1.0},
			}}, nil
		},
	}

	srv := New(Options{Store: store})
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{name: "get_cluster_status"},
		{name: "query_ctd_readonly", args: map[string]any{"sql": "SELECT metrics FROM telemetric_ledger"}},
		{name: "list_pigeonmath_views"},
	} {
		res, err := srv.GetTool(tc.name).Handler(context.Background(), makeToolReq(tc.args))
		if err != nil || res.IsError {
			t.Fatalf("%s failed with non-finite floats: res=%+v err=%v", tc.name, res, err)
		}
		txt := extractResultText(t, res)
		if !json.Valid([]byte(txt)) {
			t.Fatalf("%s produced invalid JSON: %s", tc.name, txt)
		}
		if !strings.Contains(txt, "null") {
			t.Fatalf("%s expected sanitized null fields in JSON: %s", tc.name, txt)
		}
	}

	// get_node_telemetry formats JSON per row and must also sanitize non-finite floats.
	res, err := srv.GetTool("get_node_telemetry").Handler(context.Background(), makeToolReq(map[string]any{"node_id": "rv64-compute-01"}))
	if err != nil || res.IsError {
		t.Fatalf("get_node_telemetry failed: res=%+v err=%v", res, err)
	}
	telText := extractResultText(t, res)
	if !strings.Contains(telText, `"temp_c":null`) || !strings.Contains(telText, `"ok":42.5`) {
		t.Fatalf("expected sanitized JSON metrics in get_node_telemetry, got: %s", telText)
	}

	// Verify original sharedMetrics map was NOT mutated.
	if !math.IsNaN(sharedMetrics["temp_c"].(float64)) {
		t.Fatal("sanitizeJSONValue mutated caller-owned map in place")
	}

	// Verify typed structs with float64, []float64, [2]float64, map[string]float64, *[]float64,
	// embedded structs, and json tags sanitize NaN/Inf to null without dropping fields or mutating input.
	type embeddedTelemetry struct {
		AmbientC float64 `json:"ambient_c"`
	}
	type typedSensorStruct struct {
		embeddedTelemetry
		TempC      float64            `json:"temp_c"`
		TempF32    float32            `json:"temp_f32"`
		Series     []float64          `json:"series"`
		Pair       [2]float64         `json:"pair"`
		PerCore    map[string]float64 `json:"per_core"`
		PtrSeries  *[]float64         `json:"ptr_series"`
		Ignored    float64            `json:"-"`
		LiteralHyp float64            `json:"-,"`
		OmitZero   float64            `json:"omit_zero,omitempty"`
		KeepEmpty  float64            `json:"keep_empty"`
		OmitNaN    float64            `json:"omit_nan,omitempty"`
	}
	ptrSlice := []float64{3.5, math.Inf(-1)}
	structRow := typedSensorStruct{
		embeddedTelemetry: embeddedTelemetry{AmbientC: math.NaN()},
		TempC:             math.NaN(),
		TempF32:           float32(math.Inf(1)),
		Series:            []float64{1.5, math.NaN()},
		Pair:              [2]float64{2.0, math.Inf(1)},
		PerCore:           map[string]float64{"cpu0": math.NaN(), "cpu1": 55.25},
		PtrSeries:         &ptrSlice,
		Ignored:           math.NaN(),
		LiteralHyp:        math.Inf(1),
		OmitZero:          0,
		KeepEmpty:         0,
		OmitNaN:           math.NaN(),
	}
	structStore := &configurableAgentSafeDB{
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return []map[string]any{{
				"sensor":  structRow,
				"metrics": structRow,
			}}, nil
		},
	}
	structSrv := New(Options{Store: structStore})
	structRes, err := structSrv.GetTool("query_ctd_readonly").Handler(context.Background(), makeToolReq(map[string]any{"sql": "SELECT 1"}))
	if err != nil || structRes.IsError {
		t.Fatalf("query_ctd_readonly with typed struct failed: res=%+v err=%v", structRes, err)
	}
	structText := extractResultText(t, structRes)
	if !json.Valid([]byte(structText)) {
		t.Fatalf("expected valid JSON for sanitized typed struct, got: %s", structText)
	}
	var decodedRows []map[string]any
	if err := json.Unmarshal([]byte(structText), &decodedRows); err != nil || len(decodedRows) != 1 {
		t.Fatalf("unmarshal sanitized struct output failed: %v (%s)", err, structText)
	}
	sensorMap, ok := decodedRows[0]["sensor"].(map[string]any)
	if !ok {
		t.Fatalf("expected sensor map, got %T", decodedRows[0]["sensor"])
	}
	if _, hasIgnored := sensorMap["Ignored"]; hasIgnored {
		t.Fatalf("expected json:\"-\" field to be omitted, got %v", sensorMap)
	}
	if _, hasOmitZero := sensorMap["omit_zero"]; hasOmitZero {
		t.Fatalf("expected zero omitempty field to be omitted, got %v", sensorMap)
	}
	for _, nullKey := range []string{"ambient_c", "temp_c", "temp_f32", "-", "omit_nan"} {
		v, exists := sensorMap[nullKey]
		if !exists || v != nil {
			t.Fatalf("expected key %q to be present with JSON null, got exists=%v val=%#v", nullKey, exists, v)
		}
	}
	if got := sensorMap["keep_empty"]; got != float64(0) {
		t.Fatalf("expected keep_empty=0, got %#v", got)
	}
	if seriesSlice, ok := sensorMap["series"].([]any); !ok || len(seriesSlice) != 2 || seriesSlice[0] != 1.5 || seriesSlice[1] != nil {
		t.Fatalf("expected sanitized series [1.5, null], got %#v", sensorMap["series"])
	}
	if pairSlice, ok := sensorMap["pair"].([]any); !ok || len(pairSlice) != 2 || pairSlice[0] != 2.0 || pairSlice[1] != nil {
		t.Fatalf("expected sanitized pair [2.0, null], got %#v", sensorMap["pair"])
	}
	if ptrOut, ok := sensorMap["ptr_series"].([]any); !ok || len(ptrOut) != 2 || ptrOut[0] != 3.5 || ptrOut[1] != nil {
		t.Fatalf("expected sanitized ptr_series [3.5, null], got %#v", sensorMap["ptr_series"])
	}
	if perCore, ok := sensorMap["per_core"].(map[string]any); !ok || perCore["cpu0"] != nil || perCore["cpu1"] != 55.25 {
		t.Fatalf("expected sanitized per_core {cpu0:null, cpu1:55.25}, got %#v", sensorMap["per_core"])
	}
	if !math.IsNaN(structRow.Series[1]) || !math.IsNaN(structRow.PerCore["cpu0"]) {
		t.Fatal("sanitizeJSONValue mutated typed struct slice/map fields in place")
	}

	// Verify unserializable types trigger explicit Serialization error instead of empty string.
	badStore := &configurableAgentSafeDB{
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return []map[string]any{{"bad": func() {}, "metrics": func() {}}}, nil
		},
	}
	badSrv := New(Options{Store: badStore})
	res, err = badSrv.GetTool("query_ctd_readonly").Handler(context.Background(), makeToolReq(map[string]any{"sql": "SELECT 1"}))
	if err != nil || !res.IsError {
		t.Fatalf("expected serialization error for func() value, got res=%+v err=%v", res, err)
	}
	res, err = badSrv.GetTool("get_node_telemetry").Handler(context.Background(), makeToolReq(map[string]any{"node_id": "edge-01"}))
	if err != nil || !res.IsError {
		t.Fatalf("expected serialization error in get_node_telemetry for func() metrics, got res=%+v err=%v", res, err)
	}
}

func TestMCPInputValidationNodeIDAndArtifactID(t *testing.T) {
	store := &configurableAgentSafeDB{}
	srv := New(Options{Store: store})

	badNodeIDs := []any{
		"",
		"   ",
		"node+1",
		"node#1",
		"node/1",
		"node 1",
		"node\n1",
		"node\x00",
		strings.Repeat("a", 129),
		12345,
	}
	for _, raw := range badNodeIDs {
		res, err := srv.GetTool("get_node_telemetry").Handler(context.Background(), makeToolReq(map[string]any{"node_id": raw}))
		if err != nil || !res.IsError {
			t.Fatalf("expected get_node_telemetry to reject node_id=%#v, got res=%+v err=%v", raw, res, err)
		}
	}

	badArtifactIDs := []any{
		"",
		"   ",
		"art\nid",
		"art\tid",
		"art id",
		"art\x00id",
		"\xff\xfe",
		strings.Repeat("a", maxArtifactIDBytes+1),
		42,
	}
	for _, raw := range badArtifactIDs {
		res, err := srv.GetTool("get_lineage_dag").Handler(context.Background(), makeToolReq(map[string]any{"artifact_id": raw}))
		if err != nil || !res.IsError {
			t.Fatalf("expected get_lineage_dag to reject artifact_id=%#v, got res=%+v err=%v", raw, res, err)
		}
	}

	if store.calls.Load() != 0 {
		t.Fatalf("expected store not to be called for invalid node_id/artifact_id inputs, got %d calls", store.calls.Load())
	}

	// Valid identifiers proceed to store.
	res, err := srv.GetTool("get_node_telemetry").Handler(context.Background(), makeToolReq(map[string]any{"node_id": "rv64-compute-01:fan_1"}))
	if err != nil || res.IsError {
		t.Fatalf("expected valid node_id to succeed, got res=%+v err=%v", res, err)
	}
	res, err = srv.GetTool("get_lineage_dag").Handler(context.Background(), makeToolReq(map[string]any{"artifact_id": "dataset:raw-001/run-1"}))
	if err != nil || res.IsError {
		t.Fatalf("expected valid artifact_id to succeed, got res=%+v err=%v", res, err)
	}
}

func TestMCPSanitizedDBAndLLMErrors(t *testing.T) {
	rawDBErr := errors.New(`connect postgres://pigeoncare_ro:topSecretDBPass@192.0.2.55:5432/edge_ledger?sslmode=require: dial tcp 192.0.2.55:5432: host=192.0.2.55 password=libpqSecretPW (SQLSTATE 28P01)`)
	rawLLMErr := errors.New(`Claude HTTP 401: invalid x-api-key sk-ant-secret1234567890 Authorization: Bearer bearerSecretTok at https://user:llmSecretPass@llm.example.invalid/v1/messages`)

	secretTokens := []string{
		"topSecretDBPass",
		"libpqSecretPW",
		"llmSecretPass",
		"sk-ant-secret1234567890",
		"bearerSecretTok",
		"postgres://",
		"https://",
		"192.0.2.55",
		"192.0.2.88",
		"llm.example.invalid",
		"SQLSTATE",
		"28P01",
	}

	assertNoLeak := func(t *testing.T, label, text string) {
		t.Helper()
		for _, tok := range secretTokens {
			if strings.Contains(text, tok) {
				t.Fatalf("%s leaked sensitive token %q in output: %s", label, tok, text)
			}
		}
	}

	store := &configurableAgentSafeDB{
		getMeshIdentityFn:   func(context.Context) (*core.MeshIdentity, error) { return nil, rawDBErr },
		getClusterStatusFn:  func(context.Context, time.Duration) ([]db.NodeSummary, error) { return nil, rawDBErr },
		queryReadOnlyMapsFn: func(context.Context, string, ...any) ([]map[string]any, error) { return nil, rawDBErr },
		getLineageDAGFn:     func(context.Context, string, int) (*db.DAG, error) { return nil, rawDBErr },
		getLedgerActivityFn: func(context.Context, int) ([]db.LedgerActivity, error) { return nil, rawDBErr },
		listPMVsFn: func(context.Context) ([]db.PMVRecord, error) {
			return []db.PMVRecord{{
				ViewName:         "pmv_leaked",
				Status:           "failed",
				AgentNotes:       "optimizer unavailable (" + rawLLMErr.Error() + ")",
				ValidationReport: map[string]any{"error": rawDBErr.Error()},
			}}, nil
		},
	}

	optLLM := &stubOptimizerLLM{
		chatFn: func(context.Context, string, []llm.Message) (string, error) {
			return "", rawLLMErr
		},
	}

	srv := New(Options{Store: store, Optimizer: optLLM, AllowWriteViews: true})
	for _, tc := range []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{name: "get_mesh_identity", wantError: true},
		{name: "get_cluster_status", wantError: true},
		{name: "get_node_telemetry", args: map[string]any{"node_id": "edge-01"}, wantError: true},
		{name: "check_offline_nodes", wantError: true},
		{name: "query_ctd_readonly", args: map[string]any{"sql": "SELECT 1"}, wantError: true},
		{name: "get_lineage_dag", args: map[string]any{"artifact_id": "art:1"}, wantError: true},
		{name: "get_ledger_activity", wantError: true},
		{name: "list_pigeonmath_views", wantError: false},
		{name: "create_pigeonmath_view", args: map[string]any{"view_name": "cooling", "sql": "SELECT 1"}, wantError: true},
	} {
		res, err := srv.GetTool(tc.name).Handler(context.Background(), makeToolReq(tc.args))
		if err != nil {
			t.Fatalf("%s returned unexpected Go error: %v", tc.name, err)
		}
		if res.IsError != tc.wantError {
			t.Fatalf("%s expected IsError=%v, got %v (%s)", tc.name, tc.wantError, res.IsError, extractResultText(t, res))
		}
		assertNoLeak(t, tc.name, extractResultText(t, res))
	}

	// ListPMVs DB error is also sanitized.
	errListStore := &configurableAgentSafeDB{
		listPMVsFn: func(context.Context) ([]db.PMVRecord, error) { return nil, rawDBErr },
	}
	errListSrv := New(Options{Store: errListStore})
	res, err := errListSrv.GetTool("list_pigeonmath_views").Handler(context.Background(), makeToolReq(nil))
	if err != nil || !res.IsError {
		t.Fatalf("expected list_pigeonmath_views error, got res=%+v err=%v", res, err)
	}
	assertNoLeak(t, "list_pigeonmath_views_err", extractResultText(t, res))
}

func writeTestSelfSignedCert(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdsa key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	certOut := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certOut, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyOut, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

type pipeListener struct {
	addr   pipeAddr
	conns  chan net.Conn
	closed chan struct{}
	once   atomic.Bool
}

func newPipeListener(addr string) *pipeListener {
	return &pipeListener{
		addr:   pipeAddr(addr),
		conns:  make(chan net.Conn, 16),
		closed: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	case c := <-l.conns:
		return c, nil
	}
}

func (l *pipeListener) Close() error {
	if l.once.CompareAndSwap(false, true) {
		close(l.closed)
	}
	return nil
}

func (l *pipeListener) Addr() net.Addr { return l.addr }

func (l *pipeListener) Dial() (net.Conn, error) {
	clientConn, serverConn := net.Pipe()
	select {
	case <-l.closed:
		_ = clientConn.Close()
		_ = serverConn.Close()
		return nil, net.ErrClosed
	case l.conns <- serverConn:
		return clientConn, nil
	}
}

func TestMCPHTTPHardeningAndTLSFailClosedPolicy(t *testing.T) {
	srv := New(Options{Store: stubAgentSafeDB{}})
	tmpDir := t.TempDir()
	certPath, keyPath := writeTestSelfSignedCert(t, tmpDir)

	// 1. Non-loopback with AuthToken but NO TLS and NO AllowPlaintextHTTP MUST fail closed.
	if err := ServeHTTPWithOptions(srv, "0.0.0.0:18089", HTTPOptions{AuthToken: "secret"}); err == nil {
		t.Fatal("expected non-loopback plaintext HTTP without AllowPlaintextHTTP or TLS to fail closed")
	}

	// 2. Partial TLS cert/key MUST fail closed even on loopback.
	if err := ServeHTTPWithOptions(srv, "127.0.0.1:18089", HTTPOptions{TLSCertFile: certPath}); err == nil {
		t.Fatal("expected partial TLS config (cert without key) to fail closed")
	}
	if err := ServeHTTPWithOptions(srv, "127.0.0.1:18089", HTTPOptions{TLSKeyFile: keyPath}); err == nil {
		t.Fatal("expected partial TLS config (key without cert) to fail closed")
	}

	// 3. ValidateHTTPOptions allows non-loopback with TLS OR explicit AllowPlaintextHTTP.
	if err := ValidateHTTPOptions("0.0.0.0:18089", HTTPOptions{
		AuthToken:   "secret",
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
	}); err != nil {
		t.Fatalf("expected non-loopback TLS with AuthToken to be allowed, got %v", err)
	}
	if err := ValidateHTTPOptions("0.0.0.0:18089", HTTPOptions{
		AuthToken:          "secret",
		AllowPlaintextHTTP: true,
	}); err != nil {
		t.Fatalf("expected non-loopback AllowPlaintextHTTP with AuthToken to be allowed, got %v", err)
	}

	// 4. Slowloris POST body deadline & >1 MiB body limit using in-memory net.Pipe listener.
	postDeadline := 60 * time.Millisecond
	ln := newPipeListener("127.0.0.1:8089")
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		opts := HTTPOptions{AuthToken: "secret-token", EnforceLoopbackHost: true}
		errCh <- serveHTTPListener(ctx, ln, newHTTPHandlerWithTimeouts(srv, opts, postDeadline), nil, 2*time.Second)
	}()

	// Slowloris POST, DELETE, and PUT stall mid-body and must all return 408 Request Timeout.
	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		conn, err := ln.Dial()
		if err != nil {
			t.Fatalf("dial slowloris %s: %v", method, err)
		}
		_, _ = conn.Write([]byte(method + " /mcp HTTP/1.1\r\nHost: " + addr + "\r\nAuthorization: Bearer secret-token\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{\"jsonrpc\":"))
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		statusLine, err := bufio.NewReader(conn).ReadString('\n')
		_ = conn.Close()
		if err != nil || !strings.Contains(statusLine, "408") {
			t.Fatalf("expected 408 Request Timeout for stalled %s body, got status=%q err=%v", method, statusLine, err)
		}
	}

	// Oversized POST (>1 MiB) must return 413 Request Entity Too Large.
	pipeTransport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return ln.Dial()
		},
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: pipeTransport}
	bigReq, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/mcp", bytes.NewReader(make([]byte, maxMCPRequestBodyBytes+10)))
	bigReq.Header.Set("Authorization", "Bearer secret-token")
	bigReq.Header.Set("Content-Type", "application/json")
	bigResp, err := client.Do(bigReq)
	if err != nil {
		t.Fatalf("oversized POST failed: %v", err)
	}
	_ = bigResp.Body.Close()
	if bigResp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 Request Entity Too Large, got %d", bigResp.StatusCode)
	}

	// 5. Long-lived SSE GET survives past POST body deadline and closes cleanly via BaseContext when ctx cancels.
	initBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`
	initReq, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/mcp", strings.NewReader(initBody))
	initReq.Header.Set("Authorization", "Bearer secret-token")
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("Accept", "application/json, text/event-stream")
	initResp, err := client.Do(initReq)
	if err != nil {
		t.Fatalf("initialize POST failed: %v", err)
	}
	_ = initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on initialize, got %d", initResp.StatusCode)
	}
	sessionID := initResp.Header.Get("Mcp-Session-Id")

	sseReq, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/mcp", nil)
	sseReq.Header.Set("Authorization", "Bearer secret-token")
	sseReq.Header.Set("Accept", "text/event-stream")
	if sessionID != "" {
		sseReq.Header.Set("Mcp-Session-Id", sessionID)
	}
	sseClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return ln.Dial()
			},
		},
	}
	sseResp, err := sseClient.Do(sseReq)
	if err != nil {
		t.Fatalf("SSE GET failed: %v", err)
	}
	defer sseResp.Body.Close()
	if sseResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on SSE GET, got %d", sseResp.StatusCode)
	}

	sseDone := make(chan error, 1)
	go func() {
		_, readErr := io.Copy(io.Discard, sseResp.Body)
		sseDone <- readErr
	}()

	// Wait 3x the POST body deadline and confirm SSE GET was NOT killed prematurely.
	select {
	case err := <-sseDone:
		t.Fatalf("SSE GET closed prematurely while server active (err=%v)", err)
	case <-time.After(3 * postDeadline):
	}

	// Cancel server ctx; BaseContext must terminate SSE GET and Serve must return cleanly.
	cancel()
	select {
	case sseErr := <-sseDone:
		if sseErr != nil {
			t.Fatalf("expected SSE GET to close cleanly on ctx cancel, got err=%v", sseErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("sseDone did not complete after ctx cancel")
	}
	client.CloseIdleConnections()
	sseClient.CloseIdleConnections()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected clean server shutdown on ctx cancel, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server shutdown after ctx cancel")
	}

	// 6. Verify ServeTLS works end-to-end with self-signed certificate.
	tlsLn := newPipeListener("127.0.0.1:8443")
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load test keypair: %v", err)
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	tlsCtx, tlsCancel := context.WithCancel(context.Background())
	defer tlsCancel()
	tlsErrCh := make(chan error, 1)
	go func() {
		opts := HTTPOptions{AuthToken: "tls-secret", EnforceLoopbackHost: true, TLSCertFile: certPath, TLSKeyFile: keyPath}
		tlsErrCh <- serveHTTPListener(tlsCtx, tlsLn, newHTTPHandlerWithTimeouts(srv, opts, time.Second), tlsCfg, time.Second)
	}()

	caPool := x509.NewCertPool()
	certPEM, _ := os.ReadFile(certPath)
	caPool.AppendCertsFromPEM(certPEM)
	tlsClient := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return tlsLn.Dial()
			},
			TLSClientConfig: &tls.Config{RootCAs: caPool, ServerName: "localhost"},
		},
	}
	tlsReq, _ := http.NewRequest(http.MethodPost, "https://"+tlsLn.Addr().String()+"/mcp", strings.NewReader(initBody))
	tlsReq.Header.Set("Authorization", "Bearer tls-secret")
	tlsReq.Header.Set("Content-Type", "application/json")
	tlsReq.Header.Set("Accept", "application/json, text/event-stream")
	tlsResp, err := tlsClient.Do(tlsReq)
	if err != nil {
		t.Fatalf("HTTPS initialize failed: %v", err)
	}
	_ = tlsResp.Body.Close()
	if tlsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK over HTTPS, got %d", tlsResp.StatusCode)
	}
	tlsCancel()
	if err := <-tlsErrCh; err != nil {
		t.Fatalf("expected clean TLS server shutdown, got %v", err)
	}

	// 7. Verify Shutdown error triggers httpSrv.Close() to unblock hung connections.
	hungLn := newPipeListener("127.0.0.1:8090")
	inHandler := make(chan struct{})
	releaseHandler := make(chan struct{})
	t.Cleanup(func() { close(releaseHandler) })

	hungHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(inHandler)
		<-releaseHandler
		w.WriteHeader(http.StatusOK)
	})
	hungCtx, hungCancel := context.WithCancel(context.Background())
	hungErrCh := make(chan error, 1)
	go func() {
		hungErrCh <- serveHTTPListener(hungCtx, hungLn, hungHandler, nil, 30*time.Millisecond)
	}()

	hungClient := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return hungLn.Dial()
			},
		},
	}
	go func() {
		_, _ = hungClient.Get("http://" + hungLn.Addr().String() + "/")
	}()
	<-inHandler
	hungCancel()

	select {
	case err := <-hungErrCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected Shutdown timeout error after httpSrv.Close(), got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected httpSrv.Close() on Shutdown error to unblock server promptly")
	}
}
