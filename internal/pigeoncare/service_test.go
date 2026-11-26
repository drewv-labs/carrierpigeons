package pigeoncare

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
)

type serviceMockSafeDB struct {
	stubSafeDB
	getClusterStatusFn  func(context.Context, time.Duration) ([]db.NodeSummary, error)
	queryReadOnlyMapsFn func(context.Context, string, ...any) ([]map[string]any, error)
}

func (m *serviceMockSafeDB) GetClusterStatus(ctx context.Context, d time.Duration) ([]db.NodeSummary, error) {
	if m.getClusterStatusFn != nil {
		return m.getClusterStatusFn(ctx, d)
	}
	return m.stubSafeDB.GetClusterStatus(ctx, d)
}

func (m *serviceMockSafeDB) QueryReadOnlyMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	if m.queryReadOnlyMapsFn != nil {
		return m.queryReadOnlyMapsFn(ctx, sql, args...)
	}
	return m.stubSafeDB.QueryReadOnlyMaps(ctx, sql, args...)
}

func TestServiceToolOutputSanitizesNonFiniteFloats(t *testing.T) {
	nanPtr := new(float64)
	*nanPtr = math.NaN()

	sharedMetrics := map[string]any{
		"temp_c":       math.NaN(),
		"pwm_ratio":    math.Inf(1),
		"delta":        math.Inf(-1),
		"float32_inf":  float32(math.Inf(1)),
		"ptr_nan":      nanPtr,
		"series":       []float64{10.5, math.NaN(), math.Inf(-1)},
		"nested":       map[string]any{"bad": math.NaN(), "good": 42.5},
		"json_num_nan": json.Number("NaN"),
	}

	mockDB := &serviceMockSafeDB{
		getClusterStatusFn: func(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			return []db.NodeSummary{{
				NodeID:        "rv64-compute-01",
				Arch:          "riscv64",
				Status:        "active",
				LastSeen:      time.Now().UTC(),
				IsHealthy:     true,
				LatestMetrics: sharedMetrics,
			}}, nil
		},
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return []map[string]any{{
				"component":       "host-os",
				"status":          "active",
				"event_timestamp": time.Now().UTC(),
				"metrics":         sharedMetrics,
			}}, nil
		},
	}

	svc := NewService(Options{Store: mockDB})

	statusOut := svc.Execute(context.Background(), "get_cluster_status", nil)
	if !json.Valid([]byte(statusOut)) {
		t.Fatalf("get_cluster_status produced invalid JSON: %s", statusOut)
	}
	if !strings.Contains(statusOut, "null") || !strings.Contains(statusOut, "42.5") {
		t.Fatalf("expected sanitized null and preserved 42.5 in get_cluster_status: %s", statusOut)
	}

	queryOut := svc.Execute(context.Background(), "query_readonly_ledger", map[string]any{"sql": "SELECT 1"})
	if !json.Valid([]byte(queryOut)) {
		t.Fatalf("query_readonly_ledger produced invalid JSON: %s", queryOut)
	}
	if !strings.Contains(queryOut, "null") || !strings.Contains(queryOut, "42.5") {
		t.Fatalf("expected sanitized null and preserved 42.5 in query_readonly_ledger: %s", queryOut)
	}

	telOut := svc.Execute(context.Background(), "get_node_telemetry", map[string]any{"node_id": "rv64-compute-01"})
	if !strings.Contains(telOut, `"temp_c":null`) || !strings.Contains(telOut, `"good":42.5`) {
		t.Fatalf("expected sanitized JSON metrics in get_node_telemetry: %s", telOut)
	}

	benchOut := svc.Execute(context.Background(), "analyze_latest_benchmark", map[string]any{"node_id": "rv64-compute-01"})
	if !strings.Contains(benchOut, `"delta":null`) || !strings.Contains(benchOut, `"good":42.5`) {
		t.Fatalf("expected sanitized JSON metrics in analyze_latest_benchmark: %s", benchOut)
	}

	// Verify caller-owned map was NOT mutated in place.
	if !math.IsNaN(sharedMetrics["temp_c"].(float64)) {
		t.Fatal("sanitizeJSONValue mutated sharedMetrics in place")
	}

	// Verify typed structs with float64, []float64, [2]float64, map[string]float64, *[]float64,
	// embedded structs, and json tags sanitize NaN/Inf to JSON null without dropping fields or mutating input.
	type embeddedMeta struct {
		AmbientC float64 `json:"ambient_c"`
	}
	type typedStructMetrics struct {
		embeddedMeta
		TempC      float64            `json:"temp_c"`
		Series     []float64          `json:"series"`
		Pair       [2]float64         `json:"pair"`
		PerCore    map[string]float64 `json:"per_core"`
		PtrSeries  *[]float64         `json:"ptr_series"`
		Hidden     float64            `json:"-"`
		OmitZero   float64            `json:"omit_zero,omitempty"`
		OmitNaN    float64            `json:"omit_nan,omitempty"`
	}
	ptrSlice := []float64{2.25, math.Inf(-1)}
	structMetrics := typedStructMetrics{
		embeddedMeta: embeddedMeta{AmbientC: math.Inf(1)},
		TempC:        math.NaN(),
		Series:       []float64{1.5, math.NaN()},
		Pair:         [2]float64{9.5, math.Inf(1)},
		PerCore:      map[string]float64{"cpu0": math.NaN(), "cpu1": 48.0},
		PtrSeries:    &ptrSlice,
		Hidden:       math.NaN(),
		OmitZero:     0,
		OmitNaN:      math.NaN(),
	}
	structDB := &serviceMockSafeDB{
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return []map[string]any{{
				"component":       "host-os",
				"status":          "active",
				"event_timestamp": time.Now().UTC(),
				"metrics":         structMetrics,
			}}, nil
		},
	}
	structSvc := NewService(Options{Store: structDB})
	structQueryOut := structSvc.Execute(context.Background(), "query_readonly_ledger", map[string]any{"sql": "SELECT 1"})
	if !json.Valid([]byte(structQueryOut)) {
		t.Fatalf("query_readonly_ledger produced invalid JSON for typed struct: %s", structQueryOut)
	}
	var decoded []map[string]any
	if err := json.Unmarshal([]byte(structQueryOut), &decoded); err != nil || len(decoded) != 1 {
		t.Fatalf("unmarshal typed struct output failed: %v (%s)", err, structQueryOut)
	}
	m, ok := decoded[0]["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("expected metrics map, got %T", decoded[0]["metrics"])
	}
	if _, hasHidden := m["Hidden"]; hasHidden {
		t.Fatalf("expected json:\"-\" field omitted, got %v", m)
	}
	if _, hasOmitZero := m["omit_zero"]; hasOmitZero {
		t.Fatalf("expected zero omitempty field omitted, got %v", m)
	}
	for _, nullKey := range []string{"ambient_c", "temp_c", "omit_nan"} {
		if val, exists := m[nullKey]; !exists || val != nil {
			t.Fatalf("expected %q=null, got exists=%v val=%#v", nullKey, exists, val)
		}
	}
	if s, ok := m["series"].([]any); !ok || len(s) != 2 || s[0] != 1.5 || s[1] != nil {
		t.Fatalf("expected series [1.5, null], got %#v", m["series"])
	}
	if p, ok := m["pair"].([]any); !ok || len(p) != 2 || p[0] != 9.5 || p[1] != nil {
		t.Fatalf("expected pair [9.5, null], got %#v", m["pair"])
	}
	if ps, ok := m["ptr_series"].([]any); !ok || len(ps) != 2 || ps[0] != 2.25 || ps[1] != nil {
		t.Fatalf("expected ptr_series [2.25, null], got %#v", m["ptr_series"])
	}
	if pc, ok := m["per_core"].(map[string]any); !ok || pc["cpu0"] != nil || pc["cpu1"] != 48.0 {
		t.Fatalf("expected per_core {cpu0:null, cpu1:48}, got %#v", m["per_core"])
	}
	if !math.IsNaN(structMetrics.Series[1]) || !math.IsNaN(structMetrics.PerCore["cpu0"]) {
		t.Fatal("sanitizeJSONValue mutated typed struct fields in place")
	}
}

func TestServiceToolOutputHandlesMarshalErrors(t *testing.T) {
	mockDB := &serviceMockSafeDB{
		getClusterStatusFn: func(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			return []db.NodeSummary{{
				NodeID:        "edge-01",
				LatestMetrics: map[string]any{"bad": func() {}},
			}}, nil
		},
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return []map[string]any{{
				"component":       "host-os",
				"status":          "active",
				"event_timestamp": time.Now().UTC(),
				"metrics":         func() {},
				"bad":             func() {},
			}}, nil
		},
	}

	svc := NewService(Options{Store: mockDB})

	statusOut := svc.Execute(context.Background(), "get_cluster_status", nil)
	if !strings.Contains(statusOut, "Error: failed to serialize cluster status") {
		t.Fatalf("expected explicit marshal error from get_cluster_status, got: %s", statusOut)
	}

	queryOut := svc.Execute(context.Background(), "query_readonly_ledger", map[string]any{"sql": "SELECT 1"})
	if !strings.Contains(queryOut, "Error: failed to serialize ledger query results") {
		t.Fatalf("expected explicit marshal error from query_readonly_ledger, got: %s", queryOut)
	}

	telOut := svc.Execute(context.Background(), "get_node_telemetry", map[string]any{"node_id": "edge-01"})
	if !strings.Contains(telOut, `{"error":"unserializable metrics"}`) {
		t.Fatalf("expected safe unserializable metrics marker in get_node_telemetry, got: %s", telOut)
	}
}

func TestServiceToolOutputCaps256KiBValidUTF8(t *testing.T) {
	largePayload := strings.Repeat("sensor-α-🚀-", 2000)

	mockDB := &serviceMockSafeDB{
		getClusterStatusFn: func(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			out := make([]db.NodeSummary, 25)
			for i := range out {
				out[i] = db.NodeSummary{
					NodeID:        "node-01",
					LatestMetrics: map[string]any{"blob": largePayload},
				}
			}
			return out, nil
		},
		queryReadOnlyMapsFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			out := make([]map[string]any, 25)
			for i := range out {
				out[i] = map[string]any{
					"component":       "host-os",
					"status":          "active",
					"event_timestamp": time.Now().UTC(),
					"metrics":         map[string]any{"blob": largePayload},
				}
			}
			return out, nil
		},
	}

	svc := NewService(Options{Store: mockDB})
	for _, tc := range []struct {
		name string
		args map[string]any
	}{
		{name: "get_cluster_status"},
		{name: "query_readonly_ledger", args: map[string]any{"sql": "SELECT 1"}},
		{name: "get_node_telemetry", args: map[string]any{"node_id": "edge-01"}},
		{name: "analyze_latest_benchmark", args: map[string]any{"node_id": "edge-01"}},
	} {
		out := svc.Execute(context.Background(), tc.name, tc.args)
		if len(out) > maxToolOutputBytes {
			t.Fatalf("%s exceeded %d bytes: got %d", tc.name, maxToolOutputBytes, len(out))
		}
		if !strings.HasSuffix(out, toolTruncationMark) {
			t.Fatalf("%s missing truncation marker; len=%d", tc.name, len(out))
		}
		if !utf8.ValidString(out) {
			t.Fatalf("%s truncated output is not valid UTF-8", tc.name)
		}
	}
}

func TestServiceNilStoreAndConcurrentExecuteRace(t *testing.T) {
	nilSvc := NewService(Options{})
	if out := nilSvc.Execute(context.Background(), "get_cluster_status", nil); !strings.Contains(out, "not connected") {
		t.Fatalf("expected disconnected store error, got %q", out)
	}

	shared := map[string]any{"temp": math.NaN(), "rpm": 1800}
	mockDB := &serviceMockSafeDB{
		getClusterStatusFn: func(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
			return []db.NodeSummary{{NodeID: "edge-01", LatestMetrics: shared}}, nil
		},
	}
	svc := NewService(Options{Store: mockDB})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				out := svc.Execute(context.Background(), "get_cluster_status", nil)
				if !json.Valid([]byte(out)) {
					t.Errorf("concurrent Execute produced invalid JSON: %s", out)
				}
			}
		}()
	}
	wg.Wait()
}
