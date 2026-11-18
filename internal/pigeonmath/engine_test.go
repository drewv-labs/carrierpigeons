package pigeonmath

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type fakeAgentSafeStore struct {
	pmvs         map[string]db.PMVRecord
	materialized map[string]string
	failExplain  string
}

func newFakeAgentSafeStore() *fakeAgentSafeStore {
	return &fakeAgentSafeStore{
		pmvs:         make(map[string]db.PMVRecord),
		materialized: make(map[string]string),
	}
}

func (f *fakeAgentSafeStore) GetClusterStatus(context.Context, time.Duration) ([]db.NodeSummary, error) {
	return []db.NodeSummary{{NodeID: "rv64-coop-01", Status: "active", IsHealthy: true}}, nil
}
func (f *fakeAgentSafeStore) ResolveNodes(context.Context, core.RunnerSelector) ([]string, error) {
	return []string{"rv64-coop-01"}, nil
}
func (f *fakeAgentSafeStore) GetLineageDAG(context.Context, string, int) (*db.DAG, error) {
	return &db.DAG{}, nil
}
func (f *fakeAgentSafeStore) GetLedgerActivity(context.Context, int) ([]db.LedgerActivity, error) {
	return nil, nil
}
func (f *fakeAgentSafeStore) QueryReadOnlyMaps(_ context.Context, sql string, _ ...any) ([]map[string]any, error) {
	if _, err := db.ValidateSelectOnlySQL(sql); err != nil {
		return nil, err
	}
	return []map[string]any{{"node_id": "rv64-coop-01", "avg_temp_c": 46.2}}, nil
}
func (f *fakeAgentSafeStore) ExplainReadOnly(_ context.Context, selectSQL string) (string, error) {
	if _, err := db.ValidateSelectOnlySQL(selectSQL); err != nil {
		return "", err
	}
	if f.failExplain != "" && strings.Contains(selectSQL, f.failExplain) {
		return "", errors.New("column does_not_exist does not exist")
	}
	return `[{"Plan":{"Node Type":"Seq Scan"}}]`, nil
}
func (f *fakeAgentSafeStore) EnsureMathSchema(context.Context) error { return nil }
func (f *fakeAgentSafeStore) SavePMVDraft(_ context.Context, rec db.PMVRecord) (*db.PMVRecord, error) {
	norm, err := db.NormalizePMVName(rec.ViewName)
	if err != nil {
		return nil, err
	}
	rec.ViewName = norm
	rec.Version = 1
	rec.CreatedAt = time.Now().UTC()
	rec.UpdatedAt = rec.CreatedAt
	f.pmvs[norm] = rec
	cp := rec
	return &cp, nil
}
func (f *fakeAgentSafeStore) UpdatePMVState(_ context.Context, rec db.PMVRecord) error {
	existing := f.pmvs[rec.ViewName]
	rec.RawIntentSQL = existing.RawIntentSQL // Raw intent is immutable once cached in draft!
	f.pmvs[rec.ViewName] = rec
	return nil
}
func (f *fakeAgentSafeStore) GetPMV(_ context.Context, viewName string) (*db.PMVRecord, error) {
	norm, _ := db.NormalizePMVName(viewName)
	rec, ok := f.pmvs[norm]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := rec
	return &cp, nil
}
func (f *fakeAgentSafeStore) ListPMVs(context.Context) ([]db.PMVRecord, error) {
	out := make([]db.PMVRecord, 0, len(f.pmvs))
	for _, r := range f.pmvs {
		out = append(out, r)
	}
	return out, nil
}
func (f *fakeAgentSafeStore) MaterializePMV(_ context.Context, viewName, selectSQL string) error {
	norm, err := db.NormalizePMVName(viewName)
	if err != nil {
		return err
	}
	f.materialized[norm] = selectSQL
	return nil
}
func (f *fakeAgentSafeStore) DropPMV(_ context.Context, viewName string) error {
	norm, err := db.NormalizePMVName(viewName)
	if err != nil {
		return err
	}
	delete(f.materialized, norm)
	return nil
}
func (f *fakeAgentSafeStore) Close() {}

type stubOptimizerLLM struct {
	optimizeReply string
	verifyReply   string
	calls         int
}

func (s *stubOptimizerLLM) Chat(_ context.Context, _ string, _ []llm.Message) (string, error) {
	s.calls++
	if s.calls == 1 {
		return s.optimizeReply, nil
	}
	return s.verifyReply, nil
}

func (s *stubOptimizerLLM) StreamChat(ctx context.Context, sys string, hist []llm.Message) (<-chan string, error) {
	reply, err := s.Chat(ctx, sys, hist)
	if err != nil {
		return nil, err
	}
	ch := make(chan string, 1)
	ch <- reply
	close(ch)
	return ch, nil
}

func TestSubmitAndCertifyPMVCachesRawIntentAndOptimizes(t *testing.T) {
	store := newFakeAgentSafeStore()
	eng := NewEngine(store)

	rawSQL := "SELECT node_id, metrics->>'cpu_temp_c' FROM telemetric_ledger"
	optLLM := &stubOptimizerLLM{
		optimizeReply: "```sql\nSELECT node_id, (metrics->>'cpu_temp_c')::numeric AS cpu_temp_c FROM telemetric_ledger WHERE pillar = 'observer'\n```\nAdded pillar predicate.",
		verifyReply:   "CERTIFIED: Preserves node_id and thermal projection.",
	}

	rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "thermal_summary", rawSQL, optLLM)
	if err != nil {
		t.Fatalf("SubmitAndCertifyPMV failed: %v", err)
	}
	if rec.ViewName != "pmv_thermal_summary" {
		t.Fatalf("expected pmv_thermal_summary, got %q", rec.ViewName)
	}
	if rec.Status != "certified" {
		t.Fatalf("expected status=certified, got %q", rec.Status)
	}
	if rec.RawIntentSQL != rawSQL {
		t.Fatalf("expected cached raw_intent_sql %q to remain intact, got %q", rawSQL, rec.RawIntentSQL)
	}
	if !strings.Contains(rec.OptimizedSQL, "pillar = 'observer'") {
		t.Fatalf("expected optimized SQL to be materialized, got %q", rec.OptimizedSQL)
	}
	if store.materialized["pmv_thermal_summary"] != rec.OptimizedSQL {
		t.Fatalf("unexpected materialized SQL: %q", store.materialized["pmv_thermal_summary"])
	}
}

func TestSubmitAndCertifyPMVFallsBackToRawIntentWhenOptimizerBreaksPlanner(t *testing.T) {
	store := newFakeAgentSafeStore()
	store.failExplain = "does_not_exist"
	eng := NewEngine(store)

	rawSQL := "SELECT node_id FROM ctd_nodes"
	badLLM := &stubOptimizerLLM{
		optimizeReply: "```sql\nSELECT does_not_exist FROM ctd_nodes\n```",
		verifyReply:   "CERTIFIED: Fallback matches raw intent.",
	}

	rec, err := eng.SubmitAndCertifyPMV(context.Background(), "pigeoncare", "node_inventory", rawSQL, badLLM)
	if err != nil {
		t.Fatalf("expected graceful fallback to raw SQL, got error: %v", err)
	}
	if rec.Status != "certified" || store.materialized["pmv_node_inventory"] != rawSQL {
		t.Fatalf("expected rawSQL fallback to materialize & certify, got %+v", rec)
	}
}

func TestSubmitAndCertifyPMVDoesNotMaterializeOnRejection(t *testing.T) {
	store := newFakeAgentSafeStore()
	eng := NewEngine(store)

	rawSQL := "SELECT node_id FROM ctd_nodes"
	rejectLLM := &stubOptimizerLLM{
		optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes LIMIT 1\n```",
		verifyReply:   "REJECTED: Truncates inventory with unintended LIMIT 1.",
	}

	rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "broken_view", rawSQL, rejectLLM)
	if err == nil {
		t.Fatal("expected certification rejection error")
	}
	if rec == nil || rec.Status != "failed" {
		t.Fatalf("expected draft status=failed, got %+v", rec)
	}
	if _, exists := store.materialized["pmv_broken_view"]; exists {
		t.Fatal("rejected view MUST NOT be materialized in pigeonmath schema")
	}
}

func TestSparklineAndMultiLineGraphRender(t *testing.T) {
	series := []float64{0, 2, 5, 10, 8, 4, 12, 16}
	spark := Sparkline(series, 8)
	if len([]rune(spark)) != 8 {
		t.Fatalf("expected 8 rune sparkline, got %q", spark)
	}
	graph := RenderMultiLineGraph(series, 32, 4, "/m")
	if strings.Count(graph, "\n") != 3 {
		t.Fatalf("expected 4-line plot, got:\n%s", graph)
	}
}
