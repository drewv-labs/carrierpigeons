package pigeonmath

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type fakeAgentSafeStore struct {
	pmvs            map[string]db.PMVRecord
	materialized    map[string]string
	failExplain     string
	failProbe       bool
	failMaterialize bool
	probeQueries    []string
	sampleRows      []map[string]any
	saveCalls       int
	updateCalls     int
}

func newFakeAgentSafeStore() *fakeAgentSafeStore {
	return &fakeAgentSafeStore{
		pmvs:         make(map[string]db.PMVRecord),
		materialized: make(map[string]string),
	}
}

func (f *fakeAgentSafeStore) GetMeshIdentity(context.Context) (*core.MeshIdentity, error) {
	return &core.MeshIdentity{MeshName: core.DefaultMeshName, CoopNodeID: "rv64-coop-01"}, nil
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
	f.probeQueries = append(f.probeQueries, sql)
	if _, err := db.ValidateSelectOnlySQL(sql); err != nil {
		return nil, err
	}
	if f.failProbe {
		return nil, errors.New("probe execution failed")
	}
	if f.sampleRows != nil {
		return f.sampleRows, nil
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
	f.saveCalls++
	norm, err := db.NormalizePMVName(rec.ViewName)
	if err != nil {
		return nil, err
	}
	rec.ViewName = norm
	if existing, ok := f.pmvs[norm]; ok {
		rec.Version = existing.Version + 1
		rec.CreatedAt = existing.CreatedAt
	} else {
		rec.Version = 1
		rec.CreatedAt = time.Now().UTC()
	}
	rec.UpdatedAt = time.Now().UTC()
	f.pmvs[norm] = rec
	cp := rec
	return &cp, nil
}
func (f *fakeAgentSafeStore) UpdatePMVState(_ context.Context, rec db.PMVRecord) error {
	f.updateCalls++
	existing := f.pmvs[rec.ViewName]
	rec.RawIntentSQL = existing.RawIntentSQL // Raw intent is immutable once cached in draft!
	rec.Version = existing.Version
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
	if f.failMaterialize {
		return errors.New("materialize DDL failed")
	}
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
	verifyErr     error
	calls         int
}

func (s *stubOptimizerLLM) Chat(_ context.Context, _ string, _ []llm.Message) (string, error) {
	s.calls++
	if s.calls == 1 {
		return s.optimizeReply, nil
	}
	if s.verifyErr != nil {
		return "", s.verifyErr
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

func TestSubmitAndCertifyPMVFailsClosedOnCertificationError(t *testing.T) {
	store := newFakeAgentSafeStore()
	eng := NewEngine(store)

	rawSQL := "SELECT node_id FROM ctd_nodes"
	errLLM := &stubOptimizerLLM{
		optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes\n```",
		verifyErr:     errors.New("upstream LLM timeout"),
	}

	rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "err_view", rawSQL, errLLM)
	if err == nil {
		t.Fatal("expected certification error when post-check LLM fails")
	}
	if rec == nil || rec.Status != "failed" {
		t.Fatalf("expected returned draft status=failed, got %+v", rec)
	}
	if persisted, ok := store.pmvs["pmv_err_view"]; !ok || persisted.Status != "failed" {
		t.Fatalf("expected persisted draft status=failed, got %+v (exists=%v)", persisted, ok)
	}
	if _, exists := store.materialized["pmv_err_view"]; exists {
		t.Fatal("view MUST NOT be materialized when certification check errors")
	}
}

func TestSubmitAndCertifyPMVFailsClosedOnEmptyVerdict(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply string
	}{
		{name: "empty", reply: ""},
		{name: "whitespace", reply: "   \n\t  "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeAgentSafeStore()
			eng := NewEngine(store)

			rawSQL := "SELECT node_id FROM ctd_nodes"
			emptyLLM := &stubOptimizerLLM{
				optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes\n```",
				verifyReply:   tc.reply,
			}

			rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "empty_verdict_view", rawSQL, emptyLLM)
			if err == nil {
				t.Fatal("expected error when certification verdict is empty/whitespace")
			}
			if rec == nil || rec.Status != "failed" {
				t.Fatalf("expected returned draft status=failed, got %+v", rec)
			}
			if persisted, ok := store.pmvs["pmv_empty_verdict_view"]; !ok || persisted.Status != "failed" {
				t.Fatalf("expected persisted draft status=failed, got %+v (exists=%v)", persisted, ok)
			}
			if _, exists := store.materialized["pmv_empty_verdict_view"]; exists {
				t.Fatal("view MUST NOT be materialized when certification verdict is empty")
			}
		})
	}
}

func TestSQLOptimizerSystemPromptIndexes(t *testing.T) {
	if strings.Contains(sqlOptimizerSystemPrompt, "jsonb_path_ops") {
		t.Fatal("sqlOptimizerSystemPrompt must not claim dropped GIN(metrics jsonb_path_ops) index")
	}
	requiredTokens := []string{
		"idx_ledger_node_ts",
		"idx_ledger_node_comp_ts",
		"idx_ledger_runner_ts",
		"idx_ledger_pause_node_ts",
		"idx_ledger_ts_brin",
		"idx_ledger_ingested_brin",
		"BRIN on event_timestamp",
		"BRIN on ingested_at",
	}
	for _, tok := range requiredTokens {
		if !strings.Contains(sqlOptimizerSystemPrompt, tok) {
			t.Fatalf("sqlOptimizerSystemPrompt missing expected index token %q", tok)
		}
	}
}

func TestSubmitAndCertifyPMVFailsClosedOnNonCertifiedVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply string
	}{
		{name: "rejected_prefix", reply: "REJECTED: query alters grouping."},
		{name: "ambiguous_conversational", reply: "Looks good to me! The candidate SQL preserves the intent."},
		{name: "preamble_before_certified", reply: "Sure, here is my assessment: CERTIFIED: matches raw intent."},
		{name: "markdown_backticks", reply: "`CERTIFIED: matches raw intent.`"},
		{name: "empty", reply: ""},
		{name: "whitespace", reply: "   \n\t  "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeAgentSafeStore()
			eng := NewEngine(store)

			rawSQL := "SELECT node_id FROM ctd_nodes"
			verdictLLM := &stubOptimizerLLM{
				optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes\n```",
				verifyReply:   tc.reply,
			}

			rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "fail_closed_view", rawSQL, verdictLLM)
			if err == nil {
				t.Fatalf("expected fail-closed error for verdict %q", tc.reply)
			}
			if rec == nil || rec.Status != "failed" {
				t.Fatalf("expected returned draft status=failed, got %+v", rec)
			}
			if persisted, ok := store.pmvs["pmv_fail_closed_view"]; !ok || persisted.Status != "failed" {
				t.Fatalf("expected persisted draft status=failed, got %+v (exists=%v)", persisted, ok)
			}
			if _, exists := store.materialized["pmv_fail_closed_view"]; exists {
				t.Fatalf("view MUST NOT be materialized for non-CERTIFIED verdict %q", tc.reply)
			}
		})
	}

	t.Run("certified_case_insensitive_accepted", func(t *testing.T) {
		store := newFakeAgentSafeStore()
		eng := NewEngine(store)

		rawSQL := "SELECT node_id FROM ctd_nodes"
		okLLM := &stubOptimizerLLM{
			optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes\n```",
			verifyReply:   "  certified: Preserves node_id projection.  ",
		}

		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "accepted_view", rawSQL, okLLM)
		if err != nil {
			t.Fatalf("expected CERTIFIED verdict to succeed, got error: %v", err)
		}
		if rec == nil || rec.Status != "certified" {
			t.Fatalf("expected status=certified, got %+v", rec)
		}
		if _, exists := store.materialized["pmv_accepted_view"]; !exists {
			t.Fatal("expected certified view to be materialized")
		}
	})
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

type capturingVerifierLLM struct {
	optimizeReply string
	verifyReply   string
	verifyPrompts []string
	calls         int
}

func (c *capturingVerifierLLM) Chat(_ context.Context, _ string, hist []llm.Message) (string, error) {
	c.calls++
	if c.calls == 1 {
		return c.optimizeReply, nil
	}
	if len(hist) > 0 {
		c.verifyPrompts = append(c.verifyPrompts, hist[len(hist)-1].Content)
	}
	return c.verifyReply, nil
}

func (c *capturingVerifierLLM) StreamChat(ctx context.Context, sys string, hist []llm.Message) (<-chan string, error) {
	reply, err := c.Chat(ctx, sys, hist)
	if err != nil {
		return nil, err
	}
	ch := make(chan string, 1)
	ch <- reply
	close(ch)
	return ch, nil
}

func TestSubmitAndCertifyPMVTrailingCommentProbeAndUTF8Truncation(t *testing.T) {
	t.Run("trailing SQL line comment wraps cleanly with newline before closing paren", func(t *testing.T) {
		store := newFakeAgentSafeStore()
		eng := NewEngine(store)

		rawSQL := "SELECT node_id FROM ctd_nodes -- trailing comment"
		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "trailing_comment_view", rawSQL, nil)
		if err != nil {
			t.Fatalf("expected trailing comment query to certify cleanly, got %v", err)
		}
		if rec.Status != "certified" {
			t.Fatalf("expected certified status, got %q", rec.Status)
		}
		if len(store.probeQueries) != 1 {
			t.Fatalf("expected 1 probe query, got %d", len(store.probeQueries))
		}
		wantProbe := "SELECT * FROM (SELECT node_id FROM ctd_nodes -- trailing comment\n) AS _pmv_cert_probe LIMIT 5"
		if store.probeQueries[0] != wantProbe {
			t.Fatalf("unexpected probeQuery:\n got: %q\nwant: %q", store.probeQueries[0], wantProbe)
		}
	})

	t.Run("sampleJSON truncates cleanly on multi-byte UTF-8 rune boundaries", func(t *testing.T) {
		for offset := range 4 {
			store := newFakeAgentSafeStore()
			store.sampleRows = []map[string]any{
				{
					"pad":  strings.Repeat("a", offset),
					"note": strings.Repeat("🔭", 1100), // 4-byte UTF-8 runes crossing byte 4096
				},
			}
			eng := NewEngine(store)
			opt := &capturingVerifierLLM{
				optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes\n```",
				verifyReply:   "CERTIFIED: UTF-8 boundary check passed.",
			}
			rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "utf8_sample_view", "SELECT node_id FROM ctd_nodes", opt)
			if err != nil {
				t.Fatalf("offset=%d: SubmitAndCertifyPMV failed: %v", offset, err)
			}
			if rec.Status != "certified" || len(opt.verifyPrompts) != 1 {
				t.Fatalf("offset=%d: expected 1 verify prompt, got %d", offset, len(opt.verifyPrompts))
			}
			prompt := opt.verifyPrompts[0]
			startIdx := strings.Index(prompt, "<untrusted_sample_rows_")
			if startIdx < 0 {
				t.Fatalf("offset=%d: missing sample rows delimiter in prompt", offset)
			}
			openEnd := strings.Index(prompt[startIdx:], ">\n")
			closeStart := strings.LastIndex(prompt, "\n</untrusted_sample_rows_")
			if openEnd < 0 || closeStart <= startIdx+openEnd+2 {
				t.Fatalf("offset=%d: malformed sample rows enclosure", offset)
			}
			samplePayload := prompt[startIdx+openEnd+2 : closeStart]
			if len(samplePayload) > 4096 {
				t.Fatalf("offset=%d: expected sample payload <= 4096 bytes, got %d", offset, len(samplePayload))
			}
			if !utf8.ValidString(samplePayload) {
				t.Fatalf("offset=%d: sample payload split mid-rune (invalid UTF-8)", offset)
			}
		}

		// Direct unit check on 2-byte, 3-byte, and 4-byte runes across exact boundary.
		for _, glyph := range []string{"°", "▁", "🔭"} {
			buf := []byte(strings.Repeat(glyph, 10))
			for limit := 1; limit <= len(buf); limit++ {
				trimmed := truncateUTF8Bytes(buf, limit)
				if len(trimmed) > limit {
					t.Fatalf("glyph=%s limit=%d: truncated len %d > limit", glyph, limit, len(trimmed))
				}
				if !utf8.Valid(trimmed) {
					t.Fatalf("glyph=%s limit=%d: invalid UTF-8 result", glyph, limit)
				}
			}
		}
	})
}

func TestSubmitAndCertifyPMVPreservesExistingCertifiedRowBeforeValidationPasses(t *testing.T) {
	setupCertified := func(t *testing.T) (*fakeAgentSafeStore, *Engine, string) {
		t.Helper()
		store := newFakeAgentSafeStore()
		eng := NewEngine(store)
		origSQL := "SELECT node_id FROM ctd_nodes"
		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "live_health", origSQL, nil)
		if err != nil || rec.Status != "certified" || rec.Version != 1 {
			t.Fatalf("initial certification failed: rec=%+v err=%v", rec, err)
		}
		return store, eng, origSQL
	}

	assertCertifiedUntouched := func(t *testing.T, store *fakeAgentSafeStore, origSQL string, savesBefore, updatesBefore int) {
		t.Helper()
		if store.saveCalls != savesBefore || store.updateCalls != updatesBefore {
			t.Fatalf("existing certified pmv_catalog row was mutated (saves %d->%d, updates %d->%d)",
				savesBefore, store.saveCalls, updatesBefore, store.updateCalls)
		}
		persisted, ok := store.pmvs["pmv_live_health"]
		if !ok || persisted.Status != "certified" || persisted.RawIntentSQL != origSQL || persisted.Version != 1 {
			t.Fatalf("expected existing certified catalog row preserved, got %+v (ok=%v)", persisted, ok)
		}
		if store.materialized["pmv_live_health"] != origSQL {
			t.Fatalf("expected live materialized SQL preserved, got %q", store.materialized["pmv_live_health"])
		}
	}

	t.Run("ExplainReadOnly failure leaves certified catalog row untouched", func(t *testing.T) {
		store, eng, origSQL := setupCertified(t)
		store.failExplain = "broken_col"
		savesBefore, updatesBefore := store.saveCalls, store.updateCalls

		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "live_health", "SELECT broken_col FROM ctd_nodes", nil)
		if err == nil || rec == nil || rec.Status != "failed" {
			t.Fatalf("expected failed result, got rec=%+v err=%v", rec, err)
		}
		assertCertifiedUntouched(t, store, origSQL, savesBefore, updatesBefore)
	})

	t.Run("sample probe failure leaves certified catalog row untouched", func(t *testing.T) {
		store, eng, origSQL := setupCertified(t)
		store.failProbe = true
		savesBefore, updatesBefore := store.saveCalls, store.updateCalls

		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "live_health", "SELECT node_id, status FROM ctd_nodes", nil)
		if err == nil || rec == nil || rec.Status != "failed" {
			t.Fatalf("expected probe failure, got rec=%+v err=%v", rec, err)
		}
		assertCertifiedUntouched(t, store, origSQL, savesBefore, updatesBefore)
	})

	t.Run("LLM post-check rejection leaves certified catalog row untouched", func(t *testing.T) {
		store, eng, origSQL := setupCertified(t)
		savesBefore, updatesBefore := store.saveCalls, store.updateCalls
		rejectLLM := &stubOptimizerLLM{
			optimizeReply: "```sql\nSELECT node_id FROM ctd_nodes LIMIT 1\n```",
			verifyReply:   "REJECTED: drops rows.",
		}

		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "live_health", "SELECT node_id, status FROM ctd_nodes", rejectLLM)
		if err == nil || rec == nil || rec.Status != "failed" {
			t.Fatalf("expected rejection failure, got rec=%+v err=%v", rec, err)
		}
		assertCertifiedUntouched(t, store, origSQL, savesBefore, updatesBefore)
	})

	t.Run("MaterializePMV failure leaves certified catalog row untouched", func(t *testing.T) {
		store, eng, origSQL := setupCertified(t)
		store.failMaterialize = true
		savesBefore, updatesBefore := store.saveCalls, store.updateCalls

		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "live_health", "SELECT node_id, status FROM ctd_nodes", nil)
		if err == nil || rec == nil || rec.Status != "failed" {
			t.Fatalf("expected materialize failure, got rec=%+v err=%v", rec, err)
		}
		assertCertifiedUntouched(t, store, origSQL, savesBefore, updatesBefore)
	})

	t.Run("valid re-submission upgrades certified view and increments version", func(t *testing.T) {
		store, eng, _ := setupCertified(t)
		newSQL := "SELECT node_id, status FROM ctd_nodes"
		rec, err := eng.SubmitAndCertifyPMV(context.Background(), "operator", "live_health", newSQL, nil)
		if err != nil {
			t.Fatalf("valid re-submission failed: %v", err)
		}
		if rec.Status != "certified" || rec.Version != 2 || rec.RawIntentSQL != newSQL {
			t.Fatalf("unexpected upgraded certified record: %+v", rec)
		}
		if store.materialized["pmv_live_health"] != newSQL {
			t.Fatalf("expected updated materialized SQL, got %q", store.materialized["pmv_live_health"])
		}
	})
}
