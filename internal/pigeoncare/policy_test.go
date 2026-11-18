package pigeoncare

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type stubSafeDB struct{}

func (s *stubSafeDB) GetClusterStatus(context.Context, time.Duration) ([]db.NodeSummary, error) {
	return []db.NodeSummary{{NodeID: "rv64-compute-01", Arch: "riscv64", Status: "active", IsHealthy: true}}, nil
}
func (s *stubSafeDB) ResolveNodes(context.Context, core.RunnerSelector) ([]string, error) {
	return []string{"rv64-compute-01"}, nil
}
func (s *stubSafeDB) GetLineageDAG(context.Context, string, int) (*db.DAG, error) {
	return &db.DAG{}, nil
}
func (s *stubSafeDB) GetLedgerActivity(context.Context, int) ([]db.LedgerActivity, error) {
	return nil, nil
}
func (s *stubSafeDB) QueryReadOnlyMaps(_ context.Context, sql string, _ ...any) ([]map[string]any, error) {
	if _, err := db.ValidateSelectOnlySQL(sql); err != nil {
		return nil, err
	}
	return []map[string]any{{"component": "host-os", "status": "active", "metrics": map[string]any{"cpu_fan_rpm": 2450}}}, nil
}
func (s *stubSafeDB) ExplainReadOnly(_ context.Context, sql string) (string, error) {
	if _, err := db.ValidateSelectOnlySQL(sql); err != nil {
		return "", err
	}
	return `[{"Plan":{}}]`, nil
}
func (s *stubSafeDB) EnsureMathSchema(context.Context) error { return nil }
func (s *stubSafeDB) SavePMVDraft(_ context.Context, r db.PMVRecord) (*db.PMVRecord, error) {
	cp := r
	return &cp, nil
}
func (s *stubSafeDB) UpdatePMVState(context.Context, db.PMVRecord) error { return nil }
func (s *stubSafeDB) GetPMV(context.Context, string) (*db.PMVRecord, error) {
	return &db.PMVRecord{}, nil
}
func (s *stubSafeDB) ListPMVs(context.Context) ([]db.PMVRecord, error) { return nil, nil }
func (s *stubSafeDB) MaterializePMV(context.Context, string, string) error {
	return nil
}
func (s *stubSafeDB) DropPMV(context.Context, string) error { return nil }
func (s *stubSafeDB) Close()                                {}

func TestPolicyGateModesAndEphemeralRunnerConfirmation(t *testing.T) {
	var prompts atomic.Int32
	nextDecision := DecisionDeny

	var dispatched []core.RunnerRequestCTD
	svc := NewService(Options{
		Store:      &stubSafeDB{},
		Permission: config.PermissionConfig{Mode: "default"},
		Prompter: func(_ context.Context, _ ActionProposal) (ConfirmationDecision, error) {
			prompts.Add(1)
			return nextDecision, nil
		},
		Dispatcher: func(_ context.Context, req core.RunnerRequestCTD) error {
			dispatched = append(dispatched, req)
			return nil
		},
	})

	// 1. Mutating SQL against CTD ledger MUST ALWAYS fail even before any DB hit
	out := svc.Execute(context.Background(), "query_readonly_ledger", map[string]any{
		"sql": "DELETE FROM telemetric_ledger",
	})
	if !strings.Contains(out, "rejected") {
		t.Fatalf("expected mutating SQL to be rejected, got: %s", out)
	}

	// 2. Default mode + Deny blocks ephemeral runner
	args := map[string]any{
		"title":       "Check RISC-V thermals",
		"interpreter": "sh",
		"script_body": "uname -m\n",
		"nodes":       []string{"rv64-compute-01"},
	}
	out = svc.Execute(context.Background(), "dispatch_ephemeral_runner", args)
	if !strings.Contains(out, "blocked") || len(dispatched) != 0 {
		t.Fatalf("expected Deny to block ephemeral runner, got: %s", out)
	}

	// 3. Default mode + AlwaysAllow dispatches AND caches SHA-256 so second call doesn't re-prompt!
	nextDecision = DecisionAlwaysAllow
	out = svc.Execute(context.Background(), "dispatch_ephemeral_runner", args)
	if !strings.Contains(out, "Dispatched ephemeral runner") || len(dispatched) != 1 {
		t.Fatalf("expected AlwaysAllow to dispatch runner, got: %s", out)
	}
	beforeSecond := prompts.Load()
	out = svc.Execute(context.Background(), "dispatch_ephemeral_runner", args)
	if prompts.Load() != beforeSecond || len(dispatched) != 2 {
		t.Fatalf("expected AlwaysAllow script hash to bypass second prompt (prompts=%d->%d)", beforeSecond, prompts.Load())
	}

	// 4. YOLO mode dispatches a brand new script with zero prompts!
	svc.PolicyGate().SetMode(ModeYOLO)
	nextDecision = DecisionDeny // would fail if prompter were called!
	yoloArgs := map[string]any{
		"interpreter": "python3",
		"script_body": "print('riscv64 check')\n",
		"tags":        []string{"compute-bench"},
	}
	out = svc.Execute(context.Background(), "dispatch_ephemeral_runner", yoloArgs)
	if !strings.Contains(out, "Dispatched ephemeral runner") || len(dispatched) != 3 {
		t.Fatalf("expected YOLO mode to dispatch without prompting, got: %s", out)
	}
}
