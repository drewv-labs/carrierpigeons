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
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

type stubSafeDB struct{}

func (s *stubSafeDB) GetMeshIdentity(context.Context) (*core.MeshIdentity, error) {
	return &core.MeshIdentity{MeshName: core.DefaultMeshName, CoopNodeID: "rv64-coop-01"}, nil
}
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

func TestPolicyGateAlwaysAllowScopedByInterpreterAndSelectors(t *testing.T) {
	var prompts atomic.Int32
	nextDecision := DecisionAlwaysAllow

	gate := NewPolicyGate(config.PermissionConfig{Mode: "default"}, func(_ context.Context, _ ActionProposal) (ConfirmationDecision, error) {
		prompts.Add(1)
		return nextDecision, nil
	})

	script := "uname -a\n"
	hash := runner.ComputeScriptSHA256(script)

	base := ActionProposal{
		Kind:        ActionEphemeralRunner,
		Title:       "uname check",
		Runner:      "ephemeral",
		Interpreter: "sh",
		ScriptBody:  script,
		SHA256:      hash,
		Selectors:   core.RunnerSelector{Nodes: []string{"node-b", "node-a"}, Tags: []string{"Lab-B", "lab-a"}},
	}

	// 1. Initial authorization prompts once and records AlwaysAllow for (sh, hash, canonical selectors).
	if err := gate.Authorize(context.Background(), base); err != nil {
		t.Fatalf("initial Authorize failed: %v", err)
	}
	if prompts.Load() != 1 {
		t.Fatalf("expected 1 prompt, got %d", prompts.Load())
	}

	// 2. Equivalent selectors in different order / duplicates hit the cached approval without re-prompting.
	equiv := base
	equiv.Selectors = core.RunnerSelector{
		Nodes: []string{"node-a", "node-b", "node-a"},
		Tags:  []string{"lab-a", "lab-b"},
	}
	if err := gate.Authorize(context.Background(), equiv); err != nil {
		t.Fatalf("equivalent Authorize failed: %v", err)
	}
	if prompts.Load() != 1 {
		t.Fatalf("expected canonicalized selectors to bypass second prompt, got prompts=%d", prompts.Load())
	}

	// Subsequent variations MUST re-prompt (deny to verify prompter is invoked!).
	nextDecision = DecisionDeny

	// 3. Different interpreter (cross-interpreter reuse) must re-prompt.
	diffInterp := base
	diffInterp.Interpreter = "python3"
	if err := gate.Authorize(context.Background(), diffInterp); err == nil {
		t.Fatal("expected cross-interpreter AlwaysAllow reuse (sh -> python3) to re-prompt and deny")
	}

	// 4. Different node target must re-prompt.
	diffNode := base
	diffNode.Selectors = core.RunnerSelector{Nodes: []string{"node-c"}}
	if err := gate.Authorize(context.Background(), diffNode); err == nil {
		t.Fatal("expected different target node to re-prompt and deny")
	}

	// 5. Escalation to All: true must re-prompt.
	allNodes := base
	allNodes.Selectors = core.RunnerSelector{All: true}
	if err := gate.Authorize(context.Background(), allNodes); err == nil {
		t.Fatal("expected selector escalation to All=true to re-prompt and deny")
	}

	// 6. Different tags must re-prompt.
	diffTags := base
	diffTags.Selectors = core.RunnerSelector{Nodes: []string{"node-a", "node-b"}, Tags: []string{"prod"}}
	if err := gate.Authorize(context.Background(), diffTags); err == nil {
		t.Fatal("expected modified tags selector to re-prompt and deny")
	}
	if prompts.Load() != 5 {
		t.Fatalf("expected 4 additional prompts for changed interpreter/selectors, got total=%d", prompts.Load())
	}
}

func TestPolicyGateConfigAllowedScriptHashesInterpreterBinding(t *testing.T) {
	script := "echo config-pinned\n"
	bodyHash := runner.ComputeScriptSHA256(script)
	pyBoundHash := runner.ComputeBoundScriptSHA256("python3", script)

	gate := NewPolicyGate(config.PermissionConfig{
		Mode: "default",
		AllowedScriptHashes: []string{
			" /usr/bin/sh : " + strings.ToUpper(bodyHash),
			pyBoundHash,
			bodyHash, // legacy bare body hash must not authorize arbitrary interpreters
		},
	}, nil)

	// Matching prefixed sh:<sha256> succeeds without prompter.
	if err := gate.Authorize(context.Background(), ActionProposal{
		Kind:        ActionEphemeralRunner,
		Title:       "sh pinned",
		Interpreter: "sh",
		ScriptBody:  script,
		SHA256:      bodyHash,
		Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
	}); err != nil {
		t.Fatalf("expected sh:<sha256> to authorize without prompter, got %v", err)
	}

	// Matching bound python3 hash succeeds without prompter.
	if err := gate.Authorize(context.Background(), ActionProposal{
		Kind:        ActionEphemeralRunner,
		Title:       "py bound",
		Interpreter: "python3",
		ScriptBody:  script,
		SHA256:      bodyHash,
		Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
	}); err != nil {
		t.Fatalf("expected bound python3 hash to authorize without prompter, got %v", err)
	}

	// Unbound interpreter "bash" must be blocked even though bare bodyHash is present.
	if err := gate.Authorize(context.Background(), ActionProposal{
		Kind:        ActionEphemeralRunner,
		Title:       "bash cross-interp",
		Interpreter: "bash",
		ScriptBody:  script,
		SHA256:      bodyHash,
		Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
	}); err == nil {
		t.Fatal("expected bash cross-interpreter reuse of bare body hash to be blocked")
	}
}

func TestPolicyGateRejectsInvalidEphemeralProposals(t *testing.T) {
	var prompts atomic.Int32
	gate := NewPolicyGate(config.PermissionConfig{Mode: "default"}, func(_ context.Context, _ ActionProposal) (ConfirmationDecision, error) {
		prompts.Add(1)
		return DecisionAlwaysAllow, nil
	})

	invalid := []struct {
		name     string
		proposal ActionProposal
	}{
		{
			name: "invalid_interpreter_dotdot",
			proposal: ActionProposal{
				Kind:        ActionEphemeralRunner,
				Title:       "bad interp",
				Interpreter: "..",
				Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
			},
		},
		{
			name: "empty_script_and_empty_sha256",
			proposal: ActionProposal{
				Kind:        ActionEphemeralRunner,
				Title:       "empty body and hash",
				Interpreter: "sh",
				Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
			},
		},
		{
			name: "whitespace_script_only",
			proposal: ActionProposal{
				Kind:        ActionEphemeralRunner,
				Title:       "whitespace body",
				Interpreter: "sh",
				ScriptBody:  "   \n\t ",
				Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
			},
		},
		{
			name: "mismatched_sha256",
			proposal: ActionProposal{
				Kind:        ActionEphemeralRunner,
				Title:       "mismatch hash",
				Interpreter: "sh",
				ScriptBody:  "echo hi\n",
				SHA256:      strings.Repeat("0", 64),
				Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
			},
		},
		{
			name: "non_hex_sha256",
			proposal: ActionProposal{
				Kind:        ActionEphemeralRunner,
				Title:       "bad hex hash",
				Interpreter: "sh",
				SHA256:      "not-a-valid-sha256",
				Selectors:   core.RunnerSelector{Nodes: []string{"node-01"}},
			},
		},
	}

	for _, mode := range []PermissionMode{ModeDefault, ModeYOLO} {
		gate.SetMode(mode)
		for _, tc := range invalid {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				if err := gate.Authorize(context.Background(), tc.proposal); err == nil {
					t.Fatalf("expected %s mode to reject invalid proposal %+v", mode, tc.proposal)
				}
			})
		}
	}
	if prompts.Load() != 0 {
		t.Fatalf("expected invalid ephemeral proposals to fail validation before prompting, got prompts=%d", prompts.Load())
	}
}
