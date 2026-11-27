package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func TestOpenStoreFailsClosedOnEmptyDSN(t *testing.T) {
	cfg := config.DefaultWorkstationConfig()
	cfg.ActiveMesh = "staging-mesh"
	cfg.DatabaseURL = ""
	cfg.ReadOnlyDatabaseURL = ""
	cfg.Brand = brand.Spec{EnvPrefix: "ACME", CLIName: "acmesh"}.MergeWithDefaults()

	_, err := openOperatorStoreForConfig(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected openOperatorStoreForConfig with empty DatabaseURL to fail closed")
	}
	if !strings.Contains(err.Error(), "staging-mesh") || !strings.Contains(err.Error(), "database_url") || !strings.Contains(err.Error(), "ACME_DB_URL") {
		t.Fatalf("expected error to name mesh and setting/env var, got: %v", err)
	}

	_, err = openReadOnlyStoreForConfig(context.Background(), cfg, "care")
	if err == nil {
		t.Fatal("expected openReadOnlyStoreForConfig with empty ReadOnlyDatabaseURL to fail closed")
	}
	if !strings.Contains(err.Error(), "staging-mesh") || !strings.Contains(err.Error(), "readonly_database_url") || !strings.Contains(err.Error(), "ACME_RO_DB_URL") {
		t.Fatalf("expected error to name mesh and readonly setting/env var, got: %v", err)
	}
}

func TestDialMQTTClientRedactsInvalidURLCredentials(t *testing.T) {
	cfg := config.DefaultWorkstationConfig()
	badURL := "tcp://mqttuser:supersecret%ZZpass@127.0.0.1:1883"

	_, err := dialMQTTClientWithConfig(context.Background(), cfg, badURL, "test-redact-invalid", time.Second)
	if err == nil {
		t.Fatal("expected invalid broker URL to return parse error")
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("invalid broker URL error leaked credential: %v", err)
	}

	cfg.TLSInsecureSkipVerify = true
	_, err = dialMQTTClientWithConfig(context.Background(), cfg, badURL, "test-redact-invalid-tls", time.Second)
	if err == nil {
		t.Fatal("expected invalid TLS broker URL to return parse error")
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("invalid TLS broker URL error leaked credential: %v", err)
	}
}

func TestConnectValidatesTopicRootAndStateBaseBrandAndMeshListFlagError(t *testing.T) {
	// 1. Remote topic_root mismatch against expected brand TopicRoot must fail closed.
	cfg := config.DefaultWorkstationConfig()
	cfg.BrokerURL = "tcp://127.0.0.1:1883"
	cfg.Brand.TopicRoot = "expected-root"
	now := time.Now().UTC()

	_, err := verifyRemoteMeshIdentityWithProbers(
		context.Background(), cfg, "prod-mesh", false, false,
		func(context.Context, config.WorkstationConfig) (*core.MeshIdentity, error) {
			return &core.MeshIdentity{
				MeshName:   "prod-mesh",
				CoopNodeID: "rv64-coop-01",
				TopicRoot:  "wrong-root",
				UpdatedAt:  now,
			}, nil
		},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "topic_root") || !strings.Contains(err.Error(), "expected-root") {
		t.Fatalf("expected topic_root mismatch to fail closed, got: %v", err)
	}

	// 2. State paths use state.baseBrand (not post-TOML [brand] overrides), and `mesh list --mesh <missing>` fails closed.
	dir := t.TempDir()
	baseMeshFile := filepath.Join(dir, "pigeons_current_mesh")
	tomlBrandMeshFile := filepath.Join(dir, "custom_current_mesh")
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", baseMeshFile)
	t.Setenv("CUSTOM_CURRENT_MESH_FILE", tomlBrandMeshFile)

	wsPath := filepath.Join(dir, "pigeons.toml")
	tomlContent := `operator = "operator"

[brand]
product_name = "CustomProd"
cli_name = "customcli"
env_prefix = "CUSTOM"

[meshes.alpha-mesh]
broker_url = "tcp://127.0.0.1:1883"

[meshes.beta-mesh]
broker_url = "tcp://127.0.0.1:1884"
`
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	if _, err := executeCLIForTest("--config", wsPath, "connect", "alpha-mesh", "--no-verify"); err != nil {
		t.Fatalf("connect alpha-mesh failed: %v", err)
	}
	if _, err := os.Stat(tomlBrandMeshFile); !os.IsNotExist(err) {
		t.Fatalf("expected connect to write under baseBrand state path, not post-TOML brand path %s", tomlBrandMeshFile)
	}
	if got, err := config.ReadCurrentMeshForBrand(brand.Default()); err != nil || got != "alpha-mesh" {
		t.Fatalf("ReadCurrentMeshForBrand(default) = %q (err=%v), want alpha-mesh", got, err)
	}

	if _, err := executeCLIForTest("--config", wsPath, "connect", "beta-mesh", "--no-verify"); err != nil {
		t.Fatalf("connect beta-mesh failed: %v", err)
	}
	if _, err := executeCLIForTest("--config", wsPath, "connect", "-", "--no-verify"); err != nil {
		t.Fatalf("connect - failed: %v", err)
	}
	if got, _ := config.ReadCurrentMeshForBrand(brand.Default()); got != "alpha-mesh" {
		t.Fatalf("expected connect - to toggle back to alpha-mesh via baseBrand state, got %q", got)
	}

	// Explicit `--mesh nonexistent` on `mesh list` must return error directly instead of falling back to state.
	if _, err := executeCLIForTest("--config", wsPath, "--mesh", "nonexistent-mesh", "mesh", "list"); err == nil || !strings.Contains(err.Error(), "nonexistent-mesh") {
		t.Fatalf("expected `mesh list --mesh nonexistent-mesh` to fail closed, got: %v", err)
	}

	// `disconnect` clears baseBrand state cleanly.
	if _, err := executeCLIForTest("--config", wsPath, "disconnect"); err != nil {
		t.Fatalf("disconnect failed: %v", err)
	}
	if got, _ := config.ReadCurrentMeshForBrand(brand.Default()); got != "" {
		t.Fatalf("expected disconnect to clear baseBrand current_mesh, got %q", got)
	}
}

func TestParseRunnerKeyValueParamsAndDurationValidation(t *testing.T) {
	params, err := parseRunnerKeyValueParams([]string{
		"one=1",
		"zero=0",
		"flag_true=true",
		"flag_false=false",
		"short_t=t",
		"short_f=f",
		"ratio=0.75",
		"label=rv64-node",
	})
	if err != nil {
		t.Fatalf("parseRunnerKeyValueParams valid inputs failed: %v", err)
	}
	if v, ok := params["one"].(int); !ok || v != 1 {
		t.Fatalf("expected one=1 as int, got %T (%v)", params["one"], params["one"])
	}
	if v, ok := params["zero"].(int); !ok || v != 0 {
		t.Fatalf("expected zero=0 as int, got %T (%v)", params["zero"], params["zero"])
	}
	if v, ok := params["flag_true"].(bool); !ok || !v {
		t.Fatalf("expected flag_true=true as bool, got %T (%v)", params["flag_true"], params["flag_true"])
	}
	if v, ok := params["flag_false"].(bool); !ok || v {
		t.Fatalf("expected flag_false=false as bool, got %T (%v)", params["flag_false"], params["flag_false"])
	}
	if v, ok := params["short_t"].(string); !ok || v != "t" {
		t.Fatalf("expected short_t=t as string (not bool), got %T (%v)", params["short_t"], params["short_t"])
	}
	if v, ok := params["short_f"].(string); !ok || v != "f" {
		t.Fatalf("expected short_f=f as string (not bool), got %T (%v)", params["short_f"], params["short_f"])
	}
	if v, ok := params["ratio"].(float64); !ok || v != 0.75 {
		t.Fatalf("expected ratio=0.75 as float64, got %T (%v)", params["ratio"], params["ratio"])
	}

	for _, bad := range []string{"missing_equals", "=emptykey", "   =emptykey", "x=NaN", "x=Inf", "x=-Inf", "x=+Inf", "x=Infinity"} {
		if _, err := parseRunnerKeyValueParams([]string{bad}); err == nil {
			t.Fatalf("expected malformed/non-finite --param %q to return error", bad)
		}
	}

	if _, err := buildRunnerRequest("PauseRnr", "operator", true, nil, nil, nil, -5); err == nil {
		t.Fatal("expected negative --secs to fail buildRunnerRequest")
	}
	if _, err := buildRunnerRequest("SyntheticComputeRnr", "operator", true, nil, nil, []string{"badparam"}, 10); err == nil {
		t.Fatal("expected malformed --param to fail buildRunnerRequest")
	}

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	if err := os.WriteFile(wsPath, []byte("operator = \"operator\"\n"), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}
	for _, secsArg := range []string{"0", "-10"} {
		if _, err := executeCLIForTest("--config", wsPath, "runner", "benchmark", "rv64-node-01", "--secs", secsArg); err == nil || !strings.Contains(err.Error(), "--secs") {
			t.Fatalf("expected runner benchmark --secs %s to fail validation, got: %v", secsArg, err)
		}
	}
}

func TestRunnerAbortTargetRequestIDAndCareModeValidation(t *testing.T) {
	reqDefault, err := buildAbortRunnerRequestWithTarget("SyntheticComputeRnr", "operator", "", true, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildAbortRunnerRequestWithTarget(empty) failed: %v", err)
	}
	if reqDefault.TargetRequestID != "" {
		t.Fatalf("expected empty TargetRequestID by default, got %q", reqDefault.TargetRequestID)
	}

	reqTargeted, err := buildAbortRunnerRequestWithTarget("SyntheticComputeRnr", "operator", " req-abc-123 ", false, []string{"rv64-node-01"}, nil, nil)
	if err != nil {
		t.Fatalf("buildAbortRunnerRequestWithTarget(targeted) failed: %v", err)
	}
	if reqTargeted.TargetRequestID != "req-abc-123" {
		t.Fatalf("TargetRequestID = %q, want req-abc-123", reqTargeted.TargetRequestID)
	}

	// End-to-end CLI execution of `runner abort --request-id` and `care --mode invalid`.
	brokerURL := startTestMeshBroker(t, "abort-mesh", "rv64-coop-abort")
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	if err := os.WriteFile(wsPath, []byte(fmt.Sprintf("operator = \"operator\"\nmesh_name = \"abort-mesh\"\nbroker_url = %q\n", brokerURL)), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	out, err := executeCLIForTest("--config", wsPath, "runner", "abort", "SyntheticComputeRnr", "--all", "--request-id", "req-target-99")
	if err != nil {
		t.Fatalf("runner abort --request-id failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "target_request_id=req-target-99") {
		t.Fatalf("expected abort output to include target_request_id, got: %s", out)
	}

	if _, err := executeCLIForTest("--config", wsPath, "care", "--mode", "bogus-mode"); err == nil || !strings.Contains(err.Error(), "invalid permission mode") {
		t.Fatalf("expected unrecognized care --mode to fail closed, got: %v", err)
	}
}

type stubLedgerPruner struct {
	batchDeletes []int64
	callIdx      int
	cutoffs      []time.Time
	batchLimits  []int
	errAtCall    int
}

func (s *stubLedgerPruner) PruneLedgerBefore(_ context.Context, cutoff time.Time, batchLimit int) (int64, error) {
	idx := s.callIdx
	s.callIdx++
	s.cutoffs = append(s.cutoffs, cutoff)
	s.batchLimits = append(s.batchLimits, batchLimit)
	if s.errAtCall > 0 && s.callIdx == s.errAtCall {
		return 0, errors.New("db prune error")
	}
	if idx >= len(s.batchDeletes) {
		return 0, nil
	}
	return s.batchDeletes[idx], nil
}

func TestCoopPruneDurationParsingAndBatchedExecution(t *testing.T) {
	d720h, err := parseRetentionDuration("720h")
	if err != nil || d720h != 720*time.Hour {
		t.Fatalf("parseRetentionDuration(720h) = (%v, %v), want 720h", d720h, err)
	}
	d30d, err := parseRetentionDuration("30d")
	if err != nil || d30d != 30*24*time.Hour {
		t.Fatalf("parseRetentionDuration(30d) = (%v, %v), want 720h", d30d, err)
	}
	for _, bad := range []string{"", "0s", "-24h", "0d", "-5d", "abc", "99999d"} {
		if _, err := parseRetentionDuration(bad); err == nil {
			t.Fatalf("expected invalid retention duration %q to fail", bad)
		}
	}

	pruner := &stubLedgerPruner{
		batchDeletes: []int64{100, 100, 35},
	}
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	var out bytes.Buffer
	total, err := runCoopPruneWithStore(context.Background(), &out, pruner, now, 24*time.Hour, 100)
	if err != nil {
		t.Fatalf("runCoopPruneWithStore failed: %v", err)
	}
	if total != 235 || pruner.callIdx != 3 {
		t.Fatalf("expected total=235 across 3 batches, got total=%d calls=%d", total, pruner.callIdx)
	}
	wantCutoff := now.Add(-24 * time.Hour)
	for i, c := range pruner.cutoffs {
		if !c.Equal(wantCutoff) {
			t.Fatalf("batch %d cutoff=%v, want %v", i, c, wantCutoff)
		}
	}
	if !strings.Contains(out.String(), "Pruned 235 telemetric_ledger row(s)") {
		t.Fatalf("unexpected prune output: %s", out.String())
	}

	// CLI flag validation + empty DSN fail-closed check (broker-only mesh profile clears DatabaseURL).
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	pruneTOML := "operator = \"operator\"\n\n[meshes.prune-mesh]\nbroker_url = \"tcp://127.0.0.1:1883\"\n"
	if err := os.WriteFile(wsPath, []byte(pruneTOML), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}
	if _, err := executeCLIForTest("--config", wsPath, "--mesh", "prune-mesh", "coop", "prune"); err == nil || !strings.Contains(err.Error(), "--older-than") {
		t.Fatalf("expected missing --older-than error, got: %v", err)
	}
	if _, err := executeCLIForTest("--config", wsPath, "--mesh", "prune-mesh", "coop", "prune", "--older-than", "24h", "--batch-size", "-1"); err == nil || !strings.Contains(err.Error(), "--batch-size") {
		t.Fatalf("expected negative --batch-size error, got: %v", err)
	}
	if _, err := executeCLIForTest("--config", wsPath, "--mesh", "prune-mesh", "coop", "prune", "--older-than", "24h"); err == nil || !strings.Contains(err.Error(), "database_url") {
		t.Fatalf("expected empty database_url to fail closed in coop prune, got: %v", err)
	}
}
