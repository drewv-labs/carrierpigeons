package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/pflag"
)

type noopBrokerLedger struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (n *noopBrokerLedger) InsertEvent(_ context.Context, ev *core.CTDPayload) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, ev)
	return nil
}

func (n *noopBrokerLedger) BatchInsertEvents(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, payloads...)
	return int64(len(payloads)), nil
}

func (n *noopBrokerLedger) UpsertNodeHello(_ context.Context, h core.NodeHello) ([]string, error) {
	return core.NormalizeTags(h.BootTags), nil
}

func (n *noopBrokerLedger) UpdateNodePresence(_ context.Context, _, _ string) error {
	return nil
}

func (n *noopBrokerLedger) ResolveNodes(_ context.Context, _ core.RunnerSelector) ([]string, error) {
	return nil, nil
}

func (n *noopBrokerLedger) UpsertArtifact(_ context.Context, _ core.Artifact) error {
	return nil
}

func (n *noopBrokerLedger) InsertTrace(_ context.Context, _ core.TraceEdge) error {
	return nil
}

func startTestMeshBroker(t *testing.T, meshName, coopNodeID string) string {
	t.Helper()
	emb, err := broker.StartEmbedded(broker.EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded(%s) failed: %v", meshName, err)
	}
	t.Cleanup(emb.Close)

	sub, err := broker.NewSubscriberWithOptions(broker.SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    meshName,
		CoopNodeID:  coopNodeID,
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     32,
			BatchSize:     4,
			FlushInterval: 25 * time.Millisecond,
		},
	}, &noopBrokerLedger{})
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions(%s) failed: %v", meshName, err)
	}
	t.Cleanup(sub.Disconnect)
	return emb.LocalClientURL()
}

func resetConnectFlags() {
	cfgFilePath = ""
	meshFlag = ""
	connectBrokerURL = ""
	connectDBURL = ""
	connectRODBURL = ""
	connectMQTTUser = ""
	connectMQTTPass = ""
	connectTLSCA = ""
	connectTLSCert = ""
	connectTLSKey = ""
	connectTLSInsecure = false
	connectTopicRoot = ""
	connectSkipVerify = false

	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		f.Changed = false
	})
	connectCmd.Flags().VisitAll(func(f *pflag.Flag) {
		f.Changed = false
	})
}

func executeCLIForTest(args ...string) (string, error) {
	resetConnectFlags()
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

func TestBuildRunnerRequestChainedNodesAndBraceTags(t *testing.T) {
	req, err := buildRunnerRequest(
		"SyntheticComputeRnr",
		"operator",
		false,
		[]string{"rv64-compute-01", "rv64-vision-02"},
		[]string{"{compute-bench}", "DataBroker,riscv64"},
		[]string{"target_system=true", "threads=8", "threshold=0.95"},
		45,
	)
	if err != nil {
		t.Fatalf("buildRunnerRequest failed: %v", err)
	}

	wantNodes := []string{"rv64-compute-01", "rv64-vision-02"}
	if !reflect.DeepEqual(req.Selectors.Nodes, wantNodes) {
		t.Fatalf("Nodes = %v, want %v", req.Selectors.Nodes, wantNodes)
	}

	wantTags := []string{"compute-bench", "databroker", "riscv64"}
	if !reflect.DeepEqual(req.Selectors.Tags, wantTags) {
		t.Fatalf("Tags = %v, want %v", req.Selectors.Tags, wantTags)
	}

	if req.Params["duration_sec"] != 45 || req.Params["target_system"] != true || req.Params["threads"] != 8 {
		t.Fatalf("unexpected parsed params: %+v", req.Params)
	}

	if _, err := buildRunnerRequest("SyntheticComputeRnr", "operator", false, nil, nil, nil, 0); err == nil {
		t.Fatal("expected missing selectors to return validation error")
	}
}

func TestBuildRunnerRequestPauseRnrDefaultsSecsZero(t *testing.T) {
	req, err := buildRunnerRequest("PauseRnr", "operator", true, nil, nil, nil, 0)
	if err != nil {
		t.Fatalf("buildRunnerRequest(PauseRnr) failed: %v", err)
	}
	dur, exists := req.Params["duration_sec"]
	if !exists || dur != 0 {
		t.Fatalf("expected PauseRnr default duration_sec=0, got params=%+v", req.Params)
	}

	abortReq, err := buildAbortRunnerRequest("PauseRnr", "operator", true, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildAbortRunnerRequest(--all) failed: %v", err)
	}
	if abortReq.Action != "abort" || !abortReq.Selectors.All {
		t.Fatalf("unexpected abort request: %+v", abortReq)
	}
}

func TestConnectMultiMeshSwitcherAndMNSVerification(t *testing.T) {
	brokerCompute := startTestMeshBroker(t, "compute-mesh", "rv64-coop-compute")
	brokerSensor := startTestMeshBroker(t, "sensor-mesh", "rv64-coop-sensor")

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))

	wsPath := filepath.Join(dir, "pigeons.toml")
	tomlContent := fmt.Sprintf(`# Multi-mesh workstation test config
operator = "operator"

[meshes.compute-mesh]
broker_url = %q

[meshes.sensor-mesh]
broker_url = %q

[meshes.mismatched-mesh]
broker_url = %q
`, brokerCompute, brokerSensor, brokerCompute)
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	// 1. Connect to `compute-mesh` with live MNS verification over MQTT.
	out, err := executeCLIForTest("--config", wsPath, "connect", "compute-mesh")
	if err != nil {
		t.Fatalf("pigeons connect compute-mesh failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "verified via mqtt") || !strings.Contains(out, "rv64-coop-compute") {
		t.Fatalf("expected MQTT MNS verification output, got: %s", out)
	}
	curOut, err := executeCLIForTest("--config", wsPath, "mesh", "current")
	if err != nil || strings.TrimSpace(curOut) != "compute-mesh" {
		t.Fatalf("expected `pigeons mesh current` = compute-mesh, got %q (err=%v)", curOut, err)
	}

	// 2. Switch to exclusive `sensor-mesh`.
	out, err = executeCLIForTest("--config", wsPath, "connect", "sensor-mesh")
	if err != nil {
		t.Fatalf("pigeons connect sensor-mesh failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "rv64-coop-sensor") {
		t.Fatalf("expected coop=rv64-coop-sensor in output, got: %s", out)
	}
	curOut, err = executeCLIForTest("--config", wsPath, "mesh", "current")
	if err != nil || strings.TrimSpace(curOut) != "sensor-mesh" {
		t.Fatalf("expected `pigeons mesh current` = sensor-mesh, got %q (err=%v)", curOut, err)
	}

	// 3. Mismatched MNS name (`mismatched-mesh` pointing at `compute-mesh` broker) MUST fail closed
	//    and leave `sensor-mesh` as the active working mesh!
	_, err = executeCLIForTest("--config", wsPath, "connect", "mismatched-mesh")
	if err == nil || !strings.Contains(err.Error(), "MNS verification failed") {
		t.Fatalf("expected MNS mismatch to fail closed, got err=%v", err)
	}
	curAfterFail, _ := config.ReadCurrentMesh()
	if curAfterFail != "sensor-mesh" {
		t.Fatalf("expected failed connect to preserve active mesh 'sensor-mesh', got %q", curAfterFail)
	}

	// 4. `pigeons connect -` toggles back to `compute-mesh` (`cd -` semantics).
	out, err = executeCLIForTest("--config", wsPath, "connect", "-")
	if err != nil {
		t.Fatalf("pigeons connect - failed: %v\noutput: %s", err, out)
	}
	curAfterToggle, _ := config.ReadCurrentMesh()
	if curAfterToggle != "compute-mesh" {
		t.Fatalf("expected `pigeons connect -` to toggle back to compute-mesh, got %q", curAfterToggle)
	}

	// 5. Reject invalid non-kebab-case MNS names.
	if _, err := executeCLIForTest("--config", wsPath, "connect", "Invalid_Mesh"); err == nil {
		t.Fatal("expected non-kebab-case mesh name to be rejected")
	}

	// 6. Inline profile registration with `--broker` / `--db` + `--no-verify` offline switch.
	out, err = executeCLIForTest(
		"--config", wsPath,
		"connect", "offline-lab",
		"--broker", "tcp://192.0.2.99:1883",
		"--db", "postgres://pigeoncoop_admin:secret@192.0.2.99:5432/edge_ledger",
		"--no-verify",
	)
	if err != nil {
		t.Fatalf("offline profile registration failed: %v\noutput: %s", err, out)
	}
	listOut, err := executeCLIForTest("--config", wsPath, "mesh", "list")
	if err != nil {
		t.Fatalf("pigeons mesh list failed: %v", err)
	}
	if !strings.Contains(listOut, "offline-lab") ||
		!strings.Contains(listOut, "xxxxx@192.0.2.99:5432") ||
		strings.Contains(listOut, ":secret@") {
		t.Fatalf("expected `pigeons mesh list` to show registered offline-lab with redacted DSN, got:\n%s", listOut)
	}

	// 7. `pigeons disconnect` clears current working mesh.
	if _, err := executeCLIForTest("--config", wsPath, "disconnect"); err != nil {
		t.Fatalf("pigeons disconnect failed: %v", err)
	}
	curCleared, _ := config.ReadCurrentMesh()
	if curCleared != "" {
		t.Fatalf("expected empty current mesh after disconnect, got %q", curCleared)
	}
}
