package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
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

func executeCLIForTestWithBrand(spec brand.Spec, args ...string) (string, error) {
	cmd := NewRootCommand(spec)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func executeCLIForTest(args ...string) (string, error) {
	return executeCLIForTestWithBrand(brand.Default(), args...)
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

func closedLocalTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func publishRetainedMeshPayload(t *testing.T, brokerURL, topic string, payload []byte) {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(fmt.Sprintf("test-pub-%d", time.Now().UnixNano())).
		SetConnectTimeout(3 * time.Second)
	client := mqtt.NewClient(opts)
	if tok := client.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("connect test publisher: %v", tok.Error())
	}
	defer client.Disconnect(100)

	tok := client.Publish(topic, 1, true, payload)
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("publish retained MNS payload to %s: %v", topic, tok.Error())
	}
}

func TestConnectPartialVerifyAndBrandPrefixedDBEnv(t *testing.T) {
	brokerCompute := startTestMeshBroker(t, "compute-mesh", "rv64-coop-compute")
	deadDBAddr := closedLocalTCPAddr(t)
	deadDBURL := fmt.Sprintf("postgres://operator:dbsecret@%s/edge_ledger", deadDBAddr)

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))

	wsPath := filepath.Join(dir, "pigeons.toml")
	tomlContent := fmt.Sprintf(`operator = "operator"

[meshes.compute-mesh]
broker_url = %q
database_url = %q
`, brokerCompute, deadDBURL)
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	// 1. By default, when probeDB is true and Postgres fails, connect MUST fail closed.
	_, err := executeCLIForTest("--config", wsPath, "connect", "compute-mesh")
	if err == nil || !strings.Contains(err.Error(), "postgres:") || !strings.Contains(err.Error(), "--allow-partial-verify") {
		t.Fatalf("expected partial verification failure citing postgres and --allow-partial-verify, got: %v", err)
	}
	if cur, _ := config.ReadCurrentMesh(); cur != "" {
		t.Fatalf("expected failed partial verify not to persist current_mesh, got %q", cur)
	}

	// 2. With --allow-partial-verify, MQTT succeeding alone suffices when no conflict exists.
	out, err := executeCLIForTest("--config", wsPath, "connect", "compute-mesh", "--allow-partial-verify")
	if err != nil {
		t.Fatalf("expected --allow-partial-verify to succeed when MQTT verifies, got: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "verified via mqtt") {
		t.Fatalf("expected mqtt verification summary, got: %s", out)
	}

	// 3. Even with --allow-partial-verify, an explicit mesh_name mismatch MUST still fail closed.
	wsMismatchPath := filepath.Join(dir, "mismatch.toml")
	mismatchTOML := fmt.Sprintf(`operator = "operator"

[meshes.wrong-mesh]
broker_url = %q
database_url = %q
`, brokerCompute, deadDBURL)
	if err := os.WriteFile(wsMismatchPath, []byte(mismatchTOML), 0o600); err != nil {
		t.Fatalf("write mismatch.toml: %v", err)
	}
	_, err = executeCLIForTest("--config", wsMismatchPath, "connect", "wrong-mesh", "--allow-partial-verify")
	if err == nil || !strings.Contains(err.Error(), "MNS verification failed") {
		t.Fatalf("expected mismatch to fail closed even with --allow-partial-verify, got: %v", err)
	}

	// 4. Brand-prefixed DB_URL / RO_DB_URL env vars trigger probeDB for broker-only profiles.
	customBrand := brand.Spec{
		ProductName: "AcmeTelemetry",
		CoopName:    "AcmeCoop",
		CLIName:     "acmesh",
		TopicRoot:   "pigeons",
		EnvPrefix:   "ACME",
	}
	brokerOnlyTOML := fmt.Sprintf(`operator = "operator"

[meshes.compute-mesh]
broker_url = %q
`, brokerCompute)
	brokerOnlyPath := filepath.Join(dir, "broker-only.toml")
	if err := os.WriteFile(brokerOnlyPath, []byte(brokerOnlyTOML), 0o600); err != nil {
		t.Fatalf("write broker-only.toml: %v", err)
	}

	t.Setenv("ACME_RO_DB_URL", deadDBURL)
	_, err = executeCLIForTestWithBrand(customBrand, "--config", brokerOnlyPath, "connect", "compute-mesh")
	if err == nil || !strings.Contains(err.Error(), "postgres:") {
		t.Fatalf("expected ACME_RO_DB_URL to trigger postgres probe and fail closed, got: %v", err)
	}

	// With --allow-partial-verify under the custom brand, MQTT verification succeeds.
	out, err = executeCLIForTestWithBrand(customBrand, "--config", brokerOnlyPath, "connect", "compute-mesh", "--allow-partial-verify")
	if err != nil {
		t.Fatalf("expected branded --allow-partial-verify to succeed, got: %v\noutput: %s", err, out)
	}
}

func TestConnectRedactsBrokerURLCredentials(t *testing.T) {
	brokerURL := startTestMeshBroker(t, "redacted-mesh", "rv64-coop-redact")
	credBrokerURL := strings.Replace(brokerURL, "tcp://", "tcp://mqttuser:supersecretpass@", 1)

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	if err := os.WriteFile(wsPath, []byte("operator = \"operator\"\n"), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	out, err := executeCLIForTest(
		"--config", wsPath,
		"connect", "redacted-mesh",
		"--broker", credBrokerURL,
	)
	if err != nil {
		t.Fatalf("connect with credentialed broker URL failed: %v\noutput: %s", err, out)
	}
	if strings.Contains(out, "supersecretpass") {
		t.Fatalf("connect output leaked broker password: %s", out)
	}
	if !strings.Contains(out, "mqttuser:xxxxx@") {
		t.Fatalf("expected redacted broker URL in connect output, got: %s", out)
	}

	listOut, err := executeCLIForTest("--config", wsPath, "mesh", "list")
	if err != nil {
		t.Fatalf("mesh list failed: %v", err)
	}
	if strings.Contains(listOut, "supersecretpass") {
		t.Fatalf("mesh list output leaked broker password: %s", listOut)
	}
	if !strings.Contains(listOut, "mqttuser:xxxxx@") {
		t.Fatalf("expected redacted broker URL in mesh list output, got: %s", listOut)
	}

	// Failed MQTT dial error must also redact broker URL credentials.
	deadBrokerURL := fmt.Sprintf("tcp://mqttuser:supersecretpass@%s", closedLocalTCPAddr(t))
	_, dialErr := executeCLIForTest(
		"--config", wsPath,
		"connect", "dead-redacted-mesh",
		"--broker", deadBrokerURL,
	)
	if dialErr == nil {
		t.Fatal("expected dial error for closed broker")
	}
	if strings.Contains(dialErr.Error(), "supersecretpass") {
		t.Fatalf("dial error leaked broker password: %v", dialErr)
	}
}

func TestValidateAndProbeRetainedMNSPayloads(t *testing.T) {
	now := time.Now().UTC()
	topic := core.MeshTopic()

	cases := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{
			name:    "empty_cleared_payload",
			payload: []byte("   "),
			wantErr: "empty",
		},
		{
			name:    "offline_lwt_case_insensitive",
			payload: []byte(`{"mesh_name":"compute-mesh","coop_node_id":"coop-1","status":"offline","updated_at":"` + now.Format(time.RFC3339Nano) + `"}`),
			wantErr: "OFFLINE",
		},
		{
			name:    "empty_mesh_name",
			payload: []byte(`{"mesh_name":"","coop_node_id":"coop-1","updated_at":"` + now.Format(time.RFC3339Nano) + `"}`),
			wantErr: "empty mesh_name",
		},
		{
			name:    "missing_updated_at",
			payload: []byte(`{"mesh_name":"compute-mesh","coop_node_id":"coop-1"}`),
			wantErr: "missing updated_at",
		},
		{
			name:    "stale_updated_at",
			payload: []byte(`{"mesh_name":"compute-mesh","coop_node_id":"coop-1","updated_at":"` + now.Add(-(defaultMNSRetainedMaxAge + time.Hour)).Format(time.RFC3339Nano) + `"}`),
			wantErr: "stale",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateRetainedMeshPayload(tc.payload, topic, "PigeonCoop", now)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateRetainedMeshPayload(%s) = %v, want substring %q", tc.name, err, tc.wantErr)
			}
		})
	}

	// End-to-end MQTT test with embedded broker rejecting OFFLINE LWT and stale retained MNS messages.
	emb, err := broker.StartEmbedded(broker.EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	t.Cleanup(emb.Close)

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	tomlContent := fmt.Sprintf(`operator = "operator"

[meshes.stale-mesh]
broker_url = %q
`, emb.LocalClientURL())
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	// Publish OFFLINE LWT payload -> connect MUST reject.
	offlineBytes, _ := json.Marshal(retainedMeshPayload{
		MeshName:   "stale-mesh",
		CoopNodeID: "rv64-coop-01",
		Status:     "OFFLINE",
		UpdatedAt:  time.Now().UTC(),
	})
	publishRetainedMeshPayload(t, emb.LocalClientURL(), topic, offlineBytes)
	if _, err := executeCLIForTest("--config", wsPath, "connect", "stale-mesh"); err == nil || !strings.Contains(err.Error(), "OFFLINE") {
		t.Fatalf("expected OFFLINE retained LWT to be rejected, got: %v", err)
	}

	// Publish stale MNS payload -> connect MUST reject.
	staleBytes, _ := json.Marshal(core.MeshIdentity{
		MeshName:   "stale-mesh",
		CoopNodeID: "rv64-coop-01",
		UpdatedAt:  time.Now().UTC().Add(-(defaultMNSRetainedMaxAge + 2*time.Hour)),
	})
	publishRetainedMeshPayload(t, emb.LocalClientURL(), topic, staleBytes)
	if _, err := executeCLIForTest("--config", wsPath, "connect", "stale-mesh"); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("expected stale retained MNS payload to be rejected, got: %v", err)
	}

	// Publish zero UpdatedAt payload -> connect MUST reject.
	zeroBytes := []byte(`{"mesh_name":"stale-mesh","coop_node_id":"rv64-coop-01"}`)
	publishRetainedMeshPayload(t, emb.LocalClientURL(), topic, zeroBytes)
	if _, err := executeCLIForTest("--config", wsPath, "connect", "stale-mesh"); err == nil || !strings.Contains(err.Error(), "missing updated_at") {
		t.Fatalf("expected missing updated_at retained MNS payload to be rejected, got: %v", err)
	}

	// Publish cleared empty retained payload -> connect MUST reject (either empty or no retained announcement).
	publishRetainedMeshPayload(t, emb.LocalClientURL(), topic, []byte{})
	if _, err := executeCLIForTest("--config", wsPath, "connect", "stale-mesh"); err == nil {
		t.Fatal("expected cleared retained MNS payload to be rejected")
	}

	// Publish fresh live payload -> connect MUST succeed.
	freshBytes, _ := json.Marshal(core.MeshIdentity{
		MeshName:   "stale-mesh",
		CoopNodeID: "rv64-coop-01",
		UpdatedAt:  time.Now().UTC(),
	})
	publishRetainedMeshPayload(t, emb.LocalClientURL(), topic, freshBytes)
	if out, err := executeCLIForTest("--config", wsPath, "connect", "stale-mesh"); err != nil {
		t.Fatalf("expected fresh retained MNS payload to succeed, got: %v\noutput: %s", err, out)
	}
}

func TestRunnerBenchmarkFailsFastOnGPUAndNPU(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	if err := os.WriteFile(wsPath, []byte("operator = \"operator\"\n"), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	helpOut, err := executeCLIForTest("--config", wsPath, "runner", "benchmark", "--help")
	if err != nil {
		t.Fatalf("runner benchmark --help failed: %v", err)
	}
	if !strings.Contains(helpOut, "CPU-only") || !strings.Contains(helpOut, "not yet implemented") {
		t.Fatalf("expected benchmark --help to document CPU-only scope, got:\n%s", helpOut)
	}

	for _, args := range [][]string{
		{"--config", wsPath, "runner", "benchmark", "rv64-node-01", "--gpu"},
		{"--config", wsPath, "runner", "benchmark", "rv64-node-01", "--npu"},
		{"--config", wsPath, "runner", "benchmark", "rv64-node-01", "--gpu", "--npu"},
	} {
		_, err := executeCLIForTest(args...)
		if err == nil || !strings.Contains(err.Error(), "CPU-only") {
			t.Fatalf("expected %v to fail fast client-side with CPU-only error, got: %v", args, err)
		}
	}
}

func TestNewRootCommandBrandIsolationAndConcurrency(t *testing.T) {
	initialActive := brand.Active()
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	tomlContent := `operator = "operator"

[meshes.alpha-mesh]
broker_url = "tcp://127.0.0.1:1883"

[meshes.beta-mesh]
broker_url = "tcp://127.0.0.1:1883"
`
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	specA := brand.Spec{
		ProductName: "AlphaTelemetry",
		CLIName:     "alphacli",
		TopicRoot:   "alpha",
		EnvPrefix:   "ALPHA",
	}
	specB := brand.Spec{
		ProductName: "BetaTelemetry",
		CLIName:     "betacli",
		TopicRoot:   "beta",
		EnvPrefix:   "BETA",
	}

	cmdA := NewRootCommand(specA)
	cmdB := NewRootCommand(specB)

	if cmdA == cmdB || cmdA == rootCmd || cmdB == rootCmd {
		t.Fatal("expected NewRootCommand to return isolated *cobra.Command trees")
	}
	if cmdA.Use != "alphacli" || !strings.Contains(cmdA.Short, "AlphaTelemetry") {
		t.Fatalf("unexpected cmdA metadata: use=%q short=%q", cmdA.Use, cmdA.Short)
	}
	if cmdB.Use != "betacli" || !strings.Contains(cmdB.Short, "BetaTelemetry") {
		t.Fatalf("unexpected cmdB metadata: use=%q short=%q", cmdB.Use, cmdB.Short)
	}
	if rootCmd.Use != "pigeons" {
		t.Fatalf("expected template rootCmd.Use to remain 'pigeons', got %q", rootCmd.Use)
	}

	// Mutating flags on cmdA must not affect cmdB or template rootCmd.
	if err := cmdA.PersistentFlags().Set("mesh", "alpha-mesh"); err != nil {
		t.Fatalf("set mesh flag on cmdA: %v", err)
	}
	if got, _ := cmdB.PersistentFlags().GetString("mesh"); got != "" {
		t.Fatalf("expected cmdB mesh flag to remain empty, got %q", got)
	}
	if meshFlag != "" {
		t.Fatalf("expected package-global meshFlag to remain empty, got %q", meshFlag)
	}

	// Concurrent construction + real command execution (PersistentPreRunE, mesh current, and
	// wrapped legacy `run` command) must not race or mutate global brand.Active().
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			spec := specA
			meshName := "alpha-mesh"
			if idx%2 == 1 {
				spec = specB
				meshName = "beta-mesh"
			}

			// 1. Real context-aware execution exercising PersistentPreRunE + mesh current.
			c1 := NewRootCommand(spec)
			var out1 bytes.Buffer
			c1.SetOut(&out1)
			c1.SetErr(&out1)
			c1.SetArgs([]string{"--config", wsPath, "--mesh", meshName, "mesh", "current"})
			if err := c1.Execute(); err != nil {
				t.Errorf("concurrent mesh current failed: %v\noutput: %s", err, out1.String())
			}
			if strings.TrimSpace(out1.String()) != meshName {
				t.Errorf("concurrent mesh current = %q, want %q", strings.TrimSpace(out1.String()), meshName)
			}

			// 2. Wrapped legacy subcommand execution exercising shared templateMu flag sync + RunE.
			c2 := NewRootCommand(spec)
			var out2 bytes.Buffer
			c2.SetOut(&out2)
			c2.SetErr(&out2)
			c2.SetArgs([]string{"--config", wsPath, "--mesh", meshName, "run", "SyntheticComputeRnr"})
			if err := c2.Execute(); err == nil {
				t.Errorf("expected wrapped legacy `run` without selectors to fail validation")
			}
		}(i)
	}
	wg.Wait()

	if brand.Active() != initialActive {
		t.Fatalf("global brand.Active() mutated: got %+v, want %+v", brand.Active(), initialActive)
	}
}

func TestBrandScopedMNSStateIsolation(t *testing.T) {
	initialActive := brand.Active()
	dir := t.TempDir()

	alphaMeshFile := filepath.Join(dir, "alpha", "current_mesh")
	betaMeshFile := filepath.Join(dir, "beta", "current_mesh")
	t.Setenv("ALPHA_CURRENT_MESH_FILE", alphaMeshFile)
	t.Setenv("BETA_CURRENT_MESH_FILE", betaMeshFile)

	alphaCfg := filepath.Join(dir, "alphacli.toml")
	betaCfg := filepath.Join(dir, "betacli.toml")
	if err := os.WriteFile(alphaCfg, []byte(`operator = "alpha-op"
[meshes.alpha-one]
broker_url = "tcp://127.0.0.1:1883"
[meshes.alpha-two]
broker_url = "tcp://127.0.0.1:1884"
`), 0o600); err != nil {
		t.Fatalf("write alpha config: %v", err)
	}
	if err := os.WriteFile(betaCfg, []byte(`operator = "beta-op"
[meshes.beta-one]
broker_url = "tcp://127.0.0.1:2883"
[meshes.beta-two]
broker_url = "tcp://127.0.0.1:2884"
`), 0o600); err != nil {
		t.Fatalf("write beta config: %v", err)
	}

	specA := brand.Spec{ProductName: "Alpha", CoopName: "AlphaCoop", CLIName: "alphacli", TopicRoot: "alpha", EnvPrefix: "ALPHA"}
	specB := brand.Spec{ProductName: "Beta", CoopName: "BetaCoop", CLIName: "betacli", TopicRoot: "beta", EnvPrefix: "BETA"}

	if _, err := executeCLIForTestWithBrand(specA, "--config", alphaCfg, "connect", "alpha-one", "--no-verify"); err != nil {
		t.Fatalf("alpha connect alpha-one: %v", err)
	}
	if _, err := executeCLIForTestWithBrand(specA, "--config", alphaCfg, "connect", "alpha-two", "--no-verify"); err != nil {
		t.Fatalf("alpha connect alpha-two: %v", err)
	}
	if _, err := executeCLIForTestWithBrand(specB, "--config", betaCfg, "connect", "beta-one", "--no-verify"); err != nil {
		t.Fatalf("beta connect beta-one: %v", err)
	}

	// Verify CurrentMeshStatePathForBrand + ReadCurrentMeshForBrand isolation.
	gotPathA, err := config.CurrentMeshStatePathForBrand(specA)
	if err != nil || gotPathA != alphaMeshFile {
		t.Fatalf("CurrentMeshStatePathForBrand(specA) = %q (err=%v), want %q", gotPathA, err, alphaMeshFile)
	}
	gotA, _ := config.ReadCurrentMeshForBrand(specA)
	gotB, _ := config.ReadCurrentMeshForBrand(specB)
	if gotA != "alpha-two" || gotB != "beta-one" {
		t.Fatalf("brand current meshes = (%q, %q), want (alpha-two, beta-one)", gotA, gotB)
	}

	// `connect -` on brand A must toggle to alpha-one without affecting brand B.
	if _, err := executeCLIForTestWithBrand(specA, "--config", alphaCfg, "connect", "-", "--no-verify"); err != nil {
		t.Fatalf("alpha connect -: %v", err)
	}
	gotA, _ = config.ReadCurrentMeshForBrand(specA)
	gotB, _ = config.ReadCurrentMeshForBrand(specB)
	if gotA != "alpha-one" || gotB != "beta-one" {
		t.Fatalf("after alpha toggle: brand meshes = (%q, %q), want (alpha-one, beta-one)", gotA, gotB)
	}

	// `disconnect` on brand A must clear only brand A.
	if _, err := executeCLIForTestWithBrand(specA, "--config", alphaCfg, "disconnect"); err != nil {
		t.Fatalf("alpha disconnect: %v", err)
	}
	gotA, _ = config.ReadCurrentMeshForBrand(specA)
	gotB, _ = config.ReadCurrentMeshForBrand(specB)
	if gotA != "" || gotB != "beta-one" {
		t.Fatalf("after alpha disconnect: brand meshes = (%q, %q), want ('', beta-one)", gotA, gotB)
	}

	if brand.Active() != initialActive {
		t.Fatalf("global brand.Active() mutated: got %+v, want %+v", brand.Active(), initialActive)
	}
}

func TestContextAwareCommandsDoNotHoldTemplateMuAndUseLocalFlags(t *testing.T) {
	if !isContextAwareCommand(rootCmd) {
		t.Fatal("rootCmd is not registered in isContextAwareCommand")
	}
	for _, c := range rootCmd.Commands() {
		if !isContextAwareCommand(c) {
			t.Fatalf("top-level command %q is not registered in isContextAwareCommand", c.Name())
		}
		for _, sub := range c.Commands() {
			if !isContextAwareCommand(sub) {
				t.Fatalf("subcommand %q %q is not registered in isContextAwareCommand", c.Name(), sub.Name())
			}
		}
	}

	brokerURL := startTestMeshBroker(t, "ctx-mesh", "rv64-coop-ctx")
	deadDBURL := fmt.Sprintf("postgres://operator:secret@%s/edge_ledger", closedLocalTCPAddr(t))

	dir := t.TempDir()
	t.Setenv("ALPHA_CURRENT_MESH_FILE", filepath.Join(dir, "alpha_current_mesh"))
	wsPath := filepath.Join(dir, "alpha.toml")
	tomlContent := fmt.Sprintf(`operator = "alpha-operator"
mesh_name = "ctx-mesh"
broker_url = %q
database_url = %q
`, brokerURL, deadDBURL)
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write alpha.toml: %v", err)
	}

	specA := brand.Spec{
		ProductName: "AlphaTelemetry",
		CoopName:    "AlphaCoop",
		CLIName:     "alphacli",
		TopicRoot:   "pigeons",
		EnvPrefix:   "ALPHA",
		MathSchema:  "alphamath",
		MathName:    "AlphaMath",
		AgentName:   "AlphaCare",
	}

	// Build isolated command trees first, then hold templateMu while executing context-aware commands
	// to prove execution NEVER acquires templateMu.
	runTree := NewRootCommand(specA)
	abortTree := NewRootCommand(specA)
	coopStatusTree := NewRootCommand(specA)
	coopMigrateTree := NewRootCommand(specA)
	listTree := NewRootCommand(specA)
	nodeTree := NewRootCommand(specA)
	psTree := NewRootCommand(specA)
	tagTree := NewRootCommand(specA)
	mathListTree := NewRootCommand(specA)

	templateMu.Lock()
	defer templateMu.Unlock()

	execTree := func(c *cobra.Command, args ...string) (string, error) {
		var buf bytes.Buffer
		c.SetOut(&buf)
		c.SetErr(&buf)
		c.SetArgs(args)
		err := c.Execute()
		return buf.String(), err
	}

	if out, err := execTree(runTree, "--config", wsPath, "run", "PauseRnr", "--all", "--secs", "15", "--no-tui"); err != nil {
		t.Fatalf("context-aware `run` failed: %v\n%s", err, out)
	} else if !strings.Contains(out, "AlphaCoop") {
		t.Fatalf("expected brand CoopName AlphaCoop in `run` output, got: %s", out)
	}

	if out, err := execTree(abortTree, "--config", wsPath, "runner", "abort", "PauseRnr", "--all"); err != nil {
		t.Fatalf("context-aware `runner abort` failed: %v\n%s", err, out)
	}

	if out, err := execTree(coopStatusTree, "--config", wsPath, "coop", "status"); err == nil {
		t.Fatalf("expected `coop status` with closed Postgres to return error, output:\n%s", out)
	} else if !strings.Contains(out, "[MQTT Broker]  ONLINE") {
		t.Fatalf("expected online MQTT line in `coop status`, got:\n%s", out)
	}

	for name, tc := range map[string]struct {
		tree *cobra.Command
		args []string
	}{
		"coop_migrate": {coopMigrateTree, []string{"--config", wsPath, "coop", "migrate"}},
		"list":         {listTree, []string{"--config", wsPath, "list", "--tag", "compute-bench"}},
		"node":         {nodeTree, []string{"--config", wsPath, "node", "rv64-node-01"}},
		"ps":           {psTree, []string{"--config", wsPath, "ps", "-v"}},
		"tag":          {tagTree, []string{"--config", wsPath, "tag", "compute-bench", "rv64-node-01", "--remove"}},
		"math_list":    {mathListTree, []string{"--config", wsPath, "math", "list"}},
	} {
		if _, err := execTree(tc.tree, tc.args...); err == nil {
			t.Fatalf("expected %s against closed Postgres to return error", name)
		}
	}
}

type stubAgentSafeDB struct {
	mu           sync.Mutex
	closed       atomic.Bool
	meshID       *core.MeshIdentity
	meshErr      error
	clusterNodes []db.NodeSummary
	clusterErr   error
	queryFn      func(ctx context.Context, sql string, args ...any) ([]map[string]any, error)
}

func (s *stubAgentSafeDB) GetMeshIdentity(_ context.Context) (*core.MeshIdentity, error) {
	return s.meshID, s.meshErr
}

func (s *stubAgentSafeDB) GetClusterStatus(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
	return s.clusterNodes, s.clusterErr
}

func (s *stubAgentSafeDB) ResolveNodes(_ context.Context, _ core.RunnerSelector) ([]string, error) {
	return nil, nil
}

func (s *stubAgentSafeDB) GetLineageDAG(_ context.Context, _ string, _ int) (*db.DAG, error) {
	return nil, nil
}

func (s *stubAgentSafeDB) GetLedgerActivity(_ context.Context, _ int) ([]db.LedgerActivity, error) {
	return nil, nil
}

func (s *stubAgentSafeDB) QueryReadOnlyMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	if s.queryFn != nil {
		return s.queryFn(ctx, sql, args...)
	}
	return nil, nil
}

func (s *stubAgentSafeDB) ExplainReadOnly(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (s *stubAgentSafeDB) EnsureMathSchema(_ context.Context) error { return nil }
func (s *stubAgentSafeDB) SavePMVDraft(_ context.Context, rec db.PMVRecord) (*db.PMVRecord, error) {
	return &rec, nil
}
func (s *stubAgentSafeDB) UpdatePMVState(_ context.Context, _ db.PMVRecord) error { return nil }
func (s *stubAgentSafeDB) GetPMV(_ context.Context, _ string) (*db.PMVRecord, error) {
	return nil, nil
}
func (s *stubAgentSafeDB) ListPMVs(_ context.Context) ([]db.PMVRecord, error)       { return nil, nil }
func (s *stubAgentSafeDB) MaterializePMV(_ context.Context, _, _ string) error    { return nil }
func (s *stubAgentSafeDB) DropPMV(_ context.Context, _ string) error              { return nil }
func (s *stubAgentSafeDB) Close()                                                 { s.closed.Store(true) }

func startSilentAcceptListener(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	var accepts atomic.Int64
	done := make(chan struct{})
	var connsMu sync.Mutex
	var heldConns []net.Conn

	go func() {
		for {
			c, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			accepts.Add(1)
			connsMu.Lock()
			heldConns = append(heldConns, c)
			connsMu.Unlock()
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	t.Cleanup(func() {
		close(done)
		_ = ln.Close()
		connsMu.Lock()
		for _, c := range heldConns {
			_ = c.Close()
		}
		connsMu.Unlock()
	})
	return ln.Addr().String(), &accepts
}

func startAcceptAndCloseListener(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	var hits atomic.Int64
	go func() {
		for {
			c, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			hits.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), &hits
}

func TestDialMQTTClientWithConfigTimeoutCleanupAndTLSValidation(t *testing.T) {
	// 1. Live connect disables AutoReconnect.
	brokerURL := startTestMeshBroker(t, "dial-test-mesh", "rv64-coop-dial")
	cfg := config.DefaultWorkstationConfig()
	client, err := dialMQTTClientWithConfig(context.Background(), cfg, brokerURL, "test-auto-reconn-off", 3*time.Second)
	if err != nil {
		t.Fatalf("dialMQTTClientWithConfig failed: %v", err)
	}
	reader := client.OptionsReader()
	if reader.AutoReconnect() {
		client.Disconnect(0)
		t.Fatal("expected SetAutoReconnect(false) on CLI MQTT client")
	}
	client.Disconnect(50)

	// 2. TLS configured against non-TLS schemes (tcp://, ws://) must fail fast without dialing.
	silentAddr, silentAccepts := startSilentAcceptListener(t)
	tlsCfg := config.DefaultWorkstationConfig()
	tlsCfg.TLSInsecureSkipVerify = true
	for _, badSchemeURL := range []string{
		"tcp://mqttuser:secret@" + silentAddr,
		"ws://mqttuser:secret@" + silentAddr + "/mqtt",
	} {
		_, err := dialMQTTClientWithConfig(context.Background(), tlsCfg, badSchemeURL, "test-tls-scheme", 500*time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), "TLS") {
			t.Fatalf("expected TLS-on-non-TLS fail-fast error for %q, got %v", badSchemeURL, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("TLS scheme error leaked password: %v", err)
		}
	}
	if silentAccepts.Load() != 0 {
		t.Fatalf("expected zero TCP dials on TLS-scheme validation failure, got %d", silentAccepts.Load())
	}

	// 3. Dial timeout against silent listener must fail and disconnect cleanly.
	_, err = dialMQTTClientWithConfig(context.Background(), cfg, "tcp://"+silentAddr, "test-timeout-clean", 80*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected dial timeout error, got %v", err)
	}

	// 4. Context cancellation aborts dial and publishRunnerRequestWithConfig cleanly.
	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err = dialMQTTClientWithConfig(cancelCtx, cfg, "tcp://"+silentAddr, "test-cancel-clean", 3*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on dial cancel, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("dial cancel took too long (%s)", elapsed)
	}

	canceledCtx, stop := context.WithCancel(context.Background())
	stop()
	req := core.RunnerRequestCTD{
		RequestID:   "req-canceled",
		Action:      "start",
		Runner:      "PauseRnr",
		Selectors:   core.RunnerSelector{All: true},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	}
	if err := publishRunnerRequestWithConfig(canceledCtx, cfg, brokerURL, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected publishRunnerRequestWithConfig with canceled ctx to fail with context.Canceled, got %v", err)
	}
}

func TestBrandCloningStateBrandAndEnvMeshSource(t *testing.T) {
	dir := t.TempDir()
	customMeshFile := filepath.Join(dir, "custom_current_mesh")
	t.Setenv("CUSTOM_CURRENT_MESH_FILE", customMeshFile)
	t.Setenv("CUSTOM_MESH", "env-mesh")

	wsPath := filepath.Join(dir, "custom.toml")
	tomlContent := `operator = "custom-op"

[brand]
product_name = "CustomProd"
coop_name = "CustomCoop"
cli_name = "customcli"
agent_name = "CustomCare"
topic_root = "customroot"
env_prefix = "CUSTOM"
math_schema = "custommath"

[meshes.env-mesh]
broker_url = "tcp://127.0.0.1:1883"

[meshes.mesh-a]
broker_url = "tcp://127.0.0.1:1884"
[meshes.mesh-a.brand]
topic_root = "only-a-root"

[meshes.mesh-b]
broker_url = "tcp://127.0.0.1:1885"
`
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write custom.toml: %v", err)
	}

	// Verify PersistentPreRunE updates state.brand = loaded.Brand and preserves ActiveMeshSource="env"
	// (no duplicate <BRAND>_MESH pre-read turning "env" into "flag").
	root := NewRootCommand(brand.Default())
	var preRunState *cliState
	probeCmd := &cobra.Command{
		Use: "inspect-state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			preRunState = cliStateFromCommand(cmd)
			return nil
		},
	}
	root.AddCommand(probeCmd)

	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"--config", wsPath, "inspect-state"})
	if err := root.Execute(); err != nil {
		t.Fatalf("inspect-state failed: %v\n%s", err, out.String())
	}
	if preRunState == nil {
		t.Fatal("expected cliState from command context")
	}
	if preRunState.brand.ProductName != "CustomProd" ||
		preRunState.brand.CoopName != "CustomCoop" ||
		preRunState.brand.TopicRoot != "customroot" ||
		preRunState.brand.EnvPrefix != "CUSTOM" ||
		preRunState.brand.MathSchema != "custommath" {
		t.Fatalf("expected cloneCommandTree PersistentPreRunE to set state.brand = loaded.Brand, got %+v", preRunState.brand)
	}
	if preRunState.wsCfg.ActiveMesh != "env-mesh" || preRunState.wsCfg.ActiveMeshSource != "env" {
		t.Fatalf("expected ActiveMesh=env-mesh source=env, got mesh=%q source=%q",
			preRunState.wsCfg.ActiveMesh, preRunState.wsCfg.ActiveMeshSource)
	}

	// Verify per-mesh brand override on mesh-a does not leak into mesh-b during `connect`.
	t.Setenv("CUSTOM_MESH", "")
	if _, err := executeCLIForTest("--config", wsPath, "connect", "mesh-a", "--no-verify"); err != nil {
		t.Fatalf("connect mesh-a: %v", err)
	}
	if _, err := executeCLIForTest("--config", wsPath, "connect", "mesh-b", "--no-verify"); err != nil {
		t.Fatalf("connect mesh-b: %v", err)
	}
	loadedB, err := config.LoadWorkstationConfigForMeshAndBrand(wsPath, "mesh-b", brand.Default())
	if err != nil {
		t.Fatalf("LoadWorkstationConfigForMeshAndBrand(mesh-b): %v", err)
	}
	if loadedB.Brand.TopicRoot != "customroot" {
		t.Fatalf("expected mesh-b TopicRoot='customroot' without mesh-a leak, got %q", loadedB.Brand.TopicRoot)
	}
}

func TestConnectCLIFlagPrecedenceMNSConflictsAndTimestamps(t *testing.T) {
	// 1. CLI patch flags override environment variables during live MNS probing.
	envBroker := startTestMeshBroker(t, "wrong-env-mesh", "rv64-coop-env")
	flagBroker := startTestMeshBroker(t, "flag-mesh", "rv64-coop-flag")
	deadDBURL := fmt.Sprintf("postgres://operator:secret@%s/edge_ledger", closedLocalTCPAddr(t))

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	t.Setenv("PIGEONS_BROKER_URL", envBroker)
	t.Setenv("PIGEONS_DB_URL", deadDBURL)
	t.Setenv("PIGEONS_RO_DB_URL", deadDBURL)

	wsPath := filepath.Join(dir, "pigeons.toml")
	if err := os.WriteFile(wsPath, []byte("operator = \"operator\"\n"), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	// Because PIGEONS_DB_URL / PIGEONS_RO_DB_URL are set in env while --broker is passed without --db,
	// pass --allow-partial-verify so the dead env DB doesn't fail partial verification, while verifying
	// that `--broker` beats `PIGEONS_BROKER_URL` (which points to wrong-env-mesh and would fail mismatch!).
	out, err := executeCLIForTest(
		"--config", wsPath,
		"connect", "flag-mesh",
		"--broker", flagBroker,
		"--allow-partial-verify",
	)
	if err != nil {
		t.Fatalf("expected --broker to override PIGEONS_BROKER_URL during MNS probe, got err=%v\n%s", err, out)
	}
	if !strings.Contains(out, "rv64-coop-flag") || !strings.Contains(out, flagBroker) {
		t.Fatalf("expected output from flagBroker, got:\n%s", out)
	}

	// Verify patch fields override env fields in applyMeshProfilePatchToWorkstationConfig.
	flagDB := "postgres://admin:pw@192.0.2.55:5432/edge_ledger"
	flagRODB := "postgres://ro:pw@192.0.2.56:5432/edge_ledger"
	flagUser := "flag-user"
	flagPass := "flag-pass"
	flagTopicRoot := "flagroot"
	flagInsecure := true
	envCfg := config.DefaultWorkstationConfig()
	envCfg.BrokerURL = "tcp://192.0.2.1:1883"
	envCfg.DatabaseURL = "postgres://env:pw@192.0.2.1:5432/edge_ledger"
	envCfg.ReadOnlyDatabaseURL = "postgres://envro:pw@192.0.2.1:5432/edge_ledger"
	envCfg.MQTTUsername = "env-user"
	envCfg.MQTTPassword = "env-pass"
	envCfg.Brand.TopicRoot = "envroot"

	patchedCfg, err := applyMeshProfilePatchToWorkstationConfig(envCfg, brand.Default(), config.MeshProfilePatch{
		DatabaseURL:           &flagDB,
		MQTTUsername:          &flagUser,
		MQTTPassword:          &flagPass,
		TopicRoot:             &flagTopicRoot,
		TLSInsecureSkipVerify: &flagInsecure,
	})
	if err != nil {
		t.Fatalf("applyMeshProfilePatchToWorkstationConfig failed: %v", err)
	}
	if patchedCfg.DatabaseURL != flagDB || patchedCfg.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected --db patch to set DatabaseURL and clear stale env RO URL, got db=%q ro=%q",
			patchedCfg.DatabaseURL, patchedCfg.ReadOnlyDatabaseURL)
	}
	if patchedCfg.MQTTUsername != flagUser || patchedCfg.MQTTPassword != flagPass ||
		patchedCfg.Brand.TopicRoot != flagTopicRoot || !patchedCfg.TLSInsecureSkipVerify {
		t.Fatalf("expected CLI patch fields to override env values, got %+v", patchedCfg)
	}
	patchedWithRO, err := applyMeshProfilePatchToWorkstationConfig(envCfg, brand.Default(), config.MeshProfilePatch{
		DatabaseURL:         &flagDB,
		ReadOnlyDatabaseURL: &flagRODB,
	})
	if err != nil || patchedWithRO.ReadOnlyDatabaseURL != flagRODB {
		t.Fatalf("expected explicit --ro-db to be preserved, got %q (err=%v)", patchedWithRO.ReadOnlyDatabaseURL, err)
	}

	// 2. Cross-source conflict detection (CoopNodeID / TopicRoot) across MQTT & Postgres.
	cfgBoth := config.DefaultWorkstationConfig()
	cfgBoth.BrokerURL = "tcp://127.0.0.1:1883"
	cfgBoth.DatabaseURL = "postgres://op:pw@127.0.0.1:5432/edge_ledger"
	now := time.Now().UTC()

	summary, err := verifyRemoteMeshIdentityWithProbers(
		context.Background(), cfgBoth, "prod-mesh", true, false,
		func(context.Context, config.WorkstationConfig) (*core.MeshIdentity, error) {
			return &core.MeshIdentity{MeshName: "prod-mesh", CoopNodeID: "coop-1", TopicRoot: "root-a", UpdatedAt: now}, nil
		},
		func(context.Context, config.WorkstationConfig) (*core.MeshIdentity, error) {
			return &core.MeshIdentity{MeshName: "prod-mesh", CoopNodeID: "coop-1", TopicRoot: "root-a", UpdatedAt: now}, nil
		},
	)
	if err != nil || !strings.Contains(summary, "verified via mqtt+postgres") {
		t.Fatalf("expected matching MQTT+Postgres MNS to verify cleanly, got summary=%q err=%v", summary, err)
	}

	for _, tc := range []struct {
		name    string
		pgID    *core.MeshIdentity
		pgErr   error
		wantErr string
	}{
		{
			name:    "conflicting_coop_node_id",
			pgID:    &core.MeshIdentity{MeshName: "prod-mesh", CoopNodeID: "coop-2", TopicRoot: "root-a", UpdatedAt: now},
			wantErr: "conflicting coop_node_id",
		},
		{
			name:    "conflicting_topic_root",
			pgID:    &core.MeshIdentity{MeshName: "prod-mesh", CoopNodeID: "coop-1", TopicRoot: "root-b", UpdatedAt: now},
			wantErr: "conflicting topic_root",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := verifyRemoteMeshIdentityWithProbers(
				context.Background(), cfgBoth, "prod-mesh", true, true,
				func(context.Context, config.WorkstationConfig) (*core.MeshIdentity, error) {
					return &core.MeshIdentity{MeshName: "prod-mesh", CoopNodeID: "coop-1", TopicRoot: "root-a", UpdatedAt: now}, nil
				},
				func(context.Context, config.WorkstationConfig) (*core.MeshIdentity, error) {
					return tc.pgID, tc.pgErr
				},
			)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q even with allowPartial=true, got %v", tc.wantErr, err)
			}
		})
	}

	// 3. Future UpdatedAt clock skew (>5m ahead) & stale Postgres ctd_mesh timestamp rejection.
	futureSkewed := &core.MeshIdentity{
		MeshName:   "prod-mesh",
		CoopNodeID: "coop-1",
		TopicRoot:  "pigeons",
		UpdatedAt:  now.Add(defaultMNSMaxFutureSkew + time.Minute),
	}
	if err := validateMeshIdentity("postgres ctd_mesh", futureSkewed, now); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("expected future Postgres ctd_mesh >5m ahead to fail, got %v", err)
	}
	stalePG := &core.MeshIdentity{
		MeshName:   "prod-mesh",
		CoopNodeID: "coop-1",
		TopicRoot:  "pigeons",
		UpdatedAt:  now.Add(-(defaultMNSRetainedMaxAge + time.Hour)),
	}
	if err := validateMeshIdentity("postgres ctd_mesh", stalePG, now); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("expected stale Postgres ctd_mesh to fail, got %v", err)
	}
	withinSkew := &core.MeshIdentity{
		MeshName:   "prod-mesh",
		CoopNodeID: "coop-1",
		TopicRoot:  "pigeons",
		UpdatedAt:  now.Add(2 * time.Minute),
	}
	if err := validateMeshIdentity("postgres ctd_mesh", withinSkew, now); err != nil {
		t.Fatalf("expected +2m clock skew within 5m tolerance to pass, got %v", err)
	}

	// Live MQTT retained payload with future >5m UpdatedAt must also be rejected.
	emb, err := broker.StartEmbedded(broker.EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	t.Cleanup(emb.Close)
	t.Setenv("PIGEONS_BROKER_URL", "")
	t.Setenv("PIGEONS_DB_URL", "")
	t.Setenv("PIGEONS_RO_DB_URL", "")
	futureTOML := fmt.Sprintf("operator = \"operator\"\n[meshes.future-mesh]\nbroker_url = %q\n", emb.LocalClientURL())
	futurePath := filepath.Join(dir, "future.toml")
	if err := os.WriteFile(futurePath, []byte(futureTOML), 0o600); err != nil {
		t.Fatalf("write future.toml: %v", err)
	}
	futureBytes, _ := json.Marshal(core.MeshIdentity{
		MeshName:   "future-mesh",
		CoopNodeID: "rv64-coop-future",
		UpdatedAt:  time.Now().UTC().Add(10 * time.Minute),
	})
	publishRetainedMeshPayload(t, emb.LocalClientURL(), core.MeshTopic(), futureBytes)
	if _, err := executeCLIForTest("--config", futurePath, "connect", "future-mesh"); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("expected future retained MNS payload to be rejected, got: %v", err)
	}

	// 4. probeMQTTMeshIdentity honors parent ctx deadline and cancellation.
	silentAddr, _ := startSilentAcceptListener(t)
	probeCfg := config.DefaultWorkstationConfig()
	probeCfg.BrokerURL = "tcp://" + silentAddr
	pCtx, pCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer pCancel()
	if _, err := probeMQTTMeshIdentity(pCtx, probeCfg); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected probeMQTTMeshIdentity to honor parent ctx deadline, got %v", err)
	}
	cCtx, cCancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cCancel()
	}()
	if _, err := probeMQTTMeshIdentity(cCtx, probeCfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected probeMQTTMeshIdentity to honor parent ctx cancel, got %v", err)
	}
}

func TestReadOnlyStoreUsageAndCLICommandsPolish(t *testing.T) {
	// 1. Verify read-only commands dial ReadOnlyDatabaseURL when configured, and fall back to DatabaseURL when empty.
	opAddr, opHits := startAcceptAndCloseListener(t)
	roAddr, roHits := startAcceptAndCloseListener(t)

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "ro-test.toml")
	tomlContent := fmt.Sprintf(`operator = "operator"
mesh_name = "ro-mesh"
broker_url = "tcp://%s"
database_url = "postgres://operator:pw@%s/edge_ledger"
readonly_database_url = "postgres://pigeoncare_ro:pw@%s/edge_ledger"
`, closedLocalTCPAddr(t), opAddr, roAddr)
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write ro-test.toml: %v", err)
	}

	for _, args := range [][]string{
		{"--config", wsPath, "list"},
		{"--config", wsPath, "node", "rv64-node-01"},
		{"--config", wsPath, "ps"},
		{"--config", wsPath, "coop", "status"},
		{"--config", wsPath, "coop", "activity"},
	} {
		_, err := executeCLIForTest(args...)
		if err == nil {
			t.Fatalf("expected %v against accept-and-close listener to error", args)
		}
	}
	if roHits.Load() < 5 {
		t.Fatalf("expected read-only CLI commands to dial readonly_database_url (%s), got roHits=%d opHits=%d",
			roAddr, roHits.Load(), opHits.Load())
	}
	if opHits.Load() != 0 {
		t.Fatalf("expected read-only CLI commands NOT to dial operator database_url when readonly_database_url is set, got opHits=%d", opHits.Load())
	}

	opHits.Store(0)
	fallbackCfg := config.DefaultWorkstationConfig()
	fallbackCfg.DatabaseURL = "postgres://operator:pw@" + opAddr + "/edge_ledger"
	fallbackCfg.ReadOnlyDatabaseURL = ""
	if _, err := openReadStoreForConfig(context.Background(), fallbackCfg, "list"); err == nil {
		t.Fatal("expected dial error on accept-and-close op listener")
	}
	if opHits.Load() == 0 {
		t.Fatal("expected openReadStoreForConfig to fall back to DatabaseURL when ReadOnlyDatabaseURL is empty")
	}

	// 2. ps EOF handling & closing store before launching TUI.
	if got := readSupportPromptResponse(strings.NewReader("")); got != "n" {
		t.Fatalf("expected EOF on support prompt to return 'n', got %q", got)
	}
	if got := readSupportPromptResponse(strings.NewReader("\n")); got != "" {
		t.Fatalf("expected empty newline on support prompt to return '', got %q", got)
	}
	if got := readSupportPromptResponse(strings.NewReader("y")); got != "y" {
		t.Fatalf("expected 'y' without newline to return 'y', got %q", got)
	}

	degradedNodes := []db.NodeSummary{{
		NodeID:    "rv64-node-01",
		Arch:      "riscv64",
		Tags:      []string{"compute"},
		Component: "host-os",
		LastSeen:  time.Now().UTC(),
		Status:    "error",
		IsHealthy: false,
	}}
	eofStore := &stubAgentSafeDB{clusterNodes: degradedNodes}
	var eofOut bytes.Buffer
	launched := false
	err := runPSWithStore(
		context.Background(), context.Background(), config.DefaultWorkstationConfig(),
		strings.NewReader(""), &eofOut, false, true, eofStore,
		func(context.Context, config.WorkstationConfig, tui.TabID, string, string, string) error {
			launched = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runPSWithStore(EOF) failed: %v", err)
	}
	if launched {
		t.Fatal("expected EOF on ps --add-support NOT to launch TUI")
	}
	if !eofStore.closed.Load() {
		t.Fatal("expected ps to close store")
	}

	yesStore := &stubAgentSafeDB{clusterNodes: degradedNodes}
	var yesOut bytes.Buffer
	var closedBeforeLaunch bool
	err = runPSWithStore(
		context.Background(), context.Background(), config.DefaultWorkstationConfig(),
		strings.NewReader("y\n"), &yesOut, false, true, yesStore,
		func(context.Context, config.WorkstationConfig, tui.TabID, string, string, string) error {
			closedBeforeLaunch = yesStore.closed.Load()
			return nil
		},
	)
	if err != nil {
		t.Fatalf("runPSWithStore(y) failed: %v", err)
	}
	if !closedBeforeLaunch {
		t.Fatal("expected ps to close store strictly BEFORE launching TUI")
	}

	// 3. tag empty node list and ValidateTopicToken enforcement.
	for _, badArgs := range [][]string{
		{"--config", wsPath, "tag", "compute-bench", ""},
		{"--config", wsPath, "tag", "compute-bench", ",,,"},
		{"--config", wsPath, "tag", "compute-bench", "{}"},
		{"--config", wsPath, "tag", "bad/tag", "rv64-node-01"},
		{"--config", wsPath, "tag", "bad+tag", "rv64-node-01"},
		{"--config", wsPath, "tag", "bad#tag", "rv64-node-01"},
		{"--config", wsPath, "tag", "compute-bench", "bad/node"},
		{"--config", wsPath, "tag", "compute-bench", "bad+node"},
		{"--config", wsPath, "tag", "compute-bench", "bad#node"},
	} {
		if _, err := executeCLIForTest(badArgs...); err == nil {
			t.Fatalf("expected invalid tag command %v to fail validation", badArgs)
		}
	}

	// 4. node help & tag output alignment.
	nodeHelp, err := executeCLIForTest("--config", wsPath, "node", "--help")
	if err != nil {
		t.Fatalf("node --help failed: %v", err)
	}
	if !strings.Contains(nodeHelp, "node <node_id>") || !strings.Contains(nodeHelp, "tags") {
		t.Fatalf("expected node --help to align <node_id> and tags description, got:\n%s", nodeHelp)
	}
	if _, err := executeCLIForTest("--config", wsPath, "node", "bad/node"); err == nil || !strings.Contains(err.Error(), "invalid node_id") {
		t.Fatalf("expected invalid node_id to fail ValidateTopicToken, got: %v", err)
	}

	nodeStore := &stubAgentSafeDB{
		queryFn: func(_ context.Context, sql string, _ ...any) ([]map[string]any, error) {
			if strings.Contains(sql, "FROM ctd_nodes") {
				return []map[string]any{{"tags": []string{"compute-bench", "riscv64"}}}, nil
			}
			return []map[string]any{{
				"component":       "host-os",
				"pillar":          "observer",
				"status":          "ok",
				"event_timestamp": time.Now().UTC(),
				"metrics":         map[string]any{"cpu_temp_c": 42.5},
			}}, nil
		},
	}
	var nodeOut bytes.Buffer
	if err := runNodeWithStore(context.Background(), &nodeOut, nodeStore, "rv64-node-01"); err != nil {
		t.Fatalf("runNodeWithStore failed: %v", err)
	}
	if !strings.Contains(nodeOut.String(), "Tags: compute-bench,riscv64") {
		t.Fatalf("expected node output to include node tags, got:\n%s", nodeOut.String())
	}

	// 5. coop status prints warning when count query returns an error.
	coopStore := &stubAgentSafeDB{
		meshID: &core.MeshIdentity{MeshName: "ro-mesh", CoopNodeID: "rv64-coop-01", TopicRoot: "pigeons"},
		queryFn: func(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
			return nil, errors.New("relation pmv_catalog does not exist")
		},
	}
	var coopOut bytes.Buffer
	coopCfg := config.DefaultWorkstationConfig()
	coopCfg.ActiveMesh = "ro-mesh"
	if err := queryAndPrintCoopStatus(context.Background(), &coopOut, coopCfg, coopStore); err != nil {
		t.Fatalf("queryAndPrintCoopStatus returned error: %v", err)
	}
	if !strings.Contains(coopOut.String(), "[Ledger Warning]") || !strings.Contains(coopOut.String(), "pmv_catalog") {
		t.Fatalf("expected coop status to print warning on count query error, got:\n%s", coopOut.String())
	}
	_ = io.Discard
}
