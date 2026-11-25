package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
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
