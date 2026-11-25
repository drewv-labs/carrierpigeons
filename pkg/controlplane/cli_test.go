package controlplane

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type testBrokerLedger struct{}

func (l *testBrokerLedger) InsertEvent(_ context.Context, _ *core.CTDPayload) error { return nil }
func (l *testBrokerLedger) BatchInsertEvents(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
	return int64(len(payloads)), nil
}
func (l *testBrokerLedger) UpsertNodeHello(_ context.Context, h core.NodeHello) ([]string, error) {
	return core.NormalizeTags(h.BootTags), nil
}
func (l *testBrokerLedger) UpdateNodePresence(_ context.Context, _, _ string) error { return nil }
func (l *testBrokerLedger) ResolveNodes(_ context.Context, _ core.RunnerSelector) ([]string, error) {
	return nil, nil
}
func (l *testBrokerLedger) UpsertArtifact(_ context.Context, _ core.Artifact) error   { return nil }
func (l *testBrokerLedger) InsertTrace(_ context.Context, _ core.TraceEdge) error { return nil }

func closedTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startEmbeddedMeshBroker(t *testing.T, meshName, coopNodeID, topicRoot string) string {
	t.Helper()
	emb, err := broker.StartEmbedded(broker.EmbeddedOptions{
		ListenAddr: "127.0.0.1:0",
		TopicRoot:  topicRoot,
	})
	if err != nil {
		t.Fatalf("StartEmbedded(%s) failed: %v", meshName, err)
	}
	t.Cleanup(emb.Close)

	sub, err := broker.NewSubscriberWithOptions(broker.SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    meshName,
		CoopNodeID:  coopNodeID,
		NtfyEnabled: false,
		Brand: brand.Spec{
			TopicRoot: topicRoot,
		},
		Ingest: ingest.Options{
			QueueSize:     32,
			BatchSize:     4,
			FlushInterval: 25 * time.Millisecond,
		},
	}, &testBrokerLedger{})
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions(%s) failed: %v", meshName, err)
	}
	t.Cleanup(sub.Disconnect)
	return emb.LocalClientURL()
}

func execControlPlaneCmd(cp *ControlPlane, args ...string) (string, error) {
	cmd := cp.RootCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func TestControlPlaneMultiBrandConcurrentCLIExecution(t *testing.T) {
	initialActive := brand.Active()

	dir := t.TempDir()
	t.Setenv("ALPHA_CURRENT_MESH_FILE", filepath.Join(dir, "alpha_current_mesh"))
	t.Setenv("BETA_CURRENT_MESH_FILE", filepath.Join(dir, "beta_current_mesh"))

	brokerAlpha := startEmbeddedMeshBroker(t, "alpha-mesh", "coop-alpha", "alpha")
	brokerBeta := startEmbeddedMeshBroker(t, "beta-mesh", "coop-beta", "beta")

	alphaCfgPath := filepath.Join(dir, "alphacli.toml")
	alphaTOML := fmt.Sprintf(`operator = "alpha-op"

[meshes.alpha-mesh]
broker_url = %q
`, brokerAlpha)
	if err := os.WriteFile(alphaCfgPath, []byte(alphaTOML), 0o600); err != nil {
		t.Fatalf("write alpha config: %v", err)
	}

	betaCfgPath := filepath.Join(dir, "betacli.toml")
	betaTOML := fmt.Sprintf(`operator = "beta-op"

[meshes.beta-mesh]
broker_url = %q
`, brokerBeta)
	if err := os.WriteFile(betaCfgPath, []byte(betaTOML), 0o600); err != nil {
		t.Fatalf("write beta config: %v", err)
	}

	specA := brand.Spec{
		ProductName: "AlphaTelemetry",
		CoopName:    "AlphaCoop",
		CLIName:     "alphacli",
		TopicRoot:   "alpha",
		EnvPrefix:   "ALPHA",
		MathSchema:  "alphamath",
		MathName:    "AlphaMath",
		AgentName:   "AlphaCare",
	}
	specB := brand.Spec{
		ProductName: "BetaTelemetry",
		CoopName:    "BetaCoop",
		CLIName:     "betacli",
		TopicRoot:   "beta",
		EnvPrefix:   "BETA",
		MathSchema:  "betamath",
		MathName:    "BetaMath",
		AgentName:   "BetaCare",
	}

	cpA := New(specA)
	cpB := New(specB)

	// Connect each brand to its own isolated MNS state file first.
	if out, err := execControlPlaneCmd(cpA, "--config", alphaCfgPath, "connect", "alpha-mesh"); err != nil {
		t.Fatalf("alpha connect failed: %v\n%s", err, out)
	}
	if out, err := execControlPlaneCmd(cpB, "--config", betaCfgPath, "connect", "beta-mesh"); err != nil {
		t.Fatalf("beta connect failed: %v\n%s", err, out)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cp := cpA
			cfgPath := alphaCfgPath
			wantMesh := "alpha-mesh"
			wantCoop := "AlphaCoop"
			if idx%2 == 1 {
				cp = cpB
				cfgPath = betaCfgPath
				wantMesh = "beta-mesh"
				wantCoop = "BetaCoop"
			}

			curOut, err := execControlPlaneCmd(cp, "--config", cfgPath, "mesh", "current")
			if err != nil {
				t.Errorf("concurrent mesh current failed: %v\n%s", err, curOut)
				return
			}
			if strings.TrimSpace(curOut) != wantMesh {
				t.Errorf("concurrent mesh current = %q, want %q", strings.TrimSpace(curOut), wantMesh)
			}

			runOut, err := execControlPlaneCmd(cp, "--config", cfgPath, "run", "PauseRnr", "--all", "--no-tui")
			if err != nil {
				t.Errorf("concurrent run PauseRnr failed: %v\n%s", err, runOut)
				return
			}
			if !strings.Contains(runOut, wantCoop) {
				t.Errorf("run output %q missing brand coop %q", runOut, wantCoop)
			}

			abortOut, err := execControlPlaneCmd(cp, "--config", cfgPath, "runner", "abort", "PauseRnr", "--all")
			if err != nil {
				t.Errorf("concurrent runner abort failed: %v\n%s", err, abortOut)
			}
		}(i)
	}
	wg.Wait()

	if gotA, err := config.ReadCurrentMeshForBrand(specA); err != nil || gotA != "alpha-mesh" {
		t.Fatalf("alpha brand current mesh = %q (err=%v), want alpha-mesh", gotA, err)
	}
	if gotB, err := config.ReadCurrentMeshForBrand(specB); err != nil || gotB != "beta-mesh" {
		t.Fatalf("beta brand current mesh = %q (err=%v), want beta-mesh", gotB, err)
	}
	if brand.Active() != initialActive {
		t.Fatalf("global brand.Active() mutated: got %+v, want %+v", brand.Active(), initialActive)
	}
}

func TestControlPlaneCoopStatusRedactsCredentialsAndReturnsError(t *testing.T) {
	emb, err := broker.StartEmbedded(broker.EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	t.Cleanup(emb.Close)

	credBrokerURL := strings.Replace(emb.LocalClientURL(), "tcp://", "tcp://mqttuser:supersecretmqtt@", 1)
	deadDBURL := fmt.Sprintf("postgres://operator:supersecretdb@%s/edge_ledger", closedTCPAddr(t))

	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	wsPath := filepath.Join(dir, "pigeons.toml")
	tomlContent := fmt.Sprintf(`operator = "operator"
mesh_name = "redacted-mesh"
broker_url = %q
database_url = %q
`, credBrokerURL, deadDBURL)
	if err := os.WriteFile(wsPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	cp := New(brand.Default())

	// `coop status` must return an error (not call os.Exit(1)) when Postgres is offline,
	// write status to cmd.OutOrStdout(), and redact MQTT broker credentials.
	out, statusErr := execControlPlaneCmd(cp, "--config", wsPath, "coop", "status")
	if statusErr == nil {
		t.Fatalf("expected `coop status` with unreachable Postgres to return an error, output:\n%s", out)
	}
	if !strings.Contains(out, "[MQTT Broker]  ONLINE") {
		t.Fatalf("expected online MQTT broker status line in output, got:\n%s", out)
	}
	if !strings.Contains(out, "mqttuser:xxxxx@") {
		t.Fatalf("expected redacted MQTT broker credentials in output, got:\n%s", out)
	}
	if strings.Contains(out, "supersecretmqtt") || strings.Contains(out, "supersecretdb") {
		t.Fatalf("coop status output leaked secret credentials:\n%s", out)
	}
	if strings.Contains(statusErr.Error(), "supersecretmqtt") || strings.Contains(statusErr.Error(), "supersecretdb") {
		t.Fatalf("coop status error leaked secret credentials: %v", statusErr)
	}
	if !strings.Contains(out, "[CTD Ledger]   OFFLINE") {
		t.Fatalf("expected offline CTD Ledger status line in output, got:\n%s", out)
	}

	// `coop migrate` must also return an error (not call os.Exit(1)) when Postgres is offline.
	_, migrateErr := execControlPlaneCmd(cp, "--config", wsPath, "coop", "migrate")
	if migrateErr == nil {
		t.Fatal("expected `coop migrate` with unreachable Postgres to return an error")
	}
}
