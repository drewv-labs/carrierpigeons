package broker

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
)

type memoryLedger struct {
	mu         sync.Mutex
	events     []*core.CTDPayload
	tagsByID   map[string][]string
	statusByID map[string]string
	artifacts  []core.Artifact
	traces     []core.TraceEdge
}

func newMemoryLedger() *memoryLedger {
	return &memoryLedger{
		tagsByID:   make(map[string][]string),
		statusByID: make(map[string]string),
	}
}

func (m *memoryLedger) InsertEvent(_ context.Context, ev *core.CTDPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ev)
	return nil
}

func (m *memoryLedger) BatchInsertEvents(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, payloads...)
	return int64(len(payloads)), nil
}

func (m *memoryLedger) UpsertNodeHello(_ context.Context, h core.NodeHello) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	norm := core.NormalizeTags(h.BootTags)
	m.tagsByID[h.NodeID] = norm
	m.statusByID[h.NodeID] = "ONLINE"
	return norm, nil
}

func (m *memoryLedger) UpdateNodePresence(ctx context.Context, nodeID, status string) error {
	_, _, err := m.UpdateNodePresenceTransition(ctx, nodeID, status)
	return err
}

func (m *memoryLedger) UpdateNodePresenceTransition(_ context.Context, nodeID, status string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tagsByID[nodeID]; !ok {
		m.tagsByID[nodeID] = nil
	}
	prev := m.statusByID[nodeID]
	m.statusByID[nodeID] = status
	return prev, prev != status, nil
}

func (m *memoryLedger) ResolveNodes(_ context.Context, sel core.RunnerSelector) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := make(map[string]struct{})
	if sel.All {
		for id := range m.tagsByID {
			set[id] = struct{}{}
		}
	}
	for _, n := range sel.Nodes {
		set[n] = struct{}{}
	}
	for _, tag := range core.NormalizeTags(sel.Tags) {
		for id, tags := range m.tagsByID {
			if slices.Contains(tags, tag) {
				set[id] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	slices.Sort(out)
	return out, nil
}

func (m *memoryLedger) UpsertArtifact(_ context.Context, a core.Artifact) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.artifacts = append(m.artifacts, a)
	return nil
}

func (m *memoryLedger) InsertTrace(_ context.Context, t core.TraceEdge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traces = append(m.traces, t)
	return nil
}

func TestEmbeddedBrokerAndRunnerRequestRouting(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	ledger := newMemoryLedger()
	_, _ = ledger.UpsertNodeHello(context.Background(), core.NodeHello{
		NodeID:   "rv64-compute-01",
		Arch:     "riscv64",
		OS:       "linux",
		BootTags: []string{"compute-bench", "riscv64"},
	})
	_, _ = ledger.UpsertNodeHello(context.Background(), core.NodeHello{
		NodeID:   "arm64-gateway-01",
		Arch:     "arm64",
		OS:       "linux",
		BootTags: []string{"gateway"},
	})

	sub, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "rv64-lab",
		CoopNodeID:  "rv64-coop-01",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     128,
			BatchSize:     4,
			FlushInterval: 20 * time.Millisecond,
		},
	}, ledger)
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}
	defer sub.Disconnect()

	opts := mqtt.NewClientOptions().AddBroker(emb.LocalClientURL()).SetClientID("test-cli")
	cli := mqtt.NewClient(opts)
	if tok := cli.Connect(); tok.Wait() && tok.Error() != nil {
		t.Fatalf("test client connect failed: %v", tok.Error())
	}
	defer cli.Disconnect(100)

	// Verify newly connecting workstation client receives the retained MNS identity announcement.
	meshCh := make(chan core.MeshIdentity, 1)
	if tok := cli.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
		var id core.MeshIdentity
		if err := json.Unmarshal(m.Payload(), &id); err == nil {
			select {
			case meshCh <- id:
			default:
			}
		}
	}); tok.Wait() && tok.Error() != nil {
		t.Fatalf("subscribe %s failed: %v", core.MeshTopic(), tok.Error())
	}
	select {
	case id := <-meshCh:
		if id.MeshName != "rv64-lab" || id.CoopNodeID != "rv64-coop-01" || id.TopicRoot != "pigeons" {
			t.Fatalf("unexpected retained MeshIdentity: %+v", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for retained MeshIdentity on %s", core.MeshTopic())
	}

	controlCh := make(chan runner.Envelope, 4)
	for _, nodeID := range []string{"rv64-compute-01", "arm64-gateway-01"} {
		tok := cli.Subscribe(core.ControlTopic(nodeID), 1, func(_ mqtt.Client, m mqtt.Message) {
			var env runner.Envelope
			if err := json.Unmarshal(m.Payload(), &env); err == nil {
				controlCh <- env
			}
		})
		tok.Wait()
	}

	// Publish RunnerRequestCTD chaining --node=arm64-gateway-01 AND --tag=compute-bench
	req := core.RunnerRequestCTD{
		RequestID: "req-chain-01",
		Action:    "start",
		Runner:    "SyntheticComputeRnr",
		Selectors: core.RunnerSelector{
			Nodes: []string{"arm64-gateway-01"},
			Tags:  []string{"compute-bench"},
		},
		Params:      map[string]any{"duration_sec": 5},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	}
	rawReq, _ := json.Marshal(req)
	tok := cli.Publish(core.RunnerRequestTopic(), 1, false, rawReq)
	tok.Wait()

	for range 2 {
		select {
		case env := <-controlCh:
			if env.RequestID != "req-chain-01" || env.Runner != "SyntheticComputeRnr" {
				t.Fatalf("unexpected dispatched envelope: %+v", env)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Coop to fan out RunnerRequestCTD to resolved nodes")
		}
	}

	// Verify node IDs prefixed with "pigeoneer-" preserve their full node_id on registry status updates.
	regTok := cli.Publish(core.RegistryTopic("pigeoneer-01"), 1, false, []byte("ONLINE"))
	regTok.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ledger.mu.Lock()
		st := ledger.statusByID["pigeoneer-01"]
		ledger.mu.Unlock()
		if st == "ONLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	ledger.mu.Lock()
	if ledger.statusByID["pigeoneer-01"] != "ONLINE" {
		t.Fatalf("expected pigeoneer-01 status ONLINE without prefix stripping, got %+v", ledger.statusByID)
	}
	ledger.mu.Unlock()
}

func TestEmbeddedBrokerAuthAndEdgeTopicACLs(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:   "127.0.0.1:0",
		Username:     "coop_admin",
		Password:     "admin_secret",
		EdgeUsername: "edge_node",
		EdgePassword: "edge_secret",
		TopicRoot:    "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded with auth failed: %v", err)
	}
	defer emb.Close()

	// 1. Anonymous client MUST be rejected.
	anonCli := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(emb.LocalClientURL()).SetClientID("anon-cli").SetConnectRetry(false))
	if tok := anonCli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
		anonCli.Disconnect(50)
		t.Fatal("expected anonymous MQTT connection to fail when credentials are configured")
	}

	// 2. Admin subscriber receives telemetry and blocks forged RunnerRequestCTD from edge_node.
	ledger := newMemoryLedger()
	sub, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "compute-mesh",
		CoopNodeID:  "coop-01",
		Username:    "coop_admin",
		Password:    "admin_secret",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     64,
			BatchSize:     1,
			FlushInterval: 15 * time.Millisecond,
		},
	}, ledger)
	if err != nil {
		t.Fatalf("admin subscriber connect failed: %v", err)
	}
	defer sub.Disconnect()

	// 3. Edge client can connect, publish CTD telemetry, and subscribe to its control topic,
	//    but CANNOT publish a RunnerRequestCTD to `pigeons/requests/runner`.
	edgeCli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("edge-01").
		SetUsername("edge_node").
		SetPassword("edge_secret"))
	if tok := edgeCli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("edge client connect failed: %v", tok.Error())
	}
	defer edgeCli.Disconnect(50)

	ev := core.NewCTDPayload("edge-01", "host-os", "healthy")
	rawEv, _ := json.Marshal(ev)
	if tok := edgeCli.Publish(ev.RoutingTopic("pigeons/ctd"), 1, false, rawEv); tok.Wait() && tok.Error() != nil {
		t.Fatalf("edge telemetry publish failed: %v", tok.Error())
	}

	forgedReq := core.RunnerRequestCTD{
		RequestID:   "forged-req-01",
		Action:      "start",
		Runner:      "SyntheticComputeRnr",
		Selectors:   core.RunnerSelector{All: true},
		RequestedBy: "attacker",
		RequestedAt: time.Now().UTC(),
	}
	rawForged, _ := json.Marshal(forgedReq)
	_ = edgeCli.Publish(core.RunnerRequestTopic(), 1, false, rawForged).WaitTimeout(500 * time.Millisecond)

	time.Sleep(120 * time.Millisecond)
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.artifacts) != 0 {
		t.Fatalf("expected edge_node ACL to block RunnerRequestCTD publish, got artifacts: %+v", ledger.artifacts)
	}
	if len(ledger.events) == 0 {
		t.Fatal("expected edge_node telemetry event to be ingested")
	}
}

func TestPerNodeACLHookAndTopicSpoofingGuards(t *testing.T) {
	hook := &perNodeAuthHook{
		adminUser:  "coop_admin",
		adminPass:  "admin_pw",
		edgeUser:   "edge_node",
		edgePass:   "edge_pw",
		edgePrefix: "pigeoneer-",
		topicRoot:  "pigeons",
	}

	edgeClient := &mochi.Client{
		ID: "pigeoneer-rv64-edge-01",
		Properties: mochi.ClientProperties{
			Username: []byte("edge_node"),
		},
	}

	// Own topics allowed
	if !hook.OnACLCheck(edgeClient, "pigeons/control/rv64-edge-01", false) {
		t.Fatal("expected edge node to subscribe to its own control topic")
	}
	if !hook.OnACLCheck(edgeClient, "pigeons/ctd/observer/rv64-edge-01/host-os", true) {
		t.Fatal("expected edge node to publish to its own CTD telemetry topic")
	}
	if !hook.OnACLCheck(edgeClient, "pigeons/registry/rv64-edge-01/status", true) {
		t.Fatal("expected edge node to publish to its own registry status topic")
	}

	// Wildcard, MNS registry, and cross-node topics strictly denied
	if hook.OnACLCheck(edgeClient, "pigeons/registry/mesh", true) {
		t.Fatal("expected edge node publish to MNS registry/mesh topic to be denied")
	}
	if hook.OnACLCheck(edgeClient, "pigeons/control/+", false) {
		t.Fatal("expected edge node subscription to control/+ to be denied")
	}
	if hook.OnACLCheck(edgeClient, "pigeons/control/rv64-edge-02", false) {
		t.Fatal("expected edge node subscription to sibling control topic to be denied")
	}
	if hook.OnACLCheck(edgeClient, "pigeons/ctd/observer/rv64-edge-02/host-os", true) {
		t.Fatal("expected edge node publish to sibling telemetry topic to be denied")
	}
	if hook.OnACLCheck(edgeClient, "pigeons/registry/rv64-edge-02/status", true) {
		t.Fatal("expected edge node publish to sibling registry topic to be denied")
	}
	if hook.OnACLCheck(edgeClient, "pigeons/requests/runner", true) {
		t.Fatal("expected edge node publish to runner requests topic to be denied")
	}

	// Admin client retains full access across <topicRoot>/#
	adminClient := &mochi.Client{
		ID: "pigeoncoop-ingestor",
		Properties: mochi.ClientProperties{
			Username: []byte("coop_admin"),
		},
	}
	if !hook.OnACLCheck(adminClient, "pigeons/ctd/#", false) ||
		!hook.OnACLCheck(adminClient, "pigeons/control/rv64-edge-02", true) ||
		!hook.OnACLCheck(adminClient, "pigeons/registry/mesh", true) {
		t.Fatal("expected admin client to retain full pub/sub across topicRoot")
	}

	// Subscriber topic spoofing & malformed registry topic guards (pure in-memory, no sockets required).
	ledger := newMemoryLedger()
	sub := &Subscriber{
		ledger:    ledger,
		batcher:   ingest.NewBatcher(ledger, ingest.Options{QueueSize: 16, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		brand:     brand.Default(),
		topicRoot: "pigeons",
	}
	defer sub.batcher.Close()

	// Spoofed node_id must be rejected
	spoofedNode, _ := json.Marshal(core.NewCTDPayload("victim-node", "host-os", "active"))
	sub.processTelemetry("pigeons/ctd/observer/attacker-node/host-os", spoofedNode)

	// Spoofed pillar must be rejected
	spoofedPillar, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active").WithPillar(core.PillarRunner))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", spoofedPillar)

	// Valid matching event must be ingested
	validEv, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active"))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", validEv)

	// Malformed registry topics must not panic and must be ignored
	sub.processRegistryUpdate("bad", []byte("ONLINE"))
	sub.processRegistryUpdate("pigeons/registry/bad+node/status", []byte("ONLINE"))
	sub.processRegistryUpdate("pigeons/registry/edge-01/status", []byte("ONLINE"))

	sub.batcher.Close()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.events) != 1 || ledger.events[0].NodeID != "edge-01" {
		t.Fatalf("expected exactly 1 valid non-spoofed event for edge-01, got %+v", ledger.events)
	}
	if ledger.statusByID["edge-01"] != "ONLINE" || len(ledger.statusByID) != 1 {
		t.Fatalf("unexpected registry statuses: %+v", ledger.statusByID)
	}
}
