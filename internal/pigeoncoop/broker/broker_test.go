package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
		SetClientID("pigeoneer-edge-01").
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

type fakeMQTTMessage struct {
	topic   string
	payload []byte
}

func (m *fakeMQTTMessage) Duplicate() bool   { return false }
func (m *fakeMQTTMessage) Qos() byte         { return 1 }
func (m *fakeMQTTMessage) Retained() bool    { return false }
func (m *fakeMQTTMessage) Topic() string     { return m.topic }
func (m *fakeMQTTMessage) MessageID() uint16 { return 1 }
func (m *fakeMQTTMessage) Payload() []byte   { return m.payload }
func (m *fakeMQTTMessage) Ack()              {}

type fakeToken struct {
	done chan struct{}
	err  error
}

func newCompletedToken(err error) *fakeToken {
	ch := make(chan struct{})
	close(ch)
	return &fakeToken{done: ch, err: err}
}

func newHangingToken() *fakeToken {
	return &fakeToken{done: make(chan struct{})}
}

func (t *fakeToken) Wait() bool {
	<-t.done
	return true
}

func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-t.done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-t.done:
		return true
	case <-timer.C:
		return false
	}
}

func (t *fakeToken) Done() <-chan struct{} { return t.done }
func (t *fakeToken) Error() error          { return t.err }

type fakePublishClient struct {
	mu      sync.Mutex
	handler func(topic string, payload []byte) mqtt.Token
	pubs    []string
}

func (c *fakePublishClient) IsConnected() bool      { return true }
func (c *fakePublishClient) IsConnectionOpen() bool { return true }
func (c *fakePublishClient) Connect() mqtt.Token    { return newCompletedToken(nil) }
func (c *fakePublishClient) Disconnect(uint)        {}
func (c *fakePublishClient) Publish(topic string, _ byte, _ bool, payload interface{}) mqtt.Token {
	c.mu.Lock()
	c.pubs = append(c.pubs, topic)
	c.mu.Unlock()
	raw, _ := payload.([]byte)
	if c.handler != nil {
		return c.handler(topic, raw)
	}
	return newCompletedToken(nil)
}
func (c *fakePublishClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	return newCompletedToken(nil)
}
func (c *fakePublishClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return newCompletedToken(nil)
}
func (c *fakePublishClient) Unsubscribe(...string) mqtt.Token      { return newCompletedToken(nil) }
func (c *fakePublishClient) AddRoute(string, mqtt.MessageHandler)  {}
func (c *fakePublishClient) OptionsReader() mqtt.ClientOptionsReader { return mqtt.ClientOptionsReader{} }

type traceCheckingLedger struct {
	*memoryLedger
	resolveDelay time.Duration
	traceCTXs    []context.Context
}

func (l *traceCheckingLedger) ResolveNodes(ctx context.Context, sel core.RunnerSelector) ([]string, error) {
	if l.resolveDelay > 0 {
		time.Sleep(l.resolveDelay)
	}
	return l.memoryLedger.ResolveNodes(ctx, sel)
}

func (l *traceCheckingLedger) InsertTrace(ctx context.Context, tr core.TraceEdge) error {
	l.mu.Lock()
	l.traceCTXs = append(l.traceCTXs, ctx)
	l.mu.Unlock()
	return l.memoryLedger.InsertTrace(ctx, tr)
}

func TestWaitTokenTimeoutTreatsTimeoutAsError(t *testing.T) {
	hanging := newHangingToken()
	err := waitTokenTimeout(hanging, 15*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error when WaitTimeout returns false, got %v", err)
	}

	okTok := newCompletedToken(nil)
	if err := waitTokenTimeout(okTok, 15*time.Millisecond); err != nil {
		t.Fatalf("expected nil for completed token, got %v", err)
	}

	errTok := newCompletedToken(errors.New("publish rejected"))
	if err := waitTokenTimeout(errTok, 15*time.Millisecond); err == nil || !strings.Contains(err.Error(), "publish rejected") {
		t.Fatalf("expected publish rejected error, got %v", err)
	}
}

func TestSubscriberQueueCapsAndDropCounters(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	subLive, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "cap-check",
		CoopNodeID:  "coop-cap",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     16384,
			BatchSize:     64,
			FlushInterval: 20 * time.Millisecond,
		},
	}, newMemoryLedger())
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}
	if got := cap(subLive.telemetryQueue); got != 4096 {
		subLive.Disconnect()
		t.Fatalf("expected default telemetryQueue cap 4096 even with Ingest.QueueSize=16384, got %d", got)
	}
	subLive.Disconnect()

	// Saturate telemetryQueue, ctrlQueues, and runnerReqQueue deterministically without workers draining them.
	sub := &Subscriber{
		brand:          brand.Default(),
		topicRoot:      "pigeons",
		telemetryQueue: make(chan rawTelemetryMsg, 2),
		telemetryStop:  make(chan struct{}),
		ctrlQueues:     []chan func(){make(chan func(), 2)},
		ctrlStop:       make(chan struct{}),
		runnerReqQueue: make(chan rawRunnerReqMsg, 2),
		runnerReqStop:  make(chan struct{}),
	}

	for range 5 {
		sub.handleTelemetry(nil, &fakeMQTTMessage{topic: "pigeons/ctd/observer/n1/host-os", payload: []byte("{}")})
	}
	if got := sub.TelemetryDroppedCount(); got != 3 {
		t.Fatalf("expected 3 dropped telemetry messages, got %d", got)
	}

	ctrlHandler := sub.enqueueControl(func(string, []byte) {})
	for range 6 {
		ctrlHandler(nil, &fakeMQTTMessage{topic: "pigeons/registry/n1/status", payload: []byte("ONLINE")})
	}
	if got := sub.ControlDroppedCount(); got != 4 {
		t.Fatalf("expected 4 dropped control messages, got %d", got)
	}

	for range 4 {
		sub.handleRunnerRequest(nil, &fakeMQTTMessage{topic: "pigeons/requests/runner", payload: []byte("{}")})
	}
	if got := sub.RunnerRequestDroppedCount(); got != 2 {
		t.Fatalf("expected 2 dropped runner request messages, got %d", got)
	}
}

func TestRunnerRequestIsolationUnderControlFlood(t *testing.T) {
	ledger := newMemoryLedger()
	_, _ = ledger.UpsertNodeHello(context.Background(), core.NodeHello{
		NodeID: "rv64-compute-01",
		OS:     "linux",
		Arch:   "riscv64",
	})

	gate := make(chan struct{})
	pubClient := &fakePublishClient{}
	dispatched := make(chan runner.Envelope, 2)
	pubClient.handler = func(_ string, payload []byte) mqtt.Token {
		var env runner.Envelope
		if err := json.Unmarshal(payload, &env); err == nil {
			dispatched <- env
		}
		return newCompletedToken(nil)
	}

	sub := &Subscriber{
		ledger:           ledger,
		batcher:          ingest.NewBatcher(ledger, ingest.Options{QueueSize: 16, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		coopNodeID:       "coop-01",
		brand:            brand.Default(),
		topicRoot:        "pigeons",
		telemetryStop:    make(chan struct{}),
		ctrlQueues:       []chan func(){make(chan func(), 2)},
		ctrlStop:         make(chan struct{}),
		runnerReqQueue:   make(chan rawRunnerReqMsg, 8),
		runnerReqStop:    make(chan struct{}),
		runnerPubTimeout: 500 * time.Millisecond,
	}
	sub.ctrlWG.Add(1)
	go sub.controlWorker(sub.ctrlQueues[0])
	sub.runnerReqWG.Add(1)
	go sub.runnerReqWorker()

	// Block the single control worker and saturate ctrlQueue with hello/status flood.
	sub.ctrlQueues[0] <- func() { <-gate }
	ctrlHandler := sub.enqueueControl(func(string, []byte) {})
	for range 10 {
		ctrlHandler(nil, &fakeMQTTMessage{topic: "pigeons/registry/flood/status", payload: []byte("ONLINE")})
	}
	if sub.ControlDroppedCount() == 0 {
		close(gate)
		sub.Disconnect()
		t.Fatal("expected control queue flood to drop excess status messages")
	}

	// Operator abort RunnerRequestCTD MUST still be accepted and dispatched immediately via isolated runnerReqQueue.
	abortReq := core.RunnerRequestCTD{
		RequestID:   "req-abort-urgent",
		Action:      "abort",
		Runner:      "SyntheticComputeRnr",
		Selectors:   core.RunnerSelector{Nodes: []string{"rv64-compute-01"}},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	}
	rawAbort, _ := json.Marshal(abortReq)
	sub.handleRunnerRequest(pubClient, &fakeMQTTMessage{
		topic:   core.RunnerRequestTopic(),
		payload: rawAbort,
	})

	select {
	case env := <-dispatched:
		if env.RequestID != "req-abort-urgent" || env.Action != "abort" {
			close(gate)
			sub.Disconnect()
			t.Fatalf("unexpected dispatched envelope: %+v", env)
		}
	case <-time.After(1 * time.Second):
		close(gate)
		sub.Disconnect()
		t.Fatal("abort RunnerRequestCTD was starved/dropped by control queue flood")
	}

	close(gate)
	sub.Disconnect()
}

func TestProcessRunnerRequestSharedDeadlineAndTraceFiltering(t *testing.T) {
	baseLedger := newMemoryLedger()
	for _, id := range []string{"node-err", "node-ok", "node-slow"} {
		_, _ = baseLedger.UpsertNodeHello(context.Background(), core.NodeHello{
			NodeID: id,
			OS:     "linux",
			Arch:   "riscv64",
		})
	}
	ledger := &traceCheckingLedger{
		memoryLedger: baseLedger,
		resolveDelay: 25 * time.Millisecond,
	}

	pubClient := &fakePublishClient{
		handler: func(topic string, _ []byte) mqtt.Token {
			switch {
			case strings.HasSuffix(topic, "/node-ok"):
				return newCompletedToken(nil)
			case strings.HasSuffix(topic, "/node-err"):
				return newCompletedToken(errors.New("mqtt publish error"))
			default:
				return newHangingToken()
			}
		},
	}

	sub := &Subscriber{
		ledger:           ledger,
		batcher:          ingest.NewBatcher(ledger, ingest.Options{QueueSize: 16, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		coopNodeID:       "coop-01",
		brand:            brand.Default(),
		topicRoot:        "pigeons",
		runnerPubTimeout: 80 * time.Millisecond,
	}
	defer sub.batcher.Close()

	req := core.RunnerRequestCTD{
		RequestID:   "req-fanout-01",
		Action:      "start",
		Runner:      "SyntheticComputeRnr",
		Selectors:   core.RunnerSelector{Nodes: []string{"node-err", "node-ok", "node-slow"}},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	}
	rawReq, _ := json.Marshal(req)

	start := time.Now()
	sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), rawReq)
	elapsed := time.Since(start)

	if elapsed > 400*time.Millisecond {
		t.Fatalf("expected shared 80ms fan-out deadline, took %s", elapsed)
	}

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.traces) != 1 {
		t.Fatalf("expected trace edge ONLY for node-ok (1 trace), got %d (%+v)", len(ledger.traces), ledger.traces)
	}
	if ledger.traces[0].NodeID != "node-ok" || ledger.traces[0].Verb != "dispatched_to" {
		t.Fatalf("unexpected trace edge: %+v", ledger.traces[0])
	}
	if len(ledger.traceCTXs) != 1 {
		t.Fatalf("expected 1 trace context, got %d", len(ledger.traceCTXs))
	}
	deadline, hasDeadline := ledger.traceCTXs[0].Deadline()
	if !hasDeadline || time.Until(deadline) < 3*time.Second {
		t.Fatalf("expected dedicated bounded ~5s DB context for trace writes, got hasDeadline=%v remaining=%s",
			hasDeadline, time.Until(deadline))
	}
}

func TestSubscriberCoopMNSOfflineOnDisconnectAndLWT(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	sub, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "rv64-lab",
		CoopNodeID:  "rv64-coop-01",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     32,
			BatchSize:     4,
			FlushInterval: 20 * time.Millisecond,
		},
	}, newMemoryLedger())
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}

	// Verify LWT is configured on <root>/registry/mesh with retained offline payload.
	optReader := sub.client.OptionsReader()
	if !optReader.WillEnabled() || optReader.WillTopic() != core.MeshTopic() || !optReader.WillRetained() || optReader.WillQos() != 1 {
		sub.Disconnect()
		t.Fatalf("unexpected MNS LWT config: enabled=%v topic=%q retained=%v qos=%d",
			optReader.WillEnabled(), optReader.WillTopic(), optReader.WillRetained(), optReader.WillQos())
	}
	var willPayload meshLWT
	if err := json.Unmarshal(optReader.WillPayload(), &willPayload); err != nil {
		sub.Disconnect()
		t.Fatalf("invalid MNS LWT JSON payload: %v", err)
	}
	if willPayload.MeshName != "" || willPayload.Status != "OFFLINE" || willPayload.CoopNodeID != "rv64-coop-01" || willPayload.UpdatedAt.IsZero() {
		sub.Disconnect()
		t.Fatalf("unexpected MNS LWT payload: %+v", willPayload)
	}

	// Verify live retained MNS announcement first.
	cli1 := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(emb.LocalClientURL()).SetClientID("mns-check-1"))
	if tok := cli1.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		sub.Disconnect()
		t.Fatalf("cli1 connect failed: %v", tok.Error())
	}
	liveCh := make(chan core.MeshIdentity, 1)
	if tok := cli1.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
		var id core.MeshIdentity
		if json.Unmarshal(m.Payload(), &id) == nil && id.MeshName != "" {
			select {
			case liveCh <- id:
			default:
			}
		}
	}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		cli1.Disconnect(50)
		sub.Disconnect()
		t.Fatalf("cli1 subscribe failed: %v", tok.Error())
	}
	select {
	case id := <-liveCh:
		if id.MeshName != "rv64-lab" || id.UpdatedAt.IsZero() {
			cli1.Disconnect(50)
			sub.Disconnect()
			t.Fatalf("unexpected live MeshIdentity: %+v", id)
		}
	case <-time.After(2 * time.Second):
		cli1.Disconnect(50)
		sub.Disconnect()
		t.Fatal("timed out waiting for live MeshIdentity")
	}
	cli1.Disconnect(50)

	// Graceful Disconnect must mark <root>/registry/mesh offline with populated UpdatedAt.
	sub.Disconnect()

	cli2 := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(emb.LocalClientURL()).SetClientID("mns-check-2"))
	if tok := cli2.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("cli2 connect failed: %v", tok.Error())
	}
	defer cli2.Disconnect(50)

	offlineCh := make(chan meshLWT, 1)
	if tok := cli2.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
		var lwt meshLWT
		if json.Unmarshal(m.Payload(), &lwt) == nil {
			select {
			case offlineCh <- lwt:
			default:
			}
		}
	}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("cli2 subscribe failed: %v", tok.Error())
	}

	select {
	case lwt := <-offlineCh:
		if lwt.MeshName != "" || lwt.Status != "OFFLINE" || lwt.CoopNodeID != "rv64-coop-01" || lwt.TopicRoot != "pigeons" || lwt.UpdatedAt.IsZero() {
			t.Fatalf("unexpected offline MNS retained payload: %+v", lwt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for offline MNS retained payload after Disconnect")
	}
}

func TestRegistryNtfyTrackedAndCanceledOnDisconnect(t *testing.T) {
	reqStarted := make(chan struct{})
	srvRelease := make(chan struct{})
	var reqCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqCount.Add(1) == 1 {
			close(reqStarted)
		}
		select {
		case <-r.Context().Done():
		case <-srvRelease:
		}
		w.WriteHeader(http.StatusRequestTimeout)
	}))
	defer func() {
		close(srvRelease)
		srv.CloseClientConnections()
		srv.Close()
	}()

	ntfyCtx, ntfyCancel := context.WithCancel(context.Background())
	ledger := newMemoryLedger()
	sub := &Subscriber{
		ledger:        ledger,
		batcher:       ingest.NewBatcher(ledger, ingest.Options{QueueSize: 16, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		brand:         brand.Default(),
		topicRoot:     "pigeons",
		ntfyEnabled:   true,
		ntfyURL:       srv.URL,
		ntfySem:       make(chan struct{}, ntfyMaxConcurrency),
		ntfyCtx:       ntfyCtx,
		ntfyCancel:    ntfyCancel,
		telemetryStop: make(chan struct{}),
		ctrlStop:      make(chan struct{}),
		runnerReqStop: make(chan struct{}),
	}

	sub.processRegistryUpdate("pigeons/registry/rv64-edge-01/status", []byte("OFFLINE"))

	select {
	case <-reqStarted:
	case <-time.After(2 * time.Second):
		sub.Disconnect()
		t.Fatal("timed out waiting for sendNtfyAlert HTTP request")
	}

	if len(sub.ntfySem) != 1 {
		sub.Disconnect()
		t.Fatalf("expected 1 in-flight ntfy goroutine holding ntfySem, got %d", len(sub.ntfySem))
	}

	start := time.Now()
	sub.Disconnect()
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Fatalf("expected Disconnect() to cancel in-flight Ntfy alert promptly, took %s", elapsed)
	}
	if !errors.Is(sub.ntfyCtx.Err(), context.Canceled) {
		t.Fatalf("expected ntfyCtx to be canceled, got %v", sub.ntfyCtx.Err())
	}
	if len(sub.ntfySem) != 0 {
		t.Fatalf("expected Disconnect() to wait for ntfyWG before returning (ntfySem len=%d)", len(sub.ntfySem))
	}
}
