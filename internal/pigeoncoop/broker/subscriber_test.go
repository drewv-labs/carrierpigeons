package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type mnsTrackingLedger struct {
	*memoryLedger
	mu        sync.Mutex
	upserts   []core.MeshIdentity
	upsertErr error
	notify    chan struct{}
}

func (m *mnsTrackingLedger) UpsertMeshIdentity(_ context.Context, id core.MeshIdentity) error {
	m.mu.Lock()
	err := m.upsertErr
	if err == nil {
		m.upserts = append(m.upserts, id)
	}
	m.mu.Unlock()
	if m.notify != nil {
		select {
		case m.notify <- struct{}{}:
		default:
		}
	}
	return err
}

func TestSubscriberMNSHeartbeatRefreshAndCleanStop(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	var (
		mu       sync.Mutex
		updates  []core.MeshIdentity
		offlines []meshLWT
	)
	notify := make(chan struct{}, 16)
	offlineCh := make(chan meshLWT, 4)

	obs := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("mns-heartbeat-observer"))
	if tok := obs.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("observer connect failed: %v", tok.Error())
	}
	defer obs.Disconnect(50)

	if tok := obs.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
		var lwt meshLWT
		if json.Unmarshal(m.Payload(), &lwt) == nil && lwt.Status == "OFFLINE" {
			mu.Lock()
			offlines = append(offlines, lwt)
			mu.Unlock()
			select {
			case offlineCh <- lwt:
			default:
			}
			return
		}
		var id core.MeshIdentity
		if json.Unmarshal(m.Payload(), &id) == nil && id.MeshName != "" {
			mu.Lock()
			updates = append(updates, id)
			mu.Unlock()
			select {
			case notify <- struct{}{}:
			default:
			}
		}
	}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("observer subscribe failed: %v", tok.Error())
	}

	ledger := &mnsTrackingLedger{
		memoryLedger: newMemoryLedger(),
		notify:       make(chan struct{}, 16),
	}
	sub, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:          emb.LocalClientURL(),
		MeshName:           "heartbeat-mesh",
		CoopNodeID:         "coop-hb-01",
		NtfyEnabled:        false,
		MNSRefreshInterval: 45 * time.Millisecond,
		Ingest: ingest.Options{
			QueueSize:     32,
			BatchSize:     4,
			FlushInterval: 20 * time.Millisecond,
		},
	}, ledger)
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		mqttCount := len(updates)
		mu.Unlock()
		ledger.mu.Lock()
		dbCount := len(ledger.upserts)
		ledger.mu.Unlock()
		if mqttCount >= 3 && dbCount >= 2 {
			break
		}
		select {
		case <-notify:
		case <-ledger.notify:
		case <-deadline:
			sub.Disconnect()
			t.Fatalf("expected >=3 MQTT MNS updates and >=2 Postgres MNS refreshes, got mqtt=%d db=%d", mqttCount, dbCount)
		}
	}

	mu.Lock()
	first := updates[0]
	last := updates[len(updates)-1]
	mu.Unlock()
	if !last.UpdatedAt.After(first.UpdatedAt) {
		sub.Disconnect()
		t.Fatalf("expected refreshed UpdatedAt to advance: first=%s last=%s", first.UpdatedAt, last.UpdatedAt)
	}

	ledger.mu.Lock()
	firstDB := ledger.upserts[0]
	lastDB := ledger.upserts[len(ledger.upserts)-1]
	ledger.mu.Unlock()
	if firstDB.MeshName != "heartbeat-mesh" || firstDB.CoopNodeID != "coop-hb-01" || firstDB.TopicRoot != "pigeons" {
		sub.Disconnect()
		t.Fatalf("unexpected Postgres MNS refresh identity: %+v", firstDB)
	}
	if !lastDB.UpdatedAt.After(firstDB.UpdatedAt) {
		sub.Disconnect()
		t.Fatalf("expected Postgres MNS UpdatedAt to advance: first=%s last=%s", firstDB.UpdatedAt, lastDB.UpdatedAt)
	}

	// Non-fatal DB error during MNS refresh must not crash or block MQTT refresh.
	ledger.mu.Lock()
	ledger.upsertErr = errors.New("simulated transient pg error")
	beforeErrMQTT := len(updates)
	ledger.mu.Unlock()
	select {
	case <-notify:
	case <-time.After(500 * time.Millisecond):
		sub.Disconnect()
		t.Fatal("expected MQTT MNS refresh to continue even when UpsertMeshIdentity fails")
	}
	mu.Lock()
	afterErrMQTT := len(updates)
	mu.Unlock()
	if afterErrMQTT <= beforeErrMQTT {
		sub.Disconnect()
		t.Fatalf("expected MQTT MNS updates to advance during DB error (%d -> %d)", beforeErrMQTT, afterErrMQTT)
	}

	sub.Disconnect()

	select {
	case lwt := <-offlineCh:
		if lwt.Status != "OFFLINE" || lwt.CoopNodeID != "coop-hb-01" {
			t.Fatalf("unexpected offline MNS payload: %+v", lwt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for offline MNS payload after Disconnect")
	}

	// Ensure no late heartbeat overwrites the retained offline payload.
	time.Sleep(120 * time.Millisecond)
	mu.Lock()
	afterCount := len(updates)
	offlineCount := len(offlines)
	mu.Unlock()
	if offlineCount == 0 {
		t.Fatal("expected at least one offline MNS message")
	}

	checkCli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("mns-retained-check"))
	if tok := checkCli.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("checkCli connect failed: %v", tok.Error())
	}
	defer checkCli.Disconnect(50)

	retainedCh := make(chan meshLWT, 1)
	if tok := checkCli.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
		var lwt meshLWT
		if json.Unmarshal(m.Payload(), &lwt) == nil {
			select {
			case retainedCh <- lwt:
			default:
			}
		}
	}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("checkCli subscribe failed: %v", tok.Error())
	}
	select {
	case retained := <-retainedCh:
		if retained.Status != "OFFLINE" {
			t.Fatalf("expected retained MNS state to remain OFFLINE after Disconnect (updates=%d), got %+v", afterCount, retained)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out reading retained MNS state")
	}
}

type failingBatchLedger struct {
	*memoryLedger
}

func (f *failingBatchLedger) BatchInsertEvents(_ context.Context, _ []*core.CTDPayload) (int64, error) {
	return 0, errors.New("simulated copy failure")
}

func TestSubscriberBatcherDroppedCountAndUniqueClientIDs(t *testing.T) {
	var nilSub Subscriber
	if got := nilSub.BatcherDroppedCount(); got != 0 {
		t.Fatalf("expected zero BatcherDroppedCount on nil batcher, got %d", got)
	}

	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	ledger1 := &failingBatchLedger{memoryLedger: newMemoryLedger()}
	sub1, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "drop-mesh",
		CoopNodeID:  "coop-01",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     8,
			BatchSize:     1,
			FlushInterval: 10 * time.Millisecond,
		},
	}, ledger1)
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions sub1 failed: %v", err)
	}
	defer sub1.Disconnect()

	sub2, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "drop-mesh",
		CoopNodeID:  "coop-01",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     8,
			BatchSize:     4,
			FlushInterval: 20 * time.Millisecond,
		},
	}, newMemoryLedger())
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions sub2 failed: %v", err)
	}
	defer sub2.Disconnect()

	r1 := sub1.client.OptionsReader()
	r2 := sub2.client.OptionsReader()
	id1 := r1.ClientID()
	id2 := r2.ClientID()
	if id1 == id2 {
		t.Fatalf("expected unique monotonic MQTT client IDs for concurrent subscribers, both got %q", id1)
	}
	if !sub1.client.IsConnected() || !sub2.client.IsConnected() {
		t.Fatal("expected both concurrent subscribers to remain connected without client_id collision")
	}

	ev := core.NewCTDPayload("edge-01", "host-os", "healthy")
	rawEv, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	sub1.processTelemetry(ev.RoutingTopic("pigeons/ctd"), rawEv)

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if sub1.BatcherDroppedCount() >= 1 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got := sub1.BatcherDroppedCount(); got < 1 {
		t.Fatalf("expected BatcherDroppedCount >= 1 after failing COPY retries, got %d", got)
	}
}

type blockingDBLedger struct {
	*memoryLedger
	entered atomic.Int32
	signal  chan struct{}
}

func (b *blockingDBLedger) markEntered() {
	if b.entered.Add(1) == 2 {
		close(b.signal)
	}
}

func (b *blockingDBLedger) UpsertNodeHello(ctx context.Context, _ core.NodeHello) ([]string, error) {
	b.markEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingDBLedger) ResolveNodes(ctx context.Context, _ core.RunnerSelector) ([]string, error) {
	b.markEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSubscriberDisconnectBoundedWhenDBBlocks(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	ledger := &blockingDBLedger{
		memoryLedger: newMemoryLedger(),
		signal:       make(chan struct{}),
	}
	sub, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "stalled-db-mesh",
		CoopNodeID:  "coop-stalled",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     32,
			BatchSize:     4,
			FlushInterval: 20 * time.Millisecond,
		},
	}, ledger)
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}

	pub := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("stalled-db-trigger"))
	if tok := pub.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		sub.Disconnect()
		t.Fatalf("trigger client connect failed: %v", tok.Error())
	}
	defer pub.Disconnect(50)

	rawHello, _ := json.Marshal(core.NodeHello{
		NodeID: "rv64-stalled-01",
		OS:     "linux",
		Arch:   "riscv64",
	})
	if tok := pub.Publish(core.HelloTopic("rv64-stalled-01"), 1, false, rawHello); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		sub.Disconnect()
		t.Fatalf("publish hello failed: %v", tok.Error())
	}

	rawReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID:   "req-stalled-01",
		Action:      "start",
		Runner:      "SyntheticComputeRnr",
		Selectors:   core.RunnerSelector{All: true},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	})
	if tok := pub.Publish(core.RunnerRequestTopic(), 1, false, rawReq); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		sub.Disconnect()
		t.Fatalf("publish runner request failed: %v", tok.Error())
	}

	select {
	case <-ledger.signal:
	case <-time.After(2 * time.Second):
		sub.Disconnect()
		t.Fatal("timed out waiting for control and runner workers to enter blocked DB calls")
	}

	start := time.Now()
	sub.Disconnect()
	elapsed := time.Since(start)

	if elapsed >= 6*time.Second {
		t.Fatalf("expected Disconnect() to complete in <6s when DB stalls, took %s", elapsed)
	}
	if !errors.Is(sub.workCtx.Err(), context.Canceled) {
		t.Fatalf("expected workCtx to be canceled after shutdown grace deadline, got %v", sub.workCtx.Err())
	}
}

func TestSubscriberDisconnectRaceUnderActiveNtfyAndRunnerRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(150 * time.Millisecond):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	emb, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	pub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(emb.LocalClientURL()).SetClientID("disconnect-race-pub"))
	if tok := pub.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("pub connect failed: %v", tok.Error())
	}
	defer pub.Disconnect(50)

	for iter := range 4 {
		ledger := newMemoryLedger()
		_, _ = ledger.UpsertNodeHello(context.Background(), core.NodeHello{
			NodeID: "edge-race-01",
			OS:     "linux",
			Arch:   "riscv64",
		})

		sub, err := NewSubscriberWithOptions(SubscriberOptions{
			BrokerURL:   emb.LocalClientURL(),
			MeshName:    "race-mesh",
			CoopNodeID:  fmt.Sprintf("coop-race-%d", iter),
			NtfyEnabled: true,
			NtfyURL:     srv.URL,
			Ingest: ingest.Options{
				QueueSize:     64,
				BatchSize:     4,
				FlushInterval: 10 * time.Millisecond,
			},
		}, ledger)
		if err != nil {
			t.Fatalf("iter %d NewSubscriberWithOptions failed: %v", iter, err)
		}

		var blastWG sync.WaitGroup
		blastWG.Add(3)
		go func() {
			defer blastWG.Done()
			for i := range 20 {
				nodeID := fmt.Sprintf("edge-race-%02d", i%5)
				_ = pub.Publish(core.RegistryTopic(nodeID), 1, false, []byte("OFFLINE")).WaitTimeout(200 * time.Millisecond)
			}
		}()
		go func() {
			defer blastWG.Done()
			for i := range 10 {
				rawReq, _ := json.Marshal(core.RunnerRequestCTD{
					RequestID:   fmt.Sprintf("req-race-%d-%d", iter, i),
					Action:      "start",
					Runner:      "SyntheticComputeRnr",
					Selectors:   core.RunnerSelector{Nodes: []string{"edge-race-01"}},
					RequestedBy: "operator",
					RequestedAt: time.Now().UTC(),
				})
				_ = pub.Publish(core.RunnerRequestTopic(), 1, false, rawReq).WaitTimeout(200 * time.Millisecond)
			}
		}()
		go func() {
			defer blastWG.Done()
			for i := range 30 {
				ev := core.NewCTDPayload("edge-race-01", "host-os", "active").AddMetric("seq", i)
				rawEv, _ := json.Marshal(ev)
				_ = pub.Publish(ev.RoutingTopic("pigeons/ctd"), 0, false, rawEv).WaitTimeout(200 * time.Millisecond)
			}
		}()

		time.Sleep(15 * time.Millisecond)
		sub.Disconnect()
		blastWG.Wait()
	}
}

func TestSubscriberFailFastInitialConnectError(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{
		ListenAddr: "127.0.0.1:0",
		Username:   "coop_admin",
		Password:   "correct_secret",
	})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	start := time.Now()
	sub, err := NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "auth-fail-mesh",
		CoopNodeID:  "coop-fail",
		Username:    "coop_admin",
		Password:    "wrong_secret",
		NtfyEnabled: false,
	}, newMemoryLedger())
	elapsed := time.Since(start)
	if err == nil {
		sub.Disconnect()
		t.Fatal("expected NewSubscriberWithOptions to fail with bad credentials")
	}
	if strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected fail-fast auth error instead of timeout, got %v", err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("expected initial connect failure to fail fast (<2s), took %s (err=%v)", elapsed, err)
	}
}

type orderRecordingLedger struct {
	*memoryLedger
	mu      sync.Mutex
	history []string
}

func (l *orderRecordingLedger) UpsertNodeHello(ctx context.Context, h core.NodeHello) ([]string, error) {
	l.mu.Lock()
	l.history = append(l.history, h.NodeID+":hello")
	l.mu.Unlock()
	return l.memoryLedger.UpsertNodeHello(ctx, h)
}

func (l *orderRecordingLedger) UpdateNodePresenceTransition(ctx context.Context, nodeID, status string) (string, bool, error) {
	l.mu.Lock()
	l.history = append(l.history, nodeID+":"+status)
	l.mu.Unlock()
	return l.memoryLedger.UpdateNodePresenceTransition(ctx, nodeID, status)
}

func TestSubscriberControlPerNodeStrictOrdering(t *testing.T) {
	ledger := &orderRecordingLedger{memoryLedger: newMemoryLedger()}
	ctrlQueues := make([]chan func(), controlWorkerCount)
	for i := range ctrlQueues {
		ctrlQueues[i] = make(chan func(), 64)
	}
	sub := &Subscriber{
		ledger:        ledger,
		batcher:       ingest.NewBatcher(ledger, ingest.Options{QueueSize: 16, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		brand:         brand.Default(),
		topicRoot:     "pigeons",
		telemetryStop: make(chan struct{}),
		ctrlQueues:    ctrlQueues,
		ctrlStop:      make(chan struct{}),
		runnerReqStop: make(chan struct{}),
	}
	for _, q := range sub.ctrlQueues {
		sub.ctrlWG.Add(1)
		go sub.controlWorker(q)
	}

	helloH := sub.enqueueControl(sub.processHelloUpdate)
	statusH := sub.enqueueControl(sub.processRegistryUpdate)

	rawHello, _ := json.Marshal(core.NodeHello{NodeID: "order-node-01", OS: "linux", Arch: "riscv64"})
	helloH(nil, &fakeMQTTMessage{topic: core.HelloTopic("order-node-01"), payload: rawHello})
	statusH(nil, &fakeMQTTMessage{topic: core.RegistryTopic("order-node-01"), payload: []byte("ONLINE")})
	statusH(nil, &fakeMQTTMessage{topic: core.RegistryTopic("order-node-01"), payload: []byte("OFFLINE")})
	statusH(nil, &fakeMQTTMessage{topic: core.RegistryTopic("order-node-01"), payload: []byte("ONLINE")})

	sub.Disconnect()

	want := []string{
		"order-node-01:hello",
		"order-node-01:ONLINE",
		"order-node-01:OFFLINE",
		"order-node-01:ONLINE",
	}
	ledger.mu.Lock()
	got := slices.Clone(ledger.history)
	ledger.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("expected strict per-node order %v, got %v", want, got)
	}
}

func TestProcessTelemetryValidationPoisoningAndByteBudget(t *testing.T) {
	ledger := newMemoryLedger()
	sub := &Subscriber{
		ledger:            ledger,
		batcher:           ingest.NewBatcher(ledger, ingest.Options{QueueSize: 32, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		brand:             brand.Default(),
		topicRoot:         "pigeons",
		telemetryQueue:    make(chan rawTelemetryMsg, 32),
		telemetryStop:     make(chan struct{}),
		telemetryMaxBytes: 120,
		ctrlStop:          make(chan struct{}),
		runnerReqStop:     make(chan struct{}),
	}

	// 1. Component mismatch must be dropped.
	compMismatch, _ := json.Marshal(core.NewCTDPayload("edge-01", "wrong-comp", "active"))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", compMismatch)

	// 2. Invalid pillar enum must be dropped.
	badPillarTopic, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active").WithPillar("invalid"))
	sub.processTelemetry("pigeons/ctd/invalid/edge-01/host-os", badPillarTopic)

	// 3. Cross-node Artifact.NodeID poisoning (different or empty) must be dropped.
	crossArtifact, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active").WithArtifact(core.Artifact{
		ID:     "art-cross",
		Kind:   "dataset",
		NodeID: "victim-node",
	}))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", crossArtifact)

	emptyArtifactNode, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active").WithArtifact(core.Artifact{
		ID:   "art-empty-node",
		Kind: "dataset",
	}))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", emptyArtifactNode)

	// 4. Edge runner-request:* artifact ID spoofing must be dropped.
	reservedArtifact, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active").WithArtifact(core.Artifact{
		ID:     "runner-request:req-hijack",
		Kind:   "runner_request",
		NodeID: "edge-01",
	}))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", reservedArtifact)

	reservedArtifactCase, _ := json.Marshal(core.NewCTDPayload("edge-01", "host-os", "active").WithArtifact(core.Artifact{
		ID:     " RUNNER-REQUEST:req-hijack-2",
		Kind:   "runner_request",
		NodeID: "edge-01",
	}))
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", reservedArtifactCase)

	// 5. Cross-node Trace.NodeID poisoning must be dropped.
	crossTrace := core.NewCTDPayload("edge-01", "host-os", "active").
		WithTrace("art:a", "transformed_into", "art:b", nil)
	crossTrace.Trace.NodeID = "victim-node"
	rawCrossTrace, _ := json.Marshal(crossTrace)
	sub.processTelemetry("pigeons/ctd/tracer/edge-01/host-os", rawCrossTrace)

	// 6. Valid artifact and valid trace with matching NodeID must be accepted.
	validArtEv := core.NewCTDPayload("edge-01", "host-os", "active").WithArtifact(core.Artifact{
		ID:     "ephemeral:abc123",
		Kind:   "ephemeral_runner",
		NodeID: "edge-01",
	})
	rawValidArt, _ := json.Marshal(validArtEv)
	sub.processTelemetry("pigeons/ctd/observer/edge-01/host-os", rawValidArt)

	validTraceEv := core.NewCTDPayload("edge-01", "tracer-comp", "active").
		WithTrace("art:1", "transformed_into", "art:2", nil)
	rawValidTrace, _ := json.Marshal(validTraceEv)
	sub.processTelemetry("pigeons/ctd/tracer/edge-01/tracer-comp", rawValidTrace)

	// 7. telemetryQueue byte budget enforcement (120 byte budget, no workers consuming yet).
	topic := "pigeons/ctd/observer/edge-01/host-os" // 36 bytes
	smallPayload := []byte(`{"node_id":"edge-01"}`) // 21 bytes -> 57 bytes total
	sub.handleTelemetry(nil, &fakeMQTTMessage{topic: topic, payload: smallPayload})
	sub.handleTelemetry(nil, &fakeMQTTMessage{topic: topic, payload: smallPayload}) // 114 <= 120
	sub.handleTelemetry(nil, &fakeMQTTMessage{topic: topic, payload: smallPayload}) // 171 > 120 -> dropped!
	if got := sub.TelemetryDroppedCount(); got != 1 {
		sub.Disconnect()
		t.Fatalf("expected 1 message dropped by telemetry byte budget, got %d", got)
	}

	sub.telemetryWG.Add(1)
	go sub.telemetryWorker()
	sub.Disconnect()

	if gotBytes := sub.telemetryBytes.Load(); gotBytes != 0 {
		t.Fatalf("expected telemetryBytes to drain back to 0, got %d", gotBytes)
	}

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.events) != 2 {
		t.Fatalf("expected exactly 2 valid events ingested, got %d (%+v)", len(ledger.events), ledger.events)
	}
}

type batchTraceLedger struct {
	*memoryLedger
	batchCalls atomic.Int32
	batched    []core.TraceEdge
}

func (b *batchTraceLedger) InsertTraces(_ context.Context, traces []core.TraceEdge) error {
	b.batchCalls.Add(1)
	b.mu.Lock()
	b.batched = append(b.batched, traces...)
	b.mu.Unlock()
	return nil
}

type slowTraceLedger struct {
	*memoryLedger
	delay time.Duration
}

func (s *slowTraceLedger) InsertTrace(ctx context.Context, tr core.TraceEdge) error {
	time.Sleep(s.delay)
	return s.memoryLedger.InsertTrace(ctx, tr)
}

func TestProcessRunnerRequestValidationDedupTargetsAndBatchedTraces(t *testing.T) {
	// 1. Invalid RequestID, RequestID dedup (TTL & LRU), and accurate published targets.
	baseLedger := newMemoryLedger()
	for _, id := range []string{"node-err", "node-ok", "node-slow"} {
		_, _ = baseLedger.UpsertNodeHello(context.Background(), core.NodeHello{
			NodeID: id,
			OS:     "linux",
			Arch:   "riscv64",
		})
	}
	pubClient := &fakePublishClient{
		handler: func(topic string, _ []byte) mqtt.Token {
			switch {
			case strings.HasSuffix(topic, "/node-ok"):
				return newCompletedToken(nil)
			case strings.HasSuffix(topic, "/node-err"):
				return newCompletedToken(errors.New("publish error"))
			default:
				return newHangingToken()
			}
		},
	}

	sub := &Subscriber{
		ledger:           baseLedger,
		batcher:          ingest.NewBatcher(baseLedger, ingest.Options{QueueSize: 16, BatchSize: 1, FlushInterval: 10 * time.Millisecond}),
		coopNodeID:       "coop-01",
		brand:            brand.Default(),
		topicRoot:        "pigeons",
		runnerPubTimeout: 60 * time.Millisecond,
		reqDedupMax:      2,
		reqDedupTTL:      80 * time.Millisecond,
	}

	// Invalid RequestID rejected.
	badReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID: "bad/req+id",
		Action:    "start",
		Runner:    "SyntheticComputeRnr",
		Selectors: core.RunnerSelector{Nodes: []string{"node-ok"}},
	})
	sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), badReq)
	if len(pubClient.pubs) != 0 {
		sub.batcher.Close()
		t.Fatalf("expected invalid RequestID to publish 0 times, got %d", len(pubClient.pubs))
	}

	// Invalid TargetRequestID rejected.
	badTargetReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID:       "req-abort-bad-target",
		TargetRequestID: "bad/target#id",
		Action:          "abort",
		Runner:          "SyntheticComputeRnr",
		Selectors:       core.RunnerSelector{Nodes: []string{"node-ok"}},
	})
	sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), badTargetReq)
	if len(pubClient.pubs) != 0 {
		sub.batcher.Close()
		t.Fatalf("expected invalid TargetRequestID to publish 0 times, got %d", len(pubClient.pubs))
	}

	// Valid request sent twice -> deduplicated, and artifact/event reflect ONLY node-ok.
	goodReq := core.RunnerRequestCTD{
		RequestID:   "req-dedup-1",
		Action:      "start",
		Runner:      "SyntheticComputeRnr",
		Selectors:   core.RunnerSelector{Nodes: []string{"node-err", "node-ok", "node-slow"}},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	}
	rawGood, _ := json.Marshal(goodReq)
	sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), rawGood)
	sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), rawGood)

	sub.batcher.Close()

	baseLedger.mu.Lock()
	if len(baseLedger.artifacts) != 1 {
		baseLedger.mu.Unlock()
		t.Fatalf("expected 1 artifact from deduplicated request, got %d", len(baseLedger.artifacts))
	}
	if gotTargets := baseLedger.artifacts[0].Meta["targets"]; gotTargets != "node-ok" {
		baseLedger.mu.Unlock()
		t.Fatalf("expected artifact targets to record actual published targets %q, got %q", "node-ok", gotTargets)
	}
	if len(baseLedger.events) != 1 {
		baseLedger.mu.Unlock()
		t.Fatalf("expected 1 request-dispatched event, got %d", len(baseLedger.events))
	}
	evTargets, _ := baseLedger.events[0].Metrics["targets"].([]string)
	if !slices.Equal(evTargets, []string{"node-ok"}) {
		baseLedger.mu.Unlock()
		t.Fatalf("expected request-dispatched targets [node-ok], got %#v", baseLedger.events[0].Metrics["targets"])
	}
	baseLedger.mu.Unlock()

	// TTL expiry allows reuse after TTL elapses.
	time.Sleep(100 * time.Millisecond)
	pubClient.mu.Lock()
	pubClient.pubs = nil
	pubClient.mu.Unlock()

	okOnlyReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID: "req-dedup-1",
		Action:    "start",
		Runner:    "SyntheticComputeRnr",
		Selectors: core.RunnerSelector{Nodes: []string{"node-ok"}},
	})
	sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), okOnlyReq)
	pubClient.mu.Lock()
	if len(pubClient.pubs) != 1 {
		pubClient.mu.Unlock()
		t.Fatalf("expected request after TTL expiry to dispatch once, got %d", len(pubClient.pubs))
	}
	pubClient.pubs = nil
	pubClient.mu.Unlock()

	// LRU eviction: reqDedupMax=2; inserting req-lru-2 and req-lru-3 evicts req-dedup-1.
	for _, id := range []string{"req-lru-2", "req-lru-3", "req-dedup-1"} {
		lruReq, _ := json.Marshal(core.RunnerRequestCTD{
			RequestID: id,
			Action:    "start",
			Runner:    "SyntheticComputeRnr",
			Selectors: core.RunnerSelector{Nodes: []string{"node-ok"}},
		})
		sub.processRunnerRequest(pubClient, core.RunnerRequestTopic(), lruReq)
	}
	pubClient.mu.Lock()
	if len(pubClient.pubs) != 3 {
		pubClient.mu.Unlock()
		t.Fatalf("expected LRU eviction to allow req-dedup-1 re-execution (3 publishes), got %d", len(pubClient.pubs))
	}
	pubClient.mu.Unlock()

	// 1b. TargetRequestID forwarded intact on runner.Envelope for targeted aborts.
	var (
		abortMu sync.Mutex
		gotEnv  runner.Envelope
	)
	abortPubClient := &fakePublishClient{
		handler: func(_ string, payload []byte) mqtt.Token {
			abortMu.Lock()
			_ = json.Unmarshal(payload, &gotEnv)
			abortMu.Unlock()
			return newCompletedToken(nil)
		},
	}
	abortReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID:       "req-abort-unique-1",
		TargetRequestID: "req-run-active-42",
		Action:          "abort",
		Runner:          "SyntheticComputeRnr",
		Selectors:       core.RunnerSelector{Nodes: []string{"node-ok"}},
		RequestedBy:     "operator",
		RequestedAt:     time.Now().UTC(),
	})
	sub.processRunnerRequest(abortPubClient, core.RunnerRequestTopic(), abortReq)
	abortPubClient.mu.Lock()
	pubCount := len(abortPubClient.pubs)
	abortPubClient.mu.Unlock()
	if pubCount != 1 {
		t.Fatalf("expected 1 abort envelope published, got %d", pubCount)
	}
	abortMu.Lock()
	capturedEnv := gotEnv
	abortMu.Unlock()
	if capturedEnv.RequestID != "req-abort-unique-1" || capturedEnv.TargetRequestID != "req-run-active-42" || capturedEnv.Action != "abort" {
		t.Fatalf("expected TargetRequestID forwarded on Envelope, got %+v", capturedEnv)
	}

	// 2. Optional TraceBatchLedger uses single batch call.
	bl := &batchTraceLedger{memoryLedger: newMemoryLedger()}
	subBatch := &Subscriber{
		ledger:           bl,
		coopNodeID:       "coop-01",
		brand:            brand.Default(),
		topicRoot:        "pigeons",
		runnerPubTimeout: 500 * time.Millisecond,
	}
	multiReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID: "req-batch-trace",
		Action:    "start",
		Runner:    "SyntheticComputeRnr",
		Selectors: core.RunnerSelector{Nodes: []string{"n1", "n2", "n3"}},
	})
	subBatch.processRunnerRequest(&fakePublishClient{}, core.RunnerRequestTopic(), multiReq)
	if bl.batchCalls.Load() != 1 || len(bl.batched) != 3 {
		t.Fatalf("expected 1 InsertTraces batch call with 3 edges, got calls=%d edges=%d", bl.batchCalls.Load(), len(bl.batched))
	}

	// 3. Fallback bounded parallel InsertTrace avoids N*RTT sequential stall.
	slow := &slowTraceLedger{memoryLedger: newMemoryLedger(), delay: 25 * time.Millisecond}
	subSlow := &Subscriber{
		ledger:           slow,
		coopNodeID:       "coop-01",
		brand:            brand.Default(),
		topicRoot:        "pigeons",
		runnerPubTimeout: 500 * time.Millisecond,
	}
	nodes16 := make([]string, 16)
	for i := range nodes16 {
		nodes16[i] = fmt.Sprintf("node-%02d", i)
	}
	slowReq, _ := json.Marshal(core.RunnerRequestCTD{
		RequestID: "req-parallel-trace",
		Action:    "start",
		Runner:    "SyntheticComputeRnr",
		Selectors: core.RunnerSelector{Nodes: nodes16},
	})
	start := time.Now()
	subSlow.processRunnerRequest(&fakePublishClient{}, core.RunnerRequestTopic(), slowReq)
	elapsed := time.Since(start)
	// Sequential 16 * 25ms = 400ms; 8 parallel workers = ~50ms.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected parallel trace inserts to avoid sequential 400ms stall, took %s", elapsed)
	}
	slow.mu.Lock()
	if len(slow.traces) != 16 {
		slow.mu.Unlock()
		t.Fatalf("expected 16 traces recorded, got %d", len(slow.traces))
	}
	slow.mu.Unlock()
}

type reaperMockLedger struct {
	*memoryLedger
	summaries []db.NodeSummary
}

func (r *reaperMockLedger) GetClusterStatus(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]db.NodeSummary, len(r.summaries))
	copy(out, r.summaries)
	return out, nil
}

func TestSubscriberStaleOnlineNodeReaper(t *testing.T) {
	now := time.Now().UTC()
	ledger := &reaperMockLedger{
		memoryLedger: newMemoryLedger(),
		summaries: []db.NodeSummary{
			{NodeID: "stale-online", Status: "ONLINE", LastSeen: now.Add(-10 * time.Minute)},
			{NodeID: "fresh-online", Status: "ONLINE", LastSeen: now},
			{NodeID: "paused-node", Status: "paused", LastSeen: now.Add(-10 * time.Minute)},
			{NodeID: "already-offline", Status: "OFFLINE", LastSeen: now.Add(-10 * time.Minute)},
		},
	}
	ledger.statusByID["stale-online"] = "ONLINE"
	ledger.statusByID["fresh-online"] = "ONLINE"
	ledger.statusByID["paused-node"] = "ONLINE"
	ledger.statusByID["already-offline"] = "OFFLINE"

	reaperCtx, reaperCancel := context.WithCancel(context.Background())
	sub := &Subscriber{
		ledger:             ledger,
		reaperCtx:          reaperCtx,
		reaperCancel:       reaperCancel,
		reaperStop:         make(chan struct{}),
		staleNodeInterval:  15 * time.Millisecond,
		staleNodeThreshold: 2 * time.Minute,
	}
	sub.reaperWG.Add(1)
	go sub.reaperLoop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ledger.mu.Lock()
		st := ledger.statusByID["stale-online"]
		ledger.mu.Unlock()
		if st == "OFFLINE" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	sub.Disconnect()

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.statusByID["stale-online"] != "OFFLINE" {
		t.Fatalf("expected stale-online to be reaped to OFFLINE, got %q", ledger.statusByID["stale-online"])
	}
	if ledger.statusByID["fresh-online"] != "ONLINE" {
		t.Fatalf("expected fresh-online to remain ONLINE, got %q", ledger.statusByID["fresh-online"])
	}
	if ledger.statusByID["paused-node"] != "ONLINE" {
		t.Fatalf("expected paused-node not to be marked OFFLINE, got %q", ledger.statusByID["paused-node"])
	}
}

type flakySubscribeClient struct {
	mu           sync.Mutex
	failSubUntil int
	subAttempts  int
	subEntered   chan struct{}
	subRelease   chan struct{}
	pubPayloads  [][]byte
	disconnected atomic.Bool
	onDisconnect func()
	subOnce      sync.Once
}

func (c *flakySubscribeClient) IsConnected() bool      { return !c.disconnected.Load() }
func (c *flakySubscribeClient) IsConnectionOpen() bool { return !c.disconnected.Load() }
func (c *flakySubscribeClient) Connect() mqtt.Token    { return newCompletedToken(nil) }
func (c *flakySubscribeClient) Disconnect(uint) {
	c.disconnected.Store(true)
	if c.onDisconnect != nil {
		c.onDisconnect()
	}
}
func (c *flakySubscribeClient) Publish(_ string, _ byte, _ bool, payload interface{}) mqtt.Token {
	c.mu.Lock()
	raw, _ := payload.([]byte)
	c.pubPayloads = append(c.pubPayloads, slices.Clone(raw))
	c.mu.Unlock()
	return newCompletedToken(nil)
}
func (c *flakySubscribeClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	c.mu.Lock()
	c.subAttempts++
	attempt := c.subAttempts
	c.mu.Unlock()
	if c.subEntered != nil {
		c.subOnce.Do(func() { close(c.subEntered) })
	}
	if c.subRelease != nil {
		<-c.subRelease
	}
	if attempt <= c.failSubUntil {
		return newCompletedToken(errors.New("transient subscribe failure"))
	}
	return newCompletedToken(nil)
}
func (c *flakySubscribeClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return newCompletedToken(nil)
}
func (c *flakySubscribeClient) Unsubscribe(...string) mqtt.Token     { return newCompletedToken(nil) }
func (c *flakySubscribeClient) AddRoute(string, mqtt.MessageHandler) {}
func (c *flakySubscribeClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

func TestSubscriberOnConnectRetryAndWaitBeforeOffline(t *testing.T) {
	// 1. Reconnect subscription retry: fails twice, succeeds on 3rd try, then announces MNS.
	cli := &flakySubscribeClient{failSubUntil: 2}
	sub := &Subscriber{
		meshName:      "retry-mesh",
		coopNodeID:    "coop-retry",
		brand:         brand.Default(),
		topicRoot:     "pigeons",
		mnsStop:       make(chan struct{}),
		telemetryStop: make(chan struct{}),
		ctrlStop:      make(chan struct{}),
		runnerReqStop: make(chan struct{}),
	}
	subs := []subTarget{{topic: "pigeons/ctd/#", handler: func(mqtt.Client, mqtt.Message) {}}}
	sub.handleOnConnect(cli, subs, 10*time.Millisecond, nil)

	cli.mu.Lock()
	attempts := cli.subAttempts
	pubs := len(cli.pubPayloads)
	cli.mu.Unlock()
	if attempts != 3 || pubs != 1 {
		t.Fatalf("expected 3 subscribe attempts and 1 MNS publish, got attempts=%d pubs=%d", attempts, pubs)
	}

	// 2. Disconnect must wait for active OnConnect goroutine before publishing OFFLINE MNS state.
	cliWait := &flakySubscribeClient{
		subEntered: make(chan struct{}),
		subRelease: make(chan struct{}),
	}
	subWait := &Subscriber{
		client:        cliWait,
		meshName:      "wg-mesh",
		coopNodeID:    "coop-wg",
		brand:         brand.Default(),
		topicRoot:     "pigeons",
		mnsStop:       make(chan struct{}),
		telemetryStop: make(chan struct{}),
		ctrlStop:      make(chan struct{}),
		runnerReqStop: make(chan struct{}),
	}
	go subWait.handleOnConnect(cliWait, subs, 10*time.Millisecond, nil)
	<-cliWait.subEntered

	disconnected := make(chan struct{})
	go func() {
		subWait.Disconnect()
		close(disconnected)
	}()

	// Give Disconnect time to stop mnsStop & block on onConnectWG.Wait(), then release Subscribe.
	time.Sleep(30 * time.Millisecond)
	close(cliWait.subRelease)

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Disconnect() after releasing OnConnect")
	}

	cliWait.mu.Lock()
	defer cliWait.mu.Unlock()
	if len(cliWait.pubPayloads) == 0 {
		t.Fatal("expected OFFLINE MNS publish on Disconnect()")
	}
	var last meshLWT
	if err := json.Unmarshal(cliWait.pubPayloads[len(cliWait.pubPayloads)-1], &last); err != nil {
		t.Fatalf("unmarshal final MNS publish: %v", err)
	}
	if last.Status != "OFFLINE" {
		t.Fatalf("expected final MNS publish to be OFFLINE (not overwritten by OnConnect), got %+v", last)
	}
}

type orderCheckBatchLedger struct {
	*memoryLedger
	onBatchInsert func()
}

func (l *orderCheckBatchLedger) BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error) {
	if l.onBatchInsert != nil {
		l.onBatchInsert()
	}
	return l.memoryLedger.BatchInsertEvents(ctx, payloads)
}

func TestSubscriberDisconnectStopsMQTTBeforeDrainingWorkersAndCountsPostStopDrops(t *testing.T) {
	var clientDisconnectedBeforeDrain atomic.Bool
	cli := &flakySubscribeClient{}

	ledger := &orderCheckBatchLedger{
		memoryLedger: newMemoryLedger(),
		onBatchInsert: func() {
			if cli.disconnected.Load() {
				clientDisconnectedBeforeDrain.Store(true)
			}
		},
	}

	var telemetryStopClosedAtDisconnect atomic.Bool
	ctrlQueues := make([]chan func(), 1)
	ctrlQueues[0] = make(chan func(), 8)
	sub := &Subscriber{
		client:         cli,
		ledger:         ledger,
		batcher:        ingest.NewBatcher(ledger, ingest.Options{QueueSize: 16, BatchSize: 10, FlushInterval: time.Hour}),
		meshName:       "order-mesh",
		coopNodeID:     "coop-order-01",
		brand:          brand.Default(),
		topicRoot:      "pigeons",
		mnsStop:        make(chan struct{}),
		telemetryQueue: make(chan rawTelemetryMsg, 16),
		telemetryStop:  make(chan struct{}),
		ctrlQueues:     ctrlQueues,
		ctrlStop:       make(chan struct{}),
		runnerReqQueue: make(chan rawRunnerReqMsg, 8),
		runnerReqStop:  make(chan struct{}),
	}
	cli.onDisconnect = func() {
		select {
		case <-sub.telemetryStop:
			telemetryStopClosedAtDisconnect.Store(true)
		default:
		}
	}

	sub.telemetryWG.Add(1)
	go sub.telemetryWorker()
	sub.ctrlWG.Add(1)
	go sub.controlWorker(ctrlQueues[0])
	sub.runnerReqWG.Add(1)
	go sub.runnerReqWorker()

	ev := core.NewCTDPayload("rv64-compute-01", "host-os", "active")
	rawEv, _ := json.Marshal(ev)
	sub.handleTelemetry(cli, &fakeMQTTMessage{topic: ev.RoutingTopic("pigeons/ctd"), payload: rawEv})

	sub.Disconnect()

	if telemetryStopClosedAtDisconnect.Load() {
		t.Fatal("expected client.Disconnect(250) to run BEFORE closing telemetryStop")
	}
	if !clientDisconnectedBeforeDrain.Load() {
		t.Fatal("expected client.Disconnect(250) to run BEFORE final batcher flush")
	}
	if sub.IngestedCount() != 1 {
		t.Fatalf("expected pre-disconnect telemetry event to drain cleanly, got %d", sub.IngestedCount())
	}

	// Late callbacks arriving post-stop must increment drop counters.
	ctrlHandler := sub.enqueueControl(sub.processRegistryUpdate)
	sub.handleTelemetry(cli, &fakeMQTTMessage{topic: ev.RoutingTopic("pigeons/ctd"), payload: rawEv})
	ctrlHandler(cli, &fakeMQTTMessage{topic: core.RegistryTopic("rv64-compute-01"), payload: []byte("ONLINE")})
	sub.handleRunnerRequest(cli, &fakeMQTTMessage{topic: core.RunnerRequestTopic(), payload: []byte(`{}`)})

	if got := sub.TelemetryDroppedCount(); got != 1 {
		t.Fatalf("expected TelemetryDroppedCount == 1 for post-stop callback, got %d", got)
	}
	if got := sub.ControlDroppedCount(); got != 1 {
		t.Fatalf("expected ControlDroppedCount == 1 for post-stop callback, got %d", got)
	}
	if got := sub.RunnerRequestDroppedCount(); got != 1 {
		t.Fatalf("expected RunnerRequestDroppedCount == 1 for post-stop callback, got %d", got)
	}
}
