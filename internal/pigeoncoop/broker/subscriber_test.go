package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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
	}, newMemoryLedger())
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		count := len(updates)
		mu.Unlock()
		if count >= 3 {
			break
		}
		select {
		case <-notify:
		case <-deadline:
			sub.Disconnect()
			t.Fatalf("expected >=3 MNS heartbeat updates, got %d", count)
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
