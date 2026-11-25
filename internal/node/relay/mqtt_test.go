package relay

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type fakeToken struct {
	done chan struct{}
	err  error
}

func newCompletedToken(err error) *fakeToken {
	ch := make(chan struct{})
	close(ch)
	return &fakeToken{done: ch, err: err}
}

func newStalledToken() *fakeToken {
	return &fakeToken{done: make(chan struct{})}
}

func (t *fakeToken) Wait() bool {
	<-t.done
	return true
}

func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-t.done:
		return true
	case <-timer.C:
		return false
	}
}

func (t *fakeToken) Done() <-chan struct{} {
	return t.done
}

func (t *fakeToken) Error() error {
	return t.err
}

type fakeMessage struct {
	topic   string
	payload []byte
}

func (m fakeMessage) Duplicate() bool   { return false }
func (m fakeMessage) Qos() byte         { return 1 }
func (m fakeMessage) Retained() bool    { return false }
func (m fakeMessage) Topic() string     { return m.topic }
func (m fakeMessage) MessageID() uint16 { return 1 }
func (m fakeMessage) Payload() []byte   { return m.payload }
func (m fakeMessage) Ack()              {}

type fakeMQTTClient struct {
	connected atomic.Bool

	mu          sync.Mutex
	published   []fakePublish
	subHandler  mqtt.MessageHandler
	subTopic    string
	publishHook func(topic string, payload []byte) mqtt.Token
	subHook     func(topic string) mqtt.Token
}

type fakePublish struct {
	topic   string
	payload []byte
}

func (f *fakeMQTTClient) IsConnected() bool      { return f.connected.Load() }
func (f *fakeMQTTClient) IsConnectionOpen() bool { return f.connected.Load() }
func (f *fakeMQTTClient) Connect() mqtt.Token    { return newCompletedToken(nil) }
func (f *fakeMQTTClient) Disconnect(_ uint)      { f.connected.Store(false) }

func (f *fakeMQTTClient) Publish(topic string, _ byte, _ bool, payload interface{}) mqtt.Token {
	var raw []byte
	switch v := payload.(type) {
	case []byte:
		raw = append([]byte(nil), v...)
	case string:
		raw = []byte(v)
	}
	f.mu.Lock()
	f.published = append(f.published, fakePublish{topic: topic, payload: raw})
	hook := f.publishHook
	f.mu.Unlock()
	if hook != nil {
		return hook(topic, raw)
	}
	return newCompletedToken(nil)
}

func (f *fakeMQTTClient) Subscribe(topic string, _ byte, callback mqtt.MessageHandler) mqtt.Token {
	f.mu.Lock()
	f.subTopic = topic
	f.subHandler = callback
	hook := f.subHook
	f.mu.Unlock()
	if hook != nil {
		return hook(topic)
	}
	return newCompletedToken(nil)
}

func (f *fakeMQTTClient) SubscribeMultiple(_ map[string]byte, _ mqtt.MessageHandler) mqtt.Token {
	return newCompletedToken(nil)
}

func (f *fakeMQTTClient) Unsubscribe(_ ...string) mqtt.Token       { return newCompletedToken(nil) }
func (f *fakeMQTTClient) AddRoute(_ string, _ mqtt.MessageHandler) {}
func (f *fakeMQTTClient) OptionsReader() mqtt.ClientOptionsReader  { return mqtt.ClientOptionsReader{} }

func (f *fakeMQTTClient) publishCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func (f *fakeMQTTClient) snapshotPublished() []fakePublish {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakePublish(nil), f.published...)
}

func (f *fakeMQTTClient) handler() mqtt.MessageHandler {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subHandler
}

func newTestRelay(client mqtt.Client, spoolCap int) *MQTTClient {
	bSpec := brand.Active().MergeWithDefaults()
	c := &MQTTClient{
		client:       client,
		nodeID:       "rv64-test-01",
		topicPrefix:  bSpec.CTDPrefix(),
		brand:        bSpec,
		runnerPolicy: runner.DefaultExecutionPolicy(),
		runnerReg:    runner.NewRegistry(),
		pauseCtrl:    observer.NewPauseController(),
		spool:        NewRingSpool(spoolCap),
		stopCh:       make(chan struct{}),
		flushDone:    make(chan struct{}),
	}
	return c
}

type capturePublisher struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (p *capturePublisher) Publish(e *core.CTDPayload) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
	return nil
}

func (p *capturePublisher) snapshot() []*core.CTDPayload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*core.CTDPayload(nil), p.events...)
}

type funcRunner struct {
	name string
	fn   func(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error
}

func (r *funcRunner) Name() string { return r.name }
func (r *funcRunner) Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
	return r.fn(ctx, nodeID, params, pub)
}

func TestMQTTClientDisconnectFastUnderBlackhole(t *testing.T) {
	mc := &fakeMQTTClient{
		publishHook: func(_ string, _ []byte) mqtt.Token {
			return newStalledToken()
		},
	}
	mc.connected.Store(true)

	c := newTestRelay(mc, 256)
	for i := range maxPipelinedPublishes {
		_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active").AddMetric("seq", i))
	}
	go c.flushLoop()

	deadline := time.Now().Add(2 * time.Second)
	for mc.publishCount() < maxPipelinedPublishes && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if mc.publishCount() < maxPipelinedPublishes {
		t.Fatalf("expected %d in-flight publishes before Disconnect, got %d", maxPipelinedPublishes, mc.publishCount())
	}

	start := time.Now()
	c.Disconnect()
	elapsed := time.Since(start)
	if elapsed >= 3*time.Second {
		t.Fatalf("expected Disconnect() under stalled tokens to finish in <3s, took %s", elapsed)
	}
}

func TestMQTTClientPoisonPayloadOversizeDropped(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	go c.flushLoop()
	defer c.Disconnect()

	oversize := core.NewCTDPayload("rv64-test-01", "oversize", "active").
		AddMetric("blob", strings.Repeat("a", maxMQTTPayloadBytes+1024))
	normal := core.NewCTDPayload("rv64-test-01", "healthy", "active").
		AddMetric("ok", true)

	_ = c.Publish(oversize)
	_ = c.Publish(normal)

	deadline := time.Now().Add(2 * time.Second)
	for (c.DeliveryDropped() < 1 || mc.publishCount() < 1) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := c.DeliveryDropped(); got != 1 {
		t.Fatalf("expected DeliveryDropped=1 for >1MiB payload, got %d", got)
	}
	pubs := mc.snapshotPublished()
	if len(pubs) != 1 || !strings.HasSuffix(pubs[0].topic, "/healthy") {
		t.Fatalf("expected only healthy payload published, got %+v", pubs)
	}
}

func TestMQTTClientPoisonPayloadRepeatedFailuresDropped(t *testing.T) {
	var poisonAttempts atomic.Int32
	mc := &fakeMQTTClient{
		publishHook: func(topic string, _ []byte) mqtt.Token {
			if strings.HasSuffix(topic, "/poison") {
				poisonAttempts.Add(1)
				return newCompletedToken(errors.New("broker rejected poison message"))
			}
			return newCompletedToken(nil)
		},
	}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	c.backoffBase = 5 * time.Millisecond
	c.backoffMax = 20 * time.Millisecond
	c.maxAttempts = 5
	go c.flushLoop()
	defer c.Disconnect()

	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "poison", "active"))
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "healthy", "active"))

	deadline := time.Now().Add(2 * time.Second)
	for c.DeliveryDropped() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.DeliveryDropped(); got != 1 {
		t.Fatalf("expected DeliveryDropped=1 after 5 publish failures, got %d", got)
	}
	if got := poisonAttempts.Load(); got != 5 {
		t.Fatalf("expected 5 poison publish attempts, got %d", got)
	}

	pubs := mc.snapshotPublished()
	foundHealthy := false
	for _, p := range pubs {
		if strings.HasSuffix(p.topic, "/healthy") {
			foundHealthy = true
		}
	}
	if !foundHealthy {
		t.Fatalf("expected healthy payload in batch to succeed, published=%d", len(pubs))
	}
}

func TestMQTTClientDisconnectedDoesNotDropPayloads(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(false)

	c := newTestRelay(mc, 16)
	c.backoffBase = 5 * time.Millisecond
	c.backoffMax = 10 * time.Millisecond
	go c.flushLoop()

	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active"))
	time.Sleep(50 * time.Millisecond)

	// While disconnected and running, payloads must remain spooled and never be dropped.
	if got := c.DeliveryDroppedCount(); got != 0 {
		t.Fatalf("expected 0 drops while disconnected and running, got %d", got)
	}

	// On shutdown while still disconnected, remaining spooled items are accounted as dropped.
	c.Disconnect()

	if got := c.DeliveryDroppedCount(); got != 1 {
		t.Fatalf("expected DeliveryDroppedCount=1 on disconnected shutdown, got %d", got)
	}
	if got := c.SpoolLen(); got != 0 {
		t.Fatalf("expected spool drained on shutdown, got len=%d", got)
	}
}

func TestMQTTClientWholeBatchStallDoesNotDropHealthyPayloads(t *testing.T) {
	var allowSuccess atomic.Bool
	mc := &fakeMQTTClient{
		publishHook: func(_ string, _ []byte) mqtt.Token {
			if allowSuccess.Load() {
				return newCompletedToken(nil)
			}
			return newStalledToken()
		},
	}
	mc.connected.Store(true)

	c := newTestRelay(mc, 32)
	c.batchTimeout = 10 * time.Millisecond
	c.backoffBase = 5 * time.Millisecond
	c.backoffMax = 10 * time.Millisecond
	c.maxAttempts = 5

	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active"))
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "mem", "active"))
	go c.flushLoop()
	defer c.Disconnect()

	// Wait for at least 7 consecutive whole-batch stalled timeouts (> maxAttempts=5) while connected == true.
	deadline := time.Now().Add(2 * time.Second)
	for mc.publishCount() < 14 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if mc.publishCount() < 14 {
		t.Fatalf("expected >=14 stalled publish attempts (7+ batch timeouts), got %d", mc.publishCount())
	}
	if got := c.DeliveryDropped(); got != 0 {
		t.Fatalf("expected DeliveryDropped=0 on whole-batch stall while connected, got %d", got)
	}

	// Once the link recovers, both payloads should deliver cleanly without any drops.
	allowSuccess.Store(true)
	deadline = time.Now().Add(2 * time.Second)
	for c.SpoolLen() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.DeliveryDropped(); got != 0 {
		t.Fatalf("expected DeliveryDropped=0 after blackhole recovery, got %d", got)
	}
}

func TestMQTTClientPartialBatchTimeoutPreservesDoneTokens(t *testing.T) {
	var healthyDelivered atomic.Int32
	var stalledAttempts atomic.Int32
	var recoverStalled atomic.Bool
	mc := &fakeMQTTClient{
		publishHook: func(topic string, _ []byte) mqtt.Token {
			if strings.HasSuffix(topic, "/stalled") {
				if recoverStalled.Load() {
					return newCompletedToken(nil)
				}
				stalledAttempts.Add(1)
				return newStalledToken()
			}
			healthyDelivered.Add(1)
			return newCompletedToken(nil)
		},
	}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	c.batchTimeout = 15 * time.Millisecond
	c.backoffBase = 5 * time.Millisecond
	c.backoffMax = 10 * time.Millisecond
	c.maxAttempts = 5

	// First token stalls (triggers timer.C); second token completes immediately under OrderMatters=false.
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "stalled", "active"))
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "healthy", "active"))
	go c.flushLoop()
	defer c.Disconnect()

	deadline := time.Now().Add(2 * time.Second)
	for (healthyDelivered.Load() < 1 || stalledAttempts.Load() < 2) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := healthyDelivered.Load(); got != 1 {
		t.Fatalf("expected completed sibling token not to be retried after batch timeout, got deliveries=%d", got)
	}

	recoverStalled.Store(true)
	deadline = time.Now().Add(2 * time.Second)
	for c.SpoolLen() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := healthyDelivered.Load(); got != 1 {
		t.Fatalf("expected healthy payload delivered once total, got %d", got)
	}
	if got := c.DeliveryDropped(); got != 0 {
		t.Fatalf("expected DeliveryDropped=0, got %d", got)
	}
}

func TestMQTTClientBackoffProgressionAndCap(t *testing.T) {
	c := &MQTTClient{}
	for range 20 {
		d0 := c.nextBackoff(0)
		if d0 < 250*time.Millisecond || d0 > 500*time.Millisecond {
			t.Fatalf("attempt 0 backoff %s out of expected [250ms, 500ms]", d0)
		}
		d6 := c.nextBackoff(6)
		if d6 < 15*time.Second || d6 > 30*time.Second {
			t.Fatalf("attempt 6 backoff %s out of expected [15s, 30s]", d6)
		}
		d20 := c.nextBackoff(20)
		if d20 > 30*time.Second {
			t.Fatalf("attempt 20 backoff %s exceeded 30s cap", d20)
		}
	}
}

func TestListenForRunnersSubscribeTimeout(t *testing.T) {
	mc := &fakeMQTTClient{
		subHook: func(_ string) mqtt.Token {
			return newStalledToken()
		},
	}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	c.subscribeTimeout = 40 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	err := c.ListenForRunners(ctx, "rv64-test-01", &capturePublisher{})
	if err == nil {
		t.Fatal("expected stalled Subscribe to return timeout error")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Subscribe timeout took too long: %s", time.Since(start))
	}
}

func TestListenForRunnersPanicRecoveryAndWaitRunners(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	var panicCount atomic.Int32
	c.runnerReg.Register(&funcRunner{
		name: "panic-task",
		fn: func(_ context.Context, _ string, _ map[string]any, _ core.TelemetryPublisher) error {
			panicCount.Add(1)
			panic("boom")
		},
	})

	abortedReleased := make(chan struct{})
	c.runnerReg.Register(&funcRunner{
		name: "graceful-task",
		fn: func(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
			<-ctx.Done()
			time.Sleep(30 * time.Millisecond)
			_ = pub.Publish(core.NewCTDPayload(nodeID, "runner:graceful-task", "runner-aborted").
				WithPillar(core.PillarRunner).
				AddParam("request_id", params["request_id"].(string)))
			close(abortedReleased)
			return nil
		},
	})

	pub := &capturePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.ListenForRunners(ctx, "rv64-test-01", pub); err != nil {
		t.Fatalf("ListenForRunners failed: %v", err)
	}

	h := mc.handler()
	if h == nil {
		t.Fatal("expected Subscribe to register control message handler")
	}

	dispatch := func(name, reqID string) {
		raw, err := json.Marshal(runner.Envelope{
			RequestID: reqID,
			Action:    "start",
			Runner:    name,
		})
		if err != nil {
			t.Fatal(err)
		}
		h(mc, fakeMessage{topic: "pigeons/control/rv64-test-01", payload: raw})
	}

	// 1. Panic recovery emits runner-error and clears active slot.
	dispatch("panic-task", "req-panic-1")
	deadline := time.Now().Add(2 * time.Second)
	for len(pub.snapshot()) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	events := pub.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 runner-error event after panic, got %d", len(events))
	}
	if events[0].Status != "runner-error" || events[0].Params["request_id"] != "req-panic-1" || !strings.Contains(events[0].Params["error"], "panic: boom") {
		t.Fatalf("unexpected panic CTD event: %+v", events[0])
	}

	// 2. Verify active slot was cleaned up so panic-task can be invoked again.
	dispatch("panic-task", "req-panic-2")
	deadline = time.Now().Add(2 * time.Second)
	for panicCount.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if panicCount.Load() != 2 {
		t.Fatalf("expected second panic-task dispatch to execute, count=%d", panicCount.Load())
	}

	// 3. Start graceful-task, cancel ctx, and verify WaitRunners waits for runner-aborted before returning.
	dispatch("graceful-task", "req-graceful-1")
	time.Sleep(20 * time.Millisecond)
	cancel()
	if ok := c.WaitRunners(2 * time.Second); !ok {
		t.Fatal("expected WaitRunners to wait for graceful-task completion")
	}
	select {
	case <-abortedReleased:
	default:
		t.Fatal("WaitRunners returned before runner-aborted bookend was emitted")
	}
}

func TestWaitRunnersTimeoutOnStuckRunner(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	unblock := make(chan struct{})
	started := make(chan struct{})
	c.runnerReg.Register(&funcRunner{
		name: "stuck-task",
		fn: func(_ context.Context, _ string, _ map[string]any, _ core.TelemetryPublisher) error {
			close(started)
			<-unblock
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.ListenForRunners(ctx, "rv64-test-01", &capturePublisher{}); err != nil {
		t.Fatalf("ListenForRunners failed: %v", err)
	}

	raw, _ := json.Marshal(runner.Envelope{RequestID: "req-stuck", Action: "start", Runner: "stuck-task"})
	mc.handler()(mc, fakeMessage{topic: "pigeons/control/rv64-test-01", payload: raw})
	<-started

	if ok := c.WaitRunners(30 * time.Millisecond); ok {
		close(unblock)
		t.Fatal("expected WaitRunners to return false when runner does not exit before timeout")
	}
	close(unblock)
	if ok := c.WaitRunners(time.Second); !ok {
		t.Fatal("expected WaitRunners to return true after stuck runner unblocked")
	}
}

func TestMQTTClientShutdownTimeoutAccountsUnsentAndUnacked(t *testing.T) {
	t.Run("partial_ack_unacked_and_unsent_chunks", func(t *testing.T) {
		var seq atomic.Int32
		mc := &fakeMQTTClient{
			publishHook: func(_ string, _ []byte) mqtt.Token {
				if seq.Add(1) <= 10 {
					return newCompletedToken(nil)
				}
				return newStalledToken()
			},
		}
		mc.connected.Store(true)

		total := maxPipelinedPublishes + 10
		c := newTestRelay(mc, total+16)
		for i := range total {
			_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active").AddMetric("seq", i))
		}

		c.drainRemainingBeforeClose(35 * time.Millisecond)

		// 10 acked in chunk 1; 54 unacked in chunk 1 + 10 unsent in chunk 2 = 64 dropped.
		if got := c.DeliveryDroppedCount(); got != 64 {
			t.Fatalf("expected DeliveryDroppedCount=64 (54 unacked + 10 unsent), got %d", got)
		}
		if got := c.SpoolLen(); got != 0 {
			t.Fatalf("expected spool empty after drainRemainingBeforeClose, got %d", got)
		}
	})

	t.Run("timer_expired_before_next_chunk_counts_unsent", func(t *testing.T) {
		var calls atomic.Int32
		mc := &fakeMQTTClient{
			publishHook: func(_ string, _ []byte) mqtt.Token {
				if calls.Add(1) == 1 {
					time.Sleep(25 * time.Millisecond)
				}
				return newCompletedToken(nil)
			},
		}
		mc.connected.Store(true)

		total := maxPipelinedPublishes + 5
		c := newTestRelay(mc, total+16)
		for i := range total {
			_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active").AddMetric("seq", i))
		}

		c.drainRemainingBeforeClose(10 * time.Millisecond)

		// All 64 tokens in chunk 1 completed; 5 items in chunk 2 were never sent.
		if got := c.DeliveryDroppedCount(); got != 5 {
			t.Fatalf("expected DeliveryDroppedCount=5 unsent items, got %d", got)
		}
		if got := c.SpoolLen(); got != 0 {
			t.Fatalf("expected spool empty after drainRemainingBeforeClose, got %d", got)
		}
	})

	t.Run("token_error_during_drain_counted", func(t *testing.T) {
		mc := &fakeMQTTClient{
			publishHook: func(_ string, _ []byte) mqtt.Token {
				return newCompletedToken(errors.New("publish rejected"))
			},
		}
		mc.connected.Store(true)

		c := newTestRelay(mc, 16)
		for i := range 3 {
			_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active").AddMetric("seq", i))
		}

		c.drainRemainingBeforeClose(100 * time.Millisecond)

		if got := c.DeliveryDroppedCount(); got != 3 {
			t.Fatalf("expected DeliveryDroppedCount=3 for errored shutdown tokens, got %d", got)
		}
	})
}

func TestMQTTClientConnectedInfiniteTimeoutDropsPoisonAndDrainsSubsequent(t *testing.T) {
	var poisonAttempts atomic.Int32
	var healthyDelivered atomic.Int32
	mc := &fakeMQTTClient{
		publishHook: func(topic string, _ []byte) mqtt.Token {
			if strings.HasSuffix(topic, "/poison") {
				poisonAttempts.Add(1)
				return newStalledToken()
			}
			healthyDelivered.Add(1)
			return newCompletedToken(nil)
		},
	}
	mc.connected.Store(true)

	c := newTestRelay(mc, 32)
	c.batchTimeout = 10 * time.Millisecond
	c.backoffBase = 2 * time.Millisecond
	c.backoffMax = 5 * time.Millisecond
	c.maxAttempts = 3
	c.maxTimeoutAttempts = 4

	go c.flushLoop()
	defer c.Disconnect()

	// Enqueue the poison event first so flushLoop picks it up in its own batch (succeeded == 0).
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "poison", "active"))
	deadline := time.Now().Add(2 * time.Second)
	for poisonAttempts.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if poisonAttempts.Load() < 1 {
		t.Fatal("expected poison payload to enter deliverBatchWithRetry")
	}

	// Enqueue healthy events behind the stalled poison batch.
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "healthy", "active").AddMetric("seq", 1))
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "healthy", "active").AddMetric("seq", 2))

	deadline = time.Now().Add(2 * time.Second)
	for (c.DeliveryDroppedCount() < 1 || healthyDelivered.Load() < 2) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := poisonAttempts.Load(); got != 4 {
		t.Fatalf("expected 4 poison timeout attempts before drop, got %d", got)
	}
	if got := c.DeliveryDroppedCount(); got != 1 {
		t.Fatalf("expected DeliveryDroppedCount=1 after poison timeout drop, got %d", got)
	}
	if got := healthyDelivered.Load(); got != 2 {
		t.Fatalf("expected 2 subsequent healthy payloads to drain after poison drop, got %d", got)
	}
}
