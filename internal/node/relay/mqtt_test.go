package relay

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
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
	"github.com/eclipse/paho.mqtt.golang/packets"
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
	connected       atomic.Bool
	connectCalls    atomic.Int32
	disconnectCalls atomic.Int32

	mu             sync.Mutex
	published      []fakePublish
	subHandler     mqtt.MessageHandler
	subTopic       string
	publishHook    func(topic string, payload []byte) mqtt.Token
	subHook        func(topic string) mqtt.Token
	connectHook    func() mqtt.Token
	disconnectHook func(quiesce uint)
}

type fakePublish struct {
	topic   string
	payload []byte
}

func (f *fakeMQTTClient) IsConnected() bool      { return f.connected.Load() }
func (f *fakeMQTTClient) IsConnectionOpen() bool { return f.connected.Load() }
func (f *fakeMQTTClient) Connect() mqtt.Token {
	f.connectCalls.Add(1)
	f.mu.Lock()
	hook := f.connectHook
	f.mu.Unlock()
	if hook != nil {
		return hook()
	}
	return newCompletedToken(nil)
}
func (f *fakeMQTTClient) Disconnect(quiesce uint) {
	f.disconnectCalls.Add(1)
	f.connected.Store(false)
	f.mu.Lock()
	hook := f.disconnectHook
	f.mu.Unlock()
	if hook != nil {
		hook(quiesce)
	}
}

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
		connectedCh:  make(chan struct{}),
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
	var c *MQTTClient
	mc := &fakeMQTTClient{
		publishHook: func(_ string, _ []byte) mqtt.Token {
			if allowSuccess.Load() {
				return newCompletedToken(nil)
			}
			return newStalledToken()
		},
	}
	mc.connectHook = func() mqtt.Token {
		mc.connected.Store(true)
		c.notifyConnected()
		return newCompletedToken(nil)
	}
	mc.connected.Store(true)

	c = newTestRelay(mc, 32)
	c.batchTimeout = 10 * time.Millisecond
	c.backoffBase = 5 * time.Millisecond
	c.backoffMax = 10 * time.Millisecond
	c.maxAttempts = 5
	c.deadTransportTimeouts = 2

	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active"))
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "mem", "active"))
	go c.flushLoop()
	defer c.Disconnect()

	// Wait for >= 14 stalled publish attempts (7+ batch timeouts > maxAttempts=5) and forced reconnects.
	deadline := time.Now().Add(3 * time.Second)
	for (mc.publishCount() < 14 || mc.connectCalls.Load() < 2) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if mc.publishCount() < 14 {
		t.Fatalf("expected >=14 stalled publish attempts (7+ batch timeouts), got %d", mc.publishCount())
	}
	if mc.connectCalls.Load() < 2 {
		t.Fatalf("expected dead transport to trigger >=2 forced reconnects, got %d", mc.connectCalls.Load())
	}
	if got := c.DeliveryDropped(); got != 0 {
		t.Fatalf("expected DeliveryDropped=0 on whole-batch stall across reconnects, got %d", got)
	}

	// Once the link recovers, both payloads should deliver cleanly without any drops.
	allowSuccess.Store(true)
	deadline = time.Now().Add(2 * time.Second)
	for c.SpoolLen() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c.SpoolLen() != 0 {
		t.Fatalf("expected spool drained after recovery, got %d", c.SpoolLen())
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
	var c *MQTTClient
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
	mc.connectHook = func() mqtt.Token {
		mc.connected.Store(true)
		c.notifyConnected()
		return newCompletedToken(nil)
	}
	mc.connected.Store(true)

	c = newTestRelay(mc, 32)
	c.batchTimeout = 10 * time.Millisecond
	c.backoffBase = 2 * time.Millisecond
	c.backoffMax = 5 * time.Millisecond
	c.maxAttempts = 3
	c.deadTransportTimeouts = 2

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

	// Enqueue healthy events behind the stalled poison batch. On dead-transport reconnect,
	// poison is prepended back to the spool and re-batched with the healthy events (succeeded > 0),
	// proving the link is healthy and allowing poison eviction after maxAttempts.
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "healthy", "active").AddMetric("seq", 1))
	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "healthy", "active").AddMetric("seq", 2))

	deadline = time.Now().Add(3 * time.Second)
	for (c.DeliveryDroppedCount() < 1 || healthyDelivered.Load() < 2) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := c.DeliveryDroppedCount(); got != 1 {
		t.Fatalf("expected DeliveryDroppedCount=1 after re-batched poison eviction, got %d", got)
	}
	if got := healthyDelivered.Load(); got != 2 {
		t.Fatalf("expected 2 subsequent healthy payloads to drain after poison drop, got %d", got)
	}
	if got := poisonAttempts.Load(); got < int32(c.maxAttempts) {
		t.Fatalf("expected at least %d poison attempts before eviction, got %d", c.maxAttempts, got)
	}
}

func TestMQTTClientOptionsWriteKeepAlivePingJitterAndMetaClone(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	callerMeta := map[string]string{"board": "sbc-1", "tier": "edge"}
	c := NewMQTTClientWithOptions(Options{
		BrokerURL:     "tcp://" + addr,
		NodeID:        "rv64-opts-test",
		SpoolCapacity: 16,
		MaxSpoolBytes: 64 * 1024,
		Meta:          callerMeta,
	})
	defer c.Disconnect()

	// Mutating callerMeta after construction must not race or alter c.meta.
	callerMeta["board"] = "mutated"
	callerMeta["extra"] = "bad"
	if got := c.meta["board"]; got != "sbc-1" {
		t.Fatalf("expected cloned meta['board']=sbc-1, got %q", got)
	}
	if _, exists := c.meta["extra"]; exists {
		t.Fatal("expected caller map mutation not to leak into cloned c.meta")
	}

	r := c.client.OptionsReader()
	if got := r.WriteTimeout(); got != 5*time.Second {
		t.Fatalf("expected WriteTimeout=5s, got %s", got)
	}
	if got := r.KeepAlive(); got != 10*time.Second {
		t.Fatalf("expected KeepAlive=10s, got %s", got)
	}
	if got := r.PingTimeout(); got != 5*time.Second {
		t.Fatalf("expected PingTimeout=5s, got %s", got)
	}
	if got := r.ConnectRetryInterval(); got < 3*time.Second || got > 5*time.Second {
		t.Fatalf("expected ConnectRetryInterval jittered in [3s, 5s], got %s", got)
	}
	if got := r.MaxReconnectInterval(); got < 15*time.Second || got > 20*time.Second {
		t.Fatalf("expected MaxReconnectInterval jittered in [15s, 20s], got %s", got)
	}
	if got := c.spool.MaxBytes(); got != 64*1024 {
		t.Fatalf("expected MaxBytes=65536, got %d", got)
	}
}

func TestMQTTClientHalfOpenBlackholeReconnectsWithoutDropping(t *testing.T) {
	var connNum atomic.Int32
	var blackholePublishes atomic.Int32
	var mu sync.Mutex
	var delivered []string
	var serverConns []net.Conn
	var wg sync.WaitGroup
	defer func() {
		mu.Lock()
		for _, cn := range serverConns {
			_ = cn.Close()
		}
		mu.Unlock()
		wg.Wait()
	}()

	openPipeConn := func(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		clientEnd, serverEnd := net.Pipe()
		mu.Lock()
		serverConns = append(serverConns, serverEnd)
		mu.Unlock()

		idx := connNum.Add(1)
		wg.Add(1)
		go func(cn net.Conn, connIdx int32) {
			defer wg.Done()
			defer func() { _ = cn.Close() }()
			for {
				pkt, err := packets.ReadPacket(cn)
				if err != nil {
					return
				}
				switch p := pkt.(type) {
				case *packets.ConnectPacket:
					ca := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
					ca.ReturnCode = packets.Accepted
					if err := ca.Write(cn); err != nil {
						return
					}
				case *packets.PingreqPacket:
					if connIdx > 1 {
						pr := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
						_ = pr.Write(cn)
					}
				case *packets.PublishPacket:
					if connIdx == 1 {
						// Simulate half-open blackhole: read bytes off the wire but silently drop PUBACKs.
						if strings.Contains(p.TopicName, "/ctd/") {
							blackholePublishes.Add(1)
						}
						continue
					}
					if strings.Contains(p.TopicName, "/ctd/") {
						mu.Lock()
						delivered = append(delivered, p.TopicName)
						mu.Unlock()
					}
					if p.Qos == 1 {
						pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
						pa.MessageID = p.MessageID
						if err := pa.Write(cn); err != nil {
							return
						}
					}
				case *packets.DisconnectPacket:
					return
				}
			}
		}(serverEnd, idx)

		return clientEnd, nil
	}

	c := newMQTTClientWithOptions(Options{
		BrokerURL:     "tcp://127.0.0.1:1883",
		NodeID:        "rv64-blackhole-01",
		SpoolCapacity: 64,
		MaxSpoolBytes: 1 << 20,
	}, openPipeConn)
	c.batchTimeout = 40 * time.Millisecond
	c.backoffBase = 10 * time.Millisecond
	c.backoffMax = 25 * time.Millisecond
	c.deadTransportTimeouts = 2
	defer c.Disconnect()

	deadline := time.Now().Add(2 * time.Second)
	for connNum.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if connNum.Load() < 1 {
		t.Fatal("expected initial connection to pipe mock broker")
	}

	const totalEvents = 5
	for i := range totalEvents {
		if err := c.Publish(core.NewCTDPayload("rv64-blackhole-01", "cpu", "active").AddMetric("seq", i)); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n >= totalEvents && c.SpoolLen() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if connNum.Load() < 2 {
		t.Fatalf("expected dead-transport reconnect to open connection >= 2, got %d", connNum.Load())
	}
	if blackholePublishes.Load() == 0 {
		t.Fatal("expected initial publishes to hit connection 1 blackhole before reconnect")
	}
	mu.Lock()
	gotDelivered := len(delivered)
	mu.Unlock()
	if gotDelivered != totalEvents {
		t.Fatalf("expected all %d events delivered after blackhole reconnect, got %d (spool=%d, dropped=%d)",
			totalEvents, gotDelivered, c.SpoolLen(), c.DeliveryDropped())
	}
	if got := c.DeliveryDropped(); got != 0 {
		t.Fatalf("expected 0 telemetry drops across blackhole recovery, got %d", got)
	}
}

func TestMQTTClientWriteTimeoutStallRecovery(t *testing.T) {
	var connNum atomic.Int32
	var mu sync.Mutex
	var delivered []string
	var serverConns []net.Conn
	var wg sync.WaitGroup
	stallRead := make(chan struct{})
	defer func() {
		select {
		case <-stallRead:
		default:
			close(stallRead)
		}
		mu.Lock()
		for _, cn := range serverConns {
			_ = cn.Close()
		}
		mu.Unlock()
		wg.Wait()
	}()

	openPipeConn := func(_ *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		clientEnd, serverEnd := net.Pipe()
		mu.Lock()
		serverConns = append(serverConns, serverEnd)
		mu.Unlock()

		idx := connNum.Add(1)
		wg.Add(1)
		go func(cn net.Conn, connIdx int32) {
			defer wg.Done()
			defer func() { _ = cn.Close() }()
			seenBootPubs := 0
			for {
				pkt, err := packets.ReadPacket(cn)
				if err != nil {
					return
				}
				switch p := pkt.(type) {
				case *packets.ConnectPacket:
					ca := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
					ca.ReturnCode = packets.Accepted
					if err := ca.Write(cn); err != nil {
						return
					}
				case *packets.PingreqPacket:
					pr := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
					_ = pr.Write(cn)
				case *packets.PublishPacket:
					if connIdx == 1 {
						if p.Qos == 1 {
							pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
							pa.MessageID = p.MessageID
							_ = pa.Write(cn)
						}
						seenBootPubs++
						// After ONLINE + NodeHello are read, stop reading from pipe so client Write blocks
						// until Paho's WriteTimeout (5s) expires.
						if seenBootPubs >= 2 {
							<-stallRead
							return
						}
						continue
					}
					if strings.Contains(p.TopicName, "/ctd/") {
						mu.Lock()
						delivered = append(delivered, p.TopicName)
						mu.Unlock()
					}
					if p.Qos == 1 {
						pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
						pa.MessageID = p.MessageID
						if err := pa.Write(cn); err != nil {
							return
						}
					}
				case *packets.DisconnectPacket:
					return
				}
			}
		}(serverEnd, idx)

		return clientEnd, nil
	}

	c := newMQTTClientWithOptions(Options{
		BrokerURL:     "tcp://127.0.0.1:1883",
		NodeID:        "rv64-writetimeout-01",
		SpoolCapacity: 32,
	}, openPipeConn)
	// Keep batchTimeout > WriteTimeout (5s) so Paho's WriteTimeout fires first on the stalled write.
	c.batchTimeout = 7 * time.Second
	c.backoffBase = 10 * time.Millisecond
	c.backoffMax = 25 * time.Millisecond
	defer c.Disconnect()

	deadline := time.Now().Add(2 * time.Second)
	for !c.isConnected() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !c.isConnected() {
		t.Fatal("expected initial connection before write stall")
	}
	// Give OnConnect's ONLINE + NodeHello packets a moment to reach server loop before CTD publish.
	time.Sleep(30 * time.Millisecond)

	if err := c.Publish(core.NewCTDPayload("rv64-writetimeout-01", "cpu", "active").AddMetric("seq", 1)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n >= 1 && c.SpoolLen() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stallRead)

	if connNum.Load() < 2 {
		t.Fatalf("expected WriteTimeout to force reconnect >= 2, got %d", connNum.Load())
	}
	mu.Lock()
	gotDelivered := len(delivered)
	mu.Unlock()
	if gotDelivered != 1 {
		t.Fatalf("expected 1 payload delivered after WriteTimeout recovery, got %d", gotDelivered)
	}
	if got := c.DeliveryDropped(); got != 0 {
		t.Fatalf("expected 0 drops on WriteTimeout recovery, got %d", got)
	}
}

func TestMQTTClientWaitForConnectedAndBackoffWakeOnConnect(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(false)

	c := newTestRelay(mc, 16)
	c.backoffBase = 10 * time.Second
	c.backoffMax = 10 * time.Second

	_ = c.Publish(core.NewCTDPayload("rv64-test-01", "cpu", "active"))
	go c.flushLoop()

	// While disconnected, flushLoop must wait on connectedCh without popping from spool.
	time.Sleep(40 * time.Millisecond)
	if got := c.SpoolLen(); got != 1 {
		c.Disconnect()
		t.Fatalf("expected payload to remain in spool while disconnected, got SpoolLen=%d", got)
	}

	// Simulate OnConnect firing -> flushLoop wakes immediately and delivers without waiting backoff.
	start := time.Now()
	mc.connected.Store(true)
	c.notifyConnected()

	deadline := time.Now().Add(500 * time.Millisecond)
	for mc.publishCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if mc.publishCount() != 1 {
		c.Disconnect()
		t.Fatalf("expected immediate delivery upon OnConnect, got publishCount=%d in %s", mc.publishCount(), time.Since(start))
	}
	c.Disconnect()

	// Verify waitRetryBackoff() itself wakes immediately on notifyConnected() despite 10s backoffBase.
	c2 := newTestRelay(&fakeMQTTClient{}, 16)
	c2.backoffBase = 10 * time.Second
	c2.backoffMax = 10 * time.Second
	wakeStart := time.Now()
	done := make(chan bool, 1)
	go func() {
		done <- c2.waitRetryBackoff()
	}()
	time.Sleep(15 * time.Millisecond)
	c2.notifyConnected()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("expected waitRetryBackoff to return true on OnConnect wake")
		}
		if elapsed := time.Since(wakeStart); elapsed > 500*time.Millisecond {
			t.Fatalf("expected waitRetryBackoff to wake immediately on OnConnect, took %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("waitRetryBackoff did not wake on OnConnect")
	}
}

func TestMQTTClientRejectsOversizeImmediatelyAndPostShutdownPublish(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(false)

	c := newTestRelay(mc, 16)
	go c.flushLoop()

	oversize := core.NewCTDPayload("rv64-test-01", "oversize", "active").
		AddMetric("blob", strings.Repeat("a", maxMQTTPayloadBytes+1))
	if err := c.Publish(oversize); err == nil {
		t.Fatal("expected Publish to reject >1MiB payload immediately")
	}
	if got := c.SpoolLen(); got != 0 {
		t.Fatalf("expected oversized payload not to enter spool, got SpoolLen=%d", got)
	}
	if got := c.DeliveryDropped(); got != 1 {
		t.Fatalf("expected DeliveryDropped=1 immediately after oversize Publish, got %d", got)
	}

	c.Disconnect()

	// Post-shutdown Publish must increment DeliveryDropped cleanly without stranding in spool.
	if err := c.Publish(core.NewCTDPayload("rv64-test-01", "late", "active")); err == nil {
		t.Fatal("expected post-Disconnect Publish to return error")
	}
	if got := c.SpoolLen(); got != 0 {
		t.Fatalf("expected 0 stranded events in spool post-Disconnect, got %d", got)
	}
	if got := c.DeliveryDropped(); got != 2 {
		t.Fatalf("expected DeliveryDropped=2 after post-Disconnect Publish, got %d", got)
	}
}

func TestMQTTClientOnConnectWGOrderingBeforeOffline(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	go c.flushLoop()

	inOnConnect := make(chan struct{})
	releaseOnConnect := make(chan struct{})
	disconnectDone := make(chan struct{})

	// Simulate an in-flight OnConnect guarded by onConnectMu + stopCh + onConnectWG.
	go func() {
		c.onConnectMu.Lock()
		select {
		case <-c.stopCh:
			c.onConnectMu.Unlock()
			return
		default:
		}
		c.onConnectWG.Add(1)
		c.onConnectMu.Unlock()
		defer c.onConnectWG.Done()

		close(inOnConnect)
		<-releaseOnConnect
		mc.Publish(c.brand.RegistryTopic(c.nodeID), 1, true, []byte("ONLINE"))
	}()

	<-inOnConnect
	go func() {
		c.Disconnect()
		close(disconnectDone)
	}()

	// Disconnect must block until in-flight OnConnect completes, then publish OFFLINE after ONLINE.
	select {
	case <-disconnectDone:
		t.Fatal("expected Disconnect to wait on onConnectWG before publishing OFFLINE")
	case <-time.After(40 * time.Millisecond):
	}

	close(releaseOnConnect)
	select {
	case <-disconnectDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect did not finish after OnConnect released")
	}

	pubs := mc.snapshotPublished()
	if len(pubs) < 2 {
		t.Fatalf("expected ONLINE and OFFLINE publishes, got %+v", pubs)
	}
	if string(pubs[len(pubs)-2].payload) != "ONLINE" || string(pubs[len(pubs)-1].payload) != "OFFLINE" {
		t.Fatalf("expected ONLINE strictly before OFFLINE, got %q then %q",
			string(pubs[len(pubs)-2].payload), string(pubs[len(pubs)-1].payload))
	}
}

func TestListenForRunnersSubscribeRetryAndGenerationCancel(t *testing.T) {
	var subAttempts atomic.Int32
	mc := &fakeMQTTClient{
		subHook: func(_ string) mqtt.Token {
			n := subAttempts.Add(1)
			switch n {
			case 1:
				return newCompletedToken(errors.New("transient broker error"))
			case 2:
				return newStalledToken()
			default:
				return newCompletedToken(nil)
			}
		},
	}

	c := newTestRelay(mc, 16)
	c.backoffBase = 5 * time.Millisecond
	c.backoffMax = 15 * time.Millisecond
	c.subscribeTimeout = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Offline on initial ListenForRunners; OnConnect callback triggers retry loop until attempt 3 succeeds.
	if err := c.ListenForRunners(ctx, "rv64-test-01", &capturePublisher{}); err != nil {
		t.Fatalf("ListenForRunners failed: %v", err)
	}

	c.subMu.RLock()
	subFn := c.onConnectSub
	c.subMu.RUnlock()
	if subFn == nil {
		t.Fatal("expected onConnectSub to be registered")
	}

	mc.connected.Store(true)
	gen1, nextCh1 := c.notifyConnected()
	subFn(mc, gen1, nextCh1)

	if got := subAttempts.Load(); got != 3 {
		t.Fatalf("expected 3 subscribe attempts (error -> timeout -> success), got %d", got)
	}

	// Verify generation superseding cancels stale stalled retry loop immediately.
	var gen2Attempts atomic.Int32
	mc.mu.Lock()
	mc.subHook = func(_ string) mqtt.Token {
		if c.connGen.Load() == gen1+1 {
			return newStalledToken()
		}
		gen2Attempts.Add(1)
		return newCompletedToken(nil)
	}
	mc.mu.Unlock()

	genStale, nextChStale := c.notifyConnected()
	staleDone := make(chan struct{})
	go func() {
		subFn(mc, genStale, nextChStale)
		close(staleDone)
	}()

	time.Sleep(15 * time.Millisecond)
	genNew, nextChNew := c.notifyConnected()

	select {
	case <-staleDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected stale Subscribe retry loop to abort immediately when connection generation advanced")
	}

	subFn(mc, genNew, nextChNew)
	if gen2Attempts.Load() != 1 {
		t.Fatalf("expected new generation subscribe to succeed on first attempt, got %d", gen2Attempts.Load())
	}
}

func TestListenForRunnersAbortTeardownSingleFlightAndStaleRequestID(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	var starts atomic.Int32
	entered := make(chan string, 4)
	sawCancel := make(chan string, 4)
	teardownGate := make(chan struct{})

	c.runnerReg.Register(&funcRunner{
		name: "slow-teardown",
		fn: func(ctx context.Context, _ string, params map[string]any, _ core.TelemetryPublisher) error {
			reqID, _ := params["request_id"].(string)
			starts.Add(1)
			entered <- reqID
			<-ctx.Done()
			sawCancel <- reqID
			if reqID == "req-1" {
				<-teardownGate
			}
			return nil
		},
	})

	pub := &capturePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.ListenForRunners(ctx, "rv64-test-01", pub); err != nil {
		t.Fatalf("ListenForRunners failed: %v", err)
	}
	h := mc.handler()

	sendEnv := func(env runner.Envelope) {
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		h(mc, fakeMessage{topic: "pigeons/control/rv64-test-01", payload: raw})
	}

	// 1. Start req-1.
	sendEnv(runner.Envelope{RequestID: "req-1", Action: "start", Runner: "slow-teardown"})
	if got := <-entered; got != "req-1" {
		t.Fatalf("expected req-1, got %q", got)
	}

	// 2. Stale RequestID abort must NOT cancel req-1.
	sendEnv(runner.Envelope{RequestID: "req-stale", Action: "abort", Runner: "slow-teardown"})
	select {
	case id := <-sawCancel:
		t.Fatalf("stale request_id abort canceled active runner %q", id)
	case <-time.After(30 * time.Millisecond):
	}

	// 3. Matching RequestID abort cancels req-1, but slot remains occupied while tearing down.
	sendEnv(runner.Envelope{RequestID: "req-1", Action: "abort", Runner: "slow-teardown"})
	if got := <-sawCancel; got != "req-1" {
		t.Fatalf("expected req-1 to observe cancel, got %q", got)
	}

	// Concurrent start during abort teardown must be rejected (single-flight preserved).
	sendEnv(runner.Envelope{RequestID: "req-2", Action: "start", Runner: "slow-teardown"})
	time.Sleep(30 * time.Millisecond)
	if got := starts.Load(); got != 1 {
		t.Fatalf("expected single-flight start rejection during abort teardown, starts=%d", got)
	}

	// 4. Release teardownGate so defer deletes activeRunners[runnerKey]; req-3 can now start.
	close(teardownGate)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sendEnv(runner.Envelope{RequestID: "req-3", Action: "start", Runner: "slow-teardown"})
		select {
		case got := <-entered:
			if got != "req-3" {
				t.Fatalf("expected req-3 after teardown finished, got %q", got)
			}
			// Wildcard empty RequestID abort must also cancel req-3.
			sendEnv(runner.Envelope{Action: "abort", Runner: "slow-teardown"})
			if canceled := <-sawCancel; canceled != "req-3" {
				t.Fatalf("expected empty request_id abort to cancel req-3, got %q", canceled)
			}
			return
		case <-time.After(15 * time.Millisecond):
		}
	}
	t.Fatal("expected req-3 to run after req-1 completed teardown")
}

func TestListenForRunnersValidatesComponentBeforeErrorPublish(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	longName := strings.Repeat("r", 125) // passes ValidateTopicToken(runner) <=128, but "runner:"+longName = 132 > 128
	c.runnerReg.Register(&funcRunner{
		name: longName,
		fn: func(_ context.Context, _ string, _ map[string]any, _ core.TelemetryPublisher) error {
			panic("oversize component panic")
		},
	})

	pub := &capturePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.ListenForRunners(ctx, "rv64-test-01", pub); err != nil {
		t.Fatalf("ListenForRunners failed: %v", err)
	}
	h := mc.handler()

	// 1. Malicious topic-injection runner name must not publish error event.
	badRaw, _ := json.Marshal(runner.Envelope{RequestID: "req-bad", Action: "start", Runner: "evil/+/topic/#"})
	h(mc, fakeMessage{topic: "pigeons/control/rv64-test-01", payload: badRaw})

	// 2. Unknown runner whose "runner:"+name > 128 bytes must not publish invalid component token.
	tooLongUnknown, _ := json.Marshal(runner.Envelope{RequestID: "req-long-unk", Action: "start", Runner: strings.Repeat("u", 125)})
	h(mc, fakeMessage{topic: "pigeons/control/rv64-test-01", payload: tooLongUnknown})

	// 3. Registered runner whose "runner:"+name > 128 bytes panicking must not publish invalid component token.
	tooLongPanic, _ := json.Marshal(runner.Envelope{RequestID: "req-long-panic", Action: "start", Runner: longName})
	h(mc, fakeMessage{topic: "pigeons/control/rv64-test-01", payload: tooLongPanic})

	c.WaitRunners(time.Second)
	for _, ev := range pub.snapshot() {
		if err := core.ValidateTopicToken(ev.Component); err != nil {
			t.Fatalf("published runner error event with invalid Component %q: %v", ev.Component, err)
		}
	}
	if got := len(pub.snapshot()); got != 0 {
		t.Fatalf("expected 0 invalid component events published, got %d (%+v)", got, pub.snapshot())
	}
}
