package observer

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type stubObserver struct {
	name   string
	called *atomic.Bool
}

func (s *stubObserver) Name() string { return s.name }

func (s *stubObserver) Observe(ctx context.Context, _ string, _ core.TelemetryPublisher) {
	s.called.Store(true)
	<-ctx.Done()
}

type slicePublisher struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (s *slicePublisher) Publish(ev *core.CTDPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *slicePublisher) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func TestObserverRegistryAndStartAll(t *testing.T) {
	var called atomic.Bool
	Register(&stubObserver{name: "unit-cooling-observer", called: &called})

	got, ok := Get("unit-cooling-observer")
	if !ok || got.Name() != "unit-cooling-observer" {
		t.Fatalf("expected unit-cooling-observer in registry, got ok=%v", ok)
	}
	if !slices.Contains(RegistryNames(), "unit-cooling-observer") {
		t.Fatalf("RegistryNames missing unit-cooling-observer: %v", RegistryNames())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StartAll(ctx, "node-1", nil)

	deadline := time.Now().Add(500 * time.Millisecond)
	for !called.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !called.Load() {
		t.Fatal("expected StartAll to invoke registered observer")
	}
}

func TestPauseControllerAndGatedPublisher(t *testing.T) {
	pc := NewPauseController()
	basePub := &slicePublisher{}
	gated := NewGatedPublisher(basePub, pc)

	// Unpaused: observer events flow normally.
	_ = gated.Publish(core.NewCTDPayload("node-01", "host-os", "active"))
	if basePub.len() != 1 {
		t.Fatalf("expected 1 observer event before pause, got %d", basePub.len())
	}

	// Acquire reference-counted pause hold.
	rel1 := pc.Acquire()
	rel2 := pc.Acquire()
	if !pc.IsPaused() {
		t.Fatal("expected controller to be paused after Acquire")
	}

	// While paused: observer events are suppressed, runner & tracer events pass through!
	_ = gated.Publish(core.NewCTDPayload("node-01", "host-os", "active"))
	_ = gated.Publish(core.NewCTDPayload("node-01", "runner:PauseRnr", "runner-start").WithPillar(core.PillarRunner))
	_ = gated.Publish(core.NewCTDPayload("node-01", "lineage", "certified").WithPillar(core.PillarTracer))
	if basePub.len() != 3 {
		t.Fatalf("expected runner+tracer events to pass through while observer dropped (want 3, got %d)", basePub.len())
	}

	// WaitIfPaused blocks until ALL holds release.
	ctx, cancel := context.WithCancel(WithPauseController(context.Background(), pc))
	defer cancel()

	resumed := make(chan bool, 1)
	go func() {
		resumed <- WaitIfPaused(ctx)
	}()

	rel1()
	rel1() // idempotent release
	select {
	case <-resumed:
		t.Fatal("expected WaitIfPaused to stay blocked while second hold remains active")
	case <-time.After(25 * time.Millisecond):
	}

	rel2()
	select {
	case ok := <-resumed:
		if !ok {
			t.Fatal("expected WaitIfPaused to return true upon resume")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for WaitIfPaused to wake after final release")
	}

	// Resumed: observer events flow again.
	_ = gated.Publish(core.NewCTDPayload("node-01", "host-os", "active"))
	if basePub.len() != 4 {
		t.Fatalf("expected observer event after resume (want 4, got %d)", basePub.len())
	}
}
