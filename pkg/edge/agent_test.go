package edge

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

type stubObserver struct {
	name    string
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (o *stubObserver) Name() string { return o.name }

func (o *stubObserver) Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	o.once.Do(func() { close(o.started) })
	if pub != nil {
		_ = pub.Publish(core.NewCTDPayload(nodeID, o.name, "active").AddMetric("ok", true))
	}
	<-ctx.Done()
	close(o.stopped)
}

type slowExitObserver struct {
	name    string
	started chan struct{}
	exited  atomic.Bool
	delay   time.Duration
	release chan struct{}
}

func (o *slowExitObserver) Name() string { return o.name }

func (o *slowExitObserver) Observe(ctx context.Context, _ string, _ core.TelemetryPublisher) {
	close(o.started)
	<-ctx.Done()
	if o.release != nil {
		<-o.release
	} else if o.delay > 0 {
		time.Sleep(o.delay)
	}
	o.exited.Store(true)
}

type stubRunner struct {
	name string
}

func (r *stubRunner) Name() string { return r.name }

func (r *stubRunner) Run(_ context.Context, _ string, _ map[string]any, _ core.TelemetryPublisher) error {
	return nil
}

func closedLocalBrokerURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "tcp://" + addr
}

func TestAgentStartStopLifecycle(t *testing.T) {
	cfg := config.DefaultEdgeConfig()
	cfg.NodeID = "edge-lifecycle-test"
	cfg.BrokerURL = closedLocalBrokerURL(t)
	cfg.SpoolCapacity = 64
	cfg.Observers = config.ObserversConfig{} // Isolate to stubObserver only.
	cfg.Runners.Enabled = false

	obs := &stubObserver{
		name:    "stub-sensor",
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	pc := observer.NewPauseController()

	agent := New(cfg, Options{
		ExtraObservers:  []observer.Observer{obs},
		ExtraRunners:    []runner.Runner{&stubRunner{name: "stub-runner"}},
		PauseController: pc,
	})

	if agent.PauseController() != pc {
		t.Fatal("expected PauseController() to return configured instance")
	}

	// Publish before Start must safely be a no-op.
	if err := agent.Publish(core.NewCTDPayload(cfg.NodeID, "prestart", "active")); err != nil {
		t.Fatalf("Publish before Start returned error: %v", err)
	}

	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Agent.Start failed: %v", err)
	}

	// Second Start must be rejected cleanly.
	if err := agent.Start(context.Background()); err == nil {
		agent.Stop()
		t.Fatal("expected double Agent.Start to return error")
	}

	select {
	case <-obs.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for extra observer to start")
	}

	if err := agent.Publish(core.NewCTDPayload(cfg.NodeID, "manual", "active").AddMetric("v", 1)); err != nil {
		t.Fatalf("Agent.Publish failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		agent.Stop()
		// Second Stop must be idempotent.
		agent.Stop()
		close(done)
	}()

	select {
	case <-obs.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for observer context cancellation on Agent.Stop")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Agent.Stop did not complete within 3s")
	}
}

func TestAgentStopBeforeStartAndInvalidTLS(t *testing.T) {
	cfg := config.DefaultEdgeConfig()
	cfg.NodeID = "edge-invalid-tls"
	cfg.BrokerURL = closedLocalBrokerURL(t)
	cfg.TLSCAFile = "/nonexistent/pigeons-missing-ca.pem"

	agent := New(cfg, Options{DisableObservers: true})
	// Stop before Start must not panic.
	agent.Stop()

	if err := agent.Start(context.Background()); err == nil {
		agent.Stop()
		t.Fatal("expected Agent.Start to fail when TLSCAFile does not exist")
	}
}

func TestAgentStopJoinsObserversAndBoundsHungObserver(t *testing.T) {
	t.Run("joins_cleanly_exiting_observer", func(t *testing.T) {
		cfg := config.DefaultEdgeConfig()
		cfg.NodeID = "edge-join-obs"
		cfg.BrokerURL = closedLocalBrokerURL(t)
		cfg.Observers = config.ObserversConfig{}
		cfg.Runners.Enabled = false

		obs := &slowExitObserver{
			name:    "slow-exit",
			started: make(chan struct{}),
			delay:   60 * time.Millisecond,
		}
		agent := New(cfg, Options{ExtraObservers: []observer.Observer{obs}})
		if err := agent.Start(context.Background()); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		<-obs.started

		agent.Stop()
		if !obs.exited.Load() {
			t.Fatal("expected Agent.Stop() to wait for observer WaitGroup before returning")
		}
	})

	t.Run("bounded_wait_on_hung_observer", func(t *testing.T) {
		cfg := config.DefaultEdgeConfig()
		cfg.NodeID = "edge-hung-obs"
		cfg.BrokerURL = closedLocalBrokerURL(t)
		cfg.Observers = config.ObserversConfig{}
		cfg.Runners.Enabled = false

		release := make(chan struct{})
		defer close(release)
		obs := &slowExitObserver{
			name:    "hung-obs",
			started: make(chan struct{}),
			release: release,
		}
		agent := New(cfg, Options{ExtraObservers: []observer.Observer{obs}})
		if err := agent.Start(context.Background()); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		<-obs.started

		start := time.Now()
		agent.Stop()
		elapsed := time.Since(start)
		if elapsed > 3*time.Second {
			t.Fatalf("expected Agent.Stop() bounded wait on hung observer (<3s), took %s", elapsed)
		}
	})
}

func TestAgentConcurrentStartPublishStopRace(t *testing.T) {
	for iter := range 3 {
		cfg := config.DefaultEdgeConfig()
		cfg.NodeID = "edge-concurrent-race"
		cfg.BrokerURL = closedLocalBrokerURL(t)
		cfg.SpoolCapacity = 32
		cfg.Observers = config.ObserversConfig{}
		cfg.Runners.Enabled = false

		obs := &stubObserver{
			name:    "race-obs",
			started: make(chan struct{}),
			stopped: make(chan struct{}),
		}
		agent := New(cfg, Options{
			MaxSpoolBytes:  4096,
			ExtraObservers: []observer.Observer{obs},
		})

		var startOK atomic.Int32
		var wg sync.WaitGroup

		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := agent.Start(context.Background()); err == nil {
					startOK.Add(1)
				}
			}()
		}

		for i := range 8 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := range 5 {
					_ = agent.Publish(core.NewCTDPayload(cfg.NodeID, "manual", "active").
						AddMetric("iter", iter).
						AddMetric("worker", idx).
						AddMetric("seq", j))
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if iter%2 == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			agent.Stop()
		}()

		wg.Wait()
		agent.Stop()

		if got := startOK.Load(); got > 1 {
			t.Fatalf("iter %d: expected at most 1 Start() to succeed, got %d", iter, got)
		}
	}
}

func TestAgentMaxSpoolBytesWiredToRelay(t *testing.T) {
	cfg := config.DefaultEdgeConfig()
	cfg.NodeID = "edge-spool-bytes"
	cfg.BrokerURL = closedLocalBrokerURL(t)
	cfg.SpoolCapacity = 100
	cfg.Observers = config.ObserversConfig{}
	cfg.Runners.Enabled = false

	agent := New(cfg, Options{
		DisableObservers: true,
		MaxSpoolBytes:    1024,
	})
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer agent.Stop()

	blob := strings.Repeat("m", 240)
	for i := range 10 {
		_ = agent.Publish(core.NewCTDPayload(cfg.NodeID, "sensor", "active").AddMetric("seq", i).AddMetric("blob", blob))
	}

	if got := agent.relay.SpoolBytes(); got > 1024 {
		t.Fatalf("expected relay spool bytes <= 1024, got %d", got)
	}
	if got := agent.relay.SpoolDropped(); got == 0 {
		t.Fatal("expected byte-bounded spool eviction to increment SpoolDropped")
	}
}
