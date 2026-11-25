package edge

import (
	"context"
	"net"
	"sync"
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
