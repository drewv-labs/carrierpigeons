package observer

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
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
