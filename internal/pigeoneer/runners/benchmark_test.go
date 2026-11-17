package runners

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

type capturePublisher struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (c *capturePublisher) Publish(e *core.CTDPayload) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return nil
}

func (c *capturePublisher) snapshot() []*core.CTDPayload {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*core.CTDPayload(nil), c.events...)
}

func TestBenchmarkRunnerStartAndAbortBookends(t *testing.T) {
	pub := &capturePublisher{}
	r := &BenchmarkRunner{}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	err := r.Run(ctx, "edge-rv64-01", map[string]any{
		"duration_sec":  10,
		"target_system": true,
	}, pub)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	events := pub.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected 2 bookend events, got %d", len(events))
	}

	startEv, endEv := events[0], events[1]
	if startEv.Pillar != core.PillarRunner || startEv.Component != "runner:benchmark" || startEv.Status != "runner-start" {
		t.Fatalf("unexpected start bookend: %+v", startEv)
	}
	if endEv.Pillar != core.PillarRunner || endEv.Component != "runner:benchmark" || endEv.Status != "runner-aborted" {
		t.Fatalf("unexpected abort bookend: %+v", endEv)
	}
	if startEv.Metrics["run_id"] != endEv.Metrics["run_id"] {
		t.Fatalf("mismatched run_id across bookends: %v vs %v", startEv.Metrics["run_id"], endEv.Metrics["run_id"])
	}
}
