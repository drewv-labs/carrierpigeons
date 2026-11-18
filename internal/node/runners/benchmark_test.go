package runners

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
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
	if _, ok := runner.Get("SyntheticComputeRnr"); !ok {
		t.Fatal("expected SyntheticComputeRnr alias to be registered")
	}

	pub := &capturePublisher{}
	r := &BenchmarkRunner{}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	err := r.Run(ctx, "rv64-compute-01", map[string]any{
		"request_id":    "req-bench-01",
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
	if startEv.Params["request_id"] != "req-bench-01" || endEv.Params["request_id"] != "req-bench-01" {
		t.Fatalf("expected request_id propagation across bookends: %+v / %+v", startEv.Params, endEv.Params)
	}
	if startEv.Metrics["run_id"] != endEv.Metrics["run_id"] {
		t.Fatalf("mismatched run_id across bookends: %v vs %v", startEv.Metrics["run_id"], endEv.Metrics["run_id"])
	}
}

func TestEphemeralRunnerExecutesAndEnforcesPolicy(t *testing.T) {
	pub := &capturePublisher{}
	er := &EphemeralRunner{}

	spec := runner.EphemeralSpec{
		Interpreter: "sh",
		ScriptBody:  "echo hello-from-$PIGEONS_NODE_ID\n",
		TimeoutSec:  5,
	}
	params := spec.ToParams()
	params["request_id"] = "req-eph-01"

	// Default policy must deny ephemeral execution unless explicitly opted in.
	if err := er.Run(context.Background(), "rv64-compute-01", params, &capturePublisher{}); err == nil {
		t.Fatal("expected default policy (AllowEphemeral=false) to block execution")
	}

	allowCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash", "python3"},
		MaxTimeoutSec:       30,
	})
	if err := er.Run(allowCtx, "rv64-compute-01", params, pub); err != nil {
		t.Fatalf("EphemeralRunner failed: %v", err)
	}

	events := pub.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected 2 events from ephemeral runner, got %d", len(events))
	}
	endEv := events[1]
	if endEv.Status != "runner-end" || endEv.Metrics["exit_code"] != 0 {
		t.Fatalf("expected clean runner-end exit_code=0, got %+v", endEv)
	}
	stdout, _ := endEv.Metrics["stdout"].(string)
	if !strings.Contains(stdout, "hello-from-rv64-compute-01") {
		t.Fatalf("unexpected stdout capture: %q", stdout)
	}

	// Disallowed interpreter must fail before execution
	blockedPub := &capturePublisher{}
	badParams := map[string]any{
		"interpreter": "perl",
		"script_body": "print 1;",
	}
	if err := er.Run(allowCtx, "rv64-compute-01", badParams, blockedPub); err == nil {
		t.Fatal("expected disallowed interpreter 'perl' to be rejected")
	}
}
