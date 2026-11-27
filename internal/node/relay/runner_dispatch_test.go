package relay

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

func TestRunnerDispatchAbortTargetRequestIDMatchingMismatchedAndWildcard(t *testing.T) {
	mc := &fakeMQTTClient{}
	mc.connected.Store(true)

	c := newTestRelay(mc, 16)
	entered := make(chan string, 8)
	aborted := make(chan string, 8)

	benchTask := &funcRunner{
		name: "benchmark",
		fn: func(ctx context.Context, _ string, params map[string]any, _ core.TelemetryPublisher) error {
			reqID, _ := params["request_id"].(string)
			entered <- reqID
			<-ctx.Done()
			aborted <- reqID
			return nil
		},
	}
	c.runnerReg.Register(benchTask)
	c.runnerReg.RegisterAlias("SyntheticComputeRnr", benchTask)

	pub := &capturePublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.ListenForRunners(ctx, "rv64-compute-01", pub); err != nil {
		t.Fatalf("ListenForRunners failed: %v", err)
	}

	h := mc.handler()
	send := func(env runner.Envelope) {
		t.Helper()
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshal envelope: %v", err)
		}
		h(mc, fakeMessage{topic: "pigeons/control/rv64-compute-01", payload: raw})
	}

	// 1. Start active run req-run-101.
	send(runner.Envelope{
		RequestID: "req-run-101",
		Action:    "start",
		Runner:    "benchmark",
	})
	select {
	case got := <-entered:
		if got != "req-run-101" {
			t.Fatalf("expected req-run-101, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for req-run-101 start")
	}

	// 2. Mismatched TargetRequestID must NOT abort req-run-101.
	send(runner.Envelope{
		RequestID:       "req-abort-transport-1",
		TargetRequestID: "req-other-999",
		Action:          "abort",
		Runner:          "benchmark",
	})
	select {
	case id := <-aborted:
		t.Fatalf("mismatched TargetRequestID aborted active run %q", id)
	case <-time.After(40 * time.Millisecond):
	}

	// 3. Matching TargetRequestID (with its own distinct transport RequestID) MUST abort req-run-101,
	// including when addressed via canonical alias ("SyntheticComputeRnr").
	send(runner.Envelope{
		RequestID:       "req-abort-transport-2",
		TargetRequestID: "req-run-101",
		Action:          "abort",
		Runner:          "SyntheticComputeRnr",
	})
	select {
	case got := <-aborted:
		if got != "req-run-101" {
			t.Fatalf("expected req-run-101 aborted, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for matching TargetRequestID abort")
	}

	// Wait until activeRunners slot clears, then start req-run-202 to test wildcard abort.
	var started202 atomic.Bool
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		send(runner.Envelope{
			RequestID: "req-run-202",
			Action:    "start",
			Runner:    "benchmark",
		})
		select {
		case got := <-entered:
			if got != "req-run-202" {
				t.Fatalf("expected req-run-202, got %q", got)
			}
			started202.Store(true)
		case <-time.After(15 * time.Millisecond):
		}
		if started202.Load() {
			break
		}
	}
	if !started202.Load() {
		t.Fatal("failed starting req-run-202 after req-run-101 abort")
	}

	// 4. Wildcard abort (empty TargetRequestID, non-empty transport RequestID as forwarded by Coop)
	// MUST abort the currently active run for that canonical runner.
	send(runner.Envelope{
		RequestID:       "req-abort-transport-3",
		TargetRequestID: "",
		Action:          "abort",
		Runner:          "benchmark",
	})
	select {
	case got := <-aborted:
		if got != "req-run-202" {
			t.Fatalf("expected wildcard TargetRequestID abort to cancel req-run-202, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wildcard abort with non-empty transport RequestID")
	}
}
