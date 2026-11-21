package observers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

func TestRunPeriodicObserverParksWhilePausedWithoutBurstOnResume(t *testing.T) {
	pc := observer.NewPauseController()
	ctx, cancel := context.WithCancel(WithPollInterval(observer.WithPauseController(context.Background(), pc), 20*time.Millisecond))
	defer cancel()

	var samples atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		runPeriodicObserver(ctx, true, func() {
			samples.Add(1)
		})
	}()

	// Wait for initial boot sample + at least 1 periodic tick.
	deadline := time.Now().Add(500 * time.Millisecond)
	for samples.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if samples.Load() < 2 {
		t.Fatalf("expected >=2 samples before pause, got %d", samples.Load())
	}

	release := pc.Acquire()
	// Allow any already in-flight tick to settle, then verify zero new samples across multiple intervals.
	time.Sleep(30 * time.Millisecond)
	frozen := samples.Load()
	time.Sleep(90 * time.Millisecond)
	if after := samples.Load(); after != frozen {
		t.Fatalf("expected 0 samples while paused across 4+ intervals, frozen=%d after=%d", frozen, after)
	}

	// Release pause and verify immediate resumption without burst flooding.
	release()
	deadline = time.Now().Add(500 * time.Millisecond)
	for samples.Load() <= frozen && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if samples.Load() <= frozen {
		t.Fatalf("expected observer to resume immediately after release, still at %d", samples.Load())
	}

	cancel()
	<-done
}
