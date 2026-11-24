package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type mockBatchSink struct {
	mu      sync.Mutex
	batches []int
	total   int
}

func (m *mockBatchSink) BatchInsertEvents(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, len(payloads))
	m.total += len(payloads)
	return int64(len(payloads)), nil
}

type scriptedSink struct {
	fn func(ctx context.Context, payloads []*core.CTDPayload) (int64, error)
}

func (s *scriptedSink) BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error) {
	return s.fn(ctx, payloads)
}

func TestBatcherFlushesOnThresholdAndDrain(t *testing.T) {
	sink := &mockBatchSink{}
	b := NewBatcher(sink, Options{
		QueueSize:     1024,
		BatchSize:     10,
		FlushInterval: 500 * time.Millisecond,
	})

	for i := range 25 {
		b.Enqueue(core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("m-%d", i), "active"))
	}

	b.Close()

	if b.IngestedCount() != 25 || sink.total != 25 {
		t.Fatalf("expected 25 ingested events after drain, got ingested=%d sink=%d", b.IngestedCount(), sink.total)
	}
	if b.FlushCount() < 3 {
		t.Fatalf("expected at least 3 flushes (10+10+5), got %d (%v)", b.FlushCount(), sink.batches)
	}
}

func TestBatcherDefaultQueueSize16384(t *testing.T) {
	b := NewBatcher(&mockBatchSink{}, Options{})
	defer b.Close()
	if got := cap(b.queue); got != 16384 {
		t.Fatalf("expected default queue capacity 16384, got %d", got)
	}
}

func TestBatcherRetryTransientFailure(t *testing.T) {
	var attempts atomic.Int32
	done := make(chan struct{})
	sink := &scriptedSink{
		fn: func(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
			n := attempts.Add(1)
			if n <= 2 {
				return 0, errors.New("transient pg connection reset")
			}
			select {
			case <-done:
			default:
				close(done)
			}
			return int64(len(payloads)), nil
		},
	}

	b := newBatcherWithTiming(sink, Options{
		QueueSize:     64,
		BatchSize:     10,
		FlushInterval: time.Hour,
	}, batcherTiming{
		flushTimeout: time.Second,
		closeTimeout: time.Second,
		maxRetries:   3,
		retryBase:    5 * time.Millisecond,
		retryCap:     20 * time.Millisecond,
	})

	for i := range 10 {
		b.Enqueue(core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("m-%d", i), "active"))
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for transient failure retry to succeed")
	}
	b.Close()

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got %d", got)
	}
	if b.IngestedCount() != 10 {
		t.Fatalf("expected 10 ingested events, got %d", b.IngestedCount())
	}
	if b.DroppedCount() != 0 {
		t.Fatalf("expected 0 dropped events, got %d", b.DroppedCount())
	}
}

func TestBatcherPermanentFailureAndPartialInsertCountDropped(t *testing.T) {
	t.Run("permanent failure", func(t *testing.T) {
		var attempts atomic.Int32
		sink := &scriptedSink{
			fn: func(_ context.Context, _ []*core.CTDPayload) (int64, error) {
				attempts.Add(1)
				return 0, errors.New("pg permanently unavailable")
			},
		}
		b := newBatcherWithTiming(sink, Options{
			QueueSize:     64,
			BatchSize:     10,
			FlushInterval: 10 * time.Millisecond,
		}, batcherTiming{
			flushTimeout: time.Second,
			closeTimeout: time.Second,
			maxRetries:   3,
			retryBase:    2 * time.Millisecond,
			retryCap:     10 * time.Millisecond,
		})

		for i := range 10 {
			b.Enqueue(core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("m-%d", i), "active"))
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && b.DroppedCount() < 10 {
			time.Sleep(5 * time.Millisecond)
		}
		b.Close()

		if got := attempts.Load(); got != 4 {
			t.Fatalf("expected 4 attempts (1 initial + 3 retries), got %d", got)
		}
		if b.IngestedCount() != 0 || b.DroppedCount() != 10 {
			t.Fatalf("expected ingested=0 dropped=10, got ingested=%d dropped=%d", b.IngestedCount(), b.DroppedCount())
		}
	})

	t.Run("partial insert", func(t *testing.T) {
		var attempts atomic.Int32
		sink := &scriptedSink{
			fn: func(_ context.Context, _ []*core.CTDPayload) (int64, error) {
				attempts.Add(1)
				return 4, errors.New("bisect skipped 6 malformed rows")
			},
		}
		b := newBatcherWithTiming(sink, Options{
			QueueSize:     64,
			BatchSize:     10,
			FlushInterval: time.Hour,
		}, batcherTiming{
			flushTimeout: time.Second,
			closeTimeout: time.Second,
			maxRetries:   3,
			retryBase:    5 * time.Millisecond,
			retryCap:     20 * time.Millisecond,
		})

		for i := range 10 {
			b.Enqueue(core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("p-%d", i), "active"))
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && b.IngestedCount() < 4 {
			time.Sleep(5 * time.Millisecond)
		}
		b.Close()

		if got := attempts.Load(); got != 1 {
			t.Fatalf("expected partial insert not to retry (attempts=1), got %d", got)
		}
		if b.IngestedCount() != 4 || b.DroppedCount() != 6 {
			t.Fatalf("expected ingested=4 dropped=6, got ingested=%d dropped=%d", b.IngestedCount(), b.DroppedCount())
		}
	})
}

func TestBatcherAbortsRetrySleepOnClose(t *testing.T) {
	firstFailed := make(chan struct{})
	sink := &scriptedSink{
		fn: func(_ context.Context, _ []*core.CTDPayload) (int64, error) {
			select {
			case <-firstFailed:
			default:
				close(firstFailed)
			}
			return 0, errors.New("transient error")
		},
	}

	b := newBatcherWithTiming(sink, Options{
		QueueSize:     64,
		BatchSize:     5,
		FlushInterval: time.Hour,
	}, batcherTiming{
		flushTimeout: time.Second,
		closeTimeout: time.Second,
		maxRetries:   3,
		retryBase:    5 * time.Second,
		retryCap:     10 * time.Second,
	})

	for i := range 5 {
		b.Enqueue(core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("s-%d", i), "active"))
	}
	<-firstFailed
	// Give loop a tiny window to enter the 5s retry sleep.
	time.Sleep(15 * time.Millisecond)

	start := time.Now()
	b.Close()
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected Close() to abort retry sleep immediately, took %s", elapsed)
	}
	if b.DroppedCount() != 5 {
		t.Fatalf("expected 5 dropped events after aborted retry, got %d", b.DroppedCount())
	}
}

func TestBatcherCloseDrainTimeoutDropsUndrained(t *testing.T) {
	inFlight := make(chan struct{})
	sink := &scriptedSink{
		fn: func(ctx context.Context, _ []*core.CTDPayload) (int64, error) {
			select {
			case <-inFlight:
			default:
				close(inFlight)
			}
			<-ctx.Done()
			return 0, ctx.Err()
		},
	}

	b := newBatcherWithTiming(sink, Options{
		QueueSize:     64,
		BatchSize:     5,
		FlushInterval: time.Hour,
	}, batcherTiming{
		flushTimeout: 5 * time.Second,
		closeTimeout: 80 * time.Millisecond,
		maxRetries:   3,
		retryBase:    5 * time.Millisecond,
		retryCap:     20 * time.Millisecond,
	})

	for i := range 30 {
		b.Enqueue(core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("d-%d", i), "active"))
	}
	<-inFlight

	start := time.Now()
	b.Close()
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected Close() drain to respect 80ms bound, took %s", elapsed)
	}
	if b.IngestedCount() != 0 || b.DroppedCount() != 30 {
		t.Fatalf("expected ingested=0 dropped=30 after drain timeout, got ingested=%d dropped=%d",
			b.IngestedCount(), b.DroppedCount())
	}
}
