package ingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
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
