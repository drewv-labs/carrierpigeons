package ingest

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// Sink is the batch persistence target (satisfied by *db.PostgresStore via pgx.CopyFrom).
type Sink interface {
	BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error)
}

// Options configures queue capacity, batch threshold, and maximum flush latency.
type Options struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
}

// Batcher coalesces high-velocity MQTT CTD events into PostgreSQL binary COPY batches.
type Batcher struct {
	sink          Sink
	batchSize     int
	flushInterval time.Duration
	queue         chan *core.CTDPayload

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once

	ingested atomic.Uint64
	dropped  atomic.Uint64
	flushes  atomic.Uint64
}

// NewBatcher starts a background dual-trigger batch worker (count threshold OR flush timer).
func NewBatcher(sink Sink, opts Options) *Batcher {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 65536
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 2000
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 100 * time.Millisecond
	}

	b := &Batcher{
		sink:          sink,
		batchSize:     opts.BatchSize,
		flushInterval: opts.FlushInterval,
		queue:         make(chan *core.CTDPayload, opts.QueueSize),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	go b.loop()
	return b
}

// Enqueue submits an event to the batch pipeline without blocking the MQTT network reader.
func (b *Batcher) Enqueue(ev *core.CTDPayload) {
	if ev == nil {
		return
	}
	select {
	case <-b.stopCh:
		return
	default:
	}

	select {
	case b.queue <- ev:
	default:
		n := b.dropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Ingest] Queue saturated (%d cap); dropped %d events", cap(b.queue), n)
		}
	}
}

func (b *Batcher) loop() {
	defer close(b.doneCh)

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]*core.CTDPayload, 0, b.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		n, err := b.sink.BatchInsertEvents(ctx, batch)
		cancel()
		if err != nil {
			log.Printf("[Coop Ingest] Batch COPY error (inserted %d/%d): %v", n, len(batch), err)
		}
		if n > 0 {
			b.ingested.Add(uint64(n))
			b.flushes.Add(1)
		}
		clear(batch)
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-b.queue:
			batch = append(batch, ev)
			if len(batch) >= b.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.stopCh:
			// Drain remaining queued items before exiting
			for {
				select {
				case ev := <-b.queue:
					batch = append(batch, ev)
					if len(batch) >= b.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// Close stops accepting new events and blocks until all queued batches are flushed to PostgreSQL.
func (b *Batcher) Close() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh
	})
}

func (b *Batcher) IngestedCount() uint64 { return b.ingested.Load() }
func (b *Batcher) DroppedCount() uint64  { return b.dropped.Load() }
func (b *Batcher) FlushCount() uint64    { return b.flushes.Load() }
