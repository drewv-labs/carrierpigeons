package ingest

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

const (
	defaultQueueSize     = 16384
	defaultBatchSize     = 2000
	defaultFlushInterval = 100 * time.Millisecond
	defaultFlushTimeout  = 15 * time.Second
	defaultCloseTimeout  = 10 * time.Second
	defaultMaxRetries    = 3
	defaultRetryBase     = 200 * time.Millisecond
	defaultRetryCap      = 2 * time.Second
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

type batcherTiming struct {
	flushTimeout time.Duration
	closeTimeout time.Duration
	maxRetries   int
	retryBase    time.Duration
	retryCap     time.Duration
}

// Batcher coalesces high-velocity MQTT CTD events into PostgreSQL binary COPY batches.
type Batcher struct {
	sink          Sink
	batchSize     int
	flushInterval time.Duration
	flushTimeout  time.Duration
	closeTimeout  time.Duration
	maxRetries    int
	retryBase     time.Duration
	retryCap      time.Duration
	queue         chan *core.CTDPayload

	hardCtx    context.Context
	hardCancel context.CancelFunc
	stopCh     chan struct{}
	doneCh     chan struct{}
	stopOnce   sync.Once

	ingested atomic.Uint64
	dropped  atomic.Uint64
	flushes  atomic.Uint64
}

// NewBatcher starts a background dual-trigger batch worker (count threshold OR flush timer).
func NewBatcher(sink Sink, opts Options) *Batcher {
	return newBatcherWithTiming(sink, opts, batcherTiming{})
}

func newBatcherWithTiming(sink Sink, opts Options, timing batcherTiming) *Batcher {
	if opts.QueueSize <= 0 {
		opts.QueueSize = defaultQueueSize
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if timing.flushTimeout <= 0 {
		timing.flushTimeout = defaultFlushTimeout
	}
	if timing.closeTimeout <= 0 {
		timing.closeTimeout = defaultCloseTimeout
	}
	if timing.maxRetries <= 0 {
		timing.maxRetries = defaultMaxRetries
	}
	if timing.retryBase <= 0 {
		timing.retryBase = defaultRetryBase
	}
	if timing.retryCap <= 0 {
		timing.retryCap = defaultRetryCap
	}

	hardCtx, hardCancel := context.WithCancel(context.Background())
	b := &Batcher{
		sink:          sink,
		batchSize:     opts.BatchSize,
		flushInterval: opts.FlushInterval,
		flushTimeout:  timing.flushTimeout,
		closeTimeout:  timing.closeTimeout,
		maxRetries:    timing.maxRetries,
		retryBase:     timing.retryBase,
		retryCap:      timing.retryCap,
		queue:         make(chan *core.CTDPayload, opts.QueueSize),
		hardCtx:       hardCtx,
		hardCancel:    hardCancel,
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

func (b *Batcher) retryDelay(attempt int) time.Duration {
	delay := b.retryBase << min(attempt, 10)
	if delay <= 0 || delay > b.retryCap {
		delay = b.retryCap
	}
	half := delay / 2
	if half > 0 {
		delay = delay - delay/4 + time.Duration(rand.Int64N(int64(half)))
	}
	if delay > b.retryCap {
		delay = b.retryCap
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (b *Batcher) flushBatch(batch []*core.CTDPayload) []*core.CTDPayload {
	if len(batch) == 0 {
		return batch
	}

	if b.hardCtx.Err() != nil {
		totalDropped := b.dropped.Add(uint64(len(batch)))
		log.Printf("[Coop Ingest] Close drain deadline exceeded; dropped %d queued events (total dropped=%d)", len(batch), totalDropped)
		clear(batch)
		return batch[:0]
	}

	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(b.hardCtx, b.flushTimeout)
		n, err := b.sink.BatchInsertEvents(ctx, batch)
		cancel()

		inserted := int(n)
		if inserted < 0 {
			inserted = 0
		}
		if inserted > len(batch) {
			inserted = len(batch)
		}

		if err == nil && inserted == len(batch) {
			b.ingested.Add(uint64(inserted))
			b.flushes.Add(1)
			clear(batch)
			return batch[:0]
		}

		// Partial insert: record ingested subset and count uninserted remainder as dropped.
		// Never retry a partially committed batch to avoid duplicate inserts.
		if inserted > 0 {
			b.ingested.Add(uint64(inserted))
			b.flushes.Add(1)
			uninserted := len(batch) - inserted
			if uninserted > 0 {
				totalDropped := b.dropped.Add(uint64(uninserted))
				log.Printf("[Coop Ingest] Partial batch COPY (%d/%d inserted, dropped=%d, total dropped=%d): %v",
					inserted, len(batch), uninserted, totalDropped, err)
			}
			clear(batch)
			return batch[:0]
		}

		if err == nil {
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Batch COPY inserted 0/%d events (total dropped=%d)", len(batch), totalDropped)
			clear(batch)
			return batch[:0]
		}

		if attempt == b.maxRetries || b.hardCtx.Err() != nil {
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Batch COPY permanently failed after %d attempts (%d dropped, total dropped=%d): %v",
				attempt+1, len(batch), totalDropped, err)
			clear(batch)
			return batch[:0]
		}

		select {
		case <-b.stopCh:
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Aborted batch COPY retry on shutdown (%d dropped, total dropped=%d): %v",
				len(batch), totalDropped, err)
			clear(batch)
			return batch[:0]
		default:
		}

		delay := b.retryDelay(attempt)
		log.Printf("[Coop Ingest] Batch COPY error (attempt %d/%d, retrying in %s): %v",
			attempt+1, b.maxRetries+1, delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-b.stopCh:
			timer.Stop()
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Aborted batch COPY retry sleep on shutdown (%d dropped, total dropped=%d): %v",
				len(batch), totalDropped, err)
			clear(batch)
			return batch[:0]
		case <-b.hardCtx.Done():
			timer.Stop()
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Drain deadline expired during retry (%d dropped, total dropped=%d): %v",
				len(batch), totalDropped, err)
			clear(batch)
			return batch[:0]
		case <-timer.C:
		}
	}

	clear(batch)
	return batch[:0]
}

func (b *Batcher) dropRemaining(batch []*core.CTDPayload) {
	undrained := len(batch)
	clear(batch)
drainLoop:
	for {
		select {
		case ev := <-b.queue:
			if ev != nil {
				undrained++
			}
		default:
			break drainLoop
		}
	}
	if undrained > 0 {
		totalDropped := b.dropped.Add(uint64(undrained))
		log.Printf("[Coop Ingest] Close drain deadline (%s) exceeded; dropped %d undrained events (total dropped=%d)",
			b.closeTimeout, undrained, totalDropped)
	}
}

func (b *Batcher) loop() {
	defer close(b.doneCh)

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]*core.CTDPayload, 0, b.batchSize)

	for {
		select {
		case ev := <-b.queue:
			batch = append(batch, ev)
			if len(batch) >= b.batchSize {
				batch = b.flushBatch(batch)
			}
		case <-ticker.C:
			batch = b.flushBatch(batch)
		case <-b.stopCh:
			// Drain remaining queued items before exiting, bounded by a single closeTimeout deadline.
			for {
				if b.hardCtx.Err() != nil {
					b.dropRemaining(batch)
					return
				}
				select {
				case ev := <-b.queue:
					batch = append(batch, ev)
					if len(batch) >= b.batchSize {
						batch = b.flushBatch(batch)
					}
				default:
					b.flushBatch(batch)
					return
				}
			}
		}
	}
}

// Close stops accepting new events and blocks until all queued batches are flushed to PostgreSQL
// or the bounded drain deadline expires.
func (b *Batcher) Close() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		timer := time.AfterFunc(b.closeTimeout, b.hardCancel)
		<-b.doneCh
		timer.Stop()
		b.hardCancel()
	})
}

func (b *Batcher) IngestedCount() uint64 { return b.ingested.Load() }
func (b *Batcher) DroppedCount() uint64  { return b.dropped.Load() }
func (b *Batcher) FlushCount() uint64    { return b.flushes.Load() }
