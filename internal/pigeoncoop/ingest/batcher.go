package ingest

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	defaultQueueSize      = 16384
	defaultMaxQueueBytes  = 64 << 20 // 64 MiB queue + in-flight batch memory budget
	defaultBatchSize      = 2000
	defaultFlushInterval  = 100 * time.Millisecond
	defaultFlushTimeout   = 15 * time.Second
	defaultCloseTimeout   = 10 * time.Second
	defaultMaxRetries     = 3
	defaultRetryBase      = 200 * time.Millisecond
	defaultRetryCap       = 2 * time.Second
	defaultCircuitBase    = 500 * time.Millisecond
	defaultCircuitCap     = 15 * time.Second
	defaultCircuitMaxHold = 60 * time.Second
)

// Sink is the batch persistence target (satisfied by *db.PostgresStore via pgx.CopyFrom).
type Sink interface {
	BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error)
}

// Options configures queue capacity, byte budget, batch threshold, and maximum flush latency.
type Options struct {
	QueueSize     int
	MaxQueueBytes int64
	BatchSize     int
	FlushInterval time.Duration
}

type batcherTiming struct {
	flushTimeout   time.Duration
	closeTimeout   time.Duration
	maxRetries     int
	retryBase      time.Duration
	retryCap       time.Duration
	circuitBase    time.Duration
	circuitCap     time.Duration
	circuitMaxHold time.Duration
}

type queuedEvent struct {
	ev        *core.CTDPayload
	sizeBytes int64
}

// Batcher coalesces high-velocity MQTT CTD events into PostgreSQL binary COPY batches.
type Batcher struct {
	sink           Sink
	batchSize      int
	maxQueueBytes  int64
	flushInterval  time.Duration
	flushTimeout   time.Duration
	closeTimeout   time.Duration
	maxRetries     int
	retryBase      time.Duration
	retryCap       time.Duration
	circuitBase    time.Duration
	circuitCap     time.Duration
	circuitMaxHold time.Duration
	queue          chan queuedEvent

	enqueueMu  sync.RWMutex
	closed     bool
	hardCtx    context.Context
	hardCancel context.CancelFunc
	stopCh     chan struct{}
	doneCh     chan struct{}
	stopOnce   sync.Once

	outageFailures   int
	circuitOpenUntil time.Time

	ingested    atomic.Uint64
	dropped     atomic.Uint64
	flushes     atomic.Uint64
	queuedBytes atomic.Int64
}

// NewBatcher starts a background dual-trigger batch worker (count threshold OR flush timer).
func NewBatcher(sink Sink, opts Options) *Batcher {
	return newBatcherWithTiming(sink, opts, batcherTiming{})
}

func newBatcherWithTiming(sink Sink, opts Options, timing batcherTiming) *Batcher {
	if opts.QueueSize <= 0 {
		opts.QueueSize = defaultQueueSize
	}
	if opts.MaxQueueBytes <= 0 {
		opts.MaxQueueBytes = defaultMaxQueueBytes
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
	if timing.circuitBase <= 0 {
		if timing.retryBase > 0 {
			timing.circuitBase = timing.retryBase
		} else {
			timing.circuitBase = defaultCircuitBase
		}
	}
	if timing.circuitCap <= 0 {
		if timing.retryCap > 0 {
			timing.circuitCap = timing.retryCap
		} else {
			timing.circuitCap = defaultCircuitCap
		}
	}
	if timing.circuitMaxHold <= 0 {
		timing.circuitMaxHold = defaultCircuitMaxHold
	}
	if timing.retryBase <= 0 {
		timing.retryBase = defaultRetryBase
	}
	if timing.retryCap <= 0 {
		timing.retryCap = defaultRetryCap
	}

	hardCtx, hardCancel := context.WithCancel(context.Background())
	b := &Batcher{
		sink:           sink,
		batchSize:      opts.BatchSize,
		maxQueueBytes:  opts.MaxQueueBytes,
		flushInterval:  opts.FlushInterval,
		flushTimeout:   timing.flushTimeout,
		closeTimeout:   timing.closeTimeout,
		maxRetries:     timing.maxRetries,
		retryBase:      timing.retryBase,
		retryCap:       timing.retryCap,
		circuitBase:    timing.circuitBase,
		circuitCap:     timing.circuitCap,
		circuitMaxHold: timing.circuitMaxHold,
		queue:          make(chan queuedEvent, opts.QueueSize),
		hardCtx:        hardCtx,
		hardCancel:     hardCancel,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}

	go b.loop()
	return b
}

func estimateAnyBytes(v any) int64 {
	switch val := v.(type) {
	case nil:
		return 8
	case string:
		return int64(len(val)) + 16
	case []byte:
		return int64(len(val)) + 24
	case []string:
		var total int64 = 24
		for _, item := range val {
			total += int64(len(item)) + 16
		}
		return total
	case []any:
		var total int64 = 24
		for _, item := range val {
			total += estimateAnyBytes(item)
		}
		return total
	case map[string]any:
		var total int64 = 48
		for k, item := range val {
			total += int64(len(k)) + 16 + estimateAnyBytes(item)
		}
		return total
	default:
		return 16
	}
}

func estimatePayloadBytes(ev *core.CTDPayload) int64 {
	if ev == nil {
		return 0
	}
	var n int64 = 256
	n += int64(len(ev.EventID) + len(ev.NodeID) + len(ev.Pillar) + len(ev.Component) + len(ev.Status))
	if raw := ev.RawMetrics(); len(raw) > 0 {
		n += int64(len(raw))
	} else {
		for k, v := range ev.Metrics {
			n += int64(len(k)) + 16 + estimateAnyBytes(v)
		}
	}
	if raw := ev.RawParams(); len(raw) > 0 {
		n += int64(len(raw))
	} else {
		for k, v := range ev.Params {
			n += int64(len(k) + len(v) + 32)
		}
	}
	if ev.Artifact != nil {
		n += 64 + int64(len(ev.Artifact.ID)+len(ev.Artifact.Kind)+len(ev.Artifact.NodeID))
		for k, v := range ev.Artifact.Meta {
			n += int64(len(k) + len(v) + 32)
		}
	}
	if ev.Trace != nil {
		n += 64 + int64(len(ev.Trace.SourceID)+len(ev.Trace.Verb)+len(ev.Trace.TargetID)+len(ev.Trace.NodeID)+len(ev.Trace.RunID))
		for k, v := range ev.Trace.Meta {
			n += int64(len(k) + len(v) + 32)
		}
	}
	return n
}

// Enqueue submits an event to the batch pipeline without blocking the MQTT network reader.
func (b *Batcher) Enqueue(ev *core.CTDPayload) {
	if ev == nil {
		return
	}
	evBytes := estimatePayloadBytes(ev)

	b.enqueueMu.RLock()
	defer b.enqueueMu.RUnlock()
	if b.closed {
		n := b.dropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Ingest] Batcher closed; dropped %d post-close events", n)
		}
		return
	}

	if b.maxQueueBytes > 0 && evBytes > 0 {
		for {
			cur := b.queuedBytes.Load()
			if cur+evBytes > b.maxQueueBytes {
				n := b.dropped.Add(1)
				if n == 1 || n%500 == 0 {
					log.Printf("[Coop Ingest] Queue byte budget saturated (%d/%d bytes); dropped %d events", cur, b.maxQueueBytes, n)
				}
				return
			}
			if b.queuedBytes.CompareAndSwap(cur, cur+evBytes) {
				break
			}
		}
	}

	select {
	case b.queue <- queuedEvent{ev: ev, sizeBytes: evBytes}:
	default:
		if evBytes > 0 {
			b.queuedBytes.Add(-evBytes)
		}
		n := b.dropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Ingest] Queue saturated (%d cap); dropped %d events", cap(b.queue), n)
		}
	}
}

func exponentialJitteredDelay(base, capDur time.Duration, attempt int) time.Duration {
	delay := base << min(attempt, 10)
	if delay <= 0 || delay > capDur {
		delay = capDur
	}
	half := delay / 2
	if half > 0 {
		delay = delay - delay/4 + time.Duration(rand.Int64N(int64(half)))
	}
	if delay > capDur {
		delay = capDur
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (b *Batcher) retryDelay(attempt int) time.Duration {
	return exponentialJitteredDelay(b.retryBase, b.retryCap, attempt)
}

func (b *Batcher) circuitDelay(outageAttempt int) time.Duration {
	return exponentialJitteredDelay(b.circuitBase, b.circuitCap, outageAttempt)
}

func isDBConnectivityError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		code := pgErr.Code
		return strings.HasPrefix(code, "08") ||
			code == "57P01" ||
			code == "57P02" ||
			code == "57P03" ||
			code == "53300" ||
			code == "53400"
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"closed pool",
		"connection closed",
		"bad connection",
		"unexpected eof",
		"failed to connect",
		"cannot connect now",
		"starting up",
		"shutting down",
		"terminating connection",
		"server closed the connection",
		"dial tcp",
		"i/o timeout",
		"too many clients",
		"network is unreachable",
		"no route to host",
		"temporary failure in name resolution",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// sleepInterruptible pauses for d unless shutdown (stopCh) or drain deadline (hardCtx) fires first.
// Returns (true, false) if timer completed, (false, true) if interrupted by stopCh, (false, false) if hardCtx expired.
func (b *Batcher) sleepInterruptible(d time.Duration) (completed bool, stopped bool) {
	if d <= 0 {
		return true, false
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-b.stopCh:
		return false, true
	case <-b.hardCtx.Done():
		return false, false
	case <-timer.C:
		return true, false
	}
}

func (b *Batcher) flushBatch(batch []*core.CTDPayload, batchBytes int64) ([]*core.CTDPayload, int64) {
	if len(batch) == 0 {
		return batch, batchBytes
	}

	release := func() ([]*core.CTDPayload, int64) {
		if batchBytes > 0 {
			b.queuedBytes.Add(-batchBytes)
		}
		clear(batch)
		return batch[:0], 0
	}

	if b.hardCtx.Err() != nil {
		totalDropped := b.dropped.Add(uint64(len(batch)))
		log.Printf("[Coop Ingest] Close drain deadline exceeded; dropped %d queued events (total dropped=%d)", len(batch), totalDropped)
		return release()
	}

	// If cross-batch circuit breaker is open from a prior outage, wait until the next half-open probe window.
	if b.outageFailures > 0 {
		if waitDur := time.Until(b.circuitOpenUntil); waitDur > 0 {
			select {
			case <-b.stopCh:
				totalDropped := b.dropped.Add(uint64(len(batch)))
				log.Printf("[Coop Ingest] Aborted circuit-breaker wait on shutdown (%d dropped, total dropped=%d)",
					len(batch), totalDropped)
				return release()
			default:
			}
			completed, stopped := b.sleepInterruptible(waitDur)
			if !completed {
				totalDropped := b.dropped.Add(uint64(len(batch)))
				if stopped {
					log.Printf("[Coop Ingest] Aborted circuit-breaker wait on shutdown (%d dropped, total dropped=%d)",
						len(batch), totalDropped)
				} else {
					log.Printf("[Coop Ingest] Drain deadline expired during circuit-breaker wait (%d dropped, total dropped=%d)",
						len(batch), totalDropped)
				}
				return release()
			}
		}
	}

	var outageStartedAt time.Time
	hadPriorOutage := b.outageFailures > 0
	nonConnAttempts := 0

	for {
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
			b.outageFailures = 0
			b.circuitOpenUntil = time.Time{}
			b.ingested.Add(uint64(inserted))
			b.flushes.Add(1)
			return release()
		}

		// Partial insert: record ingested subset and count uninserted remainder as dropped.
		// Never retry a partially committed batch to avoid duplicate inserts.
		if inserted > 0 {
			b.outageFailures = 0
			b.circuitOpenUntil = time.Time{}
			b.ingested.Add(uint64(inserted))
			b.flushes.Add(1)
			uninserted := len(batch) - inserted
			if uninserted > 0 {
				totalDropped := b.dropped.Add(uint64(uninserted))
				log.Printf("[Coop Ingest] Partial batch COPY (%d/%d inserted, dropped=%d, total dropped=%d): %v",
					inserted, len(batch), uninserted, totalDropped, err)
			}
			return release()
		}

		if err == nil {
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Batch COPY inserted 0/%d events (total dropped=%d)", len(batch), totalDropped)
			return release()
		}

		if b.hardCtx.Err() != nil {
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Drain deadline expired during batch COPY (%d dropped, total dropped=%d): %v",
				len(batch), totalDropped, err)
			return release()
		}

		if isDBConnectivityError(err) {
			if outageStartedAt.IsZero() {
				outageStartedAt = time.Now()
			}
			b.outageFailures++
			delay := b.circuitDelay(b.outageFailures - 1)
			b.circuitOpenUntil = time.Now().Add(delay)

			select {
			case <-b.stopCh:
				totalDropped := b.dropped.Add(uint64(len(batch)))
				log.Printf("[Coop Ingest] Aborted DB outage retry on shutdown (%d dropped, total dropped=%d): %v",
					len(batch), totalDropped, err)
				return release()
			default:
			}

			// When an outage persists across an entire hold window, drop this batch while keeping
			// the circuit open so subsequent batches wait circuitOpenUntil instead of shredding rapidly.
			if hadPriorOutage || time.Since(outageStartedAt)+delay > b.circuitMaxHold {
				totalDropped := b.dropped.Add(uint64(len(batch)))
				log.Printf("[Coop Ingest] DB outage circuit open (failures=%d, next_probe=%s); dropped %d events (total dropped=%d): %v",
					b.outageFailures, delay, len(batch), totalDropped, err)
				return release()
			}

			log.Printf("[Coop Ingest] DB connectivity error (outage_attempt=%d, circuit backoff=%s, holding %d events): %v",
				b.outageFailures, delay, len(batch), err)
			completed, stopped := b.sleepInterruptible(delay)
			if !completed {
				totalDropped := b.dropped.Add(uint64(len(batch)))
				if stopped {
					log.Printf("[Coop Ingest] Aborted DB outage backoff sleep on shutdown (%d dropped, total dropped=%d): %v",
						len(batch), totalDropped, err)
				} else {
					log.Printf("[Coop Ingest] Drain deadline expired during DB outage backoff (%d dropped, total dropped=%d): %v",
						len(batch), totalDropped, err)
				}
				return release()
			}
			continue
		}

		b.outageFailures = 0
		b.circuitOpenUntil = time.Time{}

		if nonConnAttempts == b.maxRetries {
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Batch COPY permanently failed after %d attempts (%d dropped, total dropped=%d): %v",
				nonConnAttempts+1, len(batch), totalDropped, err)
			return release()
		}

		select {
		case <-b.stopCh:
			totalDropped := b.dropped.Add(uint64(len(batch)))
			log.Printf("[Coop Ingest] Aborted batch COPY retry on shutdown (%d dropped, total dropped=%d): %v",
				len(batch), totalDropped, err)
			return release()
		default:
		}

		delay := b.retryDelay(nonConnAttempts)
		log.Printf("[Coop Ingest] Batch COPY error (attempt %d/%d, retrying in %s): %v",
			nonConnAttempts+1, b.maxRetries+1, delay, err)
		nonConnAttempts++

		completed, stopped := b.sleepInterruptible(delay)
		if !completed {
			totalDropped := b.dropped.Add(uint64(len(batch)))
			if stopped {
				log.Printf("[Coop Ingest] Aborted batch COPY retry sleep on shutdown (%d dropped, total dropped=%d): %v",
					len(batch), totalDropped, err)
			} else {
				log.Printf("[Coop Ingest] Drain deadline expired during retry (%d dropped, total dropped=%d): %v",
					len(batch), totalDropped, err)
			}
			return release()
		}
	}
}

func (b *Batcher) dropRemaining(batch []*core.CTDPayload, batchBytes int64) {
	undrained := len(batch)
	drainedBytes := batchBytes
	clear(batch)
drainLoop:
	for {
		select {
		case qe := <-b.queue:
			if qe.ev != nil {
				undrained++
			}
			drainedBytes += qe.sizeBytes
		default:
			break drainLoop
		}
	}
	if drainedBytes > 0 {
		b.queuedBytes.Add(-drainedBytes)
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
	var batchBytes int64

	for {
		select {
		case qe := <-b.queue:
			batch = append(batch, qe.ev)
			batchBytes += qe.sizeBytes
			if len(batch) >= b.batchSize {
				batch, batchBytes = b.flushBatch(batch, batchBytes)
			}
		case <-ticker.C:
			batch, batchBytes = b.flushBatch(batch, batchBytes)
		case <-b.stopCh:
			// Drain remaining queued items before exiting, bounded by a single closeTimeout deadline.
			for {
				if b.hardCtx.Err() != nil {
					b.dropRemaining(batch, batchBytes)
					return
				}
				select {
				case qe := <-b.queue:
					batch = append(batch, qe.ev)
					batchBytes += qe.sizeBytes
					if len(batch) >= b.batchSize {
						batch, batchBytes = b.flushBatch(batch, batchBytes)
					}
				default:
					_, _ = b.flushBatch(batch, batchBytes)
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
		b.enqueueMu.Lock()
		b.closed = true
		b.enqueueMu.Unlock()

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
func (b *Batcher) QueuedBytes() int64    { return b.queuedBytes.Load() }
