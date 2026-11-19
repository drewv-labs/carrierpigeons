package relay

import (
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

const DefaultSpoolCapacity = 10000

// RingSpool is a variable-capacity, thread-safe circular ring buffer for CTD payloads.
// When capacity is reached while PigeonCoop is unreachable, the oldest buffered event is
// evicted in O(1) time without blocking hardware observers or retaining stale heap pointers.
type RingSpool struct {
	mu      sync.Mutex
	buf     []*core.CTDPayload
	head    int
	count   int
	dropped uint64
	notify  chan struct{}
}

// NewRingSpool allocates a bounded circular spool of `capacity` slots.
func NewRingSpool(capacity int) *RingSpool {
	if capacity <= 0 {
		capacity = DefaultSpoolCapacity
	}
	return &RingSpool{
		buf:    make([]*core.CTDPayload, capacity),
		notify: make(chan struct{}, 1),
	}
}

// Push appends p to the tail of the ring buffer, evicting the oldest element if full.
// Returns true if an older event had to be evicted.
func (r *RingSpool) Push(p *core.CTDPayload) bool {
	if p == nil {
		return false
	}
	r.mu.Lock()
	capLen := len(r.buf)
	evicted := false
	if r.count == capLen {
		r.buf[r.head] = p
		r.head = (r.head + 1) % capLen
		r.dropped++
		evicted = true
	} else {
		tail := (r.head + r.count) % capLen
		r.buf[tail] = p
		r.count++
	}
	r.mu.Unlock()

	select {
	case r.notify <- struct{}{}:
	default:
	}
	return evicted
}

// PrependBatch re-inserts undelivered items at the head of the ring buffer in their original FIFO order
// without evicting newer elements already buffered in the spool. If capacity is insufficient, only the
// oldest items from `items` are dropped and counted toward Dropped().
func (r *RingSpool) PrependBatch(items []*core.CTDPayload) {
	if len(items) == 0 {
		return
	}
	nonNil := make([]*core.CTDPayload, 0, len(items))
	for _, p := range items {
		if p != nil {
			nonNil = append(nonNil, p)
		}
	}
	if len(nonNil) == 0 {
		return
	}

	r.mu.Lock()
	capLen := len(r.buf)
	available := capLen - r.count
	if available <= 0 {
		r.dropped += uint64(len(nonNil))
		r.mu.Unlock()
		return
	}
	startIdx := 0
	if len(nonNil) > available {
		excess := len(nonNil) - available
		r.dropped += uint64(excess)
		startIdx = excess
	}
	for i := len(nonNil) - 1; i >= startIdx; i-- {
		r.head = (r.head - 1 + capLen) % capLen
		r.buf[r.head] = nonNil[i]
		r.count++
	}
	r.mu.Unlock()

	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// PopBlocking blocks until at least one event is available or stopCh is closed.
func (r *RingSpool) PopBlocking(stopCh <-chan struct{}) (*core.CTDPayload, bool) {
	batch, ok := r.PopBatchBlocking(1, stopCh)
	if !ok || len(batch) == 0 {
		return nil, false
	}
	return batch[0], true
}

// PopBatchBlocking blocks until at least one event is available (or stopCh closes) and pops up to maxItems in FIFO order.
func (r *RingSpool) PopBatchBlocking(maxItems int, stopCh <-chan struct{}) ([]*core.CTDPayload, bool) {
	if maxItems <= 0 {
		maxItems = 1
	}
	for {
		r.mu.Lock()
		if r.count > 0 {
			n := r.count
			if n > maxItems {
				n = maxItems
			}
			out := make([]*core.CTDPayload, n)
			capLen := len(r.buf)
			for i := range n {
				out[i] = r.buf[r.head]
				r.buf[r.head] = nil // release reference for GC
				r.head = (r.head + 1) % capLen
				r.count--
			}
			hasMore := r.count > 0
			r.mu.Unlock()
			if hasMore {
				select {
				case r.notify <- struct{}{}:
				default:
				}
			}
			return out, true
		}
		r.mu.Unlock()

		select {
		case <-stopCh:
			return nil, false
		case <-r.notify:
		}
	}
}

// DrainAll atomically extracts all currently buffered events in FIFO order (used during graceful Disconnect).
func (r *RingSpool) DrainAll() []*core.CTDPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == 0 {
		return nil
	}
	out := make([]*core.CTDPayload, r.count)
	capLen := len(r.buf)
	for i := range r.count {
		out[i] = r.buf[r.head]
		r.buf[r.head] = nil
		r.head = (r.head + 1) % capLen
	}
	r.count = 0
	return out
}

// Len returns the current number of buffered events awaiting delivery.
func (r *RingSpool) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Cap returns the configured maximum capacity of the ring buffer.
func (r *RingSpool) Cap() int {
	return len(r.buf)
}

// Dropped returns the cumulative count of evicted events due to spool saturation.
func (r *RingSpool) Dropped() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}
