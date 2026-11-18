package relay

import (
	"sync"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
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

// PopBlocking blocks until at least one event is available or stopCh is closed.
func (r *RingSpool) PopBlocking(stopCh <-chan struct{}) (*core.CTDPayload, bool) {
	for {
		r.mu.Lock()
		if r.count > 0 {
			item := r.buf[r.head]
			r.buf[r.head] = nil // release reference for GC
			r.head = (r.head + 1) % len(r.buf)
			r.count--
			hasMore := r.count > 0
			r.mu.Unlock()
			if hasMore {
				select {
				case r.notify <- struct{}{}:
				default:
				}
			}
			return item, true
		}
		r.mu.Unlock()

		select {
		case <-stopCh:
			return nil, false
		case <-r.notify:
		}
	}
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
