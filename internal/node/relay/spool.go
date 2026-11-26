package relay

import (
	"encoding/json"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

const (
	DefaultSpoolCapacity = 10000
	DefaultMaxSpoolBytes = 16 << 20 // 16 MiB bound for memory-constrained SBCs
)

type spoolEntry struct {
	payload *core.CTDPayload
	bytes   int64
}

// RingSpool is a variable-capacity, byte-bounded, thread-safe circular ring buffer for CTD payloads.
// When item capacity or byte capacity is reached while PigeonCoop is unreachable, the oldest buffered
// events are evicted in O(1) amortized time without blocking observers or retaining stale heap pointers.
type RingSpool struct {
	mu       sync.Mutex
	buf      []spoolEntry
	head     int
	count    int
	curBytes int64
	maxBytes int64
	dropped  uint64
	closed   bool
	notify   chan struct{}
}

// NewRingSpool allocates a bounded circular spool of `capacity` slots with the default 16 MiB byte ceiling.
func NewRingSpool(capacity int) *RingSpool {
	return NewRingSpoolWithByteLimit(capacity, DefaultMaxSpoolBytes)
}

// NewRingSpoolWithByteLimit allocates a circular spool bounded by both `capacity` slots and `maxBytes` serialized payload bytes.
func NewRingSpoolWithByteLimit(capacity int, maxBytes int64) *RingSpool {
	if capacity <= 0 {
		capacity = DefaultSpoolCapacity
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSpoolBytes
	}
	return &RingSpool{
		buf:      make([]spoolEntry, capacity),
		maxBytes: maxBytes,
		notify:   make(chan struct{}, 1),
	}
}

func payloadByteSize(p *core.CTDPayload) int64 {
	if p == nil {
		return 0
	}
	if raw, err := json.Marshal(p); err == nil && len(raw) > 0 {
		return int64(len(raw))
	}
	est := int64(128 + len(p.NodeID) + len(p.Component) + len(p.Status) + len(p.EventID))
	for k, v := range p.Params {
		est += int64(len(k) + len(v) + 8)
	}
	if est < 1 {
		est = 1
	}
	return est
}

// Push appends p to the tail of the ring buffer, evicting oldest elements if slot or byte capacity is full.
// Returns true if an older event had to be evicted (or p was dropped due to exceeding MaxBytes).
func (r *RingSpool) Push(p *core.CTDPayload) bool {
	evicted, _ := r.PushSized(p, 0)
	return evicted
}

// PushSized appends p with a known serialized byte size, enforcing both slot and byte caps and rejecting pushes once closed.
// Returns (evicted, accepted). When closed or when sizeBytes exceeds maxBytes, accepted is false.
func (r *RingSpool) PushSized(p *core.CTDPayload, sizeBytes int64) (evicted bool, accepted bool) {
	if p == nil {
		return false, false
	}
	itemBytes := sizeBytes
	if itemBytes <= 0 {
		itemBytes = payloadByteSize(p)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false, false
	}
	if itemBytes > r.maxBytes {
		r.dropped++
		r.mu.Unlock()
		return true, false
	}

	capLen := len(r.buf)
	for r.count > 0 && (r.count >= capLen || r.curBytes+itemBytes > r.maxBytes) {
		oldest := r.buf[r.head]
		r.buf[r.head] = spoolEntry{}
		r.head = (r.head + 1) % capLen
		r.count--
		r.curBytes -= oldest.bytes
		r.dropped++
		evicted = true
	}

	tail := (r.head + r.count) % capLen
	r.buf[tail] = spoolEntry{payload: p, bytes: itemBytes}
	r.count++
	r.curBytes += itemBytes
	r.mu.Unlock()

	select {
	case r.notify <- struct{}{}:
	default:
	}
	return evicted, true
}

// PrependBatch re-inserts undelivered items at the head of the ring buffer in their original FIFO order
// without evicting newer elements already buffered in the spool. If slot or byte capacity is insufficient,
// only the oldest items from `items` are dropped and counted toward Dropped().
func (r *RingSpool) PrependBatch(items []*core.CTDPayload) {
	if len(items) == 0 {
		return
	}
	entries := make([]spoolEntry, 0, len(items))
	for _, p := range items {
		if p != nil {
			entries = append(entries, spoolEntry{
				payload: p,
				bytes:   payloadByteSize(p),
			})
		}
	}
	if len(entries) == 0 {
		return
	}

	r.mu.Lock()
	if r.closed {
		r.dropped += uint64(len(entries))
		r.mu.Unlock()
		return
	}
	capLen := len(r.buf)
	prepended := 0
	for i := len(entries) - 1; i >= 0; i-- {
		if r.count >= capLen || r.curBytes+entries[i].bytes > r.maxBytes {
			r.dropped += uint64(i + 1)
			break
		}
		r.head = (r.head - 1 + capLen) % capLen
		r.buf[r.head] = entries[i]
		r.count++
		r.curBytes += entries[i].bytes
		prepended++
	}
	r.mu.Unlock()

	if prepended > 0 {
		select {
		case r.notify <- struct{}{}:
		default:
		}
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

// PopBatchBlocking blocks until at least one event is available (or stopCh/spool closes) and pops up to maxItems in FIFO order.
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
				entry := r.buf[r.head]
				out[i] = entry.payload
				r.curBytes -= entry.bytes
				r.buf[r.head] = spoolEntry{} // release reference for GC
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
		if r.closed {
			r.mu.Unlock()
			return nil, false
		}
		r.mu.Unlock()

		select {
		case <-stopCh:
			return nil, false
		case <-r.notify:
		}
	}
}

func (r *RingSpool) drainLocked() []*core.CTDPayload {
	if r.count == 0 {
		return nil
	}
	out := make([]*core.CTDPayload, r.count)
	capLen := len(r.buf)
	for i := range r.count {
		out[i] = r.buf[r.head].payload
		r.buf[r.head] = spoolEntry{}
		r.head = (r.head + 1) % capLen
	}
	r.count = 0
	r.curBytes = 0
	return out
}

// DrainAll atomically extracts all currently buffered events in FIFO order.
func (r *RingSpool) DrainAll() []*core.CTDPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drainLocked()
}

// Close marks the spool closed under r.mu so subsequent pushes are rejected and waiting consumers unblock.
func (r *RingSpool) Close() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// CloseAndDrain atomically marks the spool closed and drains all buffered events under r.mu so no late push can be stranded.
func (r *RingSpool) CloseAndDrain() []*core.CTDPayload {
	r.mu.Lock()
	r.closed = true
	out := r.drainLocked()
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
	return out
}

// IsClosed reports whether Close or CloseAndDrain has been called on the spool.
func (r *RingSpool) IsClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// Len returns the current number of buffered events awaiting delivery.
func (r *RingSpool) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Bytes returns the current serialized byte size of all buffered events.
func (r *RingSpool) Bytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.curBytes
}

// Cap returns the configured maximum slot capacity of the ring buffer.
func (r *RingSpool) Cap() int {
	return len(r.buf)
}

// MaxBytes returns the configured maximum byte capacity of the ring buffer.
func (r *RingSpool) MaxBytes() int64 {
	return r.maxBytes
}

// Dropped returns the cumulative count of evicted events due to spool saturation.
func (r *RingSpool) Dropped() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}
