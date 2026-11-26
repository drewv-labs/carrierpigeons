package relay

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func TestRingSpoolEvictsOldestWhenFull(t *testing.T) {
	spool := NewRingSpool(3)
	if spool.Cap() != 3 {
		t.Fatalf("expected cap 3, got %d", spool.Cap())
	}
	if spool.MaxBytes() != DefaultMaxSpoolBytes {
		t.Fatalf("expected default maxBytes %d, got %d", DefaultMaxSpoolBytes, spool.MaxBytes())
	}

	for i := 1; i <= 5; i++ {
		ev := core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("seq-%d", i), "active")
		evicted := spool.Push(ev)
		if i <= 3 && evicted {
			t.Fatalf("unexpected eviction at item %d", i)
		}
		if i > 3 && !evicted {
			t.Fatalf("expected eviction at item %d", i)
		}
	}

	if spool.Len() != 3 {
		t.Fatalf("expected len 3, got %d", spool.Len())
	}
	if spool.Dropped() != 2 {
		t.Fatalf("expected dropped=2, got %d", spool.Dropped())
	}

	stopCh := make(chan struct{})
	want := []string{"seq-3", "seq-4", "seq-5"}
	for _, w := range want {
		got, ok := spool.PopBlocking(stopCh)
		if !ok || got.Component != w {
			t.Fatalf("expected %s, got %+v (ok=%v)", w, got, ok)
		}
	}
	if spool.Len() != 0 {
		t.Fatalf("expected empty spool after draining, got %d", spool.Len())
	}
	if spool.Bytes() != 0 {
		t.Fatalf("expected 0 spool bytes after draining, got %d", spool.Bytes())
	}

	close(stopCh)
	if _, ok := spool.PopBlocking(stopCh); ok {
		t.Fatal("expected PopBlocking to return ok=false when stopCh closed")
	}
}

func TestRingSpoolPrependBatchPreservesFIFOAndNewerItems(t *testing.T) {
	spool := NewRingSpool(4)
	spool.Push(core.NewCTDPayload("rv64-compute-01", "seq-3", "active"))
	spool.Push(core.NewCTDPayload("rv64-compute-01", "seq-4", "active"))

	spool.PrependBatch([]*core.CTDPayload{
		core.NewCTDPayload("rv64-compute-01", "seq-1", "active"),
		core.NewCTDPayload("rv64-compute-01", "seq-2", "active"),
	})

	got := spool.DrainAll()
	want := []string{"seq-1", "seq-2", "seq-3", "seq-4"}
	if len(got) != len(want) {
		t.Fatalf("expected %d items, got %d", len(want), len(got))
	}
	for i, w := range want {
		if got[i].Component != w {
			t.Fatalf("index %d: expected %s, got %s", i, w, got[i].Component)
		}
	}

	// When spool already has 3 newer items (cap=4), prepending 2 older items keeps only 1 older item
	// without evicting any newer items.
	spool.Push(core.NewCTDPayload("rv64-compute-01", "new-1", "active"))
	spool.Push(core.NewCTDPayload("rv64-compute-01", "new-2", "active"))
	spool.Push(core.NewCTDPayload("rv64-compute-01", "new-3", "active"))

	spool.PrependBatch([]*core.CTDPayload{
		core.NewCTDPayload("rv64-compute-01", "old-1", "active"),
		core.NewCTDPayload("rv64-compute-01", "old-2", "active"),
	})
	if spool.Dropped() != 1 {
		t.Fatalf("expected 1 dropped overflow item from prepend, got %d", spool.Dropped())
	}
	got2 := spool.DrainAll()
	want2 := []string{"old-2", "new-1", "new-2", "new-3"}
	for i, w := range want2 {
		if got2[i].Component != w {
			t.Fatalf("index %d: expected %s, got %s", i, w, got2[i].Component)
		}
	}
}

func TestRingSpoolByteCapEvictsOldestAndBoundsMemory(t *testing.T) {
	// High slot capacity (100), tight byte limit (1024 bytes).
	spool := NewRingSpoolWithByteLimit(100, 1024)
	if spool.MaxBytes() != 1024 {
		t.Fatalf("expected MaxBytes=1024, got %d", spool.MaxBytes())
	}

	blob := strings.Repeat("x", 220) // each serialized payload ~330-380 bytes
	for i := 1; i <= 8; i++ {
		ev := core.NewCTDPayload("rv64-compute-01", fmt.Sprintf("seq-%d", i), "active").AddMetric("blob", blob)
		spool.Push(ev)
		if spool.Bytes() > 1024 {
			t.Fatalf("spool bytes %d exceeded cap 1024 at push %d", spool.Bytes(), i)
		}
	}

	if spool.Dropped() == 0 {
		t.Fatal("expected oldest events to be evicted under byte cap")
	}
	if spool.Len() >= 8 {
		t.Fatalf("expected fewer than 8 items retained under 1024-byte cap, got %d", spool.Len())
	}

	items := spool.DrainAll()
	if spool.Bytes() != 0 {
		t.Fatalf("expected 0 bytes after DrainAll, got %d", spool.Bytes())
	}
	// Retained items must be the strictly newest contiguous suffix in FIFO order.
	firstIdx := 8 - len(items) + 1
	for i, item := range items {
		wantComp := fmt.Sprintf("seq-%d", firstIdx+i)
		if item.Component != wantComp {
			t.Fatalf("index %d: expected %s, got %s", i, wantComp, item.Component)
		}
	}
}

func TestRingSpoolByteCapRejectsSingleOversizeItemAndPrependRespectsBytes(t *testing.T) {
	spool := NewRingSpoolWithByteLimit(16, 600)
	small1 := core.NewCTDPayload("rv64-compute-01", "small-1", "active").AddMetric("v", strings.Repeat("a", 80))
	small2 := core.NewCTDPayload("rv64-compute-01", "small-2", "active").AddMetric("v", strings.Repeat("b", 80))
	spool.Push(small1)
	spool.Push(small2)
	prevLen := spool.Len()

	huge := core.NewCTDPayload("rv64-compute-01", "huge", "active").AddMetric("blob", strings.Repeat("z", 700))
	evicted, accepted := spool.PushSized(huge, 800)
	if accepted || !evicted {
		t.Fatalf("expected oversized single item to be rejected without replacing buffer (evicted=%v accepted=%v)", evicted, accepted)
	}
	if spool.Len() != prevLen {
		t.Fatalf("expected existing items preserved when single item exceeds MaxBytes, got len=%d want=%d", spool.Len(), prevLen)
	}

	// PrependBatch must stop prepending once byte cap would be exceeded, keeping newer items intact.
	spool.PrependBatch([]*core.CTDPayload{
		core.NewCTDPayload("rv64-compute-01", "old-1", "active").AddMetric("v", strings.Repeat("c", 180)),
		core.NewCTDPayload("rv64-compute-01", "old-2", "active").AddMetric("v", strings.Repeat("d", 180)),
	})
	if spool.Bytes() > 600 {
		t.Fatalf("PrependBatch exceeded byte cap: %d > 600", spool.Bytes())
	}
	got := spool.DrainAll()
	if len(got) < 2 || got[len(got)-2].Component != "small-1" || got[len(got)-1].Component != "small-2" {
		t.Fatalf("expected newer items small-1, small-2 preserved at tail, got %+v", got)
	}
}

func TestRingSpoolCloseRejectsPushesAndUnblocksPop(t *testing.T) {
	spool := NewRingSpool(8)
	spool.Push(core.NewCTDPayload("rv64-compute-01", "cpu", "active"))

	drained := spool.CloseAndDrain()
	if len(drained) != 1 || drained[0].Component != "cpu" {
		t.Fatalf("expected CloseAndDrain to return 1 cpu item, got %+v", drained)
	}
	if !spool.IsClosed() {
		t.Fatal("expected IsClosed() == true after CloseAndDrain")
	}

	evicted, accepted := spool.PushSized(core.NewCTDPayload("rv64-compute-01", "late", "active"), 100)
	if accepted || evicted {
		t.Fatalf("expected PushSized after Close to return (evicted=false, accepted=false), got (%v, %v)", evicted, accepted)
	}
	if spool.Len() != 0 {
		t.Fatalf("expected late push not stranded in closed spool, got len=%d", spool.Len())
	}

	stopCh := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		_, ok := spool.PopBatchBlocking(4, stopCh)
		done <- ok
	}()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected PopBatchBlocking on closed empty spool to return ok=false")
		}
	case <-time.After(time.Second):
		t.Fatal("PopBatchBlocking blocked on closed spool")
	}
}
