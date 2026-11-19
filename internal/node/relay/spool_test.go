package relay

import (
	"fmt"
	"testing"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func TestRingSpoolEvictsOldestWhenFull(t *testing.T) {
	spool := NewRingSpool(3)
	if spool.Cap() != 3 {
		t.Fatalf("expected cap 3, got %d", spool.Cap())
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
