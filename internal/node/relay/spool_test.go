package relay

import (
	"fmt"
	"testing"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
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
