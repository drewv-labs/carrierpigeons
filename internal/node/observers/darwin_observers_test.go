//go:build darwin

package observers

import (
	"testing"
)

func TestDarwinZeroForkNetAndMemoryObservers(t *testing.T) {
	counters, err := collectDarwinNetCounters()
	if err != nil {
		t.Fatalf("collectDarwinNetCounters failed: %v", err)
	}
	if len(counters) == 0 {
		t.Fatal("expected at least one active Darwin network interface from NET_RT_IFLIST2")
	}

	availMB, ok := readDarwinAvailableMemoryMB()
	if !ok || availMB <= 0 {
		t.Fatalf("expected positive macOS available memory via sysctl, got ok=%v availMB=%.1f", ok, availMB)
	}
}
