//go:build linux

package observers

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func TestLinuxProcNetDevSanitizationAndMetricOmission(t *testing.T) {
	tmp := t.TempDir()
	sysNet := filepath.Join(tmp, "sys", "class", "net")

	// eth0: up, valid counters
	if err := os.MkdirAll(filepath.Join(sysNet, "eth0"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sysNet, "eth0", "operstate"), []byte("up\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// eth0.10: unknown operstate (valid VLAN interface, treated as active), corrupt rx_packets ("bad") -> omit rx_packets
	if err := os.MkdirAll(filepath.Join(sysNet, "eth0.10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sysNet, "eth0.10", "operstate"), []byte("unknown\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// ethdown: down -> skipped
	if err := os.MkdirAll(filepath.Join(sysNet, "ethdown"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sysNet, "ethdown", "operstate"), []byte("down\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Also place a trap operstate above sysNet to prove ".." traversal is never read.
	if err := os.WriteFile(filepath.Join(tmp, "sys", "class", "operstate"), []byte("up\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	procNetDev := filepath.Join(tmp, "proc_net_dev")
	content := strings.Join([]string{
		"Inter-|   Receive                                                |  Transmit",
		" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed",
		"    lo: 1000 10 0 0 0 0 0 0 1000 10 0 0 0 0 0 0",
		"  eth0: 2048 16 0 0 0 0 0 0 4096 32 0 0 0 0 0 0",
		"eth0.10: 512 bad 0 0 0 0 0 0 1024 8 0 0 0 0 0 0",
		"ethdown: 999 9 0 0 0 0 0 0 999 9 0 0 0 0 0 0",
		"eth+0: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
		"bad#if: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
		"bad/if: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
		"    ..: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
		"     .: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
		"space if: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
		strings.Repeat("a", 126) + ": 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0",
	}, "\n")
	if err := os.WriteFile(procNetDev, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	pub := &capturePublisher{}
	emitProcNetDevSamplesFrom(procNetDev, sysNet, pub, "linux-node", false)

	events := pub.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected exactly 2 valid events (eth0 and eth0.10), got %d: %+v", len(events), events)
	}

	for _, ev := range events {
		if err := core.ValidateTopicToken(ev.Component); err != nil {
			t.Fatalf("published invalid MQTT component token %q: %v", ev.Component, err)
		}
	}

	if events[0].Component != "net-eth0" {
		t.Fatalf("expected first event net-eth0, got %q", events[0].Component)
	}
	if events[1].Component != "net-eth0.10" {
		t.Fatalf("expected second event net-eth0.10, got %q", events[1].Component)
	}
	if _, hasRxPkts := events[1].Metrics["rx_packets"]; hasRxPkts {
		t.Fatalf("expected unparseable rx_packets on eth0.10 to be omitted, got %v", events[1].Metrics["rx_packets"])
	}
	if events[1].Metrics["rx_bytes"] != 512.0 || events[1].Metrics["tx_bytes"] != 1024.0 || events[1].Metrics["tx_packets"] != 8.0 {
		t.Fatalf("unexpected metrics on eth0.10: %+v", events[1].Metrics)
	}
}
