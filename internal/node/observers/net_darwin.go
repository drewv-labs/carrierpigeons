//go:build darwin

package observers

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNetPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[Net Observer] macOS networking detected. Booting interface byte-counter observer.")
	go observeNetDarwin(ctx, pub, nodeID)
}

func emitDarwinNetSamples(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	out, err := runCommand(ctx, defaultCmdTimeout, "netstat", "-ibn")
	if err != nil {
		return
	}

	type counters struct {
		rxBytes float64
		txBytes float64
	}
	seen := make(map[string]counters)

	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		// `netstat -ibn` `<Link#N>` rows contain exact interface hardware byte counters:
		// Name Mtu Network Address Ipkts Ierrs Ibytes Opkts Oerrs Obytes Coll (11 cols, or 10 cols when Address is omitted)
		if len(fields) < 10 || !strings.HasPrefix(fields[2], "<Link#") {
			continue
		}
		iface := fields[0]
		if !strings.HasPrefix(iface, "en") && !strings.HasPrefix(iface, "utun") {
			continue
		}

		var rxIdx, txIdx int
		if len(fields) >= 11 {
			rxIdx, txIdx = 6, 9
		} else {
			rxIdx, txIdx = 5, 8
		}
		rx, errRx := strconv.ParseFloat(fields[rxIdx], 64)
		tx, errTx := strconv.ParseFloat(fields[txIdx], 64)
		if errRx != nil || errTx != nil || (rx == 0 && tx == 0) {
			continue
		}
		seen[iface] = counters{rxBytes: rx, txBytes: tx}
	}

	for iface, c := range seen {
		ev := core.NewCTDPayload(nodeID, "net-"+iface, "active").
			AddMetric("rx_bytes", c.rxBytes).
			AddMetric("tx_bytes", c.txBytes)
		if err := pub.Publish(ev); err != nil {
			log.Printf("[Net Observer] Publish failed for net-%s: %v", iface, err)
		}
	}
}

func observeNetDarwin(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emitDarwinNetSamples(ctx, pub, nodeID)

	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emitDarwinNetSamples(ctx, pub, nodeID)
		}
	}
}
