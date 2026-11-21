//go:build linux

package observers

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNetPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if pathExists("/etc/openwrt_release") {
		log.Println("[Net Observer] OpenWrt networking detected. Booting zero-fork /proc/net/dev observer.")
		observeNetOpenWrt(ctx, pub, nodeID)
		return
	}
	log.Println("[Net Observer] Linux networking detected. Booting /proc/net/dev observer.")
	observeNetLinux(ctx, pub, nodeID)
}

func isInterfaceUp(iface string) bool {
	operBytes, err := os.ReadFile(filepath.Join("/sys/class/net", iface, "operstate"))
	if err != nil {
		return true
	}
	state := strings.TrimSpace(string(operBytes))
	return state == "up" || state == "unknown"
}

func emitProcNetDevSamples(pub core.TelemetryPublisher, nodeID string, allowBridges bool) {
	raw, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return
	}

	lineIdx := 0
	for line := range strings.SplitSeq(string(raw), "\n") {
		lineIdx++
		if lineIdx <= 2 {
			continue // Skip 2-line /proc/net/dev header
		}
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}

		iface := strings.TrimSpace(line[:colon])
		if iface == "" || iface == "lo" || strings.HasPrefix(iface, "veth") || strings.HasPrefix(iface, "docker") {
			continue
		}
		if !allowBridges && strings.HasPrefix(iface, "br-") {
			continue
		}
		if !isInterfaceUp(iface) {
			continue
		}

		fields := strings.Fields(line[colon+1:])
		if len(fields) < 10 {
			continue
		}

		rxBytes, errRx := strconv.ParseFloat(fields[0], 64)
		rxPkts, _ := strconv.ParseFloat(fields[1], 64)
		txBytes, errTx := strconv.ParseFloat(fields[8], 64)
		txPkts, _ := strconv.ParseFloat(fields[9], 64)
		if errRx != nil || errTx != nil {
			continue
		}

		componentID := "net-" + iface
		event := core.NewCTDPayload(nodeID, componentID, "active").
			AddMetric("rx_bytes", rxBytes).
			AddMetric("tx_bytes", txBytes).
			AddMetric("rx_packets", rxPkts).
			AddMetric("tx_packets", txPkts)

		if err := pub.Publish(event); err != nil {
			log.Printf("[Net Observer] Publish failed for %s: %v", componentID, err)
		}
	}
}

func observeNetLinux(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	runPeriodicObserver(ctx, true, func() {
		emitProcNetDevSamples(pub, nodeID, false)
	})
}
