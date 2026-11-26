//go:build linux

package observers

import (
	"context"
	"log"
	"os"
	"path/filepath"
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

func isSafeLinuxNetIface(iface string) bool {
	if iface == "" || iface == "." || iface == ".." {
		return false
	}
	if core.ValidateTopicToken(iface) != nil {
		return false
	}
	return core.ValidateTopicToken("net-"+iface) == nil
}

func isInterfaceUpIn(sysNetRoot, iface string) bool {
	if !isSafeLinuxNetIface(iface) {
		return false
	}
	operBytes, err := os.ReadFile(filepath.Join(sysNetRoot, iface, "operstate"))
	if err != nil {
		return true
	}
	state := strings.TrimSpace(string(operBytes))
	return state == "up" || state == "unknown"
}

func emitProcNetDevSamples(pub core.TelemetryPublisher, nodeID string, allowBridges bool) {
	emitProcNetDevSamplesFrom("/proc/net/dev", "/sys/class/net", pub, nodeID, allowBridges)
}

func emitProcNetDevSamplesFrom(procNetDevPath, sysNetRoot string, pub core.TelemetryPublisher, nodeID string, allowBridges bool) {
	raw, err := os.ReadFile(procNetDevPath)
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
		if !isSafeLinuxNetIface(iface) {
			continue
		}
		if !isInterfaceUpIn(sysNetRoot, iface) {
			continue
		}

		fields := strings.Fields(line[colon+1:])
		if len(fields) < 10 {
			continue
		}

		componentID := "net-" + iface
		event := core.NewCTDPayload(nodeID, componentID, "active")

		if rxBytes, ok := parseFiniteFloat(fields[0]); ok {
			event.AddMetric("rx_bytes", rxBytes)
		}
		if txBytes, ok := parseFiniteFloat(fields[8]); ok {
			event.AddMetric("tx_bytes", txBytes)
		}
		if rxPkts, ok := parseFiniteFloat(fields[1]); ok {
			event.AddMetric("rx_packets", rxPkts)
		}
		if txPkts, ok := parseFiniteFloat(fields[9]); ok {
			event.AddMetric("tx_packets", txPkts)
		}

		if len(event.Metrics) == 0 {
			continue
		}

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
