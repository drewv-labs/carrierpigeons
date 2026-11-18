//go:build linux

package observers

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeNetPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if isInstalled("ubus") {
		log.Println("[Net Observer] OpenWrt ubus detected. Booting embedded network observer.")
		go observeNetOpenWrt(ctx, pub, nodeID)
		return
	}
	log.Println("[Net Observer] Linux networking detected. Booting /proc/net/dev observer.")
	go observeNetLinux(ctx, pub, nodeID)
}

func isInterfaceUp(iface string) bool {
	operBytes, err := os.ReadFile(filepath.Join("/sys/class/net", iface, "operstate"))
	if err != nil {
		return true
	}
	state := strings.TrimSpace(string(operBytes))
	return state == "up" || state == "unknown"
}

func observeNetLinux(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			raw, err := os.ReadFile("/proc/net/dev")
			if err != nil {
				continue
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
				if iface == "" || iface == "lo" || strings.HasPrefix(iface, "veth") || strings.HasPrefix(iface, "docker") || strings.HasPrefix(iface, "br-") {
					continue
				}
				if !isInterfaceUp(iface) {
					continue
				}

				fields := strings.Fields(line[colon+1:])
				if len(fields) < 9 {
					continue
				}

				rxBytes, errRx := strconv.ParseFloat(fields[0], 64)
				txBytes, errTx := strconv.ParseFloat(fields[8], 64)
				if errRx != nil || errTx != nil {
					continue
				}

				componentID := "net-" + iface
				event := core.NewCTDPayload(nodeID, componentID, "active").
					AddMetric("rx_bytes", rxBytes).
					AddMetric("tx_bytes", txBytes)

				if err := pub.Publish(event); err != nil {
					log.Printf("[Net Observer] Publish failed for %s: %v", componentID, err)
				}
			}
		}
	}
}
