//go:build darwin

package collectors

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[OS Router] macOS/Darwin host detected.")

	var memTotalMB float64
	if out, err := runCommand(ctx, defaultCmdTimeout, "sysctl", "-n", "hw.memsize"); err == nil {
		if bytesTotal, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); err == nil {
			memTotalMB = bytesTotal / 1024.0 / 1024.0
		}
	}

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := core.NewCTDPayload(nodeID, "host-os", "active")
			if memTotalMB > 0 {
				event.AddMetric("mem_total_mb", memTotalMB)
			}

			// Example output: "{ 2.14 1.98 1.85 }"
			if out, err := runCommand(ctx, defaultCmdTimeout, "sysctl", "-n", "vm.loadavg"); err == nil {
				trimmed := strings.Trim(strings.TrimSpace(string(out)), "{}")
				fields := strings.Fields(trimmed)
				if len(fields) >= 1 {
					if load1, err := strconv.ParseFloat(fields[0], 64); err == nil {
						event.AddMetric("load_1m", load1)
					}
				}
				if len(fields) >= 2 {
					if load5, err := strconv.ParseFloat(fields[1], 64); err == nil {
						event.AddMetric("load_5m", load5)
					}
				}
			}

			if err := pub.Publish(event); err != nil {
				log.Printf("[OS Collector] Publish failed: %v", err)
			}
		}
	}
}
