//go:build darwin

package collectors

import (
	"context"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorNPUPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if os.Geteuid() != 0 {
		log.Println("[NPU Router] macOS detected; skipping Apple ANE powermetrics collector (requires root).")
		return
	}
	log.Println("[NPU Router] Apple Neural Engine detected. Booting ANE collector.")
	go monitorNPUDarwin(ctx, pub, nodeID)
}

func monitorNPUDarwin(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	powerRe := regexp.MustCompile(`ANE Power:\s+(\d+)\s+mW`)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "powermetrics", "--samplers", "ane_power", "-n", "1", "-i", "100")
			if err != nil {
				continue
			}

			if match := powerRe.FindStringSubmatch(string(out)); len(match) > 1 {
				if mw, err := strconv.ParseFloat(match[1], 64); err == nil && mw > 0 {
					event := core.NewCTDPayload(nodeID, "npu-ane", "active").
						AddMetric("npu_power_w", mw/1000.0)

					if err := pub.Publish(event); err != nil {
						log.Printf("[Darwin NPU] Publish failed: %v", err)
					}
				}
			}
		}
	}
}
