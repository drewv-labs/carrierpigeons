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

func monitorGPUPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) bool {
	if os.Geteuid() != 0 {
		log.Println("[GPU Router] macOS detected; skipping Apple GPU powermetrics collector (requires root).")
		return true
	}
	log.Println("[GPU Router] Apple/Darwin hardware detected. Booting powermetrics collector.")
	go monitorGPUDarwin(ctx, pub, nodeID)
	return true
}

func monitorGPUDarwin(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	powerRe := regexp.MustCompile(`GPU Power:\s+(\d+)\s+mW`)
	residencyRe := regexp.MustCompile(`GPU active residency:\s+([\d\.]+)\s*%`)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "powermetrics", "--samplers", "gpu_power", "-n", "1", "-i", "100")
			if err != nil {
				log.Printf("[Darwin GPU] Failed to read powermetrics: %v", err)
				continue
			}

			output := string(out)
			event := core.NewCTDPayload(nodeID, "gpu-darwin", "active")
			metricAdded := false

			if match := powerRe.FindStringSubmatch(output); len(match) > 1 {
				if mw, err := strconv.ParseFloat(match[1], 64); err == nil {
					event.AddMetric("gpu_power_w", mw/1000.0)
					metricAdded = true
				}
			}

			if match := residencyRe.FindStringSubmatch(output); len(match) > 1 {
				if res, err := strconv.ParseFloat(match[1], 64); err == nil {
					event.AddMetric("gpu_utilization_pct", res)
					metricAdded = true
				}
			}

			if metricAdded {
				if err := pub.Publish(event); err != nil {
					log.Printf("[Darwin GPU] Publish failed: %v", err)
				}
			}
		}
	}
}
