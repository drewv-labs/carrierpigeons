//go:build darwin

package observers

import (
	"context"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var (
	darwinGPUPowerRe     = regexp.MustCompile(`GPU Power:\s+(\d+)\s+mW`)
	darwinGPUResidencyRe = regexp.MustCompile(`GPU active residency:\s+([\d\.]+)\s*%`)
)

func observeGPUPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) bool {
	if os.Geteuid() != 0 {
		log.Println("[GPU Observer] macOS detected; skipping Apple GPU powermetrics observer (requires root).")
		return true
	}
	log.Println("[GPU Observer] Apple/Darwin hardware detected. Booting shared powermetrics observer.")
	go observeGPUDarwin(ctx, pub, nodeID)
	return true
}

func observeGPUDarwin(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emit := func() {
		output, err := sampleDarwinPowermetrics(ctx)
		if err != nil {
			log.Printf("[Darwin GPU Observer] Failed to read powermetrics: %v", err)
			return
		}

		event := core.NewCTDPayload(nodeID, "gpu-darwin", "active")
		metricAdded := false

		if match := darwinGPUPowerRe.FindStringSubmatch(output); len(match) > 1 {
			if mw, err := strconv.ParseFloat(match[1], 64); err == nil {
				event.AddMetric("gpu_power_w", mw/1000.0)
				metricAdded = true
			}
		}

		if match := darwinGPUResidencyRe.FindStringSubmatch(output); len(match) > 1 {
			if res, err := strconv.ParseFloat(match[1], 64); err == nil {
				event.AddMetric("gpu_utilization_pct", res)
				metricAdded = true
			}
		}

		if metricAdded {
			if err := pub.Publish(event); err != nil {
				log.Printf("[Darwin GPU Observer] Publish failed: %v", err)
			}
		}
	}

	emit()

	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emit()
		}
	}
}
