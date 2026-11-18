//go:build darwin

package observers

import (
	"context"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var darwinANEPowerRe = regexp.MustCompile(`ANE Power:\s+(\d+)\s+mW`)

func observeNPUPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if runtime.GOARCH != "arm64" {
		return // Intel Macs do not have an Apple Neural Engine
	}
	if os.Geteuid() != 0 {
		log.Println("[NPU Observer] macOS arm64 detected; skipping Apple ANE powermetrics observer (requires root).")
		return
	}
	log.Println("[NPU Observer] Apple Neural Engine detected. Booting shared ANE observer.")
	go observeNPUDarwin(ctx, pub, nodeID)
}

func observeNPUDarwin(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emit := func() {
		out, err := sampleDarwinPowermetrics(ctx)
		if err != nil {
			return
		}
		if match := darwinANEPowerRe.FindStringSubmatch(out); len(match) > 1 {
			if mw, err := strconv.ParseFloat(match[1], 64); err == nil && mw >= 0 {
				event := core.NewCTDPayload(nodeID, "npu-ane", "active").
					AddMetric("npu_power_w", mw/1000.0)
				if err := pub.Publish(event); err != nil {
					log.Printf("[Darwin NPU Observer] Publish failed: %v", err)
				}
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
