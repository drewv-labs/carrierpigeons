//go:build !linux && !darwin && !windows

package collectors

import (
	"context"
	"log"
	"runtime"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Printf("[OS Router] Generic host collector started for %s.", runtime.GOOS)

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := core.NewCTDPayload(nodeID, "host-os", "active").
				AddMetric("num_cpu", runtime.NumCPU())
			if err := pub.Publish(event); err != nil {
				log.Printf("[OS Collector] Publish failed: %v", err)
			}
		}
	}
}
