//go:build !linux && !darwin && !windows

package observers

import (
	"context"
	"log"
	"runtime"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Printf("[OS Observer] Generic host observer started for %s.", runtime.GOOS)

	runPeriodicObserver(ctx, false, func() {
		event := core.NewCTDPayload(nodeID, "host-os", "active").
			AddMetric("num_cpu", runtime.NumCPU())
		if err := pub.Publish(event); err != nil {
			log.Printf("[OS Observer] Publish failed: %v", err)
		}
	})
}
