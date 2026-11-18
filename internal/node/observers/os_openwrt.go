//go:build linux

package observers

import (
	"context"
	"log"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// observeOSOpenWrt uses zero-fork /proc and /sys readers directly rather than spawning `ubus` every tick,
// saving CPU cycles and PID churn on constrained embedded MIPS/ARM/RISC-V routers.
func observeOSOpenWrt(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emit := func() {
		ev := collectLinuxHostSample(nodeID).AddParam("platform", "openwrt")
		if err := pub.Publish(ev); err != nil {
			log.Printf("[OS Observer] Publish failed: %v", err)
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
