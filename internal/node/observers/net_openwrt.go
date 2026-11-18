//go:build linux

package observers

import (
	"context"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// observeNetOpenWrt reads /proc/net/dev directly in-process (including `br-lan` bridges) without spawning `ubus`.
func observeNetOpenWrt(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emitProcNetDevSamples(pub, nodeID, true)

	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emitProcNetDevSamples(pub, nodeID, true)
		}
	}
}
