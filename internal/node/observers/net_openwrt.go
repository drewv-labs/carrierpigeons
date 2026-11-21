//go:build linux

package observers

import (
	"context"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// observeNetOpenWrt reads /proc/net/dev directly in-process (including `br-lan` bridges) without spawning `ubus`.
func observeNetOpenWrt(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	runPeriodicObserver(ctx, true, func() {
		emitProcNetDevSamples(pub, nodeID, true)
	})
}
