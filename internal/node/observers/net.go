package observers

import (
	"context"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// observeNet dispatches network interface telemetry observation for the target platform.
func observeNet(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeNetPlatform(ctx, pub, nodeID)
}
