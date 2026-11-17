package collectors

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// monitorNet dispatches network interface telemetry collection for the target platform.
func monitorNet(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	monitorNetPlatform(ctx, pub, nodeID)
}
