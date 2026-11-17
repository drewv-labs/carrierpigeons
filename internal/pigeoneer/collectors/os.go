package collectors

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// MonitorOS dispatches host OS telemetry collection for the target platform.
func MonitorOS(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	go monitorOSPlatform(ctx, pub, nodeID)
}
