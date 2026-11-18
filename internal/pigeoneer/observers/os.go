package observers

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/observer"
)

type hostOSObserver struct{}

func init() {
	observer.Register(&hostOSObserver{})
}

func (h *hostOSObserver) Name() string {
	return "host-os"
}

func (h *hostOSObserver) Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	observeOSPlatform(ctx, pub, nodeID)
}

// ObserveOS dispatches host OS and CPU cooler telemetry observation for the target platform.
func ObserveOS(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	go observeOSPlatform(ctx, pub, nodeID)
}
