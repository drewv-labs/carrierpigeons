//go:build !linux

package collectors

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorNetPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) {
	// Non-Linux network collectors are currently no-ops.
}
