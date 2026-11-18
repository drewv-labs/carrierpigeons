//go:build !linux

package observers

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeNetPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) {
	// Non-Linux network observers are currently no-ops.
}
