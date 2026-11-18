//go:build !linux && !darwin && !windows

package observers

import (
	"context"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNetPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) {
	// Unsupported OS network observer stub.
}
