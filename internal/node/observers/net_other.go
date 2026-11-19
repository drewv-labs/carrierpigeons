//go:build !linux && !darwin && !windows

package observers

import (
	"context"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNetPlatform(ctx context.Context, _ core.TelemetryPublisher, _ string) {
	// Unsupported OS network observer stub; block cleanly until context cancellation.
	<-ctx.Done()
}
