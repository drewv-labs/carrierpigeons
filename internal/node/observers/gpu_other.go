//go:build !darwin

package observers

import (
	"context"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeGPUPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) bool {
	return false
}
