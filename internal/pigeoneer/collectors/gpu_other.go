//go:build !darwin

package collectors

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorGPUPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) bool {
	return false
}
