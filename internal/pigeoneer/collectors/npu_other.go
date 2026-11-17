//go:build !darwin

package collectors

import (
	"context"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorNPUPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) {
	// Non-Darwin NPU collectors (Hailo, Coral) are dispatched in npu.go.
}
