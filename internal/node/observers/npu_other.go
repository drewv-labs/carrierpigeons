//go:build !darwin

package observers

import (
	"context"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNPUPlatform(_ context.Context, _ core.TelemetryPublisher, _ string) {
	// Non-Darwin NPU observers (Hailo, Coral) are dispatched in npu.go.
}
