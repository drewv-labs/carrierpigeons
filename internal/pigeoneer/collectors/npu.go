package collectors

import (
	"context"
	"log"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// monitorNPU detects local neural processing units and routes to the matching collector.
func monitorNPU(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	monitorNPUPlatform(ctx, pub, nodeID)

	if isInstalled("hailortcli") {
		log.Println("[NPU Router] Hailo NPU detected. Booting Hailo collector.")
		go monitorNPUHailo(ctx, pub, nodeID)
	}
	if pathExists("/sys/class/apex") || isInstalled("edgetpu_compiler") {
		log.Println("[NPU Router] Google Coral TPU detected. Booting Coral collector.")
		go monitorNPUCoral(ctx, pub, nodeID)
	}
}
