package observers

import (
	"context"
	"log"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// observeNPU detects local neural processing units and routes to the matching observer.
func observeNPU(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeNPUPlatform(ctx, pub, nodeID)

	if isInstalled("hailortcli") {
		log.Println("[NPU Observer] Hailo NPU detected. Booting Hailo observer.")
		go observeNPUHailo(ctx, pub, nodeID)
	}
	if pathExists("/sys/class/apex") || isInstalled("edgetpu_compiler") {
		log.Println("[NPU Observer] Google Coral TPU detected. Booting Coral observer.")
		go observeNPUCoral(ctx, pub, nodeID)
	}
}
