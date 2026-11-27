package observers

import (
	"context"
	"log"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// observeNPU detects local neural processing units, routes to matching observers, and blocks until all exit.
func observeNPU(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		observeNPUPlatform(ctx, pub, nodeID)
	}()

	if isInstalled("hailortcli") {
		log.Println("[NPU Observer] Hailo NPU detected. Booting Hailo observer.")
		wg.Add(1)
		go func() {
			defer wg.Done()
			observeNPUHailo(ctx, pub, nodeID)
		}()
	}
	if pathExists("/sys/class/apex") {
		log.Println("[NPU Observer] Google Coral TPU detected. Booting Coral observer.")
		wg.Add(1)
		go func() {
			defer wg.Done()
			observeNPUCoral(ctx, pub, nodeID)
		}()
	}
	wg.Wait()
}
