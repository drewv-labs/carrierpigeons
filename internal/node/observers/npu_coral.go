package observers

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNPUCoral(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	basePath := "/sys/class/apex"

	runPeriodicObserver(ctx, true, func() {
		devices, err := filepath.Glob(filepath.Join(basePath, "apex_*"))
		if err != nil || len(devices) == 0 {
			return
		}

		for _, devPath := range devices {
			devName := filepath.Base(devPath)

			tempBytes, err := os.ReadFile(filepath.Join(devPath, "temp"))
			if err != nil {
				continue
			}

			tempStr := strings.TrimSpace(string(tempBytes))
			tempMilli, err := strconv.ParseFloat(tempStr, 64)
			if err != nil {
				continue
			}

			event := core.NewCTDPayload(nodeID, "npu-coral-"+devName, "active").
				AddMetric("temperature_c", tempMilli/1000.0)

			if err := pub.Publish(event); err != nil {
				log.Printf("[Coral NPU Observer] Publish failed for %s: %v", devName, err)
			}
		}
	})
}
