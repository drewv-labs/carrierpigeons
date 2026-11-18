package observers

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeNPUCoral(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	basePath := "/sys/class/apex"

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			devices, err := filepath.Glob(filepath.Join(basePath, "apex_*"))
			if err != nil || len(devices) == 0 {
				continue
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
					AddMetric("npu_temp_c", tempMilli/1000.0)

				if err := pub.Publish(event); err != nil {
					log.Printf("[Coral NPU Observer] Publish failed for %s: %v", devName, err)
				}
			}
		}
	}
}
