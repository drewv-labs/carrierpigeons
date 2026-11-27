package observers

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"slices"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeNPUCoral(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeNPUCoralWithRoot(ctx, pub, nodeID, "/sys/class/apex")
}

func collectCoralSamples(basePath, nodeID string) []*core.CTDPayload {
	if basePath == "" {
		return nil
	}
	devices, err := filepath.Glob(filepath.Join(basePath, "apex_*"))
	if err != nil || len(devices) == 0 {
		return nil
	}
	slices.Sort(devices)

	var out []*core.CTDPayload
	for _, devPath := range devices {
		devName := filepath.Base(devPath)
		componentID := "npu-coral-" + devName
		if core.ValidateTopicToken(componentID) != nil {
			continue
		}

		tempBytes, err := os.ReadFile(filepath.Join(devPath, "temp"))
		if err != nil {
			continue
		}

		tempC, ok := parseSysfsMillidegreeTemp(tempBytes)
		if !ok {
			continue
		}

		event := core.NewCTDPayload(nodeID, componentID, "active").
			AddMetric("temperature_c", tempC)
		out = append(out, event)
	}
	return out
}

func observeNPUCoralWithRoot(ctx context.Context, pub core.TelemetryPublisher, nodeID, basePath string) {
	runPeriodicObserver(ctx, true, func() {
		for _, event := range collectCoralSamples(basePath, nodeID) {
			if err := pub.Publish(event); err != nil {
				log.Printf("[Coral NPU Observer] Publish failed for %s: %v", event.Component, err)
			}
		}
	})
}
