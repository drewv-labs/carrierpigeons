package collectors

import (
	"context"
	"encoding/json"
	"log"
	"os/exec"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorGPUIntel(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	cmd := exec.CommandContext(ctx, "intel_gpu_top", "-J", "-s", "5000")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("[Intel Collector] Failed to open stdout pipe: %v", err)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("[Intel Collector] Failed to start intel_gpu_top: %v", err)
		return
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	decoder := json.NewDecoder(stdout)
	if _, err := decoder.Token(); err != nil {
		log.Printf("[Intel Collector] Failed to read opening JSON bracket: %v", err)
		return
	}

	log.Println("[Intel Collector] Attached to intel_gpu_top stream.")

	for decoder.More() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var data map[string]any
		if err := decoder.Decode(&data); err != nil {
			log.Printf("[Intel Collector] JSON stream decode error: %v", err)
			break
		}

		event := core.NewCTDPayload(nodeID, "intel-gpu", "active")

		if powerMap, ok := data["power"].(map[string]any); ok {
			if gpuPower, exists := powerMap["GPU"].(float64); exists {
				event.AddMetric("power_w", gpuPower)
			}
		}

		if engines, ok := data["engines"].(map[string]any); ok {
			if render, exists := engines["Render/3D/0"].(map[string]any); exists {
				if busy, isFloat := render["busy"].(float64); isFloat {
					event.AddMetric("utilization_pct", busy)
				}
			}
		}

		if err := pub.Publish(event); err != nil {
			log.Printf("[Intel Collector] Publish failed: %v", err)
		}
	}

	if ctx.Err() == nil {
		log.Println("[Intel Collector] intel_gpu_top stream ended.")
	}
}
