package collectors

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorGPUAMD(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "rocm-smi", "--showtemp", "--showuse", "--showmeminfo", "vram", "--json")
			if err != nil {
				log.Printf("[AMD GPU Collector] Sensor timeout: %v", err)
				continue
			}

			var amdData map[string]map[string]any
			if err := json.Unmarshal(out, &amdData); err != nil {
				log.Printf("[AMD GPU Collector] Failed to parse JSON: %v", err)
				continue
			}

			for cardName, metrics := range amdData {
				tempStr, _ := metrics["Temperature (Sensor edge) (C)"].(string)
				utilStr, _ := metrics["GPU use (%)"].(string)
				memStr, _ := metrics["VRAM Total Used Memory (B)"].(string)

				temp, _ := strconv.ParseFloat(tempStr, 64)
				util, _ := strconv.ParseFloat(utilStr, 64)
				memBytes, _ := strconv.ParseFloat(memStr, 64)

				event := core.NewCTDPayload(nodeID, "amd-gpu-"+cardName, "active").
					AddMetric("temperature_c", temp).
					AddMetric("utilization_pct", util).
					AddMetric("memory_used_mb", memBytes/1024.0/1024.0)

				if err := pub.Publish(event); err != nil {
					log.Printf("[AMD GPU Collector] Publish failed for %s: %v", cardName, err)
				}
			}
		}
	}
}
