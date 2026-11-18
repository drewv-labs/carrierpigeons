package observers

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeGPUAMD(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "rocm-smi", "--showtemp", "--showuse", "--showfan", "--showmeminfo", "vram", "--json")
			if err != nil {
				log.Printf("[AMD GPU Observer] Sensor timeout: %v", err)
				continue
			}

			var amdData map[string]map[string]any
			if err := json.Unmarshal(out, &amdData); err != nil {
				log.Printf("[AMD GPU Observer] Failed to parse JSON: %v", err)
				continue
			}

			for cardName, metrics := range amdData {
				if !strings.HasPrefix(strings.ToLower(cardName), "card") {
					continue
				}

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

				if fanRPMStr, ok := metrics["Fan RPM"].(string); ok {
					if fanRPM, err := strconv.Atoi(strings.TrimSpace(fanRPMStr)); err == nil && fanRPM >= 0 {
						event.AddMetric("fan_rpm", fanRPM)
					}
				}
				if fanPctStr, ok := metrics["Fan speed (%)"].(string); ok {
					if fanPct, err := strconv.ParseFloat(strings.TrimSpace(fanPctStr), 64); err == nil && fanPct >= 0 {
						event.AddMetric("fan_speed_pct", fanPct)
					}
				}

				if err := pub.Publish(event); err != nil {
					log.Printf("[AMD GPU Observer] Publish failed for %s: %v", cardName, err)
				}
			}
		}
	}
}
