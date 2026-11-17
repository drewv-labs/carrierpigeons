package collectors

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorGPUNvidia(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(
				ctx,
				defaultCmdTimeout,
				"nvidia-smi",
				"--query-gpu=index,temperature.gpu,utilization.gpu,memory.used,power.draw",
				"--format=csv,noheader,nounits",
			)
			if err != nil {
				log.Printf("[Nvidia Collector] Sensor timeout: %v", err)
				continue
			}

			rawOutput := strings.TrimSpace(string(out))
			if rawOutput == "" {
				continue
			}

			for line := range strings.SplitSeq(rawOutput, "\n") {
				fields := strings.Split(line, ",")
				if len(fields) < 5 {
					continue
				}

				gpuIdx := strings.TrimSpace(fields[0])
				temp, _ := strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
				util, _ := strconv.ParseFloat(strings.TrimSpace(fields[2]), 64)
				memUsed, _ := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
				powerW, _ := strconv.ParseFloat(strings.TrimSpace(fields[4]), 64)

				componentID := "nvidia-gpu-" + gpuIdx
				event := core.NewCTDPayload(nodeID, componentID, "active").
					AddMetric("temperature_c", temp).
					AddMetric("utilization_pct", util).
					AddMetric("memory_used_mb", memUsed).
					AddMetric("power_w", powerW)

				if err := pub.Publish(event); err != nil {
					log.Printf("[Nvidia Collector] Publish failed for %s: %v", componentID, err)
				}
			}
		}
	}
}
