package observers

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeGPUNvidia(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeGPUNvidiaWithBin(ctx, pub, nodeID, "nvidia-smi")
}

func observeGPUNvidiaWithBin(ctx context.Context, pub core.TelemetryPublisher, nodeID, nvidiaBin string) {
	if strings.TrimSpace(nvidiaBin) == "" {
		nvidiaBin = "nvidia-smi"
	}

	emit := func() {
		out, err := runCommand(
			ctx,
			defaultCmdTimeout,
			nvidiaBin,
			"--query-gpu=index,name,temperature.gpu,utilization.gpu,memory.used,memory.total,power.draw,fan.speed",
			"--format=csv,noheader,nounits",
		)
		if err != nil {
			log.Printf("[Nvidia GPU Observer] Sensor query failed: %v", err)
			return
		}

		rawOutput := strings.TrimSpace(string(out))
		if rawOutput == "" {
			return
		}

		for line := range strings.SplitSeq(rawOutput, "\n") {
			fields := strings.Split(line, ",")
			if len(fields) < 7 {
				continue
			}

			gpuIdx := strings.TrimSpace(fields[0])
			gpuName := strings.TrimSpace(fields[1])
			temp, _ := strconv.ParseFloat(strings.TrimSpace(fields[2]), 64)
			util, _ := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
			memUsed, _ := strconv.ParseFloat(strings.TrimSpace(fields[4]), 64)
			memTotal, _ := strconv.ParseFloat(strings.TrimSpace(fields[5]), 64)
			powerW, _ := strconv.ParseFloat(strings.TrimSpace(fields[6]), 64)

			componentID := "nvidia-gpu-" + gpuIdx
			event := core.NewCTDPayload(nodeID, componentID, "active").
				AddMetric("temperature_c", temp).
				AddMetric("utilization_pct", util).
				AddMetric("memory_used_mb", memUsed).
				AddMetric("memory_total_mb", memTotal).
				AddMetric("power_w", powerW)
			if gpuName != "" {
				event.AddParam("gpu_name", gpuName)
			}

			if len(fields) >= 8 {
				if fanPct, err := strconv.ParseFloat(strings.TrimSpace(fields[7]), 64); err == nil && fanPct >= 0 {
					event.AddMetric("fan_speed_pct", fanPct)
				}
			}

			if err := pub.Publish(event); err != nil {
				log.Printf("[Nvidia GPU Observer] Publish failed for %s: %v", componentID, err)
			}
		}
	}

	emit()

	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emit()
		}
	}
}
