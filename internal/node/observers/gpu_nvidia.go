package observers

import (
	"context"
	"log"
	"strings"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

func observeGPUNvidia(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeGPUNvidiaWithBin(ctx, pub, nodeID, "nvidia-smi")
}

func parseNvidiaSMIOutput(rawOutput, nodeID string) []*core.CTDPayload {
	trimmed := strings.TrimSpace(rawOutput)
	if trimmed == "" {
		return nil
	}

	var events []*core.CTDPayload
	for line := range strings.SplitSeq(trimmed, "\n") {
		fields := strings.Split(line, ",")
		if len(fields) < 7 {
			continue
		}

		gpuIdx := strings.TrimSpace(fields[0])
		componentID := "nvidia-gpu-" + gpuIdx
		if gpuIdx == "" || core.ValidateTopicToken(componentID) != nil {
			continue
		}

		gpuName := strings.TrimSpace(fields[1])
		event := core.NewCTDPayload(nodeID, componentID, "active")

		if temp, ok := parseFiniteFloat(fields[2]); ok {
			event.AddMetric("temperature_c", temp)
		}
		if util, ok := parseFiniteFloat(fields[3]); ok {
			event.AddMetric("utilization_pct", util)
		}
		if memUsed, ok := parseFiniteFloat(fields[4]); ok {
			event.AddMetric("memory_used_mb", memUsed)
		}
		if memTotal, ok := parseFiniteFloat(fields[5]); ok {
			event.AddMetric("memory_total_mb", memTotal)
		}
		if powerW, ok := parseFiniteFloat(fields[6]); ok {
			event.AddMetric("power_w", powerW)
		}
		if len(fields) >= 8 {
			if fanPct, ok := parseFiniteFloat(fields[7]); ok && fanPct >= 0 {
				event.AddMetric("fan_speed_pct", fanPct)
			}
		}

		if len(event.Metrics) == 0 {
			continue
		}
		if gpuName != "" {
			event.AddParam("gpu_name", gpuName)
		}
		events = append(events, event)
	}
	return events
}

func observeGPUNvidiaWithBin(ctx context.Context, pub core.TelemetryPublisher, nodeID, nvidiaBin string) {
	if strings.TrimSpace(nvidiaBin) == "" {
		nvidiaBin = "nvidia-smi"
	}

	runPeriodicObserver(ctx, true, func() {
		cmdCtx, cancelCmd := observer.ContextUntilPaused(ctx)
		out, err := runCommand(
			cmdCtx,
			defaultCmdTimeout,
			nvidiaBin,
			"--query-gpu=index,name,temperature.gpu,utilization.gpu,memory.used,memory.total,power.draw,fan.speed",
			"--format=csv,noheader,nounits",
		)
		cancelCmd()
		if err != nil {
			log.Printf("[Nvidia GPU Observer] Sensor query failed: %v", err)
			return
		}

		for _, event := range parseNvidiaSMIOutput(string(out), nodeID) {
			if err := pub.Publish(event); err != nil {
				log.Printf("[Nvidia GPU Observer] Publish failed for %s: %v", event.Component, err)
			}
		}
	})
}
