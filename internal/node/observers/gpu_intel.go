package observers

import (
	"context"
	"encoding/json"
	"log"
	"os/exec"
	"strconv"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeGPUIntel(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		startedAt := time.Now()
		runIntelGPUTopOnce(ctx, pub, nodeID)
		if ctx.Err() != nil {
			return
		}
		if time.Since(startedAt) >= 30*time.Second {
			backoff = time.Second
		}
		log.Printf("[Intel GPU Observer] intel_gpu_top exited; restarting after %s backoff.", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func runIntelGPUTopOnce(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	intervalMs := max(pollInterval(ctx).Milliseconds(), int64(250))
	cmd := exec.CommandContext(ctx, "intel_gpu_top", "-J", "-s", strconv.FormatInt(intervalMs, 10))
	configureProcessGroup(cmd)
	cmd.WaitDelay = 2 * time.Second

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("[Intel GPU Observer] Failed to open stdout pipe: %v", err)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("[Intel GPU Observer] Failed to start intel_gpu_top: %v", err)
		return
	}
	defer func() {
		killProcessGroup(cmd)
		_ = cmd.Wait()
	}()

	decoder := json.NewDecoder(stdout)
	if _, err := decoder.Token(); err != nil {
		log.Printf("[Intel GPU Observer] Failed to read opening JSON bracket: %v", err)
		return
	}

	log.Printf("[Intel GPU Observer] Attached to intel_gpu_top stream (interval=%dms).", intervalMs)

	for decoder.More() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var data map[string]any
		if err := decoder.Decode(&data); err != nil {
			log.Printf("[Intel GPU Observer] JSON stream decode error: %v", err)
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
			log.Printf("[Intel GPU Observer] Publish failed: %v", err)
		}
	}

	if ctx.Err() == nil {
		log.Println("[Intel GPU Observer] intel_gpu_top stream ended.")
	}
}
