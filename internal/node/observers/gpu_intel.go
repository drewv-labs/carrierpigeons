package observers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os/exec"
	"strconv"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

const maxIntelGPUFrameBytes = 256 * 1024

var errIntelGPUFrameTooLarge = errors.New("intel_gpu_top JSON frame exceeded max byte budget")

type frameBudgetReader struct {
	r         io.Reader
	remaining int64
}

func (f *frameBudgetReader) reset(limit int64) {
	f.remaining = limit
}

func (f *frameBudgetReader) Read(p []byte) (int, error) {
	if f.remaining <= 0 {
		return 0, errIntelGPUFrameTooLarge
	}
	if int64(len(p)) > f.remaining {
		p = p[:f.remaining]
	}
	n, err := f.r.Read(p)
	f.remaining -= int64(n)
	return n, err
}

func observeGPUIntel(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if !observer.WaitIfPaused(ctx) {
			return
		}
		startedAt := time.Now()
		runIntelGPUTopOnce(ctx, pub, nodeID)
		if ctx.Err() != nil {
			return
		}
		if observer.IsPaused(ctx) {
			backoff = time.Second
			continue
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
	streamCtx, cancelStream := observer.ContextUntilPaused(ctx)
	defer cancelStream()

	intervalMs := max(pollInterval(ctx).Milliseconds(), int64(250))
	cmd := exec.CommandContext(streamCtx, "intel_gpu_top", "-J", "-s", strconv.FormatInt(intervalMs, 10))
	configureProcessGroup(cmd)
	cmd.Cancel = func() error {
		killProcessGroup(cmd)
		return nil
	}
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

	log.Printf("[Intel GPU Observer] Attached to intel_gpu_top stream (interval=%dms).", intervalMs)
	if err := decodeIntelGPUTopStream(streamCtx, stdout, pub, nodeID); err != nil && streamCtx.Err() == nil {
		log.Printf("[Intel GPU Observer] JSON stream decode error: %v", err)
		return
	}

	if streamCtx.Err() == nil {
		log.Println("[Intel GPU Observer] intel_gpu_top stream ended.")
	}
}

func parseIntelGPUTopFrame(data map[string]any, nodeID string) *core.CTDPayload {
	event := core.NewCTDPayload(nodeID, "intel-gpu", "active")

	if powerMap, ok := data["power"].(map[string]any); ok {
		if gpuPower, ok := parseJSONMetricFloat(powerMap["GPU"]); ok && gpuPower >= 0 {
			event.AddMetric("power_w", gpuPower)
		}
	}

	if engines, ok := data["engines"].(map[string]any); ok {
		addedUtil := false
		if render, exists := engines["Render/3D/0"].(map[string]any); exists {
			if busy, ok := parseJSONMetricFloat(render["busy"]); ok && busy >= 0 {
				event.AddMetric("utilization_pct", busy)
				addedUtil = true
			}
		}
		if !addedUtil {
			if render, exists := engines["Render/3D"].(map[string]any); exists {
				if busy, ok := parseJSONMetricFloat(render["busy"]); ok && busy >= 0 {
					event.AddMetric("utilization_pct", busy)
				}
			}
		}
	}

	if len(event.Metrics) == 0 {
		return nil
	}
	return event
}

func decodeIntelGPUTopStream(streamCtx context.Context, r io.Reader, pub core.TelemetryPublisher, nodeID string) error {
	budget := &frameBudgetReader{r: r, remaining: maxIntelGPUFrameBytes}
	decoder := json.NewDecoder(budget)
	if _, err := decoder.Token(); err != nil {
		return err
	}

	for {
		budget.reset(maxIntelGPUFrameBytes)
		if !decoder.More() {
			return nil
		}
		select {
		case <-streamCtx.Done():
			return streamCtx.Err()
		default:
		}

		var data map[string]any
		if err := decoder.Decode(&data); err != nil {
			return err
		}

		event := parseIntelGPUTopFrame(data, nodeID)
		if event == nil || pub == nil {
			continue
		}
		if err := pub.Publish(event); err != nil {
			log.Printf("[Intel GPU Observer] Publish failed: %v", err)
		}
	}
}
