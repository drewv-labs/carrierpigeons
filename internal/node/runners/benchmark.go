package runners

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

type BenchmarkRunner struct{}

// Automatically bind to the execution registry on binary boot alongside generic compute benchmark alias.
func init() {
	b := &BenchmarkRunner{}
	runner.Register(b)
	runner.RegisterAlias("SyntheticComputeRnr", b)
}

func (b *BenchmarkRunner) Name() string {
	return "benchmark"
}

func parseIntParam(v any, fallback int) int {
	switch val := v.(type) {
	case float64:
		if int(val) > 0 {
			return int(val)
		}
	case int:
		if val > 0 {
			return val
		}
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func parseBoolParam(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		b, _ := strconv.ParseBool(strings.TrimSpace(val))
		return b
	default:
		return false
	}
}

func (b *BenchmarkRunner) Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
	policy := runner.PolicyFromContext(ctx)
	durationSec := parseIntParam(params["duration_sec"], 60)
	maxTimeout := policy.MaxTimeoutSec
	if maxTimeout <= 0 {
		maxTimeout = 300
	}
	if durationSec > maxTimeout {
		durationSec = maxTimeout
	}

	targetSystem := parseBoolParam(params["target_system"])
	targetNPU := parseBoolParam(params["target_npu"])
	targetGPU := parseBoolParam(params["target_gpu"])
	if !targetSystem && !targetNPU && !targetGPU {
		targetSystem = true
	}

	reqID, _ := params["request_id"].(string)
	invokedAs, _ := params["invoked_as"].(string)
	if invokedAs == "" {
		invokedAs = b.Name()
	}

	if targetGPU || targetNPU {
		var unsupported []string
		if targetGPU {
			unsupported = append(unsupported, "target_gpu")
		}
		if targetNPU {
			unsupported = append(unsupported, "target_npu")
		}
		reason := fmt.Sprintf("hardware burn kernels not implemented for %s", strings.Join(unsupported, ", "))
		errEv := core.NewCTDPayload(nodeID, "runner:"+invokedAs, "runner-error").
			WithPillar(core.PillarRunner).
			AddParam("error", reason)
		if reqID != "" {
			errEv.AddParam("request_id", reqID)
		}
		if pub != nil {
			_ = pub.Publish(errEv)
		}
		return errors.New(reason)
	}

	runID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	startedAt := time.Now()

	// 1. Emit CTD Start Bookend under the Runner pillar
	startEvent := core.NewCTDPayload(nodeID, "runner:"+invokedAs, "runner-start").
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("duration_sec", durationSec).
		AddMetric("target_system", targetSystem).
		AddMetric("target_npu", targetNPU).
		AddMetric("target_gpu", targetGPU)
	if reqID != "" {
		startEvent.AddParam("request_id", reqID)
	}
	_ = pub.Publish(startEvent)

	// Enforce the requested duration while respecting external abort cancellation
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(durationSec)*time.Second)
	defer cancel()

	// 2. Execute Hardware Workloads
	var wg sync.WaitGroup
	var totalHashes atomic.Uint64

	if targetSystem {
		payload := []byte("carrier-pigeons-burn")
		for range runtime.NumCPU() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var local uint64
				defer func() {
					totalHashes.Add(local)
				}()
				for {
					select {
					case <-runCtx.Done():
						return
					default:
						for range 256 {
							_ = sha256.Sum256(payload)
						}
						local += 256
					}
				}
			}()
		}
	}

	// 3. Block until duration expires or abort is triggered, then drain workers
	<-runCtx.Done()
	wg.Wait()

	endStatus := "runner-end"
	if ctx.Err() != nil {
		endStatus = "runner-aborted"
	}

	// 4. Emit CTD End Bookend under the Runner pillar
	endEvent := core.NewCTDPayload(nodeID, "runner:"+invokedAs, endStatus).
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("elapsed_ms", time.Since(startedAt).Milliseconds()).
		AddMetric("sha256_ops", totalHashes.Load())
	if reqID != "" {
		endEvent.AddParam("request_id", reqID)
	}
	_ = pub.Publish(endEvent)

	return nil
}
