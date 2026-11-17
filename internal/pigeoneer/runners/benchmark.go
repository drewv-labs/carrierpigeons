package runners

import (
	"context"
	"crypto/sha256"
	"runtime"
	"sync"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"
)

type BenchmarkRunner struct{}

// Automatically bind to the execution registry on binary boot.
func init() {
	runner.Register(&BenchmarkRunner{})
}

func (b *BenchmarkRunner) Name() string {
	return "benchmark"
}

func (b *BenchmarkRunner) Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
	durationSec := 60
	switch d := params["duration_sec"].(type) {
	case float64:
		if d > 0 {
			durationSec = int(d)
		}
	case int:
		if d > 0 {
			durationSec = d
		}
	}

	targetSystem, _ := params["target_system"].(bool)
	targetNPU, _ := params["target_npu"].(bool)
	targetGPU, _ := params["target_gpu"].(bool)

	runID := time.Now().UTC().UnixNano()

	// 1. Emit CTD Start Bookend under the Runner pillar
	startEvent := core.NewCTDPayload(nodeID, "runner:benchmark", "runner-start").
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("duration_sec", durationSec).
		AddMetric("target_system", targetSystem).
		AddMetric("target_npu", targetNPU).
		AddMetric("target_gpu", targetGPU)
	_ = pub.Publish(startEvent)

	// Enforce the requested duration while respecting external abort cancellation
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(durationSec)*time.Second)
	defer cancel()

	// 2. Execute Hardware Workloads
	var wg sync.WaitGroup
	if targetSystem {
		payload := []byte("carrier-pigeons-burn")
		for range runtime.NumCPU() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-runCtx.Done():
						return
					default:
						for range 256 {
							_ = sha256.Sum256(payload)
						}
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
	endEvent := core.NewCTDPayload(nodeID, "runner:benchmark", endStatus).
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID)
	_ = pub.Publish(endEvent)

	return nil
}
