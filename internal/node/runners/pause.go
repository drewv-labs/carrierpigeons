package runners

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

// PauseRunner suspends all node Observer loops and PillarObserver publishing while active.
// Phase 1 scopes PauseRnr strictly to Observer telemetry suppression; active Runner awareness is Phase 2.
type PauseRunner struct{}

func init() {
	p := &PauseRunner{}
	runner.Register(p)
	runner.RegisterAlias("PauseRnr", p)
}

func (p *PauseRunner) Name() string {
	return "pause"
}

func parsePauseDurationSec(params map[string]any) int {
	for _, key := range []string{"duration_sec", "duration_secs", "secs"} {
		raw, exists := params[key]
		if !exists || raw == nil {
			continue
		}
		switch val := raw.(type) {
		case int:
			if val > 0 {
				return val
			}
			return 0
		case int64:
			if val > 0 {
				return int(val)
			}
			return 0
		case float64:
			if int(val) > 0 {
				return int(val)
			}
			return 0
		case string:
			if parsed, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
				if parsed > 0 {
					return parsed
				}
				return 0
			}
		}
	}
	return 0
}

func (p *PauseRunner) Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
	reqID, _ := params["request_id"].(string)
	invokedAs, _ := params["invoked_as"].(string)
	if strings.TrimSpace(invokedAs) == "" {
		invokedAs = "PauseRnr"
	}

	pc := observer.PauseControllerFromContext(ctx)
	if pc == nil {
		errEv := core.NewCTDPayload(nodeID, "runner:"+invokedAs, "runner-error").
			WithPillar(core.PillarRunner).
			AddParam("error", "observer pause controller unavailable on node runtime")
		if reqID != "" {
			errEv.AddParam("request_id", reqID)
		}
		_ = pub.Publish(errEv)
		return fmt.Errorf("observer pause controller unavailable on node %s", nodeID)
	}

	durationSec := parsePauseDurationSec(params)
	mode := "indefinite"
	if durationSec > 0 {
		mode = "timed"
	}

	// Engage observer pause BEFORE emitting runner-start so zero observer events leak inside the bookends.
	release := pc.Acquire()
	defer release()

	runID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	startedAt := time.Now()

	startEvent := core.NewCTDPayload(nodeID, "runner:"+invokedAs, "runner-start").
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("duration_sec", durationSec).
		AddMetric("mode", mode).
		AddMetric("observers_paused", true)
	if reqID != "" {
		startEvent.AddParam("request_id", reqID)
	}
	_ = pub.Publish(startEvent)

	endStatus := "runner-end"
	if durationSec <= 0 {
		<-ctx.Done()
		endStatus = "runner-aborted"
	} else {
		timer := time.NewTimer(time.Duration(durationSec) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			endStatus = "runner-aborted"
		case <-timer.C:
			endStatus = "runner-end"
		}
	}

	// Enqueue closing PillarRunner bookend FIRST (passes through gatedPublisher while paused)
	// before releasing pc so post-resume observer samples strictly follow runner-end/runner-aborted in FIFO order.
	endEvent := core.NewCTDPayload(nodeID, "runner:"+invokedAs, endStatus).
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("duration_sec", durationSec).
		AddMetric("mode", mode).
		AddMetric("elapsed_ms", time.Since(startedAt).Milliseconds()).
		AddMetric("observers_paused", false)
	if reqID != "" {
		endEvent.AddParam("request_id", reqID)
	}
	_ = pub.Publish(endEvent)
	release()

	return nil
}
