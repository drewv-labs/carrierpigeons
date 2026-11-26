package runners

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

const maxPauseDurationSec int64 = math.MaxInt64 / int64(time.Second)

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

func clampPauseDurationSec(secs int64) int64 {
	if secs <= 0 {
		return 0
	}
	if secs > maxPauseDurationSec {
		return maxPauseDurationSec
	}
	return secs
}

func isPositiveDecimalDigits(s string) bool {
	s = strings.TrimPrefix(s, "+")
	if s == "" {
		return false
	}
	nonZero := false
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
		if s[i] != '0' {
			nonZero = true
		}
	}
	return nonZero
}

func parsePauseDurationSec(params map[string]any) int64 {
	for _, key := range []string{"duration_sec", "duration_secs", "secs"} {
		raw, exists := params[key]
		if !exists || raw == nil {
			continue
		}
		switch val := raw.(type) {
		case int:
			return clampPauseDurationSec(int64(val))
		case int64:
			return clampPauseDurationSec(val)
		case int32:
			return clampPauseDurationSec(int64(val))
		case uint:
			if uint64(val) >= uint64(maxPauseDurationSec) {
				return maxPauseDurationSec
			}
			return clampPauseDurationSec(int64(val))
		case uint64:
			if val >= uint64(maxPauseDurationSec) {
				return maxPauseDurationSec
			}
			return clampPauseDurationSec(int64(val))
		case float64:
			if math.IsNaN(val) || val <= 0 {
				return 0
			}
			if val >= float64(maxPauseDurationSec) {
				return maxPauseDurationSec
			}
			return clampPauseDurationSec(int64(val))
		case float32:
			f := float64(val)
			if math.IsNaN(f) || f <= 0 {
				return 0
			}
			if f >= float64(maxPauseDurationSec) {
				return maxPauseDurationSec
			}
			return clampPauseDurationSec(int64(f))
		case string:
			trimmed := strings.TrimSpace(val)
			if trimmed == "" {
				return 0
			}
			parsed, err := strconv.ParseInt(trimmed, 10, 64)
			if err == nil {
				return clampPauseDurationSec(parsed)
			}
			if errors.Is(err, strconv.ErrRange) && isPositiveDecimalDigits(trimmed) {
				return maxPauseDurationSec
			}
			return 0
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
		if pub != nil {
			_ = pub.Publish(errEv)
		}
		return fmt.Errorf("observer pause controller unavailable on node %s", nodeID)
	}

	durationSec := clampPauseDurationSec(parsePauseDurationSec(params))
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
	if pub != nil {
		_ = pub.Publish(startEvent)
	}

	endStatus := "runner-end"
	if durationSec <= 0 {
		<-ctx.Done()
		endStatus = "runner-aborted"
	} else {
		cappedSec := clampPauseDurationSec(durationSec)
		timer := time.NewTimer(time.Duration(cappedSec) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			endStatus = "runner-aborted"
		case <-timer.C:
			endStatus = "runner-end"
		}
	}

	// Release our pause hold first so observers_paused reflects the true post-release pause state
	// (e.g., remaining true when an external manual Pause() or nested Acquire() hold is still active).
	release()
	observersPaused := pc.IsPaused()

	endEvent := core.NewCTDPayload(nodeID, "runner:"+invokedAs, endStatus).
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("duration_sec", durationSec).
		AddMetric("mode", mode).
		AddMetric("elapsed_ms", time.Since(startedAt).Milliseconds()).
		AddMetric("observers_paused", observersPaused)
	if reqID != "" {
		endEvent.AddParam("request_id", reqID)
	}
	if pub != nil {
		_ = pub.Publish(endEvent)
	}

	return nil
}
