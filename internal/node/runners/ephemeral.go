package runners

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"
)

const maxCapturedStreamBytes = 16 * 1024

type boundedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	total := len(p)
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return total, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, _ = b.buf.Write(p)
	return total, nil
}

func (b *boundedBuffer) String() string {
	s := b.buf.String()
	if b.truncated {
		return s + "\n...[truncated]"
	}
	return s
}

// EphemeralRunner executes ad-hoc diagnostic scripts synthesized by PigeonCare or an operator
// inside an isolated temporary directory with strict interpreter allowlists, SHA-256 verification, and timeouts.
type EphemeralRunner struct{}

func init() {
	runner.Register(&EphemeralRunner{})
}

func (e *EphemeralRunner) Name() string {
	return "ephemeral"
}

func (e *EphemeralRunner) Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
	policy := runner.PolicyFromContext(ctx)
	reqID, _ := params["request_id"].(string)

	emitErr := func(reason string) error {
		ev := core.NewCTDPayload(nodeID, "runner:ephemeral", "runner-error").
			WithPillar(core.PillarRunner).
			AddParam("error", reason)
		if reqID != "" {
			ev.AddParam("request_id", reqID)
		}
		_ = pub.Publish(ev)
		return errors.New(reason)
	}

	if !policy.AllowEphemeral {
		return emitErr("ephemeral runners are disabled by node policy")
	}

	scriptBody, _ := params["script_body"].(string)
	if strings.TrimSpace(scriptBody) == "" {
		return emitErr("missing script_body parameter")
	}

	interp, _ := params["interpreter"].(string)
	interp = strings.ToLower(strings.TrimSpace(filepath.Base(interp)))
	if interp == "" {
		interp = "sh"
	}

	allowed := policy.AllowedInterpreters
	if len(allowed) == 0 {
		allowed = []string{"sh", "bash", "python3"}
	}
	if !slices.ContainsFunc(allowed, func(item string) bool {
		return strings.EqualFold(strings.TrimSpace(item), interp)
	}) {
		return emitErr(fmt.Sprintf("interpreter %q is not in node allowlist (%v)", interp, allowed))
	}

	interpBin, err := exec.LookPath(interp)
	if err != nil {
		return emitErr(fmt.Sprintf("interpreter %q not installed on node: %v", interp, err))
	}

	actualHash := runner.ComputeScriptSHA256(scriptBody)
	if expectedHash, _ := params["sha256"].(string); strings.TrimSpace(expectedHash) != "" {
		if !strings.EqualFold(strings.TrimSpace(expectedHash), actualHash) {
			return emitErr("ephemeral script SHA-256 digest mismatch")
		}
	}

	timeoutSec := parseIntParam(params["timeout_sec"], 30)
	maxTimeout := policy.MaxTimeoutSec
	if maxTimeout <= 0 {
		maxTimeout = 300
	}
	if timeoutSec > maxTimeout {
		timeoutSec = maxTimeout
	}

	workDir, err := os.MkdirTemp("", "cp-ephemeral-*")
	if err != nil {
		return emitErr(fmt.Sprintf("failed creating ephemeral workspace: %v", err))
	}
	defer os.RemoveAll(workDir)

	ext := ".sh"
	if strings.HasPrefix(interp, "python") {
		ext = ".py"
	}
	scriptPath := filepath.Join(workDir, "task"+ext)
	if err := os.WriteFile(scriptPath, []byte(scriptBody), 0o700); err != nil {
		return emitErr(fmt.Sprintf("failed writing ephemeral script: %v", err))
	}

	shortHash := actualHash
	if len(shortHash) > 12 {
		shortHash = shortHash[:12]
	}
	artifactID := "ephemeral:" + shortHash
	runID := time.Now().UTC().UnixNano()
	startedAt := time.Now()

	startEv := core.NewCTDPayload(nodeID, "runner:ephemeral", "runner-start").
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("timeout_sec", timeoutSec).
		AddParam("interpreter", interp).
		AddParam("sha256", actualHash).
		WithArtifact(core.Artifact{
			ID:     artifactID,
			Kind:   "ephemeral_runner",
			NodeID: nodeID,
			Meta: map[string]string{
				"interpreter": interp,
				"sha256":      actualHash,
			},
		})
	if reqID != "" {
		startEv.AddParam("request_id", reqID)
	}
	_ = pub.Publish(startEv)

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(execCtx, interpBin, scriptPath)
	cmd.Dir = workDir
	cmd.Env = []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=" + workDir,
		"TMPDIR=" + workDir,
		"PIGEONS_NODE_ID=" + nodeID,
	}
	if envMap, ok := params["env"].(map[string]any); ok {
		for k, v := range envMap {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, v))
		}
	} else if envStrMap, ok := params["env"].(map[string]string); ok {
		for k, v := range envStrMap {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	stdoutBuf := &boundedBuffer{limit: maxCapturedStreamBytes}
	stderrBuf := &boundedBuffer{limit: maxCapturedStreamBytes}
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	runErr := cmd.Run()
	exitCode := 0
	endStatus := "runner-end"

	if runErr != nil {
		if ctx.Err() != nil {
			endStatus = "runner-aborted"
			exitCode = -1
		} else if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			endStatus = "runner-error"
			exitCode = 124
		} else {
			endStatus = "runner-error"
			var exitErr *exec.ExitError
			if errors.As(runErr, &exitErr) {
				exitCode = exitErr.ExitCode()
			} else {
				exitCode = 1
			}
		}
	}

	endEv := core.NewCTDPayload(nodeID, "runner:ephemeral", endStatus).
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("exit_code", exitCode).
		AddMetric("elapsed_ms", time.Since(startedAt).Milliseconds()).
		AddMetric("stdout", strings.TrimSpace(stdoutBuf.String())).
		AddMetric("stderr", strings.TrimSpace(stderrBuf.String())).
		AddParam("interpreter", interp).
		AddParam("sha256", actualHash)
	if reqID != "" {
		endEv.AddParam("request_id", reqID)
	}
	_ = pub.Publish(endEv)

	return runErr
}
