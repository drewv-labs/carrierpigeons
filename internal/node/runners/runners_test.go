package runners

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

type memPublisher struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (m *memPublisher) Publish(ev *core.CTDPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ev)
	return nil
}

func TestEphemeralRunnerDefaultDenyAndEnvSandbox(t *testing.T) {
	pub := &memPublisher{}
	r := &EphemeralRunner{}

	// Default context has AllowEphemeral=false
	if err := r.Run(context.Background(), "edge-01", map[string]any{"script_body": "echo hi"}, pub); err == nil {
		t.Fatal("expected EphemeralRunner with default policy (AllowEphemeral=false) to reject execution")
	}

	enabledCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh"},
		MaxTimeoutSec:       10,
	})
	err := r.Run(enabledCtx, "edge-01", map[string]any{
		"script_body": "echo hi",
		"env":         map[string]any{"LD_PRELOAD": "/tmp/evil.so"},
	}, pub)
	if err == nil {
		t.Fatal("expected LD_PRELOAD env injection to be rejected")
	}

	// AllowedScriptHashes enforcement
	allowedScript := "echo deterministic\n"
	sum := sha256.Sum256([]byte(allowedScript))
	hashHex := hex.EncodeToString(sum[:])

	pinnedCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh"},
		AllowedScriptHashes: []string{hashHex},
		MaxTimeoutSec:       10,
	})
	if err := r.Run(pinnedCtx, "edge-01", map[string]any{"script_body": "echo tampered\n"}, pub); err == nil {
		t.Fatal("expected unpinned script hash to be rejected")
	}
	if runtime.GOOS != "windows" {
		if err := r.Run(pinnedCtx, "edge-01", map[string]any{
			"script_body": allowedScript,
			"env":         map[string]any{"BENCH_ITERATIONS": "4"},
		}, pub); err != nil {
			t.Fatalf("expected pinned script with safe env to succeed, got %v", err)
		}
	}
}

func TestEphemeralRunnerProcessGroupKillReapsBackgroundChildren(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process-group test")
	}

	pub := &memPublisher{}
	r := &EphemeralRunner{}

	// Spawn a background child process that sleeps 30s and print its PID before blocking.
	script := `sleep 30 &
CHILD_PID=$!
echo "CHILD_PID=${CHILD_PID}"
wait ${CHILD_PID}
`
	polCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh"},
		MaxTimeoutSec:       5,
	})
	ctx, cancel := context.WithTimeout(polCtx, 350*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := r.Run(ctx, "edge-01", map[string]any{
		"script_body": script,
		"timeout_sec": 10,
	}, pub)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected canceled script execution to return error")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("expected WaitDelay + process-group SIGKILL to return promptly, took %s", elapsed)
	}

	// Verify if CHILD_PID was captured in stdout and confirm it is no longer alive.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	for _, ev := range pub.events {
		stdout, _ := ev.Metrics["stdout"].(string)
		if strings.HasPrefix(stdout, "CHILD_PID=") {
			pidStr := strings.TrimSpace(strings.TrimPrefix(stdout, "CHILD_PID="))
			if pid, convErr := strconv.Atoi(pidStr); convErr == nil && pid > 1 {
				deadline := time.Now().Add(500 * time.Millisecond)
				for time.Now().Before(deadline) {
					if killErr := syscall.Kill(pid, 0); killErr != nil {
						return
					}
					time.Sleep(25 * time.Millisecond)
				}
				t.Fatalf("background child process %d survived process-group SIGKILL", pid)
			}
		}
	}
}

func TestBenchmarkRunnerRespectsMaxTimeoutSec(t *testing.T) {
	pub := &memPublisher{}
	b := &BenchmarkRunner{}
	polCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		MaxTimeoutSec: 1,
	})
	start := time.Now()
	_ = b.Run(polCtx, "edge-01", map[string]any{
		"duration_sec": 30,
		"threads":      1,
	}, pub)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("expected MaxTimeoutSec=1 to clamp duration_sec=30, took %s", elapsed)
	}
}

func TestPauseRunnerIndefiniteDefaultAndTimedResume(t *testing.T) {
	if _, ok := runner.Get("PauseRnr"); !ok {
		t.Fatal("expected PauseRnr alias to be registered")
	}
	if runner.CanonicalKey("PauseRnr") != "pause" {
		t.Fatalf("CanonicalKey(PauseRnr) = %q, want %q", runner.CanonicalKey("PauseRnr"), "pause")
	}

	pc := observer.NewPauseController()
	basePub := &memPublisher{}
	obsPub := observer.NewGatedPublisher(basePub, pc)

	// 1. Indefinite default (--secs=0 / omitted) pauses observers until aborted.
	runCtx, abortPause := context.WithCancel(observer.WithPauseController(context.Background(), pc))
	doneCh := make(chan error, 1)
	p := &PauseRunner{}

	go func() {
		doneCh <- p.Run(runCtx, "edge-01", map[string]any{
			"request_id": "req-pause-01",
			"invoked_as": "PauseRnr",
		}, obsPub)
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for !pc.IsPaused() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !pc.IsPaused() {
		t.Fatal("expected PauseRnr to pause PauseController")
	}

	// Attempt to emit an observer event while paused -> must be suppressed!
	_ = obsPub.Publish(core.NewCTDPayload("edge-01", "host-os", "active"))

	abortPause()
	if err := <-doneCh; err != nil {
		t.Fatalf("PauseRunner indefinite abort returned unexpected error: %v", err)
	}
	if pc.IsPaused() {
		t.Fatal("expected PauseController to unpause after PauseRnr abort")
	}

	// Emit observer event after unpause -> must succeed!
	_ = obsPub.Publish(core.NewCTDPayload("edge-01", "host-os", "active"))

	basePub.mu.Lock()
	if len(basePub.events) != 3 {
		t.Fatalf("expected [runner-start, runner-aborted, post-resume host-os] (3 events), got %d", len(basePub.events))
	}
	if basePub.events[0].Component != "runner:PauseRnr" || basePub.events[0].Status != "runner-start" || basePub.events[0].Metrics["mode"] != "indefinite" {
		t.Fatalf("unexpected start bookend: %+v", basePub.events[0])
	}
	if basePub.events[1].Status != "runner-aborted" {
		t.Fatalf("expected runner-aborted on canceled indefinite pause, got %q", basePub.events[1].Status)
	}
	if basePub.events[2].Component != "host-os" {
		t.Fatalf("expected post-resume host-os event, got %q", basePub.events[2].Component)
	}
	basePub.mu.Unlock()

	// 2. Timed pause (--secs=1) ignores MaxTimeoutSec=0/small policies and closes with runner-end.
	timedPub := &memPublisher{}
	timedCtx := observer.WithPauseController(context.Background(), pc)
	if err := p.Run(timedCtx, "edge-01", map[string]any{
		"duration_sec": 1,
		"invoked_as":   "PauseRnr",
	}, timedPub); err != nil {
		t.Fatalf("timed PauseRnr failed: %v", err)
	}
	if pc.IsPaused() {
		t.Fatal("expected timed PauseRnr to unpause upon completion")
	}
	timedPub.mu.Lock()
	defer timedPub.mu.Unlock()
	if len(timedPub.events) != 2 || timedPub.events[1].Status != "runner-end" || timedPub.events[0].Metrics["mode"] != "timed" {
		t.Fatalf("unexpected timed PauseRnr bookends: %+v", timedPub.events)
	}
}
