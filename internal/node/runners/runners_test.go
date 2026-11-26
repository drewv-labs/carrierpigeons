package runners

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
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

func TestEphemeralRunnerBlockedEnvKeysAndPrefixes(t *testing.T) {
	pub := &memPublisher{}
	r := &EphemeralRunner{}
	marker := filepath.Join(t.TempDir(), "pwned")

	enabledCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash", "python3"},
		MaxTimeoutSec:       10,
	})

	blockedCases := []struct {
		name string
		env  map[string]any
	}{
		{
			name: "shellopts_xtrace_ps4_injection",
			env: map[string]any{
				"SHELLOPTS": "xtrace",
				"PS4":       fmt.Sprintf("$(touch %s)", marker),
			},
		},
		{name: "ps4_only", env: map[string]any{"PS4": "$(id)"}},
		{name: "bashopts", env: map[string]any{"BASHOPTS": "xtrace"}},
		{name: "env_posix", env: map[string]any{"ENV": "/tmp/evil.sh"}},
		{name: "pythonuserbase", env: map[string]any{"PYTHONUSERBASE": "/tmp/pyevil"}},
		{name: "pythonwarnings", env: map[string]any{"PYTHONWARNINGS": "all"}},
		{name: "openssl_conf", env: map[string]any{"OPENSSL_CONF": "/tmp/openssl.cnf"}},
		{name: "pigeons_node_id_spoof", env: map[string]any{"PIGEONS_NODE_ID": "spoofed-node"}},
		{name: "pigeons_custom_prefix", env: map[string]any{"PIGEONS_TOKEN": "secret"}},
		{name: "xdg_config_home", env: map[string]any{"XDG_CONFIG_HOME": "/tmp/xdg"}},
		{name: "git_ssh_command", env: map[string]any{"GIT_SSH_COMMAND": "id"}},
		{name: "curl_home", env: map[string]any{"CURL_HOME": "/tmp/curl"}},
		{name: "pgpassfile", env: map[string]any{"PGPASSFILE": "/tmp/pgpass"}},
		{name: "pghost", env: map[string]any{"PGHOST": "192.0.2.1"}},
	}

	for _, tc := range blockedCases {
		t.Run(tc.name, func(t *testing.T) {
			err := r.Run(enabledCtx, "edge-01", map[string]any{
				"interpreter": "sh",
				"script_body": "echo ok\n",
				"env":         tc.env,
			}, pub)
			if err == nil {
				t.Fatalf("expected env %v to be rejected by sandbox policy", tc.env)
			}
		})
	}

	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("PS4 payload marker should never be created, stat err=%v", statErr)
	}

	// Legitimate benign custom env var still works and PIGEONS_NODE_ID reflects nodeID.
	if runtime.GOOS != "windows" {
		okPub := &memPublisher{}
		if err := r.Run(enabledCtx, "edge-01", map[string]any{
			"interpreter": "sh",
			"script_body": "printf '%s|%s' \"$BENCH_ITERATIONS\" \"$PIGEONS_NODE_ID\"\n",
			"env":         map[string]string{"BENCH_ITERATIONS": "7"},
		}, okPub); err != nil {
			t.Fatalf("expected benign custom env to succeed, got %v", err)
		}
		endEv := okPub.events[len(okPub.events)-1]
		if got, _ := endEv.Metrics["stdout"].(string); got != "7|edge-01" {
			t.Fatalf("unexpected stdout %q, want %q", got, "7|edge-01")
		}
	}
}

func TestEphemeralRunnerInterpreterIsolationFixedPATHAndScriptCap(t *testing.T) {
	if args := interpreterArgs("python3", "/work/task.py"); !slices.Equal(args, []string{"-I", "/work/task.py"}) {
		t.Fatalf("python3 args = %v, want [-I /work/task.py]", args)
	}
	if args := interpreterArgs("bash", "/work/task.sh"); !slices.Equal(args, []string{"--norc", "--noprofile", "/work/task.sh"}) {
		t.Fatalf("bash args = %v, want [--norc --noprofile /work/task.sh]", args)
	}

	r := &EphemeralRunner{}
	enabledCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash", "python3"},
		MaxTimeoutSec:       10,
	})

	// 256 KiB cap enforcement.
	oversize := strings.Repeat("#", maxEphemeralScriptBytes+1)
	if err := r.Run(enabledCtx, "edge-01", map[string]any{
		"interpreter": "sh",
		"script_body": oversize,
	}, &memPublisher{}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected script_body > 256 KiB to fail size cap, got %v", err)
	}

	if runtime.GOOS == "windows" {
		return
	}

	// Poison ambient PATH with a fake sh binary; EphemeralRunner must resolve from fixed sandboxPATH instead.
	fakeBinDir := t.TempDir()
	fakeSH := filepath.Join(fakeBinDir, "sh")
	if err := os.WriteFile(fakeSH, []byte("#!/bin/sh\necho HIJACKED_PATH\n"), 0o700); err != nil {
		t.Fatalf("write fake sh: %v", err)
	}
	t.Setenv("PATH", fakeBinDir+":"+os.Getenv("PATH"))

	pub := &memPublisher{}
	if err := r.Run(enabledCtx, "edge-01", map[string]any{
		"interpreter": "sh",
		"script_body": "echo REAL_SANDBOX_SH\n",
	}, pub); err != nil {
		t.Fatalf("expected sandbox sh execution to succeed despite poisoned PATH, got %v", err)
	}
	endEv := pub.events[len(pub.events)-1]
	if got, _ := endEv.Metrics["stdout"].(string); got != "REAL_SANDBOX_SH" {
		t.Fatalf("expected real sandbox sh output, got %q (ambient PATH may have been used!)", got)
	}

	// Live python3 -I check if python3 exists in sandboxPATH.
	if _, err := resolveSandboxInterpreter("python3"); err == nil {
		pyPub := &memPublisher{}
		if err := r.Run(enabledCtx, "edge-01", map[string]any{
			"interpreter": "python3",
			"script_body": "import sys\nprint(sys.flags.isolated)\n",
		}, pyPub); err != nil {
			t.Fatalf("python3 -I execution failed: %v", err)
		}
		pyEnd := pyPub.events[len(pyPub.events)-1]
		if got, _ := pyEnd.Metrics["stdout"].(string); got != "1" {
			t.Fatalf("expected python3 sys.flags.isolated == 1, got %q", got)
		}
	}

	// Live bash --norc --noprofile check if bash exists in sandboxPATH.
	if _, err := resolveSandboxInterpreter("bash"); err == nil {
		bashPub := &memPublisher{}
		if err := r.Run(enabledCtx, "edge-01", map[string]any{
			"interpreter": "bash",
			"script_body": "echo bash-ok\n",
		}, bashPub); err != nil {
			t.Fatalf("bash execution failed: %v", err)
		}
		bashEnd := bashPub.events[len(bashPub.events)-1]
		if got, _ := bashEnd.Metrics["stdout"].(string); got != "bash-ok" {
			t.Fatalf("expected bash-ok, got %q", got)
		}
	}
}

func TestEphemeralRunnerCrossInterpreterHashBinding(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX interpreter test")
	}

	r := &EphemeralRunner{}
	script := "echo cross-interp-test\n"
	bodyHash := runner.ComputeScriptSHA256(script)
	shBoundHash := runner.ComputeBoundScriptSHA256("sh", script)

	// 1. Raw bodyHash MUST be rejected when multiple interpreters are allowed (prevents cross-interpreter reuse).
	multiRawCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash"},
		AllowedScriptHashes: []string{bodyHash},
		MaxTimeoutSec:       10,
	})
	if err := r.Run(multiRawCtx, "edge-01", map[string]any{
		"interpreter": "sh",
		"script_body": script,
	}, &memPublisher{}); err == nil {
		t.Fatal("expected bare body hash to be rejected when multiple interpreters are permitted")
	}

	// 2. Bound hash (sha256(interp + "\x00" + scriptBody)) permits matching interpreter and rejects others.
	boundCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash"},
		AllowedScriptHashes: []string{shBoundHash},
		MaxTimeoutSec:       10,
	})
	if err := r.Run(boundCtx, "edge-01", map[string]any{
		"interpreter": "sh",
		"script_body": script,
	}, &memPublisher{}); err != nil {
		t.Fatalf("expected sh bound hash to execute under sh, got %v", err)
	}
	if err := r.Run(boundCtx, "edge-01", map[string]any{
		"interpreter": "bash",
		"script_body": script,
	}, &memPublisher{}); err == nil {
		t.Fatal("expected sh bound hash to be rejected under bash")
	}

	// 3. Prefixed <interp>:<sha256> entry (including path/whitespace normalization) permits matching interpreter and rejects cross-interpreter reuse.
	prefixedCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash"},
		AllowedScriptHashes: []string{" /usr/bin/sh : " + strings.ToUpper(bodyHash)},
		MaxTimeoutSec:       10,
	})
	if err := r.Run(prefixedCtx, "edge-01", map[string]any{
		"interpreter": "sh",
		"script_body": script,
	}, &memPublisher{}); err != nil {
		t.Fatalf("expected prefixed sh:<sha256> entry to execute under sh, got %v", err)
	}
	if err := r.Run(prefixedCtx, "edge-01", map[string]any{
		"interpreter": "bash",
		"script_body": script,
	}, &memPublisher{}); err == nil {
		t.Fatal("expected prefixed sh:<sha256> entry to be rejected under bash")
	}
}

func TestPauseRunnerDurationOverflowAndActualPausedStateAfterRelease(t *testing.T) {
	p := &PauseRunner{}

	overflowInputs := []struct {
		name string
		val  any
	}{
		{name: "max_int64", val: int64(math.MaxInt64)},
		{name: "float64_overflow", val: float64(1e20)},
		{name: "string_overflow", val: "999999999999999999999999"},
	}

	for _, tc := range overflowInputs {
		t.Run(tc.name, func(t *testing.T) {
			pc := observer.NewPauseController()
			pub := &memPublisher{}
			ctx, cancel := context.WithCancel(observer.WithPauseController(context.Background(), pc))
			defer cancel()

			doneCh := make(chan error, 1)
			go func() {
				doneCh <- p.Run(ctx, "edge-01", map[string]any{
					"duration_sec": tc.val,
					"invoked_as":   "PauseRnr",
				}, pub)
			}()

			// Must remain paused (not return immediately due to negative int64 duration overflow or mode="indefinite").
			select {
			case err := <-doneCh:
				t.Fatalf("PauseRunner exited prematurely on overflow input %v: err=%v events=%+v", tc.val, err, pub.events)
			case <-time.After(60 * time.Millisecond):
			}
			if !pc.IsPaused() {
				t.Fatal("expected PauseController to be paused while capped timed pause is active")
			}

			cancel()
			if err := <-doneCh; err != nil {
				t.Fatalf("PauseRunner cancel returned error: %v", err)
			}

			pub.mu.Lock()
			defer pub.mu.Unlock()
			if len(pub.events) != 2 {
				t.Fatalf("expected 2 events, got %d", len(pub.events))
			}
			if pub.events[0].Metrics["mode"] != "timed" {
				t.Fatalf("expected mode=timed on capped overflow duration, got %v", pub.events[0].Metrics["mode"])
			}
			if dur, ok := pub.events[0].Metrics["duration_sec"].(int64); !ok || dur != maxPauseDurationSec {
				t.Fatalf("expected capped duration_sec=%d, got %T(%v)", maxPauseDurationSec, pub.events[0].Metrics["duration_sec"], pub.events[0].Metrics["duration_sec"])
			}
			if pub.events[1].Status != "runner-aborted" {
				t.Fatalf("expected runner-aborted, got %q", pub.events[1].Status)
			}
			if pausedMetric, ok := pub.events[1].Metrics["observers_paused"].(bool); !ok || pausedMetric {
				t.Fatalf("expected observers_paused=false after release, got %v", pub.events[1].Metrics["observers_paused"])
			}
		})
	}

	// When an external PauseController hold or manual Pause() remains active, closing bookend must emit observers_paused=true.
	pc := observer.NewPauseController()
	outerRelease := pc.Acquire()
	defer outerRelease()

	pub := &memPublisher{}
	ctx, cancel := context.WithCancel(observer.WithPauseController(context.Background(), pc))
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	if err := p.Run(ctx, "edge-01", map[string]any{"duration_sec": 10}, pub); err != nil {
		t.Fatalf("PauseRunner with outer hold failed: %v", err)
	}
	if !pc.IsPaused() {
		t.Fatal("expected outer PauseController hold to keep pc paused")
	}
	pub.mu.Lock()
	endEv := pub.events[len(pub.events)-1]
	pub.mu.Unlock()
	if pausedMetric, ok := endEv.Metrics["observers_paused"].(bool); !ok || !pausedMetric {
		t.Fatalf("expected observers_paused=true when external hold keeps pc paused after release(), got %v", endEv.Metrics["observers_paused"])
	}
	outerRelease()
	if pc.IsPaused() {
		t.Fatal("expected pc unpaused after releasing outer hold")
	}
}
