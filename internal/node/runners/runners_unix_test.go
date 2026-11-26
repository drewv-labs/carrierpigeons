//go:build !windows

package runners

import (
	"context"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

func TestEphemeralRunnerProcessGroupKillReapsBackgroundChildren(t *testing.T) {
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

func TestEphemeralRunnerExit0KillsBackgroundedUnixOrphans(t *testing.T) {
	r := &EphemeralRunner{}
	polCtx := runner.WithExecutionPolicy(context.Background(), runner.ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh"},
		MaxTimeoutSec:       10,
	})

	cases := []struct {
		name   string
		script string
	}{
		{
			name: "detached_stdio",
			script: `sleep 30 >/dev/null 2>&1 &
echo "CHILD_PID=$!"
exit 0
`,
		},
		{
			name: "inherited_stdio",
			script: `sleep 30 &
echo "CHILD_PID=$!"
exit 0
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &memPublisher{}
			start := time.Now()
			if err := r.Run(polCtx, "edge-01", map[string]any{
				"interpreter": "sh",
				"script_body": tc.script,
				"timeout_sec": 10,
			}, pub); err != nil {
				t.Fatalf("expected clean exit 0, got %v", err)
			}
			elapsed := time.Since(start)
			if elapsed > 1500*time.Millisecond {
				t.Fatalf("expected exit 0 with background child teardown to return promptly without WaitDelay hang, took %s", elapsed)
			}

			var childPID int
			pub.mu.Lock()
			for _, ev := range pub.events {
				stdout, _ := ev.Metrics["stdout"].(string)
				if strings.HasPrefix(stdout, "CHILD_PID=") {
					pidStr := strings.TrimSpace(strings.TrimPrefix(stdout, "CHILD_PID="))
					if parsed, convErr := strconv.Atoi(pidStr); convErr == nil {
						childPID = parsed
					}
				}
			}
			pub.mu.Unlock()
			if childPID <= 1 {
				t.Fatalf("failed to capture CHILD_PID from stdout, events=%+v", pub.events)
			}

			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				if killErr := syscall.Kill(childPID, 0); killErr != nil {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			_ = syscall.Kill(childPID, syscall.SIGKILL)
			t.Fatalf("background child process %d survived exit 0 process-group teardown", childPID)
		})
	}
}
