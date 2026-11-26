//go:build unix

package observers

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

func TestSubprocessPauseGatingKillsNvidiaAndROCmPerTickCmds(t *testing.T) {
	tmp := t.TempDir()
	markerNV := filepath.Join(tmp, "nv.started")
	binNV := filepath.Join(tmp, "fake-nvidia-smi")
	scriptNV := "#!/bin/sh\ntouch '" + markerNV + "'\nsleep 30\n"
	if err := os.WriteFile(binNV, []byte(scriptNV), 0o755); err != nil {
		t.Fatalf("write fake nvidia-smi: %v", err)
	}

	pc := observer.NewPauseController()
	ctx, cancel := context.WithCancel(WithPollInterval(observer.WithPauseController(context.Background(), pc), 50*time.Millisecond))
	defer cancel()

	pub := &capturePublisher{}
	doneNV := make(chan struct{})
	go func() {
		defer close(doneNV)
		observeGPUNvidiaWithBin(ctx, pub, "unix-node", binNV)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !pathExists(markerNV) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !pathExists(markerNV) {
		t.Fatal("fake nvidia-smi never started")
	}

	// Pause must immediately cancel ContextUntilPaused(ctx) and kill the sleeping child without waiting 4s timeout.
	startedAt := time.Now()
	release := pc.Acquire()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-doneNV:
	case <-time.After(2 * time.Second):
		t.Fatal("nvidia observer did not abort sleeping subprocess promptly on pause")
	}
	release()
	if elapsed := time.Since(startedAt); elapsed >= 3*time.Second {
		t.Fatalf("expected pause to kill nvidia-smi well before defaultCmdTimeout, took %v", elapsed)
	}

	// Verify the same ContextUntilPaused gating in observeGPUAMDWithBin.
	markerAMD := filepath.Join(tmp, "amd.started")
	binAMD := filepath.Join(tmp, "fake-rocm-smi")
	scriptAMD := "#!/bin/sh\ntouch '" + markerAMD + "'\nsleep 30\n"
	if err := os.WriteFile(binAMD, []byte(scriptAMD), 0o755); err != nil {
		t.Fatalf("write fake rocm-smi: %v", err)
	}

	pc2 := observer.NewPauseController()
	ctx2, cancel2 := context.WithCancel(WithPollInterval(observer.WithPauseController(context.Background(), pc2), 50*time.Millisecond))
	defer cancel2()

	doneAMD := make(chan struct{})
	go func() {
		defer close(doneAMD)
		observeGPUAMDWithBin(ctx2, pub, "unix-node", binAMD)
	}()

	deadline = time.Now().Add(2 * time.Second)
	for !pathExists(markerAMD) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !pathExists(markerAMD) {
		t.Fatal("fake rocm-smi never started")
	}

	release2 := pc2.Acquire()
	time.Sleep(150 * time.Millisecond)
	cancel2()
	select {
	case <-doneAMD:
	case <-time.After(2 * time.Second):
		t.Fatal("amd observer did not abort sleeping subprocess promptly on pause")
	}
	release2()
}

func TestKillProcessGroupFallsBackToProcessKill(t *testing.T) {
	// Launch without Setpgid so cmd.Process.Pid is NOT a process group leader;
	// syscall.Kill(-pid, SIGKILL) returns ESRCH, exercising cmd.Process.Kill() fallback.
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}

	killProcessGroup(cmd)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case err := <-waitDone:
		if err == nil {
			t.Fatal("expected killed sleep process to exit with error")
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("killProcessGroup failed to fall back to cmd.Process.Kill() when -pid pgid did not exist")
	}
}
