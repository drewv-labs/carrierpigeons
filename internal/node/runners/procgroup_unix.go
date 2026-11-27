//go:build !windows

package runners

import (
	"context"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

type pgidAnchor struct {
	cmd   *exec.Cmd
	holdW *os.File
	pgid  int
}

// startPGIDAnchor spawns a minimal quiescent group leader blocked on a pipe read so pgid remains
// reserved by an unreaped process in the kernel until after syscall.Kill(-pgid, SIGKILL) completes.
func startPGIDAnchor() *pgidAnchor {
	shBin, err := resolveSandboxInterpreter("sh")
	if err != nil {
		return nil
	}
	holdR, holdW, err := os.Pipe()
	if err != nil {
		return nil
	}
	holder := exec.Command(shBin, "-c", "IFS= read -r _")
	holder.Stdin = holdR
	holder.Env = []string{"PATH=" + posixSandboxPATH}
	holder.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := holder.Start(); err != nil {
		_ = holdR.Close()
		_ = holdW.Close()
		return nil
	}
	_ = holdR.Close()

	return &pgidAnchor{
		cmd:   holder,
		holdW: holdW,
		pgid:  holder.Process.Pid,
	}
}

func (a *pgidAnchor) teardown() {
	if a == nil {
		return
	}
	if a.holdW != nil {
		_ = a.holdW.Close()
		a.holdW = nil
	}
	if a.pgid > 0 {
		_ = syscall.Kill(-a.pgid, syscall.SIGKILL)
	}
	if a.cmd != nil && a.cmd.Process != nil {
		_, _ = a.cmd.Process.Wait()
	}
}

func runProcessGroupCmd(ctx context.Context, cmd *exec.Cmd, stdoutDst, stderrDst io.Writer) error {
	return runCommandWithPipes(ctx, cmd, stdoutDst, stderrDst, func(ctx context.Context, cmd *exec.Cmd, onStarted func()) error {
		anchor := startPGIDAnchor()
		if anchor != nil {
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
				Pgid:    anchor.pgid,
			}
			if err := cmd.Start(); err != nil {
				anchor.teardown()
				return err
			}
			onStarted()

			type waitResult struct {
				state *os.ProcessState
				err   error
			}
			waitCh := make(chan waitResult, 1)
			go func() {
				state, err := cmd.Process.Wait()
				waitCh <- waitResult{state: state, err: err}
			}()

			var res waitResult
			select {
			case res = <-waitCh:
				// cmd exited; anchor still holds pgid unreaped in the kernel, so -pgid kill has zero PID-reuse race.
				anchor.teardown()
			case <-ctx.Done():
				// Kill entire process group while both cmd and anchor are unreaped, then reap both.
				_ = syscall.Kill(-anchor.pgid, syscall.SIGKILL)
				res = <-waitCh
				anchor.teardown()
			}
			return evalProcessWaitResult(ctx, res.state, res.err)
		}

		// Fallback if sandbox sh is unavailable: create group on cmd itself and never signal -pgid after reap.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			return err
		}
		onStarted()

		pgid := cmd.Process.Pid

		type waitResult struct {
			state *os.ProcessState
			err   error
		}
		waitCh := make(chan waitResult, 1)

		var (
			mu          sync.Mutex
			reaped      bool
			groupKilled bool
		)

		go func() {
			state, err := cmd.Process.Wait()
			mu.Lock()
			reaped = true
			mu.Unlock()
			waitCh <- waitResult{state: state, err: err}
		}()

		var res waitResult
		select {
		case res = <-waitCh:
			// Fallback without anchor: leader is already reaped, do not signal -pgid post-reap.
		case <-ctx.Done():
			mu.Lock()
			if !reaped && !groupKilled && pgid > 0 {
				groupKilled = true
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			}
			mu.Unlock()
			res = <-waitCh
		}

		return evalProcessWaitResult(ctx, res.state, res.err)
	})
}
