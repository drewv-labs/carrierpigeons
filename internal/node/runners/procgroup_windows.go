//go:build windows

package runners

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func runProcessGroupCmd(ctx context.Context, cmd *exec.Cmd, stdoutDst, stderrDst io.Writer) error {
	return runCommandWithPipes(ctx, cmd, stdoutDst, stderrDst, func(ctx context.Context, cmd *exec.Cmd, onStarted func()) error {
		job, err := windows.CreateJobObject(nil, nil)
		if err != nil {
			return fmt.Errorf("create windows job object: %w", err)
		}
		var jobClosed atomic.Bool
		closeJob := func() {
			if jobClosed.CompareAndSwap(false, true) {
				_ = windows.CloseHandle(job)
			}
		}
		defer closeJob()

		var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
		info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
		ret, err := windows.SetInformationJobObject(
			job,
			windows.JobObjectExtendedLimitInformation,
			uintptr(unsafe.Pointer(&info)),
			uint32(unsafe.Sizeof(info)),
		)
		if ret == 0 {
			if err == nil {
				err = windows.GetLastError()
			}
			return fmt.Errorf("configure windows job object: %w", err)
		}

		cmd.SysProcAttr = &syscall.SysProcAttr{
			CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_NO_WINDOW,
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		onStarted()

		pid := uint32(cmd.Process.Pid)
		procHandle, openErr := windows.OpenProcess(
			windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
			false,
			pid,
		)
		if openErr != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			return fmt.Errorf("open child process for windows job object assignment: %w", openErr)
		}
		assignErr := windows.AssignProcessToJobObject(job, procHandle)
		_ = windows.CloseHandle(procHandle)
		if assignErr != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			return fmt.Errorf("assign process to windows job object: %w", assignErr)
		}

		type waitResult struct {
			state *os.ProcessState
			err   error
		}
		waitCh := make(chan waitResult, 1)
		go func() {
			state, waitErr := cmd.Process.Wait()
			waitCh <- waitResult{state: state, err: waitErr}
		}()

		var res waitResult
		select {
		case res = <-waitCh:
			_ = windows.TerminateJobObject(job, 1)
			closeJob()
		case <-ctx.Done():
			_ = windows.TerminateJobObject(job, 1)
			closeJob()
			_ = cmd.Process.Kill()
			res = <-waitCh
		}

		return evalProcessWaitResult(ctx, res.state, res.err)
	})
}
