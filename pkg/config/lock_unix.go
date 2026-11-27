//go:build unix

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func withAdvisoryFileLock(targetPath string, fn func() error) error {
	lockPath := targetPath + ".lock"
	dir := filepath.Dir(lockPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create lock directory %s: %w", dir, err)
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	defer func() {
		_ = f.Close()
	}()

	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("acquire flock on %s: %w", lockPath, err)
	}
	defer func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	}()

	return fn()
}
