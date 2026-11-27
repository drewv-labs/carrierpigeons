//go:build !unix

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	lockFileWaitTimeout = 10 * time.Second
	lockFileStaleAge    = 30 * time.Second
)

func withAdvisoryFileLock(targetPath string, fn func() error) error {
	lockPath := targetPath + ".lock"
	dir := filepath.Dir(lockPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create lock directory %s: %w", dir, err)
	}

	deadline := time.Now().Add(lockFileWaitTimeout)
	backoff := 5 * time.Millisecond
	var lockFile *os.File

	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			lockFile = f
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create lock file %s: %w", lockPath, err)
		}
		if info, statErr := os.Stat(lockPath); statErr == nil && time.Since(info.ModTime()) > lockFileStaleAge {
			_ = os.Remove(lockPath)
			continue
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out acquiring lock file %s", lockPath)
		}
		time.Sleep(backoff)
		if backoff < 50*time.Millisecond {
			backoff *= 2
		}
	}

	defer func() {
		_ = lockFile.Close()
		_ = os.Remove(lockPath)
	}()

	return fn()
}
