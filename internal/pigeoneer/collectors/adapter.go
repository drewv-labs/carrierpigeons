package collectors

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

const defaultPollInterval = 5 * time.Second
const defaultCmdTimeout = 4 * time.Second

// MonitorAdapters initializes all peripheral hardware sensor loops bound to ctx.
func MonitorAdapters(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	go monitorGPU(ctx, pub, nodeID)
	go monitorNPU(ctx, pub, nodeID)
	go monitorNet(ctx, pub, nodeID)
}

// isInstalled checks if an executable exists in the system PATH.
func isInstalled(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

// pathExists checks whether a sysfs/procfs path exists on disk.
func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// runCommand executes an external sensor binary with a strict timeout so hung drivers never deadlock collectors.
func runCommand(ctx context.Context, timeout time.Duration, name string, args ...string) ([]byte, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
