package observers

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/observer"
)

const defaultPollInterval = 5 * time.Second
const defaultCmdTimeout = 4 * time.Second

type adaptersObserver struct{}

func init() {
	observer.Register(&adaptersObserver{})
}

func (a *adaptersObserver) Name() string {
	return "adapters"
}

func (a *adaptersObserver) Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	ObserveAdapters(ctx, pub, nodeID)
}

// ObserveAdapters initializes all peripheral hardware observer loops bound to ctx.
func ObserveAdapters(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	go observeGPU(ctx, pub, nodeID)
	go observeNPU(ctx, pub, nodeID)
	go observeNet(ctx, pub, nodeID)
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

// runCommand executes an external sensor binary with a strict timeout so hung drivers never deadlock observers.
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
