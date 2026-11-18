package observers

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/config"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/observer"
)

const defaultPollInterval = 5 * time.Second
const defaultCmdTimeout = 4 * time.Second

type pollIntervalKey struct{}

// WithPollInterval attaches a custom observer polling cadence to ctx.
func WithPollInterval(ctx context.Context, d time.Duration) context.Context {
	if d <= 0 {
		d = defaultPollInterval
	}
	return context.WithValue(ctx, pollIntervalKey{}, d)
}

func pollInterval(ctx context.Context) time.Duration {
	if ctx != nil {
		if d, ok := ctx.Value(pollIntervalKey{}).(time.Duration); ok && d > 0 {
			return d
		}
	}
	return defaultPollInterval
}

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

// StartConfigured launches enabled built-in hardware observers alongside any custom SDK observers.
func StartConfigured(
	ctx context.Context,
	nodeID string,
	pub core.TelemetryPublisher,
	cfg config.ObserversConfig,
	interval time.Duration,
) []string {
	obsCtx := WithPollInterval(ctx, interval)
	var active []string

	if cfg.OS {
		active = append(active, "host-os")
		go observeOSPlatform(obsCtx, pub, nodeID)
	}
	if cfg.Net {
		active = append(active, "net")
		go observeNet(obsCtx, pub, nodeID)
	}
	if cfg.GPU {
		active = append(active, "gpu")
		go observeGPU(obsCtx, pub, nodeID)
	}
	if cfg.NPU {
		active = append(active, "npu")
		go observeNPU(obsCtx, pub, nodeID)
	}

	// Launch any custom downstream SDK observers registered outside built-in host-os/adapters.
	for _, name := range observer.RegistryNames() {
		if name == "host-os" || name == "adapters" {
			continue
		}
		if custom, ok := observer.Get(name); ok {
			active = append(active, name)
			go custom.Observe(obsCtx, nodeID, pub)
		}
	}

	return active
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
