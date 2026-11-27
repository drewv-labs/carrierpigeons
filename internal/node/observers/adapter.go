package observers

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

const (
	defaultPollInterval    = 5 * time.Second
	defaultCmdTimeout      = 4 * time.Second
	maxObserverStdoutBytes = 256 * 1024
)

type pollIntervalKey struct{}

// WithPollInterval attaches a custom observer polling cadence to ctx.
func WithPollInterval(ctx context.Context, d time.Duration) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
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

// runPeriodicObserver executes emit on the configured poll cadence while parking cleanly across PauseController windows
// without executing syscalls/subprocesses while paused or double-emitting buffered ticks upon resumption.
func runPeriodicObserver(ctx context.Context, emitOnBoot bool, emit func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	interval := pollInterval(ctx)
	if emitOnBoot {
		if !observer.WaitIfPaused(ctx) {
			return
		}
		if ctx.Err() != nil {
			return
		}
		emit()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if observer.IsPaused(ctx) {
				if !observer.WaitIfPaused(ctx) {
					return
				}
				ticker.Reset(interval)
				select {
				case <-ticker.C:
				default:
				}
			}
			if ctx.Err() != nil {
				return
			}
			emit()
		}
	}
}

type limitedWriter struct {
	buf bytes.Buffer
	max int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	total := len(p)
	rem := l.max - l.buf.Len()
	if rem <= 0 {
		return total, nil
	}
	if len(p) > rem {
		p = p[:rem]
	}
	_, _ = l.buf.Write(p)
	return total, nil
}

type adaptersObserver struct{}

func init() {
	observer.Register(&hostOSObserver{})
	observer.Register(&adaptersObserver{})
}

func (a *adaptersObserver) Name() string {
	return "adapters"
}

func (a *adaptersObserver) Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	ObserveAdapters(ctx, pub, nodeID)
}

// ObserveAdapters runs all peripheral hardware observer loops bound to ctx and blocks until ctx is done.
func ObserveAdapters(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if pub == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		observeGPU(ctx, pub, nodeID)
	}()
	go func() {
		defer wg.Done()
		observeNPU(ctx, pub, nodeID)
	}()
	go func() {
		defer wg.Done()
		observeNet(ctx, pub, nodeID)
	}()
	wg.Wait()
}

// StartConfigured launches enabled built-in hardware observers alongside any custom SDK observers in the default registry.
func StartConfigured(
	ctx context.Context,
	nodeID string,
	pub core.TelemetryPublisher,
	cfg config.ObserversConfig,
	interval time.Duration,
) []string {
	return StartConfiguredWithRegistry(ctx, nodeID, pub, cfg, interval, nil)
}

// StartConfiguredWithRegistry launches enabled built-in hardware observers alongside custom observers from reg.
func StartConfiguredWithRegistry(
	ctx context.Context,
	nodeID string,
	pub core.TelemetryPublisher,
	cfg config.ObserversConfig,
	interval time.Duration,
	reg *observer.Registry,
) []string {
	return StartConfiguredWithRegistryAndWaitGroup(ctx, nodeID, pub, cfg, interval, reg, nil)
}

// StartConfiguredWithRegistryAndWaitGroup launches enabled built-in hardware observers alongside custom observers from reg,
// tracking every spawned observer goroutine in wg when non-nil so callers can join all observers before disconnecting relays.
func StartConfiguredWithRegistryAndWaitGroup(
	ctx context.Context,
	nodeID string,
	pub core.TelemetryPublisher,
	cfg config.ObserversConfig,
	interval time.Duration,
	reg *observer.Registry,
	wg *sync.WaitGroup,
) []string {
	if pub == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return nil
	}

	pc := observer.PauseControllerFromContext(ctx)
	if pc == nil {
		pc = observer.NewPauseController()
		ctx = observer.WithPauseController(ctx, pc)
	}
	pub = observer.NewGatedPublisher(pub, pc)

	obsCtx := WithPollInterval(ctx, interval)
	var active []string

	launch := func(fn func()) {
		if wg != nil {
			wg.Add(1)
		}
		go func() {
			if wg != nil {
				defer wg.Done()
			}
			fn()
		}()
	}

	if cfg.OS {
		active = append(active, "host-os")
		launch(func() { observeOSPlatform(obsCtx, pub, nodeID) })
	}
	if cfg.Net {
		active = append(active, "net")
		launch(func() { observeNet(obsCtx, pub, nodeID) })
	}
	if cfg.GPU {
		active = append(active, "gpu")
		launch(func() { observeGPU(obsCtx, pub, nodeID) })
	}
	if cfg.NPU {
		active = append(active, "npu")
		launch(func() { observeNPU(obsCtx, pub, nodeID) })
	}

	var names []string
	if reg != nil {
		names = reg.Names()
	} else {
		names = observer.RegistryNames()
	}
	for _, name := range names {
		if name == "host-os" || name == "adapters" {
			continue
		}
		var (
			custom observer.Observer
			ok     bool
		)
		if reg != nil {
			custom, ok = reg.Get(name)
		} else {
			custom, ok = observer.Get(name)
		}
		if ok {
			obs := custom
			active = append(active, name)
			launch(func() { obs.Observe(obsCtx, nodeID, pub) })
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

// runCommand executes an external sensor binary with a strict timeout and bounded stdout buffer so hung drivers never deadlock observers.
func runCommand(ctx context.Context, timeout time.Duration, name string, args ...string) ([]byte, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, name, args...)
	configureProcessGroup(cmd)
	cmd.Cancel = func() error {
		killProcessGroup(cmd)
		return nil
	}
	cmd.WaitDelay = 2 * time.Second
	out := &limitedWriter{max: maxObserverStdoutBytes}
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return out.buf.Bytes(), nil
}
