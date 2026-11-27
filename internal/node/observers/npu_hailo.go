package observers

import (
	"context"
	"log"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

const (
	hailoRescanMinInterval = 60 * time.Second
	hailoMaxProbeBackoff   = 60 * time.Second
)

var (
	tempRegex   = regexp.MustCompile(`([0-9.]+)\s*C`)
	powerRegex  = regexp.MustCompile(`([0-9.]+)\s*W`)
	deviceRegex = regexp.MustCompile(`(?i)(?:Device|PCIe):?\s+([0-9a-fA-F:\.]+)`)
)

type hailoHooks struct {
	hasDeviceNode func() bool
	runCmd        func(ctx context.Context, args ...string) ([]byte, error)
	now           func() time.Time
}

type hailoProbeBackoff struct {
	failures    int
	nextProbeAt time.Time
}

func defaultHailoHooks() hailoHooks {
	return hailoHooks{
		hasDeviceNode: hasHailoDeviceNode,
		runCmd: func(ctx context.Context, args ...string) ([]byte, error) {
			return runCommand(ctx, defaultCmdTimeout, "hailortcli", args...)
		},
		now: time.Now,
	}
}

func hasHailoDeviceNode() bool {
	if runtime.GOOS != "linux" {
		return true
	}
	devs, _ := filepath.Glob("/dev/hailo*")
	if len(devs) > 0 {
		return true
	}
	sysDevs, _ := filepath.Glob("/sys/class/hailo*/*")
	return len(sysDevs) > 0
}

func isHailoDeviceInUse(out []byte, err error) bool {
	if err == nil {
		return false
	}
	combined := strings.ToUpper(string(out) + " " + err.Error())
	return strings.Contains(combined, "HAILO_DEVICE_IN_USE") || strings.Contains(combined, "DEVICE_IN_USE")
}

func computeHailoProbeBackoff(baseInterval time.Duration, consecutiveFailures int) time.Duration {
	base := max(baseInterval*2, time.Second)
	if base > hailoMaxProbeBackoff {
		return hailoMaxProbeBackoff
	}
	backoff := base
	for i := 1; i < consecutiveFailures; i++ {
		if backoff >= hailoMaxProbeBackoff/2 {
			return hailoMaxProbeBackoff
		}
		backoff *= 2
	}
	return min(backoff, hailoMaxProbeBackoff)
}

func observeNPUHailo(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeNPUHailoWithHooks(ctx, pub, nodeID, defaultHailoHooks())
}

func observeNPUHailoWithHooks(ctx context.Context, pub core.TelemetryPublisher, nodeID string, hooks hailoHooks) {
	if hooks.hasDeviceNode == nil {
		hooks.hasDeviceNode = hasHailoDeviceNode
	}
	if hooks.runCmd == nil {
		hooks.runCmd = defaultHailoHooks().runCmd
	}
	if hooks.now == nil {
		hooks.now = time.Now
	}
	if !hooks.hasDeviceNode() {
		log.Printf("[Hailo NPU Observer] hailortcli present on %s, but no /dev/hailo* accelerator attached; observer idle.", nodeID)
		return
	}

	var (
		cachedDevices []string
		hasScanned    bool
		lastScanAt    time.Time
		needsRescan   bool
		probeBackoff  = make(map[string]hailoProbeBackoff)
	)
	basePoll := pollInterval(ctx)

	runPeriodicObserver(ctx, true, func() {
		if !hooks.hasDeviceNode() {
			cachedDevices = nil
			hasScanned = false
			needsRescan = true
			clear(probeBackoff)
			return
		}

		now := hooks.now()
		rescanDue := !hasScanned || now.Sub(lastScanAt) >= hailoRescanMinInterval
		if rescanDue || (needsRescan && now.Sub(lastScanAt) >= hailoRescanMinInterval) {
			cmdCtx, cancelCmd := observer.ContextUntilPaused(ctx)
			scanned, err := scanHailoDevicesWithCmd(cmdCtx, hooks.runCmd)
			canceled := cmdCtx.Err() != nil
			cancelCmd()
			if canceled {
				// Paused or canceled during scan; retry scan cleanly after resumption.
				needsRescan = true
				return
			}
			hasScanned = true
			lastScanAt = hooks.now()
			if err != nil {
				cachedDevices = nil
			} else {
				cachedDevices = scanned
			}
			needsRescan = false
		}

		if len(cachedDevices) == 0 {
			return
		}

		for i, device := range cachedDevices {
			if st, exists := probeBackoff[device]; exists && now.Before(st.nextProbeAt) {
				continue
			}

			var (
				temp, power       float64
				hasTemp, hasPower bool
			)

			cmdCtx, cancelTemp := observer.ContextUntilPaused(ctx)
			tempOut, tempErr := hooks.runCmd(cmdCtx, "measure-temp", "-d", device)
			tempCanceled := cmdCtx.Err() != nil
			cancelTemp()
			if tempCanceled {
				return
			}
			if tempErr == nil {
				if matches := tempRegex.FindStringSubmatch(string(tempOut)); len(matches) > 1 {
					temp, hasTemp = parseFiniteFloat(matches[1])
				}
			}

			var powerErr error
			if isHailoDeviceInUse(tempOut, tempErr) {
				powerErr = tempErr
			} else {
				cmdCtx, cancelPower := observer.ContextUntilPaused(ctx)
				powerOut, pErr := hooks.runCmd(cmdCtx, "measure-power", "-d", device)
				powerCanceled := cmdCtx.Err() != nil
				cancelPower()
				if powerCanceled {
					return
				}
				powerErr = pErr
				if powerErr == nil {
					if matches := powerRegex.FindStringSubmatch(string(powerOut)); len(matches) > 1 {
						power, hasPower = parseFiniteFloat(matches[1])
					}
				}
			}

			if tempErr != nil && powerErr != nil {
				st := probeBackoff[device]
				st.failures++
				st.nextProbeAt = hooks.now().Add(computeHailoProbeBackoff(basePoll, st.failures))
				probeBackoff[device] = st
				needsRescan = true
				continue
			}
			if !hasTemp && !hasPower {
				continue
			}
			delete(probeBackoff, device)

			componentID := "hailo-npu-" + strconv.Itoa(i)
			event := core.NewCTDPayload(nodeID, componentID, "active").
				AddMetric("device_id", device)
			if hasTemp {
				event.AddMetric("temperature_c", temp)
			}
			if hasPower {
				event.AddMetric("power_w", power)
			}

			if err := pub.Publish(event); err != nil {
				log.Printf("[Hailo NPU Observer] Publish failed for %s: %v", componentID, err)
			}
		}
	})
}

func scanHailoDevices(ctx context.Context) []string {
	cmdCtx, cancelCmd := observer.ContextUntilPaused(ctx)
	defer cancelCmd()
	devices, err := scanHailoDevicesWithCmd(cmdCtx, defaultHailoHooks().runCmd)
	if err != nil {
		return nil
	}
	return devices
}

func scanHailoDevicesWithCmd(ctx context.Context, runCmd func(context.Context, ...string) ([]byte, error)) ([]string, error) {
	out, err := runCmd(ctx, "scan")
	if err != nil {
		return nil, err
	}

	var devices []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if matches := deviceRegex.FindStringSubmatch(line); len(matches) > 1 {
			devices = append(devices, strings.TrimSpace(matches[1]))
		}
	}
	return devices, nil
}
