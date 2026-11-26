package observers

import (
	"context"
	"log"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

var (
	tempRegex   = regexp.MustCompile(`([0-9.]+)\s*C`)
	powerRegex  = regexp.MustCompile(`([0-9.]+)\s*W`)
	deviceRegex = regexp.MustCompile(`(?i)(?:Device|PCIe)\s+([0-9a-fA-F:\.]+)`)
)

type hailoHooks struct {
	hasDeviceNode func() bool
	runCmd        func(ctx context.Context, args ...string) ([]byte, error)
}

func defaultHailoHooks() hailoHooks {
	return hailoHooks{
		hasDeviceNode: hasHailoDeviceNode,
		runCmd: func(ctx context.Context, args ...string) ([]byte, error) {
			return runCommand(ctx, defaultCmdTimeout, "hailortcli", args...)
		},
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
	if !hooks.hasDeviceNode() {
		log.Printf("[Hailo NPU Observer] hailortcli present on %s, but no /dev/hailo* accelerator attached; observer idle.", nodeID)
		return
	}

	var (
		cachedDevices []string
		ticks         int
		needsRescan   bool
	)

	runPeriodicObserver(ctx, true, func() {
		if !hooks.hasDeviceNode() {
			cachedDevices = nil
			needsRescan = true
			ticks++
			return
		}

		if ticks == 0 || ticks%60 == 0 || needsRescan {
			cmdCtx, cancelCmd := observer.ContextUntilPaused(ctx)
			scanned, err := scanHailoDevicesWithCmd(cmdCtx, hooks.runCmd)
			canceled := cmdCtx.Err() != nil
			cancelCmd()
			if canceled {
				// Paused or canceled during scan; retry scan cleanly after resumption.
				needsRescan = true
				return
			}
			if err != nil {
				cachedDevices = nil
			} else {
				cachedDevices = scanned
			}
			needsRescan = false
		}
		ticks++

		if len(cachedDevices) == 0 {
			return
		}

		for i, device := range cachedDevices {
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

			cmdCtx, cancelPower := observer.ContextUntilPaused(ctx)
			powerOut, powerErr := hooks.runCmd(cmdCtx, "measure-power", "-d", device)
			powerCanceled := cmdCtx.Err() != nil
			cancelPower()
			if powerCanceled {
				return
			}
			if powerErr == nil {
				if matches := powerRegex.FindStringSubmatch(string(powerOut)); len(matches) > 1 {
					power, hasPower = parseFiniteFloat(matches[1])
				}
			}

			if tempErr != nil && powerErr != nil {
				needsRescan = true
			}
			if !hasTemp && !hasPower {
				continue
			}

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

		if needsRescan {
			// Clear stale detached device cache immediately when all probes for a device failed.
			cmdCtx, cancelCmd := observer.ContextUntilPaused(ctx)
			scanned, err := scanHailoDevicesWithCmd(cmdCtx, hooks.runCmd)
			canceled := cmdCtx.Err() != nil
			cancelCmd()
			if !canceled {
				if err != nil {
					cachedDevices = nil
				} else {
					cachedDevices = scanned
				}
				needsRescan = false
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
