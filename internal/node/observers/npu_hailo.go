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
)

var (
	tempRegex   = regexp.MustCompile(`([0-9.]+)\s*C`)
	powerRegex  = regexp.MustCompile(`([0-9.]+)\s*W`)
	deviceRegex = regexp.MustCompile(`(?i)(?:Device|PCIe)\s+([0-9a-fA-F:\.]+)`)
)

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
	if !hasHailoDeviceNode() {
		log.Printf("[Hailo NPU Observer] hailortcli present on %s, but no /dev/hailo* accelerator attached; observer idle.", nodeID)
		return
	}

	var (
		cachedDevices []string
		ticks         int
	)

	runPeriodicObserver(ctx, true, func() {
		if (ticks == 0 || ticks%60 == 0) && hasHailoDeviceNode() {
			if scanned := scanHailoDevices(ctx); len(scanned) > 0 {
				cachedDevices = scanned
			}
		}
		ticks++
		if len(cachedDevices) == 0 {
			return
		}

		for i, device := range cachedDevices {
			var temp, power float64

			if tempOut, err := runCommand(ctx, defaultCmdTimeout, "hailortcli", "measure-temp", "-d", device); err == nil {
				if matches := tempRegex.FindStringSubmatch(string(tempOut)); len(matches) > 1 {
					temp, _ = strconv.ParseFloat(matches[1], 64)
				}
			}

			if powerOut, err := runCommand(ctx, defaultCmdTimeout, "hailortcli", "measure-power", "-d", device); err == nil {
				if matches := powerRegex.FindStringSubmatch(string(powerOut)); len(matches) > 1 {
					power, _ = strconv.ParseFloat(matches[1], 64)
				}
			}

			if temp == 0 && power == 0 {
				continue
			}

			componentID := "hailo-npu-" + strconv.Itoa(i)
			event := core.NewCTDPayload(nodeID, componentID, "active").
				AddMetric("device_id", device).
				AddMetric("temperature_c", temp).
				AddMetric("power_w", power)

			if err := pub.Publish(event); err != nil {
				log.Printf("[Hailo NPU Observer] Publish failed for %s: %v", componentID, err)
			}
		}
	})
}

func scanHailoDevices(ctx context.Context) []string {
	out, err := runCommand(ctx, defaultCmdTimeout, "hailortcli", "scan")
	if err != nil {
		return nil
	}

	var devices []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if matches := deviceRegex.FindStringSubmatch(line); len(matches) > 1 {
			devices = append(devices, strings.TrimSpace(matches[1]))
		}
	}
	return devices
}
