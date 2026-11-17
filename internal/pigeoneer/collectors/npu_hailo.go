package collectors

import (
	"context"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

var (
	tempRegex   = regexp.MustCompile(`([0-9.]+)\s*C`)
	powerRegex  = regexp.MustCompile(`([0-9.]+)\s*W`)
	deviceRegex = regexp.MustCompile(`(?i)(?:Device|PCIe)\s+([0-9a-fA-F:\.]+)`)
)

func monitorNPUHailo(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			devices := scanHailoDevices(ctx)
			if len(devices) == 0 {
				continue
			}

			for i, device := range devices {
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
					log.Printf("[Hailo Collector] Publish failed for %s: %v", componentID, err)
				}
			}
		}
	}
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
