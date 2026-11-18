//go:build darwin

package observers

import (
	"context"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

var darwinSMCFanRe = regexp.MustCompile(`(?i)Fan(?:\s*(\d+))?\s*:\s*([\d\.]+)\s*rpm`)

func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[OS Observer] macOS/Darwin host detected.")

	isRoot := os.Geteuid() == 0

	var memTotalMB float64
	if out, err := runCommand(ctx, defaultCmdTimeout, "sysctl", "-n", "hw.memsize"); err == nil {
		if bytesTotal, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); err == nil {
			memTotalMB = bytesTotal / 1024.0 / 1024.0
		}
	}

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := core.NewCTDPayload(nodeID, "host-os", "active")
			if memTotalMB > 0 {
				event.AddMetric("mem_total_mb", memTotalMB)
			}

			// Example output: "{ 2.14 1.98 1.85 }"
			if out, err := runCommand(ctx, defaultCmdTimeout, "sysctl", "-n", "vm.loadavg"); err == nil {
				trimmed := strings.Trim(strings.TrimSpace(string(out)), "{}")
				fields := strings.Fields(trimmed)
				if len(fields) >= 1 {
					if load1, err := strconv.ParseFloat(fields[0], 64); err == nil {
						event.AddMetric("load_1m", load1)
					}
				}
				if len(fields) >= 2 {
					if load5, err := strconv.ParseFloat(fields[1], 64); err == nil {
						event.AddMetric("load_5m", load5)
					}
				}
			}

			// Observe Apple SMC cooler fan RPM when running with root privileges
			if isRoot {
				if smcOut, err := runCommand(ctx, defaultCmdTimeout, "powermetrics", "--samplers", "smc", "-n", "1", "-i", "100"); err == nil {
					matches := darwinSMCFanRe.FindAllStringSubmatch(string(smcOut), -1)
					if len(matches) > 0 {
						fans := make(map[string]int, len(matches))
						firstSet := false
						for i, m := range matches {
							rpmFloat, err := strconv.ParseFloat(m[2], 64)
							if err != nil {
								continue
							}
							rpm := int(rpmFloat)
							idx := m[1]
							if idx == "" {
								idx = strconv.Itoa(i)
							}
							fans["smc:fan"+idx] = rpm
							if !firstSet {
								event.AddMetric("cpu_fan_rpm", rpm)
								firstSet = true
							}
						}
						if len(fans) > 0 {
							event.AddMetric("fans_rpm", fans)
						}
					}
				}
			}

			if err := pub.Publish(event); err != nil {
				log.Printf("[OS Observer] Publish failed: %v", err)
			}
		}
	}
}
