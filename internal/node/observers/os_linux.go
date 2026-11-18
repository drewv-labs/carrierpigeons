//go:build linux

package observers

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if isInstalled("ubus") {
		log.Println("[OS Observer] OpenWrt host detected.")
		observeOSOpenWrt(ctx, pub, nodeID)
		return
	}
	log.Println("[OS Observer] Linux host detected.")
	observeOSLinux(ctx, pub, nodeID)
}

func observeOSLinux(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := core.NewCTDPayload(nodeID, "host-os", "active")

			// Parse System Load Averages (1m, 5m)
			if loadBytes, err := os.ReadFile("/proc/loadavg"); err == nil {
				fields := strings.Fields(string(loadBytes))
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

			// Parse Memory Capacity & Availability
			if memBytes, err := os.ReadFile("/proc/meminfo"); err == nil {
				var gotTotal, gotAvail bool
				for line := range strings.SplitSeq(string(memBytes), "\n") {
					if strings.HasPrefix(line, "MemTotal:") {
						if fields := strings.Fields(line); len(fields) >= 2 {
							if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
								event.AddMetric("mem_total_mb", kb/1024.0)
								gotTotal = true
							}
						}
					} else if strings.HasPrefix(line, "MemAvailable:") {
						if fields := strings.Fields(line); len(fields) >= 2 {
							if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
								event.AddMetric("mem_available_mb", kb/1024.0)
								gotAvail = true
							}
						}
					}
					if gotTotal && gotAvail {
						break
					}
				}
			}

			// Parse SoC Thermal Zone 0 if exposed by the BSP
			if tempBytes, err := os.ReadFile("/sys/class/thermal/thermal_zone0/temp"); err == nil {
				if milliC, err := strconv.ParseFloat(strings.TrimSpace(string(tempBytes)), 64); err == nil {
					event.AddMetric("soc_temp_c", milliC/1000.0)
				}
			}

			// Observe CPU cooler fan speed (cpu_fan_rpm, cpu_fan_pwm_pct) and host fans via sysfs
			observeSysfsCooling("/sys", event)

			if err := pub.Publish(event); err != nil {
				log.Printf("[OS Observer] Publish failed: %v", err)
			}
		}
	}
}
