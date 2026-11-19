//go:build linux

package observers

import (
	"context"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if pathExists("/etc/openwrt_release") {
		log.Println("[OS Observer] OpenWrt host detected (zero-fork /proc + /sys observer).")
		observeOSOpenWrt(ctx, pub, nodeID)
		return
	}
	log.Println("[OS Observer] Linux host detected.")
	observeOSLinux(ctx, pub, nodeID)
}

func collectLinuxHostSample(nodeID string) *core.CTDPayload {
	event := core.NewCTDPayload(nodeID, "host-os", "active")

	// 1. Parse System Uptime (/proc/uptime)
	if upBytes, err := os.ReadFile("/proc/uptime"); err == nil {
		if fields := strings.Fields(string(upBytes)); len(fields) >= 1 {
			if upSec, err := strconv.ParseFloat(fields[0], 64); err == nil {
				event.AddMetric("uptime_s", math.Round(upSec))
			}
		}
	}

	// 2. Parse System Load Averages (1m, 5m, 15m)
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
		if len(fields) >= 3 {
			if load15, err := strconv.ParseFloat(fields[2], 64); err == nil {
				event.AddMetric("load_15m", load15)
			}
		}
	}

	// 3. Parse Memory Capacity, Availability & Active Usage (/proc/meminfo)
	if memBytes, err := os.ReadFile("/proc/meminfo"); err == nil {
		var (
			memTotalMB, memAvailMB, memFreeMB float64
			gotTotal, gotAvail, gotFree       bool
		)
		for line := range strings.SplitSeq(string(memBytes), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
						memTotalMB = math.Round((kb/1024.0)*10) / 10
						event.AddMetric("mem_total_mb", memTotalMB)
						gotTotal = true
					}
				}
			} else if strings.HasPrefix(line, "MemAvailable:") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
						memAvailMB = math.Round((kb/1024.0)*10) / 10
						event.AddMetric("mem_available_mb", memAvailMB)
						gotAvail = true
					}
				}
			} else if strings.HasPrefix(line, "MemFree:") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
						memFreeMB = math.Round((kb/1024.0)*10) / 10
						event.AddMetric("mem_free_mb", memFreeMB)
						gotFree = true
					}
				}
			}
			if gotTotal && gotAvail && gotFree {
				break
			}
		}
		if gotTotal && gotAvail && memTotalMB >= memAvailMB {
			event.AddMetric("mem_used_mb", math.Round((memTotalMB-memAvailMB)*10)/10)
		} else if gotTotal && gotFree && memTotalMB >= memFreeMB {
			event.AddMetric("mem_used_mb", math.Round((memTotalMB-memFreeMB)*10)/10)
		}
	}

	// 4. Observe CPU/SoC Thermals across thermal_zone* and hwmon* (sfctemp, coretemp, k10temp, etc.)
	observeSysfsThermals("/sys", event)

	// 5. Observe CPU cooler fan speed (cpu_fan_rpm, cpu_fan_pwm_pct) and host fans via sysfs
	observeSysfsCooling("/sys", event)

	return event
}

func observeOSLinux(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emit := func() {
		if err := pub.Publish(collectLinuxHostSample(nodeID)); err != nil {
			log.Printf("[OS Observer] Publish failed: %v", err)
		}
	}

	// Emit immediate initial telemetry on boot before waiting for first ticker cycle.
	emit()

	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emit()
		}
	}
}
