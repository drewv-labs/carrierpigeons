//go:build linux

package observers

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeOSOpenWrt(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	type UbusSystemInfo struct {
		Uptime float64 `json:"uptime"`
		Memory struct {
			Total float64 `json:"total"`
			Free  float64 `json:"free"`
		} `json:"memory"`
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "ubus", "call", "system", "info")
			if err != nil {
				continue
			}

			var sysInfo UbusSystemInfo
			if err := json.Unmarshal(out, &sysInfo); err != nil {
				log.Printf("[OS Observer] Failed to parse ubus system info: %v", err)
				continue
			}

			event := core.NewCTDPayload(nodeID, "host-os", "active").
				AddMetric("uptime_s", sysInfo.Uptime).
				AddMetric("mem_total_mb", sysInfo.Memory.Total/1024.0/1024.0).
				AddMetric("mem_free_mb", sysInfo.Memory.Free/1024.0/1024.0)

			if tempBytes, err := os.ReadFile("/sys/class/thermal/thermal_zone0/temp"); err == nil {
				if milliC, err := strconv.ParseFloat(strings.TrimSpace(string(tempBytes)), 64); err == nil {
					event.AddMetric("soc_temp_c", milliC/1000.0)
				}
			}

			// Observe SoC / CPU cooler fan speed & PWM duty cycle via Linux sysfs
			observeSysfsCooling("/sys", event)

			if err := pub.Publish(event); err != nil {
				log.Printf("[OS Observer] Publish failed: %v", err)
			}
		}
	}
}
