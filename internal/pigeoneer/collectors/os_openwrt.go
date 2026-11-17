//go:build linux

package collectors

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorOSOpenWrt(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(defaultPollInterval)
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
				log.Printf("[OS Collector] Failed to parse ubus system info: %v", err)
				continue
			}

			event := core.NewCTDPayload(nodeID, "host-os", "active").
				AddMetric("uptime_s", sysInfo.Uptime).
				AddMetric("mem_total_mb", sysInfo.Memory.Total/1024.0/1024.0).
				AddMetric("mem_free_mb", sysInfo.Memory.Free/1024.0/1024.0)

			if err := pub.Publish(event); err != nil {
				log.Printf("[OS Collector] Publish failed: %v", err)
			}
		}
	}
}
