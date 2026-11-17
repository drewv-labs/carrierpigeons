//go:build windows

package collectors

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func monitorOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[OS Router] Windows host detected. Booting PowerShell CIM collector.")

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	type WinMem struct {
		TotalVisibleMemorySize uint64 `json:"TotalVisibleMemorySize"`
		FreePhysicalMemory     uint64 `json:"FreePhysicalMemory"`
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "powershell", "-NoProfile", "-Command",
				"Get-CimInstance Win32_OperatingSystem | Select-Object TotalVisibleMemorySize, FreePhysicalMemory | ConvertTo-Json")
			if err != nil {
				continue
			}

			var mem WinMem
			if err := json.Unmarshal(out, &mem); err != nil {
				log.Printf("[Windows OS Collector] JSON parse error: %v", err)
				continue
			}

			event := core.NewCTDPayload(nodeID, "host-os", "active").
				AddMetric("mem_total_mb", float64(mem.TotalVisibleMemorySize)/1024.0).
				AddMetric("mem_free_mb", float64(mem.FreePhysicalMemory)/1024.0)

			if err := pub.Publish(event); err != nil {
				log.Printf("[Windows OS Collector] Publish failed: %v", err)
			}
		}
	}
}
