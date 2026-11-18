//go:build windows

package observers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[OS Observer] Windows host detected. Booting PowerShell CIM observer.")

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	type WinHostSnapshot struct {
		TotalVisibleMemorySize uint64  `json:"TotalVisibleMemorySize"`
		FreePhysicalMemory     uint64  `json:"FreePhysicalMemory"`
		CPUFanRPM              *uint64 `json:"CPUFanRPM"`
	}

	const cimScript = `
$os = Get-CimInstance Win32_OperatingSystem | Select-Object TotalVisibleMemorySize, FreePhysicalMemory
$fan = Get-CimInstance Win32_Fan -ErrorAction SilentlyContinue | Where-Object { $_.DesiredSpeed -ne $null } | Select-Object -First 1
[PSCustomObject]@{
  TotalVisibleMemorySize = $os.TotalVisibleMemorySize
  FreePhysicalMemory     = $os.FreePhysicalMemory
  CPUFanRPM              = if ($fan) { [uint64]$fan.DesiredSpeed } else { $null }
} | ConvertTo-Json -Compress`

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "powershell", "-NoProfile", "-Command", cimScript)
			if err != nil {
				continue
			}

			var snap WinHostSnapshot
			if err := json.Unmarshal(out, &snap); err != nil {
				log.Printf("[Windows OS Observer] JSON parse error: %v", err)
				continue
			}

			event := core.NewCTDPayload(nodeID, "host-os", "active").
				AddMetric("mem_total_mb", float64(snap.TotalVisibleMemorySize)/1024.0).
				AddMetric("mem_free_mb", float64(snap.FreePhysicalMemory)/1024.0)

			if snap.CPUFanRPM != nil {
				event.AddMetric("cpu_fan_rpm", int(*snap.CPUFanRPM))
			}

			if err := pub.Publish(event); err != nil {
				log.Printf("[Windows OS Observer] Publish failed: %v", err)
			}
		}
	}
}
