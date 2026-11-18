//go:build windows

package observers

import (
	"context"
	"log"
	"math"
	"syscall"
	"time"
	"unsafe"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var (
	modKernel32              = syscall.NewLazyDLL("kernel32.dll")
	procGlobalMemoryStatusEx = modKernel32.NewProc("GlobalMemoryStatusEx")
	procGetTickCount64       = modKernel32.NewProc("GetTickCount64")
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// observeOSPlatform queries Win32 kernel32.dll directly in-process (`CGO_ENABLED=0`) without spawning PowerShell.
func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[OS Observer] Windows host detected (zero-fork Win32 GlobalMemoryStatusEx observer).")

	emit := func() {
		var mem memoryStatusEx
		mem.Length = uint32(unsafe.Sizeof(mem))
		r1, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&mem)))
		if r1 == 0 {
			return
		}

		totalMB := math.Round((float64(mem.TotalPhys)/1024.0/1024.0)*10) / 10
		availMB := math.Round((float64(mem.AvailPhys)/1024.0/1024.0)*10) / 10
		usedMB := math.Round((totalMB-availMB)*10) / 10

		event := core.NewCTDPayload(nodeID, "host-os", "active").
			AddMetric("mem_total_mb", totalMB).
			AddMetric("mem_available_mb", availMB).
			AddMetric("mem_free_mb", availMB).
			AddMetric("mem_used_mb", usedMB).
			AddMetric("memory_load_pct", float64(mem.MemoryLoad))

		if tickMs, _, _ := procGetTickCount64.Call(); tickMs > 0 {
			event.AddMetric("uptime_s", math.Round(float64(tickMs)/1000.0))
		}

		if err := pub.Publish(event); err != nil {
			log.Printf("[Windows OS Observer] Publish failed: %v", err)
		}
	}

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
