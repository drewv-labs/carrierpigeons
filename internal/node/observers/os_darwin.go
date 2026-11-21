//go:build darwin

package observers

import (
	"context"
	"encoding/binary"
	"log"
	"math"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"golang.org/x/sys/unix"
)

var (
	darwinSMCFanRe = regexp.MustCompile(`(?i)Fan(?:\s*(\d+))?\s*:\s*([\d\.]+)\s*rpm`)
	darwinTempRe   = regexp.MustCompile(`(?i)(?:CPU die temperature|SMC.*temp).*?:\s*([\d\.]+)\s*C`)

	darwinPMCacheMu sync.Mutex
	darwinPMCacheAt time.Time
	darwinPMCache   string
)

// sampleDarwinPowermetrics consolidates SMC, GPU, and ANE `powermetrics` queries into a single cached subprocess call per interval.
func sampleDarwinPowermetrics(ctx context.Context) (string, error) {
	ttl := min(pollInterval(ctx)/2, 2*time.Second)
	if ttl < 500*time.Millisecond {
		ttl = 500 * time.Millisecond
	}

	darwinPMCacheMu.Lock()
	if darwinPMCache != "" && time.Since(darwinPMCacheAt) < ttl {
		cached := darwinPMCache
		darwinPMCacheMu.Unlock()
		return cached, nil
	}
	darwinPMCacheMu.Unlock()

	samplers := "smc,gpu_power"
	if runtime.GOARCH == "arm64" {
		samplers = "smc,gpu_power,ane_power"
	}
	out, err := runCommand(ctx, defaultCmdTimeout, "powermetrics", "--samplers", samplers, "-n", "1", "-i", "100")
	if err != nil {
		return "", err
	}

	s := string(out)
	darwinPMCacheMu.Lock()
	darwinPMCache = s
	darwinPMCacheAt = time.Now()
	darwinPMCacheMu.Unlock()
	return s, nil
}

func readDarwinLoadAvg() (load1, load5, load15 float64, ok bool) {
	raw, err := unix.SysctlRaw("vm.loadavg")
	if err != nil || len(raw) < 24 {
		return 0, 0, 0, false
	}
	l1 := binary.LittleEndian.Uint32(raw[0:4])
	l5 := binary.LittleEndian.Uint32(raw[4:8])
	l15 := binary.LittleEndian.Uint32(raw[8:12])
	scale := int64(binary.LittleEndian.Uint64(raw[16:24]))
	if scale <= 0 {
		scale = 2048
	}
	fScale := float64(scale)
	return math.Round((float64(l1)/fScale)*100) / 100,
		math.Round((float64(l5)/fScale)*100) / 100,
		math.Round((float64(l15)/fScale)*100) / 100,
		true
}

func readDarwinAvailableMemoryMB() (float64, bool) {
	pageSize, err := unix.SysctlUint32("hw.pagesize")
	if err != nil || pageSize == 0 {
		pageSize = 4096
	}
	freePages, err := unix.SysctlUint32("vm.page_free_count")
	if err != nil {
		return 0, false
	}
	specPages, _ := unix.SysctlUint32("vm.page_speculative_count")
	availBytes := (uint64(freePages) + uint64(specPages)) * uint64(pageSize)
	return math.Round((float64(availBytes)/1024.0/1024.0)*10) / 10, true
}

func observeOSPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[OS Observer] macOS/Darwin host detected (zero-fork sysctl observer).")

	isRoot := os.Geteuid() == 0
	var memTotalMB float64
	if memBytes, err := unix.SysctlUint64("hw.memsize"); err == nil && memBytes > 0 {
		memTotalMB = math.Round((float64(memBytes)/1024.0/1024.0)*10) / 10
	}

	runPeriodicObserver(ctx, true, func() {
		event := core.NewCTDPayload(nodeID, "host-os", "active")
		if memTotalMB > 0 {
			event.AddMetric("mem_total_mb", memTotalMB)
			if memAvailMB, ok := readDarwinAvailableMemoryMB(); ok {
				if memAvailMB > memTotalMB {
					memAvailMB = memTotalMB
				}
				memUsedMB := math.Round((memTotalMB-memAvailMB)*10) / 10
				memUsedPct := math.Round(((memTotalMB-memAvailMB)/memTotalMB)*1000) / 10
				event.AddMetric("mem_available_mb", memAvailMB)
				event.AddMetric("mem_used_mb", memUsedMB)
				event.AddMetric("mem_used_pct", memUsedPct)
			}
		}
		if tv, err := unix.SysctlTimeval("kern.boottime"); err == nil && tv.Sec > 0 {
			uptime := time.Since(time.Unix(tv.Sec, int64(tv.Usec)*1000)).Seconds()
			if uptime > 0 {
				event.AddMetric("uptime_s", math.Round(uptime))
			}
		}
		if l1, l5, l15, ok := readDarwinLoadAvg(); ok {
			event.AddMetric("load_1m", l1)
			event.AddMetric("load_5m", l5)
			event.AddMetric("load_15m", l15)
		}

		if isRoot {
			if smcOut, err := sampleDarwinPowermetrics(ctx); err == nil {
				matches := darwinSMCFanRe.FindAllStringSubmatch(smcOut, -1)
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
				if tMatch := darwinTempRe.FindStringSubmatch(smcOut); len(tMatch) > 1 {
					if tC, err := strconv.ParseFloat(tMatch[1], 64); err == nil && tC > 0 {
						event.AddMetric("cpu_temp_c", tC)
					}
				}
			}
		}

		if err := pub.Publish(event); err != nil {
			log.Printf("[OS Observer] Publish failed: %v", err)
		}
	})
}
