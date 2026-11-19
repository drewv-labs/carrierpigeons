package observers

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func discoverSysfsAMDCards(sysRoot string) []string {
	cards, _ := filepath.Glob(filepath.Join(sysRoot, "class", "drm", "card*"))
	slices.Sort(cards)
	var out []string
	for _, c := range cards {
		base := filepath.Base(c)
		if strings.Contains(base, "-") {
			continue // skip connector subdirs like card0-DP-1
		}
		vendorRaw, err := os.ReadFile(filepath.Join(c, "device", "vendor"))
		if err != nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(string(vendorRaw)), "0x1002") {
			out = append(out, c)
		}
	}
	return out
}

func hasSysfsAMDGPU(sysRoot string) bool {
	return len(discoverSysfsAMDCards(sysRoot)) > 0
}

func readSysfsFloat(path string) (float64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// observeGPUAMDSysfs reads AMD Radeon / Instinct telemetry directly from Linux `/sys/class/drm/card*/device`
// without spawning Python `rocm-smi` processes.
func observeGPUAMDSysfs(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emit := func() {
		for _, cardPath := range discoverSysfsAMDCards("/sys") {
			cardName := filepath.Base(cardPath)
			devDir := filepath.Join(cardPath, "device")
			ev := core.NewCTDPayload(nodeID, "amd-gpu-"+cardName, "active")
			added := false

			if util, ok := readSysfsFloat(filepath.Join(devDir, "gpu_busy_percent")); ok {
				ev.AddMetric("utilization_pct", util)
				added = true
			}
			if usedB, ok := readSysfsFloat(filepath.Join(devDir, "mem_info_vram_used")); ok {
				ev.AddMetric("memory_used_mb", math.Round((usedB/1024.0/1024.0)*10)/10)
				added = true
			}
			if totalB, ok := readSysfsFloat(filepath.Join(devDir, "mem_info_vram_total")); ok {
				ev.AddMetric("memory_total_mb", math.Round((totalB/1024.0/1024.0)*10)/10)
				added = true
			}

			hwmons, _ := filepath.Glob(filepath.Join(devDir, "hwmon", "hwmon*"))
			if len(hwmons) > 0 {
				hw := hwmons[0]
				if milliC, ok := readSysfsFloat(filepath.Join(hw, "temp1_input")); ok {
					ev.AddMetric("temperature_c", math.Round((milliC/1000.0)*10)/10)
					added = true
				}
				if microW, ok := readSysfsFloat(filepath.Join(hw, "power1_average")); ok {
					ev.AddMetric("power_w", math.Round((microW/1_000_000.0)*10)/10)
					added = true
				} else if microW, ok := readSysfsFloat(filepath.Join(hw, "power1_input")); ok {
					ev.AddMetric("power_w", math.Round((microW/1_000_000.0)*10)/10)
					added = true
				}
				if rpm, ok := readSysfsFloat(filepath.Join(hw, "fan1_input")); ok && rpm >= 0 {
					ev.AddMetric("fan_rpm", int(rpm))
				}
				if pwmPct, ok := readHwmonPWMPct(filepath.Join(hw, "pwm1")); ok {
					ev.AddMetric("fan_speed_pct", pwmPct)
				}
			}

			if added {
				if err := pub.Publish(ev); err != nil {
					log.Printf("[AMD GPU Observer] Publish failed for %s: %v", cardName, err)
				}
			}
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

func observeGPUAMD(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emit := func() {
		out, err := runCommand(ctx, defaultCmdTimeout, "rocm-smi", "--showtemp", "--showuse", "--showfan", "--showmeminfo", "vram", "--json")
		if err != nil {
			log.Printf("[AMD GPU Observer] Sensor timeout: %v", err)
			return
		}

		var amdData map[string]map[string]any
		if err := json.Unmarshal(out, &amdData); err != nil {
			log.Printf("[AMD GPU Observer] Failed to parse JSON: %v", err)
			return
		}

		for cardName, metrics := range amdData {
			if !strings.HasPrefix(strings.ToLower(cardName), "card") {
				continue
			}

			tempStr, _ := metrics["Temperature (Sensor edge) (C)"].(string)
			utilStr, _ := metrics["GPU use (%)"].(string)
			memStr, _ := metrics["VRAM Total Used Memory (B)"].(string)

			temp, _ := strconv.ParseFloat(tempStr, 64)
			util, _ := strconv.ParseFloat(utilStr, 64)
			memBytes, _ := strconv.ParseFloat(memStr, 64)

			event := core.NewCTDPayload(nodeID, "amd-gpu-"+cardName, "active").
				AddMetric("temperature_c", temp).
				AddMetric("utilization_pct", util).
				AddMetric("memory_used_mb", memBytes/1024.0/1024.0)

			if fanRPMStr, ok := metrics["Fan RPM"].(string); ok {
				if fanRPM, err := strconv.Atoi(strings.TrimSpace(fanRPMStr)); err == nil && fanRPM >= 0 {
					event.AddMetric("fan_rpm", fanRPM)
				}
			}
			if fanPctStr, ok := metrics["Fan speed (%)"].(string); ok {
				if fanPct, err := strconv.ParseFloat(strings.TrimSpace(fanPctStr), 64); err == nil && fanPct >= 0 {
					event.AddMetric("fan_speed_pct", fanPct)
				}
			}

			if err := pub.Publish(event); err != nil {
				log.Printf("[AMD GPU Observer] Publish failed for %s: %v", cardName, err)
			}
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
