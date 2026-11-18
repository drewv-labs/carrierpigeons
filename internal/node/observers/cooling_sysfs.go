package observers

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// nonHostCoolingDrivers lists hwmon chip names belonging to discrete GPUs, NVMe drives,
// or PSUs so their fans are never misclassified as the host CPU / SoC cooler fan.
var nonHostCoolingDrivers = map[string]bool{
	"amdgpu":     true,
	"radeon":     true,
	"nouveau":    true,
	"nvidia":     true,
	"i915":       true,
	"xe":         true,
	"nvme":       true,
	"corsairpsu": true,
}

type fanCandidate struct {
	key      string
	index    int
	rpm      int
	hasPWM   bool
	pwmPct   float64
	priority int
}

// observeSysfsCooling inspects Linux sysfs (/sys/class/hwmon and /sys/class/thermal)
// in-process with zero subprocess forks, attaching CPU cooler fan speed (cpu_fan_rpm),
// optional PWM duty cycle (cpu_fan_pwm_pct), and active host fan tachometers (fans_rpm).
func observeSysfsCooling(sysRoot string, event *core.CTDPayload) {
	if event == nil || sysRoot == "" {
		return
	}

	var (
		bestCPU     *fanCandidate
		bestPWMOnly *float64
		fansRPM     = make(map[string]int)
	)

	hwmonGlob := filepath.Join(sysRoot, "class", "hwmon", "hwmon*")
	hwmonDirs, _ := filepath.Glob(hwmonGlob)
	slices.Sort(hwmonDirs)

	for _, hwDir := range hwmonDirs {
		chipRaw, _ := os.ReadFile(filepath.Join(hwDir, "name"))
		chip := sanitizeSensorToken(string(chipRaw))
		if chip == "" {
			chip = sanitizeSensorToken(filepath.Base(hwDir))
		}
		if nonHostCoolingDrivers[chip] {
			continue
		}

		dedicatedCoolerChip := isDedicatedCoolerChip(chip)

		fanInputs, _ := filepath.Glob(filepath.Join(hwDir, "fan*_input"))
		slices.Sort(fanInputs)

		for _, inputPath := range fanInputs {
			base := filepath.Base(inputPath) // e.g., fan1_input
			idxStr := strings.TrimSuffix(strings.TrimPrefix(base, "fan"), "_input")
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				continue
			}

			rpmBytes, err := os.ReadFile(inputPath)
			if err != nil {
				continue
			}
			rpm, err := strconv.Atoi(strings.TrimSpace(string(rpmBytes)))
			if err != nil || rpm < 0 {
				continue
			}

			labelBytes, _ := os.ReadFile(filepath.Join(hwDir, "fan"+idxStr+"_label"))
			rawLabel := strings.TrimSpace(string(labelBytes))
			normLabel := sanitizeSensorToken(rawLabel)

			sensorKey := chip + ":fan" + idxStr
			if normLabel != "" {
				sensorKey = chip + ":" + normLabel
			}

			pwmPct, hasPWM := readHwmonPWMPct(filepath.Join(hwDir, "pwm"+idxStr))

			prio := classifyCPUCoolerPriority(chip, rawLabel, idx, rpm, dedicatedCoolerChip)
			cand := fanCandidate{
				key:      sensorKey,
				index:    idx,
				rpm:      rpm,
				hasPWM:   hasPWM,
				pwmPct:   pwmPct,
				priority: prio,
			}

			if prio > 0 && isBetterCPUCandidate(&cand, bestCPU) {
				cp := cand
				bestCPU = &cp
			}

			// Record active, labeled, or dedicated controller fans while skipping
			// unconnected 0-RPM Super I/O headers.
			if rpm > 0 || rawLabel != "" || dedicatedCoolerChip {
				fansRPM[sensorKey] = rpm
			}
		}

		// Handle 2-wire / 3-wire SoC PWM coolers that expose pwm1 without a fan1_input tach wire.
		if len(fanInputs) == 0 && dedicatedCoolerChip && bestPWMOnly == nil {
			if pwmPct, ok := readHwmonPWMPct(filepath.Join(hwDir, "pwm1")); ok {
				bestPWMOnly = &pwmPct
			}
		}
	}

	hasPWMMetric := false
	if bestCPU != nil {
		event.AddMetric("cpu_fan_rpm", bestCPU.rpm)
		fansRPM[bestCPU.key] = bestCPU.rpm
		if bestCPU.hasPWM {
			event.AddMetric("cpu_fan_pwm_pct", bestCPU.pwmPct)
			hasPWMMetric = true
		}
	} else if bestPWMOnly != nil {
		event.AddMetric("cpu_fan_pwm_pct", *bestPWMOnly)
		hasPWMMetric = true
	}

	if len(fansRPM) > 0 {
		event.AddMetric("fans_rpm", fansRPM)
	}

	// Fallback to /sys/class/thermal/cooling_device* for kernel-governed PWM/GPIO fans
	// when no hwmon PWM channel reported duty cycle.
	if !hasPWMMetric {
		if pct, ok := readThermalCoolingFanPct(sysRoot); ok {
			event.AddMetric("cpu_fan_pwm_pct", pct)
		}
	}
}

func isDedicatedCoolerChip(chip string) bool {
	switch chip {
	case "pwmfan", "pwm_fan", "cooling_fan", "gpio_fan", "rpi_poe_fan", "es_bmcd", "cpu_fan":
		return true
	default:
		return strings.Contains(chip, "pwmfan") || strings.Contains(chip, "pwm_fan") || strings.Contains(chip, "cpufan")
	}
}

func classifyCPUCoolerPriority(chip, rawLabel string, idx, rpm int, dedicatedChip bool) int {
	lbl := strings.ToLower(strings.TrimSpace(rawLabel))

	// Explicitly exclude non-CPU fans (chassis, case, PSU, chipset/PCH, VRM, aux, GPU)
	for _, nonCPU := range []string{"chassis", "case", "sys", "rear", "front", "intake", "exhaust", "aux", "pch", "chipset", "vrm", "psu", "gpu"} {
		if strings.Contains(lbl, nonCPU) {
			return 0
		}
	}

	// Tier 4: Explicit primary CPU / SoC / SOM heatsink cooler label
	for _, cpuTok := range []string{"cpu", "proc", "som", "soc", "heatsink", "cooler"} {
		if strings.Contains(lbl, cpuTok) {
			if strings.Contains(lbl, "opt") || strings.Contains(lbl, "pump") {
				return 3
			}
			return 4
		}
	}

	// Tier 3: AIO liquid cooler pump or dedicated SoC PWM/BMC fan controller
	if strings.Contains(lbl, "aio") || strings.Contains(lbl, "pump") || dedicatedChip {
		return 3
	}

	// Tier 2: Unlabeled spinning header (fan1 preferred via index tie-breaker, matching ATX CPU_FAN = fan1)
	if rpm > 0 {
		return 2
	}

	// Tier 1: Unlabeled primary fan1 header spun down to 0 RPM (semi-passive idle)
	if idx == 1 && chip != "" {
		return 1
	}

	return 0
}

func isBetterCPUCandidate(cand, cur *fanCandidate) bool {
	if cur == nil {
		return true
	}
	if cand.priority != cur.priority {
		return cand.priority > cur.priority
	}
	// Within the same priority tier, prefer an actively spinning fan over a 0-RPM header
	if (cand.rpm > 0) != (cur.rpm > 0) {
		return cand.rpm > 0
	}
	// Prefer lower fan channel index (e.g., fan1 before fan2)
	return cand.index < cur.index
}

func readHwmonPWMPct(pwmPath string) (float64, bool) {
	raw, err := os.ReadFile(pwmPath)
	if err != nil {
		return 0, false
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
	if err != nil || val < 0 {
		return 0, false
	}
	pct := (val / 255.0) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}
	return math.Round(pct*10) / 10, true
}

func readThermalCoolingFanPct(sysRoot string) (float64, bool) {
	cdevs, _ := filepath.Glob(filepath.Join(sysRoot, "class", "thermal", "cooling_device*"))
	slices.Sort(cdevs)

	for _, dir := range cdevs {
		typeBytes, err := os.ReadFile(filepath.Join(dir, "type"))
		if err != nil {
			continue
		}
		devType := strings.ToLower(strings.TrimSpace(string(typeBytes)))
		if !strings.Contains(devType, "fan") {
			continue
		}

		curBytes, errCur := os.ReadFile(filepath.Join(dir, "cur_state"))
		maxBytes, errMax := os.ReadFile(filepath.Join(dir, "max_state"))
		if errCur != nil || errMax != nil {
			continue
		}

		cur, errCur := strconv.ParseFloat(strings.TrimSpace(string(curBytes)), 64)
		max, errMax := strconv.ParseFloat(strings.TrimSpace(string(maxBytes)), 64)
		if errCur != nil || errMax != nil || max <= 0 || cur < 0 {
			continue
		}

		pct := (cur / max) * 100.0
		if pct > 100.0 {
			pct = 100.0
		}
		return math.Round(pct*10) / 10, true
	}

	return 0, false
}

func sanitizeSensorToken(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return ""
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
		} else if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
