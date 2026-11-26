package observers

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

const (
	sysfsDiscoveryTTL = 5 * time.Minute
	minValidTempC     = -40.0
	maxValidTempC     = 150.0
)

// nonHostCoolingDrivers lists hwmon chip names and thermal_zone types belonging to discrete GPUs,
// NVMe drives, Wi-Fi PHYs, or PSUs so their fans/thermals are never misclassified as the host CPU / SoC package.
var nonHostCoolingDrivers = map[string]bool{
	"amdgpu":      true,
	"radeon":      true,
	"nouveau":     true,
	"nvidia":      true,
	"i915":        true,
	"xe":          true,
	"nvme":        true,
	"corsairpsu":  true,
	"drivetemp":   true,
	"iwlwifi_1":   true,
	"mt7915_phy0": true,
}

type fanCandidate struct {
	key      string
	index    int
	rpm      int
	hasPWM   bool
	pwmPct   float64
	priority int
}

type cachedThermalSensor struct {
	tempPath  string
	faultPath string
	prio      int
}

type cachedThermalTopology struct {
	discoveredAt time.Time
	dirty        bool
	sensors      []cachedThermalSensor
}

type cachedFanSensor struct {
	key           string
	index         int
	rawLabel      string
	dedicatedChip bool
	inputPath     string
	pwmPath       string
	hasPWMFile    bool
}

type cachedCoolingDevice struct {
	curStatePath string
	maxStatePath string
}

type cachedCoolingTopology struct {
	discoveredAt   time.Time
	dirty          bool
	fans           []cachedFanSensor
	pwmOnlyPaths   []string
	coolingDevices []cachedCoolingDevice
}

type sysfsDiscoveryCache struct {
	mu       sync.Mutex
	now      func() time.Time
	thermals map[string]*cachedThermalTopology
	coolings map[string]*cachedCoolingTopology
}

func newSysfsDiscoveryCache() *sysfsDiscoveryCache {
	return &sysfsDiscoveryCache{
		now:      time.Now,
		thermals: make(map[string]*cachedThermalTopology),
		coolings: make(map[string]*cachedCoolingTopology),
	}
}

var defaultSysfsCache = newSysfsDiscoveryCache()

func parseFiniteFloat(s string) (float64, bool) {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}

func parseSysfsMillidegreeTemp(raw []byte) (float64, bool) {
	milliC, ok := parseFiniteFloat(string(raw))
	if !ok {
		return 0, false
	}
	tempC := milliC / 1000.0
	if tempC < minValidTempC || tempC > maxValidTempC {
		return 0, false
	}
	rounded := math.Round(tempC*10) / 10
	if rounded == 0 {
		rounded = 0
	}
	return rounded, true
}

func hwmonFaultPathForInput(inputPath string) string {
	base := filepath.Base(inputPath)
	if !strings.HasSuffix(base, "_input") {
		return ""
	}
	return filepath.Join(filepath.Dir(inputPath), strings.TrimSuffix(base, "_input")+"_fault")
}

func isHwmonTempFault(faultPath string) bool {
	if faultPath == "" {
		return false
	}
	raw, err := os.ReadFile(faultPath)
	if err != nil {
		return false
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	return err == nil && v == 1
}

func discoverThermalTopology(sysRoot string, now time.Time) *cachedThermalTopology {
	topo := &cachedThermalTopology{discoveredAt: now}

	// 1. Scan /sys/class/thermal/thermal_zone*
	tzDirs, _ := filepath.Glob(filepath.Join(sysRoot, "class", "thermal", "thermal_zone*"))
	slices.Sort(tzDirs)
	for i, tzDir := range tzDirs {
		typeRaw, _ := os.ReadFile(filepath.Join(tzDir, "type"))
		tzToken := sanitizeSensorToken(string(typeRaw))
		if nonHostCoolingDrivers[tzToken] {
			continue
		}
		tempPath := filepath.Join(tzDir, "temp")
		if !pathExists(tempPath) {
			continue
		}
		tzType := strings.ToLower(strings.TrimSpace(string(typeRaw)))
		prio := 2
		if strings.Contains(tzType, "cpu") || strings.Contains(tzType, "soc") || strings.Contains(tzType, "x86_pkg") || strings.Contains(tzType, "tctl") {
			prio = 4
		} else if i == 0 {
			prio = 3
		}
		topo.sensors = append(topo.sensors, cachedThermalSensor{
			tempPath: tempPath,
			prio:     prio,
		})
	}

	// 2. Scan /sys/class/hwmon/hwmon*/temp*_input (catches StarFive JH7110 sfctemp, Intel coretemp, AMD k10temp)
	hwmonDirs, _ := filepath.Glob(filepath.Join(sysRoot, "class", "hwmon", "hwmon*"))
	slices.Sort(hwmonDirs)
	for _, hwDir := range hwmonDirs {
		chipRaw, _ := os.ReadFile(filepath.Join(hwDir, "name"))
		chip := sanitizeSensorToken(string(chipRaw))
		if nonHostCoolingDrivers[chip] {
			continue
		}
		chipPrio := 1
		switch {
		case chip == "coretemp" || chip == "k10temp" || chip == "zenpower" || chip == "sfctemp" || chip == "pvt" ||
			strings.Contains(chip, "cpu") || strings.Contains(chip, "soc") || strings.Contains(chip, "eswin"):
			chipPrio = 4
		}
		tempInputs, _ := filepath.Glob(filepath.Join(hwDir, "temp*_input"))
		slices.Sort(tempInputs)
		for _, tPath := range tempInputs {
			topo.sensors = append(topo.sensors, cachedThermalSensor{
				tempPath:  tPath,
				faultPath: hwmonFaultPathForInput(tPath),
				prio:      chipPrio,
			})
		}
	}

	return topo
}

func (c *sysfsDiscoveryCache) getThermals(sysRoot string, forceRefresh bool) *cachedThermalTopology {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if !forceRefresh {
		if entry, ok := c.thermals[sysRoot]; ok && !entry.dirty && now.Sub(entry.discoveredAt) < sysfsDiscoveryTTL {
			return entry
		}
	}
	entry := discoverThermalTopology(sysRoot, now)
	c.thermals[sysRoot] = entry
	return entry
}

func (c *sysfsDiscoveryCache) markThermalsDirty(sysRoot string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.thermals[sysRoot]; ok {
		entry.dirty = true
	}
}

func discoverCoolingTopology(sysRoot string, now time.Time) *cachedCoolingTopology {
	topo := &cachedCoolingTopology{discoveredAt: now}

	hwmonDirs, _ := filepath.Glob(filepath.Join(sysRoot, "class", "hwmon", "hwmon*"))
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

			labelBytes, _ := os.ReadFile(filepath.Join(hwDir, "fan"+idxStr+"_label"))
			rawLabel := strings.TrimSpace(string(labelBytes))
			normLabel := sanitizeSensorToken(rawLabel)

			sensorKey := chip + ":fan" + idxStr
			if normLabel != "" {
				sensorKey = chip + ":" + normLabel
			}

			pwmPath := filepath.Join(hwDir, "pwm"+idxStr)
			topo.fans = append(topo.fans, cachedFanSensor{
				key:           sensorKey,
				index:         idx,
				rawLabel:      rawLabel,
				dedicatedChip: dedicatedCoolerChip,
				inputPath:     inputPath,
				pwmPath:       pwmPath,
				hasPWMFile:    pathExists(pwmPath),
			})
		}

		// Handle 2-wire / 3-wire SoC PWM coolers that expose pwm1 without a fan1_input tach wire.
		if len(fanInputs) == 0 && dedicatedCoolerChip {
			pwm1Path := filepath.Join(hwDir, "pwm1")
			if pathExists(pwm1Path) {
				topo.pwmOnlyPaths = append(topo.pwmOnlyPaths, pwm1Path)
			}
		}
	}

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
		curPath := filepath.Join(dir, "cur_state")
		maxPath := filepath.Join(dir, "max_state")
		if pathExists(curPath) && pathExists(maxPath) {
			topo.coolingDevices = append(topo.coolingDevices, cachedCoolingDevice{
				curStatePath: curPath,
				maxStatePath: maxPath,
			})
		}
	}

	return topo
}

func (c *sysfsDiscoveryCache) getCoolings(sysRoot string, forceRefresh bool) *cachedCoolingTopology {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if !forceRefresh {
		if entry, ok := c.coolings[sysRoot]; ok && !entry.dirty && now.Sub(entry.discoveredAt) < sysfsDiscoveryTTL {
			return entry
		}
	}
	entry := discoverCoolingTopology(sysRoot, now)
	c.coolings[sysRoot] = entry
	return entry
}

func (c *sysfsDiscoveryCache) markCoolingsDirty(sysRoot string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.coolings[sysRoot]; ok {
		entry.dirty = true
	}
}

// observeSysfsThermals inspects /sys/class/thermal/thermal_zone* and /sys/class/hwmon/hwmon*/temp*_input
// in-process with zero subprocess forks, emitting CPU/SoC temperatures across RISC-V (StarFive sfctemp, ESWIN),
// ARM64, and x86 (Intel coretemp, AMD k10temp/zenpower).
func observeSysfsThermals(sysRoot string, event *core.CTDPayload) {
	defaultSysfsCache.observeThermals(sysRoot, event)
}

func (c *sysfsDiscoveryCache) observeThermals(sysRoot string, event *core.CTDPayload) {
	if event == nil || sysRoot == "" {
		return
	}

	readPass := func(topo *cachedThermalTopology) (bestTemp float64, bestPrio int, readErr bool) {
		for _, s := range topo.sensors {
			if isHwmonTempFault(s.faultPath) {
				continue
			}
			raw, err := os.ReadFile(s.tempPath)
			if err != nil {
				readErr = true
				continue
			}
			tempC, ok := parseSysfsMillidegreeTemp(raw)
			if !ok {
				continue
			}
			if s.prio > bestPrio || (s.prio == bestPrio && tempC > bestTemp) {
				bestPrio = s.prio
				bestTemp = tempC
			}
		}
		return bestTemp, bestPrio, readErr
	}

	topo := c.getThermals(sysRoot, false)
	bestTemp, bestPrio, readErr := readPass(topo)
	if readErr {
		topo = c.getThermals(sysRoot, true)
		bestTemp, bestPrio, readErr = readPass(topo)
		if readErr {
			c.markThermalsDirty(sysRoot)
		}
	}

	if bestPrio > 0 {
		event.AddMetric("soc_temp_c", bestTemp)
		event.AddMetric("cpu_temp_c", bestTemp)
	}
}

// observeSysfsCooling inspects Linux sysfs (/sys/class/hwmon and /sys/class/thermal)
// in-process with zero subprocess forks, attaching CPU cooler fan speed (cpu_fan_rpm),
// optional PWM duty cycle (cpu_fan_pwm_pct), and active host fan tachometers (fans_rpm).
func observeSysfsCooling(sysRoot string, event *core.CTDPayload) {
	defaultSysfsCache.observeCooling(sysRoot, event)
}

func (c *sysfsDiscoveryCache) observeCooling(sysRoot string, event *core.CTDPayload) {
	if event == nil || sysRoot == "" {
		return
	}

	readPass := func(topo *cachedCoolingTopology) (bestCPU *fanCandidate, bestPWMOnly *float64, fansRPM map[string]int, coolingFallback *float64, readErr bool) {
		fansRPM = make(map[string]int)

		for _, f := range topo.fans {
			rpmBytes, err := os.ReadFile(f.inputPath)
			if err != nil {
				readErr = true
				continue
			}
			rpm, err := strconv.Atoi(strings.TrimSpace(string(rpmBytes)))
			if err != nil || rpm < 0 {
				continue
			}

			var (
				pwmPct float64
				hasPWM bool
			)
			if f.hasPWMFile {
				var pwmReadErr bool
				pwmPct, hasPWM, pwmReadErr = readHwmonPWMPctChecked(f.pwmPath)
				if pwmReadErr {
					readErr = true
				}
			}

			prio := classifyCPUCoolerPriority(f.key, f.rawLabel, f.index, rpm, f.dedicatedChip)
			cand := fanCandidate{
				key:      f.key,
				index:    f.index,
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
			if rpm > 0 || f.rawLabel != "" || f.dedicatedChip {
				fansRPM[f.key] = rpm
			}
		}

		for _, pwmPath := range topo.pwmOnlyPaths {
			pct, ok, pwmReadErr := readHwmonPWMPctChecked(pwmPath)
			if pwmReadErr {
				readErr = true
				continue
			}
			if ok && bestPWMOnly == nil {
				val := pct
				bestPWMOnly = &val
				break
			}
		}

		if bestCPU == nil || !bestCPU.hasPWM {
			if bestPWMOnly == nil {
				for _, cdev := range topo.coolingDevices {
					pct, ok, devReadErr := readCoolingDevicePct(cdev)
					if devReadErr {
						readErr = true
						continue
					}
					if ok {
						val := pct
						coolingFallback = &val
						break
					}
				}
			}
		}

		return bestCPU, bestPWMOnly, fansRPM, coolingFallback, readErr
	}

	topo := c.getCoolings(sysRoot, false)
	bestCPU, bestPWMOnly, fansRPM, coolingFallback, readErr := readPass(topo)
	if readErr {
		topo = c.getCoolings(sysRoot, true)
		bestCPU, bestPWMOnly, fansRPM, coolingFallback, readErr = readPass(topo)
		if readErr {
			c.markCoolingsDirty(sysRoot)
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
	if !hasPWMMetric && coolingFallback != nil {
		event.AddMetric("cpu_fan_pwm_pct", *coolingFallback)
	}
}

func isDedicatedCoolerChip(chip string) bool {
	switch chip {
	case "pwmfan", "pwm_fan", "cooling_fan", "gpio_fan", "rpi_poe_fan", "es_bmcd", "eswin_fan_control", "cpu_fan":
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

func readHwmonPWMPctChecked(pwmPath string) (float64, bool, bool) {
	raw, err := os.ReadFile(pwmPath)
	if err != nil {
		return 0, false, true
	}
	val, ok := parseFiniteFloat(string(raw))
	if !ok || val < 0 {
		return 0, false, false
	}
	pct := (val / 255.0) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}
	return math.Round(pct*10) / 10, true, false
}

func readCoolingDevicePct(cdev cachedCoolingDevice) (float64, bool, bool) {
	curBytes, errCur := os.ReadFile(cdev.curStatePath)
	maxBytes, errMax := os.ReadFile(cdev.maxStatePath)
	if errCur != nil || errMax != nil {
		return 0, false, true
	}

	cur, okCur := parseFiniteFloat(string(curBytes))
	maxVal, okMax := parseFiniteFloat(string(maxBytes))
	if !okCur || !okMax || maxVal <= 0 || cur < 0 {
		return 0, false, false
	}

	pct := (cur / maxVal) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}
	return math.Round(pct*10) / 10, true, false
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
