package observers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

type cachedAMDGPUCard struct {
	name           string
	devDir         string
	hwmonDir       string
	hasBusy        bool
	hasVRAMUsed    bool
	hasVRAMTotal   bool
	hasTemp1       bool
	temp1FaultPath string
	hasPowerAvg    bool
	hasPowerInput  bool
	hasFan1        bool
	hasPWM1        bool
}

type amdSysfsCache struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]*amdSysfsEntry
}

type amdSysfsEntry struct {
	discoveredAt time.Time
	dirty        bool
	cards        []cachedAMDGPUCard
}

func newAMDSysfsCache() *amdSysfsCache {
	return &amdSysfsCache{
		now:     time.Now,
		entries: make(map[string]*amdSysfsEntry),
	}
}

var defaultAMDSysfsCache = newAMDSysfsCache()

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

func readSysfsFloatChecked(path string) (float64, bool, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false, errors.Is(err, os.ErrNotExist)
	}
	v, ok := parseFiniteFloat(string(b))
	if !ok {
		return 0, false, false
	}
	return v, true, false
}

func discoverCachedAMDCards(sysRoot string) []cachedAMDGPUCard {
	cardPaths := discoverSysfsAMDCards(sysRoot)
	cards := make([]cachedAMDGPUCard, 0, len(cardPaths))
	for _, cardPath := range cardPaths {
		cardName := filepath.Base(cardPath)
		if core.ValidateTopicToken("amd-gpu-"+cardName) != nil {
			continue
		}
		devDir := filepath.Join(cardPath, "device")
		card := cachedAMDGPUCard{
			name:         cardName,
			devDir:       devDir,
			hasBusy:      pathExists(filepath.Join(devDir, "gpu_busy_percent")),
			hasVRAMUsed:  pathExists(filepath.Join(devDir, "mem_info_vram_used")),
			hasVRAMTotal: pathExists(filepath.Join(devDir, "mem_info_vram_total")),
		}
		hwmons, _ := filepath.Glob(filepath.Join(devDir, "hwmon", "hwmon*"))
		slices.Sort(hwmons)
		if len(hwmons) > 0 {
			hw := hwmons[0]
			card.hwmonDir = hw
			temp1Path := filepath.Join(hw, "temp1_input")
			card.hasTemp1 = pathExists(temp1Path)
			card.temp1FaultPath = hwmonFaultPathForInput(temp1Path)
			card.hasPowerAvg = pathExists(filepath.Join(hw, "power1_average"))
			card.hasPowerInput = pathExists(filepath.Join(hw, "power1_input"))
			card.hasFan1 = pathExists(filepath.Join(hw, "fan1_input"))
			card.hasPWM1 = pathExists(filepath.Join(hw, "pwm1"))
		}
		cards = append(cards, card)
	}
	return cards
}

func (c *amdSysfsCache) getCards(sysRoot string, forceRefresh bool) ([]cachedAMDGPUCard, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if !forceRefresh {
		if entry, ok := c.entries[sysRoot]; ok && !entry.dirty && now.Sub(entry.discoveredAt) < sysfsDiscoveryTTL {
			return entry.cards, false
		}
	}
	cards := discoverCachedAMDCards(sysRoot)
	c.entries[sysRoot] = &amdSysfsEntry{
		discoveredAt: now,
		cards:        cards,
	}
	return cards, true
}

func (c *amdSysfsCache) markDirty(sysRoot string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[sysRoot]; ok {
		entry.dirty = true
	}
}

func (c *amdSysfsCache) collectSamples(sysRoot, nodeID string) ([]*core.CTDPayload, bool) {
	readPass := func(cards []cachedAMDGPUCard) ([]*core.CTDPayload, bool) {
		var payloads []*core.CTDPayload
		missingErr := false

		for _, card := range cards {
			ev := core.NewCTDPayload(nodeID, "amd-gpu-"+card.name, "active")

			if card.hasBusy {
				if util, ok, rMissing := readSysfsFloatChecked(filepath.Join(card.devDir, "gpu_busy_percent")); rMissing {
					missingErr = true
				} else if ok {
					ev.AddMetric("utilization_pct", util)
				}
			}
			if card.hasVRAMUsed {
				if usedB, ok, rMissing := readSysfsFloatChecked(filepath.Join(card.devDir, "mem_info_vram_used")); rMissing {
					missingErr = true
				} else if ok {
					ev.AddMetric("memory_used_mb", math.Round((usedB/1024.0/1024.0)*10)/10)
				}
			}
			if card.hasVRAMTotal {
				if totalB, ok, rMissing := readSysfsFloatChecked(filepath.Join(card.devDir, "mem_info_vram_total")); rMissing {
					missingErr = true
				} else if ok {
					ev.AddMetric("memory_total_mb", math.Round((totalB/1024.0/1024.0)*10)/10)
				}
			}

			if card.hwmonDir != "" {
				if card.hasTemp1 && !isHwmonTempFault(card.temp1FaultPath) {
					raw, err := os.ReadFile(filepath.Join(card.hwmonDir, "temp1_input"))
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							missingErr = true
						}
					} else if tempC, ok := parseSysfsMillidegreeTemp(raw); ok {
						ev.AddMetric("temperature_c", tempC)
					}
				}

				powerAdded := false
				if card.hasPowerAvg {
					if microW, ok, rMissing := readSysfsFloatChecked(filepath.Join(card.hwmonDir, "power1_average")); rMissing {
						missingErr = true
					} else if ok {
						ev.AddMetric("power_w", math.Round((microW/1_000_000.0)*10)/10)
						powerAdded = true
					}
				}
				if !powerAdded && card.hasPowerInput {
					if microW, ok, rMissing := readSysfsFloatChecked(filepath.Join(card.hwmonDir, "power1_input")); rMissing {
						missingErr = true
					} else if ok {
						ev.AddMetric("power_w", math.Round((microW/1_000_000.0)*10)/10)
					}
				}

				if card.hasFan1 {
					if rpm, ok, rMissing := readSysfsFloatChecked(filepath.Join(card.hwmonDir, "fan1_input")); rMissing {
						missingErr = true
					} else if ok && rpm >= 0 {
						ev.AddMetric("fan_rpm", int(rpm))
					}
				}
				if card.hasPWM1 {
					if pwmPct, ok, rMissing := readHwmonPWMPctChecked(filepath.Join(card.hwmonDir, "pwm1")); rMissing {
						missingErr = true
					} else if ok {
						ev.AddMetric("fan_speed_pct", pwmPct)
					}
				}
			}

			if len(ev.Metrics) > 0 {
				payloads = append(payloads, ev)
			}
		}

		return payloads, missingErr
	}

	cards, refreshed := c.getCards(sysRoot, false)
	payloads, missingErr := readPass(cards)
	if missingErr && !refreshed {
		cards, _ = c.getCards(sysRoot, true)
		payloads, missingErr = readPass(cards)
	}
	if missingErr {
		c.markDirty(sysRoot)
	}
	return payloads, missingErr
}

// observeGPUAMDSysfs reads AMD Radeon / Instinct telemetry directly from Linux `/sys/class/drm/card*/device`
// without spawning Python `rocm-smi` processes.
func observeGPUAMDSysfs(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeGPUAMDSysfsWithRoot(ctx, pub, nodeID, "/sys", defaultAMDSysfsCache)
}

func observeGPUAMDSysfsWithRoot(ctx context.Context, pub core.TelemetryPublisher, nodeID, sysRoot string, cache *amdSysfsCache) {
	if cache == nil {
		cache = defaultAMDSysfsCache
	}
	runPeriodicObserver(ctx, true, func() {
		payloads, _ := cache.collectSamples(sysRoot, nodeID)
		for _, ev := range payloads {
			if err := pub.Publish(ev); err != nil {
				log.Printf("[AMD GPU Observer] Publish failed for %s: %v", ev.Component, err)
			}
		}
	})
}

func parseJSONMetricFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case string:
		return parseFiniteFloat(val)
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return 0, false
		}
		return val, true
	default:
		return 0, false
	}
}

func parseROCmSMIOutput(raw []byte, nodeID string) ([]*core.CTDPayload, error) {
	var amdData map[string]map[string]any
	if err := json.Unmarshal(raw, &amdData); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(amdData))
	for k := range amdData {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var events []*core.CTDPayload
	for _, cardName := range keys {
		if !strings.HasPrefix(strings.ToLower(cardName), "card") {
			continue
		}
		componentID := "amd-gpu-" + cardName
		if core.ValidateTopicToken(componentID) != nil {
			continue
		}

		metrics := amdData[cardName]
		event := core.NewCTDPayload(nodeID, componentID, "active")

		if temp, ok := parseJSONMetricFloat(metrics["Temperature (Sensor edge) (C)"]); ok {
			event.AddMetric("temperature_c", temp)
		}
		if util, ok := parseJSONMetricFloat(metrics["GPU use (%)"]); ok {
			event.AddMetric("utilization_pct", util)
		}
		if memBytes, ok := parseJSONMetricFloat(metrics["VRAM Total Used Memory (B)"]); ok && memBytes >= 0 {
			event.AddMetric("memory_used_mb", memBytes/1024.0/1024.0)
		}
		if fanRPM, ok := parseJSONMetricFloat(metrics["Fan RPM"]); ok && fanRPM >= 0 {
			event.AddMetric("fan_rpm", int(math.Round(fanRPM)))
		}
		if fanPct, ok := parseJSONMetricFloat(metrics["Fan speed (%)"]); ok && fanPct >= 0 {
			event.AddMetric("fan_speed_pct", fanPct)
		}

		if len(event.Metrics) == 0 {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

func observeGPUAMD(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeGPUAMDWithBin(ctx, pub, nodeID, "rocm-smi")
}

func observeGPUAMDWithBin(ctx context.Context, pub core.TelemetryPublisher, nodeID, rocmBin string) {
	if strings.TrimSpace(rocmBin) == "" {
		rocmBin = "rocm-smi"
	}
	runPeriodicObserver(ctx, true, func() {
		cmdCtx, cancelCmd := observer.ContextUntilPaused(ctx)
		out, err := runCommand(cmdCtx, defaultCmdTimeout, rocmBin, "--showtemp", "--showuse", "--showfan", "--showmeminfo", "vram", "--json")
		cancelCmd()
		if err != nil {
			log.Printf("[AMD GPU Observer] Sensor timeout: %v", err)
			return
		}

		events, err := parseROCmSMIOutput(out, nodeID)
		if err != nil {
			log.Printf("[AMD GPU Observer] Failed to parse JSON: %v", err)
			return
		}

		for _, event := range events {
			if err := pub.Publish(event); err != nil {
				log.Printf("[AMD GPU Observer] Publish failed for %s: %v", event.Component, err)
			}
		}
	})
}
