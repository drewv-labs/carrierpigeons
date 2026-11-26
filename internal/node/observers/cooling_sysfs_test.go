package observers

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func writeSysfsFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", full, err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
}

func TestObserveSysfsCoolingSoCPWMFan(t *testing.T) {
	sysRoot := t.TempDir()
	// Simulate RISC-V SBC / SiFive P550 SOM PWM fan controller
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/name", "pwmfan\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_input", "3120\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/pwm1", "128\n")

	ev := core.NewCTDPayload("rv64-coop-01", "host-os", "active")
	observeSysfsCooling(sysRoot, ev)

	if got, ok := ev.Metrics["cpu_fan_rpm"].(int); !ok || got != 3120 {
		t.Fatalf("expected cpu_fan_rpm=3120, got %v", ev.Metrics["cpu_fan_rpm"])
	}
	if got, ok := ev.Metrics["cpu_fan_pwm_pct"].(float64); !ok || got != 50.2 {
		t.Fatalf("expected cpu_fan_pwm_pct=50.2, got %v", ev.Metrics["cpu_fan_pwm_pct"])
	}
	fans, ok := ev.Metrics["fans_rpm"].(map[string]int)
	if !ok || fans["pwmfan:fan1"] != 3120 {
		t.Fatalf("expected fans_rpm[pwmfan:fan1]=3120, got %+v", ev.Metrics["fans_rpm"])
	}
}

func TestObserveSysfsCoolingDistinguishesCPUFromChassisAndGPU(t *testing.T) {
	sysRoot := t.TempDir()

	// Discrete AMD GPU fan (must be ignored by host CPU cooler observer)
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/name", "amdgpu\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_input", "2900\n")

	// Super I/O chip where fan1 is Chassis Intake, fan2 is CPU Cooler, and fan3 is unconnected (0 RPM)
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/name", "nct6775\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/fan1_label", "Chassis Fan 1\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/fan1_input", "950\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/fan2_label", "CPU Cooler Fan\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/fan2_input", "1640\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/pwm2", "255\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/fan3_input", "0\n")

	ev := core.NewCTDPayload("rv64-compute-01", "host-os", "active")
	observeSysfsCooling(sysRoot, ev)

	if got := ev.Metrics["cpu_fan_rpm"]; got != 1640 {
		t.Fatalf("expected cpu_fan_rpm=1640 (CPU Cooler Fan), got %v", got)
	}
	if got := ev.Metrics["cpu_fan_pwm_pct"]; got != 100.0 {
		t.Fatalf("expected cpu_fan_pwm_pct=100.0, got %v", got)
	}

	fans, ok := ev.Metrics["fans_rpm"].(map[string]int)
	if !ok {
		t.Fatalf("expected fans_rpm map, got %T", ev.Metrics["fans_rpm"])
	}
	if fans["nct6775:cpu_cooler_fan"] != 1640 || fans["nct6775:chassis_fan_1"] != 950 {
		t.Fatalf("unexpected fans_rpm entries: %+v", fans)
	}
	if _, exists := fans["nct6775:fan3"]; exists {
		t.Fatalf("unconnected 0-RPM unlabeled header should be omitted, got %+v", fans)
	}
	if _, exists := fans["amdgpu:fan1"]; exists {
		t.Fatalf("discrete GPU hwmon should be excluded from host fans_rpm, got %+v", fans)
	}
}

func TestObserveSysfsCoolingThermalDeviceFallback(t *testing.T) {
	sysRoot := t.TempDir()
	// 2-wire / 3-wire PWM fan without tachometer exposed via /sys/class/thermal/cooling_device0
	writeSysfsFile(t, sysRoot, "class/thermal/cooling_device0/type", "pwm-fan\n")
	writeSysfsFile(t, sysRoot, "class/thermal/cooling_device0/cur_state", "3\n")
	writeSysfsFile(t, sysRoot, "class/thermal/cooling_device0/max_state", "4\n")

	ev := core.NewCTDPayload("rv64-edge-01", "host-os", "active")
	observeSysfsCooling(sysRoot, ev)

	if _, hasRPM := ev.Metrics["cpu_fan_rpm"]; hasRPM {
		t.Fatalf("did not expect cpu_fan_rpm without tachometer, got %v", ev.Metrics["cpu_fan_rpm"])
	}
	if got := ev.Metrics["cpu_fan_pwm_pct"]; got != 75.0 {
		t.Fatalf("expected cpu_fan_pwm_pct=75.0 from cooling_device0, got %v", got)
	}
}

func TestObserveSysfsThermalsAndDiscoverSysfsAMDCards(t *testing.T) {
	sysRoot := t.TempDir()
	// Simulate JH7110 sfctemp hwmon without thermal_zone0
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/name", "sfctemp\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp1_input", "51250\n")
	// Discrete GPU hwmon should be ignored by host CPU thermal observer
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/name", "amdgpu\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/temp1_input", "88000\n")

	ev := core.NewCTDPayload("rv64-edge-02", "host-os", "active")
	observeSysfsThermals(sysRoot, ev)

	if got := ev.Metrics["cpu_temp_c"]; got != 51.3 {
		t.Fatalf("expected cpu_temp_c=51.3 from sfctemp hwmon, got %v", got)
	}
	if got := ev.Metrics["soc_temp_c"]; got != 51.3 {
		t.Fatalf("expected soc_temp_c=51.3 from sfctemp hwmon, got %v", got)
	}

	// Simulate DRM card discovery
	writeSysfsFile(t, sysRoot, "class/drm/card0/device/vendor", "0x1002\n")
	writeSysfsFile(t, sysRoot, "class/drm/card0-DP-1/status", "connected\n")
	writeSysfsFile(t, sysRoot, "class/drm/card1/device/vendor", "0x10de\n")

	cards := discoverSysfsAMDCards(sysRoot)
	if len(cards) != 1 || filepath.Base(cards[0]) != "card0" {
		t.Fatalf("expected only AMD card0 to be discovered, got %v", cards)
	}
}

func TestObserveSysfsThermalsMillidegreeScalingAndBounds(t *testing.T) {
	cases := []struct {
		name     string
		rawTemp  string
		wantOK   bool
		wantTemp float64
	}{
		{name: "near_zero_millidegrees", rawTemp: "150\n", wantOK: true, wantTemp: 0.2},
		{name: "exact_zero_c", rawTemp: "0\n", wantOK: true, wantTemp: 0.0},
		{name: "subzero_cold_soak", rawTemp: "-15240\n", wantOK: true, wantTemp: -15.2},
		{name: "min_bound_minus_40", rawTemp: "-40000\n", wantOK: true, wantTemp: -40.0},
		{name: "max_bound_150", rawTemp: "150000\n", wantOK: true, wantTemp: 150.0},
		{name: "below_min_bound", rawTemp: "-40100\n", wantOK: false},
		{name: "above_max_bound", rawTemp: "150100\n", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sysRoot := t.TempDir()
			writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone0/type", "cpu-thermal\n")
			writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone0/temp", tc.rawTemp)

			ev := core.NewCTDPayload("rv64-cold", "host-os", "active")
			observeSysfsThermals(sysRoot, ev)

			gotCPU, hasCPU := ev.Metrics["cpu_temp_c"]
			gotSoC, hasSoC := ev.Metrics["soc_temp_c"]
			if tc.wantOK {
				if !hasCPU || !hasSoC {
					t.Fatalf("expected cpu_temp_c and soc_temp_c for %q, got metrics=%+v", tc.rawTemp, ev.Metrics)
				}
				if gotCPU != tc.wantTemp || gotSoC != tc.wantTemp {
					t.Fatalf("expected %.1f C, got cpu=%v soc=%v", tc.wantTemp, gotCPU, gotSoC)
				}
			} else if hasCPU || hasSoC {
				t.Fatalf("expected out-of-range temp %q to be rejected, got cpu=%v soc=%v", tc.rawTemp, gotCPU, gotSoC)
			}
		})
	}
}

func TestObserveSysfsThermalsFiltersNonHostThermalZoneTypes(t *testing.T) {
	sysRoot := t.TempDir()
	writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone0/type", "iwlwifi_1\n")
	writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone0/temp", "95000\n")
	writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone1/type", "mt7915_phy0\n")
	writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone1/temp", "92000\n")
	writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone2/type", "acpitz\n")
	writeSysfsFile(t, sysRoot, "class/thermal/thermal_zone2/temp", "42000\n")

	ev := core.NewCTDPayload("edge-node", "host-os", "active")
	observeSysfsThermals(sysRoot, ev)

	if got := ev.Metrics["cpu_temp_c"]; got != 42.0 {
		t.Fatalf("expected non-host thermal_zone types to be filtered leaving 42.0 C, got %v", got)
	}
}

func TestObserveSysfsThermalsIgnoresFaultyHwmonSensors(t *testing.T) {
	sysRoot := t.TempDir()
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/name", "coretemp\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp1_input", "110000\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp1_fault", "1\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp2_input", "48500\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp2_fault", "0\n")

	ev := core.NewCTDPayload("edge-node", "host-os", "active")
	observeSysfsThermals(sysRoot, ev)
	if got := ev.Metrics["cpu_temp_c"]; got != 48.5 {
		t.Fatalf("expected faulty temp1 to be ignored and temp2=48.5 selected, got %v", got)
	}

	// Flip temp2_fault to 1 dynamically; cached reader must immediately ignore temp2 as well.
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp2_fault", "1\n")
	ev2 := core.NewCTDPayload("edge-node", "host-os", "active")
	observeSysfsThermals(sysRoot, ev2)
	if _, hasTemp := ev2.Metrics["cpu_temp_c"]; hasTemp {
		t.Fatalf("expected all faulty hwmon sensors to be ignored, got %v", ev2.Metrics["cpu_temp_c"])
	}
}

func TestSysfsDiscoveryCacheReuseReadErrorAndTTLExpiry(t *testing.T) {
	sysRoot := t.TempDir()
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/name", "coretemp\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp1_input", "45000\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_label", "CPU Fan\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_input", "1500\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/pwm1", "128\n")

	now := time.Unix(1_700_000_000, 0)
	var nowMu sync.Mutex
	cache := &sysfsDiscoveryCache{
		now: func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return now
		},
		thermals: make(map[string]*cachedThermalTopology),
		coolings: make(map[string]*cachedCoolingTopology),
	}

	// Tick 1: initial discovery.
	ev1 := core.NewCTDPayload("cache-node", "host-os", "active")
	cache.observeThermals(sysRoot, ev1)
	cache.observeCooling(sysRoot, ev1)
	if ev1.Metrics["cpu_temp_c"] != 45.0 || ev1.Metrics["cpu_fan_rpm"] != 1500 {
		t.Fatalf("unexpected initial cached sample: %+v", ev1.Metrics)
	}

	// Tick 2: update existing cached files AND add hwmon1; within 5m TTL, cached hwmon0 paths/labels must be reused.
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp1_input", "47000\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_input", "1800\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_label", "Changed Label Should Wait For Refresh\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/name", "coretemp\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/temp1_input", "80000\n")

	ev2 := core.NewCTDPayload("cache-node", "host-os", "active")
	cache.observeThermals(sysRoot, ev2)
	cache.observeCooling(sysRoot, ev2)
	if ev2.Metrics["cpu_temp_c"] != 47.0 {
		t.Fatalf("expected cached hwmon0 temp 47.0 (ignoring uncached hwmon1 80.0 within TTL), got %v", ev2.Metrics["cpu_temp_c"])
	}
	fans2 := ev2.Metrics["fans_rpm"].(map[string]int)
	if fans2["coretemp:cpu_fan"] != 1800 {
		t.Fatalf("expected cached fan1_label key coretemp:cpu_fan=1800, got %+v", fans2)
	}

	// Tick 3: remove cached hwmon0 files to force read error -> cache must refresh immediately and discover hwmon1.
	if err := os.Remove(filepath.Join(sysRoot, "class/hwmon/hwmon0/temp1_input")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(sysRoot, "class/hwmon/hwmon0/fan1_input")); err != nil {
		t.Fatal(err)
	}
	ev3 := core.NewCTDPayload("cache-node", "host-os", "active")
	cache.observeThermals(sysRoot, ev3)
	cache.observeCooling(sysRoot, ev3)
	if ev3.Metrics["cpu_temp_c"] != 80.0 {
		t.Fatalf("expected read error to refresh thermal cache and read hwmon1=80.0, got %v", ev3.Metrics["cpu_temp_c"])
	}
	if _, hasFan := ev3.Metrics["cpu_fan_rpm"]; hasFan {
		t.Fatalf("expected cooling cache refresh after fan1_input removal to omit stale fan, got %v", ev3.Metrics["cpu_fan_rpm"])
	}

	// Tick 4: add hwmon2 and advance clock >= 5m -> TTL expiry refreshes cache.
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon2/name", "coretemp\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon2/temp1_input", "91000\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon2/fan1_label", "CPU Cooler\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon2/fan1_input", "2400\n")

	nowMu.Lock()
	now = now.Add(5*time.Minute + time.Second)
	nowMu.Unlock()

	ev4 := core.NewCTDPayload("cache-node", "host-os", "active")
	cache.observeThermals(sysRoot, ev4)
	cache.observeCooling(sysRoot, ev4)
	if ev4.Metrics["cpu_temp_c"] != 91.0 || ev4.Metrics["cpu_fan_rpm"] != 2400 {
		t.Fatalf("expected 5m TTL expiry to refresh caches and pick up hwmon2, got %+v", ev4.Metrics)
	}
}
