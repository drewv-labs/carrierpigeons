package observers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
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

	ev := core.NewCTDPayload("edge-rv64-01", "host-os", "active")
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

	ev := core.NewCTDPayload("edge-node-02", "host-os", "active")
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

	ev := core.NewCTDPayload("edge-sbc-1", "host-os", "active")
	observeSysfsCooling(sysRoot, ev)

	if _, hasRPM := ev.Metrics["cpu_fan_rpm"]; hasRPM {
		t.Fatalf("did not expect cpu_fan_rpm without tachometer, got %v", ev.Metrics["cpu_fan_rpm"])
	}
	if got := ev.Metrics["cpu_fan_pwm_pct"]; got != 75.0 {
		t.Fatalf("expected cpu_fan_pwm_pct=75.0 from cooling_device0, got %v", got)
	}
}
