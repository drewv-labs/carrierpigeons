package observers

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

func TestF08SysfsSplitPWMCollidingLabelsFaultPruningAndNonENOENTStability(t *testing.T) {
	sysRoot := t.TempDir()

	// 1. Split tach/PWM + colliding fan labels:
	// hwmon0 (nct6775) has CPU tach on fan1_input (no pwm1) + two colliding "Chassis Fan" labels on fan2/fan3.
	// hwmon1 (pwmfan) has pwm1 without fan1_input.
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/name", "nct6775\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_label", "CPU Cooler\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan1_input", "1720\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan2_label", "Chassis Fan\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan2_input", "920\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan3_label", "Chassis Fan\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/fan3_input", "1040\n")
	// Thermal sensor WITHOUT temp1_fault file.
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon0/temp1_input", "52000\n")

	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/name", "pwmfan\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon1/pwm1", "191\n") // ~74.9%

	now := time.Unix(1_700_000_000, 0)
	cache := &sysfsDiscoveryCache{
		now:      func() time.Time { return now },
		thermals: make(map[string]*cachedThermalTopology),
		coolings: make(map[string]*cachedCoolingTopology),
	}

	ev := core.NewCTDPayload("rv64-split", "host-os", "active")
	cache.observeThermals(sysRoot, ev)
	cache.observeCooling(sysRoot, ev)

	// Verify missing temp1_fault was pruned during discovery.
	tTopo, refreshed := cache.getThermals(sysRoot, false)
	if refreshed {
		t.Fatal("expected cached thermal topology hit")
	}
	if len(tTopo.sensors) != 1 || tTopo.sensors[0].faultPath != "" {
		t.Fatalf("expected non-existent temp1_fault path to be pruned during discovery, got %+v", tTopo.sensors)
	}

	// Verify BOTH cpu_fan_rpm (from hwmon0) and cpu_fan_pwm_pct (from hwmon1 pwmfan) were emitted.
	if got := ev.Metrics["cpu_fan_rpm"]; got != 1720 {
		t.Fatalf("expected cpu_fan_rpm=1720, got %v", got)
	}
	if got := ev.Metrics["cpu_fan_pwm_pct"]; got != 74.9 {
		t.Fatalf("expected split tach/PWM cpu_fan_pwm_pct=74.9, got %v", got)
	}

	// Verify colliding "Chassis Fan" labels preserved both entries via :fan3 suffix.
	fans, ok := ev.Metrics["fans_rpm"].(map[string]int)
	if !ok {
		t.Fatalf("expected fans_rpm map, got %T", ev.Metrics["fans_rpm"])
	}
	if fans["nct6775:chassis_fan"] != 920 || fans["nct6775:chassis_fan:fan3"] != 1040 {
		t.Fatalf("expected deduplicated colliding fan labels, got %+v", fans)
	}

	// 2. Non-ENOENT read error (EISDIR by replacing temp1_input with a directory) must NOT trigger
	// immediate sysfs re-discovery or mark the cache dirty within TTL.
	temp1Path := filepath.Join(sysRoot, "class/hwmon/hwmon0/temp1_input")
	if err := os.Remove(temp1Path); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(temp1Path, 0o755); err != nil {
		t.Fatal(err)
	}
	// Add hwmon2 that would ONLY appear if re-discovery occurred prematurely.
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon2/name", "sfctemp\n")
	writeSysfsFile(t, sysRoot, "class/hwmon/hwmon2/temp1_input", "71000\n")

	evNonENOENT := core.NewCTDPayload("rv64-split", "host-os", "active")
	cache.observeThermals(sysRoot, evNonENOENT)
	if _, hasTemp := evNonENOENT.Metrics["cpu_temp_c"]; hasTemp {
		t.Fatalf("expected non-ENOENT EISDIR not to force premature sysfs re-discovery (got cpu_temp_c=%v)", evNonENOENT.Metrics["cpu_temp_c"])
	}
	if cache.thermals[sysRoot].dirty {
		t.Fatal("expected non-ENOENT read error not to mark thermal topology dirty")
	}
}

func TestF09StartConfiguredNilPublisherAndCanceledContext(t *testing.T) {
	var wg sync.WaitGroup
	active := StartConfiguredWithRegistryAndWaitGroup(
		context.Background(),
		"node-nil-pub",
		nil,
		config.ObserversConfig{OS: true, Net: true, GPU: true, NPU: true},
		20*time.Millisecond,
		observer.NewRegistry(),
		&wg,
	)
	if len(active) != 0 {
		t.Fatalf("expected nil publisher to early-return nil active observers, got %v", active)
	}
	wg.Wait()

	// Canceled context before runPeriodicObserver boot must not invoke emit().
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	var emitted atomic.Bool
	runPeriodicObserver(canceledCtx, true, func() {
		emitted.Store(true)
	})
	if emitted.Load() {
		t.Fatal("expected runPeriodicObserver not to emit when ctx is already canceled")
	}
}

func TestF15HailoColonDeviceRegexRateLimitedRescanAndProbeBackoff(t *testing.T) {
	scanOut := "[0] Hailo-8 PCIe: 0000:01:00.0\n[-] Device: 0000:02:00.0\n"
	devs, err := scanHailoDevicesWithCmd(context.Background(), func(context.Context, ...string) ([]byte, error) {
		return []byte(scanOut), nil
	})
	if err != nil {
		t.Fatalf("scanHailoDevicesWithCmd failed: %v", err)
	}
	if len(devs) != 2 || devs[0] != "0000:01:00.0" || devs[1] != "0000:02:00.0" {
		t.Fatalf("expected colon PCIe/Device IDs parsed, got %v", devs)
	}

	// Verify >=60s rescan rate limit and exponential backoff on HAILO_DEVICE_IN_USE.
	var (
		clockMu    sync.Mutex
		fakeNow    = time.Unix(1_700_000_000, 0)
		scanCalls  atomic.Int64
		tempCalls  atomic.Int64
		powerCalls atomic.Int64
		busyDevice atomic.Bool
	)
	busyDevice.Store(true)

	advance := func(d time.Duration) {
		clockMu.Lock()
		fakeNow = fakeNow.Add(d)
		clockMu.Unlock()
	}

	pub := &capturePublisher{}
	ctx, cancel := context.WithCancel(WithPollInterval(context.Background(), 15*time.Millisecond))
	defer cancel()

	hooks := hailoHooks{
		hasDeviceNode: func() bool { return true },
		now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return fakeNow
		},
		runCmd: func(cmdCtx context.Context, args ...string) ([]byte, error) {
			switch args[0] {
			case "scan":
				scanCalls.Add(1)
				return []byte("[0] Hailo-8 PCIe: 0000:01:00.0\n"), nil
			case "measure-temp":
				tempCalls.Add(1)
				if busyDevice.Load() {
					return []byte("HailoRT Status: HAILO_DEVICE_IN_USE(74)\n"), errors.New("exit status 1")
				}
				return []byte("Temp: 49.2 C\n"), nil
			case "measure-power":
				powerCalls.Add(1)
				if busyDevice.Load() {
					return []byte("HAILO_DEVICE_IN_USE\n"), errors.New("exit status 1")
				}
				return []byte("Power: 3.1 W\n"), nil
			default:
				return nil, errors.New("unexpected command")
			}
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		observeNPUHailoWithHooks(ctx, pub, "rv64-npu", hooks)
	}()

	// Wait for initial boot scan + first busy probe.
	deadline := time.Now().Add(time.Second)
	for tempCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond) // Multiple 15ms ticks pass while fakeNow is stationary!

	if got := scanCalls.Load(); got != 1 {
		t.Fatalf("expected rescan rate-limited to 1 call within <60s window, got %d", got)
	}
	if got := tempCalls.Load(); got != 1 {
		t.Fatalf("expected exponential backoff (1s) to suppress repeated HAILO_DEVICE_IN_USE probes, got %d", got)
	}
	if got := powerCalls.Load(); got != 0 {
		t.Fatalf("expected HAILO_DEVICE_IN_USE on measure-temp to skip redundant measure-power call, got %d", got)
	}

	// Advance fakeNow by +1.1s -> second probe attempt fires, fails, and backs off by +2s (still <60s so no rescan!).
	advance(1100 * time.Millisecond)
	deadline = time.Now().Add(time.Second)
	for tempCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(45 * time.Millisecond)
	if got := tempCalls.Load(); got != 2 {
		t.Fatalf("expected exactly 2nd probe after +1.1s backoff expiry, got %d", got)
	}
	if got := scanCalls.Load(); got != 1 {
		t.Fatalf("expected still 1 scan before 60s elapsed, got %d", got)
	}

	// Clear busy state and advance fakeNow by +61s -> rescan runs AND telemetry recovers!
	busyDevice.Store(false)
	advance(61 * time.Second)
	deadline = time.Now().Add(time.Second)
	for len(pub.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(pub.snapshot()) == 0 {
		t.Fatal("expected Hailo telemetry after device released and backoff/rescan window elapsed")
	}
	if got := scanCalls.Load(); got < 2 {
		t.Fatalf("expected periodic rescan after >=60s elapsed, got %d", got)
	}

	cancel()
	<-done
}

func TestF15CoralValidationAndMillidegreeBounds(t *testing.T) {
	base := t.TempDir()
	writeSysfsFile(t, base, "apex_0/temp", "54320\n")
	writeSysfsFile(t, base, "apex_1/temp", "999999\n") // >150 C -> rejected
	writeSysfsFile(t, base, "apex_2/temp", "NaN\n")    // non-finite -> rejected
	writeSysfsFile(t, base, "apex bad/temp", "45000\n")

	evs := collectCoralSamples(base, "rv64-coral")
	if len(evs) != 1 {
		t.Fatalf("expected only valid apex_0 event, got %d: %+v", len(evs), evs)
	}
	if evs[0].Component != "npu-coral-apex_0" || evs[0].Metrics["temperature_c"] != 54.3 {
		t.Fatalf("unexpected Coral payload: component=%q metrics=%+v", evs[0].Component, evs[0].Metrics)
	}
}

func TestF21IntelGPUStreamROCmNumericFanAndNetHelpers(t *testing.T) {
	// 1. Intel GPU stream: empty frame omitted, Render/3D fallback parsed, oversized frame rejected.
	stream := `[
		{"period":{"duration":1000}},
		{"power":{"GPU":12.5},"engines":{"Render/3D":{"busy":64.2}}}
	]`
	pub := &capturePublisher{}
	if err := decodeIntelGPUTopStream(context.Background(), strings.NewReader(stream), pub, "node-xe"); err != nil {
		t.Fatalf("decodeIntelGPUTopStream failed: %v", err)
	}
	evs := pub.snapshot()
	if len(evs) != 1 {
		t.Fatalf("expected 1 Intel GPU event (empty frame skipped), got %d", len(evs))
	}
	if evs[0].Metrics["power_w"] != 12.5 || evs[0].Metrics["utilization_pct"] != 64.2 {
		t.Fatalf("unexpected Intel GPU metrics: %+v", evs[0].Metrics)
	}

	oversized := `[{"engines":{"Render/3D":{"busy":10,"pad":"` + strings.Repeat("x", maxIntelGPUFrameBytes+1024) + `"}}}]`
	if err := decodeIntelGPUTopStream(context.Background(), strings.NewReader(oversized), pub, "node-xe"); !errors.Is(err, errIntelGPUFrameTooLarge) {
		t.Fatalf("expected errIntelGPUFrameTooLarge on unbounded frame, got %v", err)
	}

	// 2. ROCm SMI numeric Fan RPM support.
	rocmRaw := `{"card0":{"Fan RPM": 2150, "Fan speed (%)": 62.5}}`
	rocmEvs, err := parseROCmSMIOutput([]byte(rocmRaw), "node-rocm")
	if err != nil || len(rocmEvs) != 1 {
		t.Fatalf("parseROCmSMIOutput numeric Fan RPM failed: err=%v len=%d", err, len(rocmEvs))
	}
	if got := rocmEvs[0].Metrics["fan_rpm"]; got != 2150 {
		t.Fatalf("expected numeric fan_rpm=2150, got %v (%T)", got, got)
	}

	// 3. Linux /proc/net/dev colon-alias splitting & Windows NDIS filter row skipping.
	iface, fields, ok := parseProcNetDevLine("  eth0:0: 4096 32 0 0 0 0 0 0 8192 64 0 0 0 0 0 0")
	if !ok || iface != "eth0:0" || fields[0] != "4096" || fields[8] != "8192" {
		t.Fatalf("expected LastIndexByte split for eth0:0, got ok=%v iface=%q fields=%v", ok, iface, fields)
	}
	if isEligibleWindowsNetRow(6, winIfOperStatusUp, 0x02, 1024, 2048) {
		t.Fatal("expected Windows NDIS FilterInterface row (flags & 0x02 != 0) to be skipped")
	}
	if !isEligibleWindowsNetRow(6, winIfOperStatusUp, 0x01, 1024, 2048) {
		t.Fatal("expected hardware Windows NIC row (flags=0x01) to be eligible")
	}
}
