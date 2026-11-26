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

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
)

type capturePublisher struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (c *capturePublisher) Publish(ev *core.CTDPayload) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
	return nil
}

func (c *capturePublisher) snapshot() []*core.CTDPayload {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*core.CTDPayload, len(c.events))
	copy(out, c.events)
	return out
}

func TestParseNvidiaSMIOmitsUnavailableMetrics(t *testing.T) {
	raw := strings.Join([]string{
		"0, Test GPU 0, 52, [N/A], 1024, 24564, [Not Supported], [N/A]",
		"1, Test GPU 1, [N/A], [N/A], [N/A], [N/A], [N/A], [N/A]",
		"2, Test GPU 2, 61.5, 88, 4096, 16384, 145.2, 42",
	}, "\n")

	events := parseNvidiaSMIOutput(raw, "node-a")
	if len(events) != 2 {
		t.Fatalf("expected 2 events (all-N/A row omitted), got %d", len(events))
	}

	ev0 := events[0]
	if ev0.Component != "nvidia-gpu-0" {
		t.Fatalf("unexpected component %q", ev0.Component)
	}
	if got := ev0.Metrics["temperature_c"]; got != 52.0 {
		t.Fatalf("expected temperature_c=52.0, got %v", got)
	}
	if got := ev0.Metrics["memory_used_mb"]; got != 1024.0 {
		t.Fatalf("expected memory_used_mb=1024.0, got %v", got)
	}
	if got := ev0.Metrics["memory_total_mb"]; got != 24564.0 {
		t.Fatalf("expected memory_total_mb=24564.0, got %v", got)
	}
	for _, omitted := range []string{"utilization_pct", "power_w", "fan_speed_pct"} {
		if _, exists := ev0.Metrics[omitted]; exists {
			t.Fatalf("expected unavailable metric %q to be omitted instead of 0.0, got %v", omitted, ev0.Metrics[omitted])
		}
	}

	ev2 := events[1]
	if ev2.Component != "nvidia-gpu-2" || ev2.Metrics["power_w"] != 145.2 || ev2.Metrics["fan_speed_pct"] != 42.0 {
		t.Fatalf("unexpected parsed metrics for gpu 2: %+v", ev2.Metrics)
	}
}

func TestParseROCmSMIOmitsUnavailableMetrics(t *testing.T) {
	raw := `{
		"card0": {
			"Temperature (Sensor edge) (C)": "48.0",
			"GPU use (%)": "N/A",
			"VRAM Total Used Memory (B)": "",
			"Fan RPM": "N/A",
			"Fan speed (%)": "bad"
		},
		"card1": {
			"Temperature (Sensor edge) (C)": "N/A",
			"GPU use (%)": "N/A",
			"VRAM Total Used Memory (B)": "N/A"
		}
	}`

	events, err := parseROCmSMIOutput([]byte(raw), "node-amd")
	if err != nil {
		t.Fatalf("parseROCmSMIOutput failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event (all-N/A card1 omitted), got %d", len(events))
	}

	ev := events[0]
	if ev.Component != "amd-gpu-card0" {
		t.Fatalf("unexpected component %q", ev.Component)
	}
	if got := ev.Metrics["temperature_c"]; got != 48.0 {
		t.Fatalf("expected temperature_c=48.0, got %v", got)
	}
	for _, omitted := range []string{"utilization_pct", "memory_used_mb", "fan_rpm", "fan_speed_pct"} {
		if _, exists := ev.Metrics[omitted]; exists {
			t.Fatalf("expected unavailable metric %q to be omitted instead of 0.0, got %v", omitted, ev.Metrics[omitted])
		}
	}
}

func TestHailoObserverOmitsUnavailableAndClearsDetachedDevices(t *testing.T) {
	var (
		attached    atomic.Bool
		scanEnabled atomic.Bool
		powerFails  atomic.Bool
		scanCount   atomic.Int64
		tempCount   atomic.Int64
	)
	attached.Store(true)
	scanEnabled.Store(true)
	powerFails.Store(true) // simulate unsupported measure-power; must omit power_w instead of 0.0

	pub := &capturePublisher{}
	pc := observer.NewPauseController()
	ctx, cancel := context.WithCancel(WithPollInterval(observer.WithPauseController(context.Background(), pc), 20*time.Millisecond))
	defer cancel()

	hooks := hailoHooks{
		hasDeviceNode: func() bool {
			return attached.Load()
		},
		runCmd: func(cmdCtx context.Context, args ...string) ([]byte, error) {
			if cmdCtx.Err() != nil {
				return nil, cmdCtx.Err()
			}
			switch args[0] {
			case "scan":
				scanCount.Add(1)
				if !scanEnabled.Load() {
					return []byte("No Hailo devices found\n"), nil
				}
				return []byte("[-] Device 0000:01:00.0\n"), nil
			case "measure-temp":
				tempCount.Add(1)
				if !scanEnabled.Load() {
					return nil, errors.New("device disconnected")
				}
				return []byte("Temp: 46.5 C\n"), nil
			case "measure-power":
				if !scanEnabled.Load() {
					return nil, errors.New("device disconnected")
				}
				if powerFails.Load() {
					return nil, errors.New("not supported")
				}
				return []byte("Power: 2.75 W\n"), nil
			default:
				return nil, errors.New("unknown subcommand")
			}
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		observeNPUHailoWithHooks(ctx, pub, "node-hailo", hooks)
	}()

	deadline := time.Now().Add(time.Second)
	for len(pub.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	evs := pub.snapshot()
	if len(evs) == 0 {
		t.Fatal("expected Hailo telemetry sample")
	}
	if got := evs[0].Metrics["temperature_c"]; got != 46.5 {
		t.Fatalf("expected temperature_c=46.5, got %v", got)
	}
	if _, hasPower := evs[0].Metrics["power_w"]; hasPower {
		t.Fatalf("expected power_w to be omitted on command/parse failure, got %v", evs[0].Metrics["power_w"])
	}

	// Detach device node and verify cachedDevices is cleared so measure-temp stops executing.
	attached.Store(false)
	time.Sleep(60 * time.Millisecond)
	frozenTempCalls := tempCount.Load()
	time.Sleep(80 * time.Millisecond)
	if after := tempCount.Load(); after != frozenTempCalls {
		t.Fatalf("expected detached Hailo cachedDevices to clear and stop measure-temp calls (frozen=%d, after=%d)", frozenTempCalls, after)
	}

	// Reattach node, wait for scan recovery, then simulate scan returning empty output (detach while /dev node lingers).
	attached.Store(true)
	powerFails.Store(false)
	deadline = time.Now().Add(time.Second)
	for tempCount.Load() == frozenTempCalls && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if tempCount.Load() == frozenTempCalls {
		t.Fatal("expected Hailo observer to re-discover device after reattach")
	}

	scanEnabled.Store(false)
	time.Sleep(80 * time.Millisecond)
	frozenAfterEmptyScan := tempCount.Load()
	time.Sleep(80 * time.Millisecond)
	if after := tempCount.Load(); after != frozenAfterEmptyScan {
		t.Fatalf("expected empty scan / probe failure to clear cachedDevices (frozen=%d, after=%d)", frozenAfterEmptyScan, after)
	}

	cancel()
	<-done
}

func TestHailoObserverPauseGatingCancelsInFlightCommand(t *testing.T) {
	pc := observer.NewPauseController()
	ctx, cancel := context.WithCancel(WithPollInterval(observer.WithPauseController(context.Background(), pc), 30*time.Millisecond))
	defer cancel()

	cmdStarted := make(chan struct{})
	cmdCanceled := make(chan struct{})
	hooks := hailoHooks{
		hasDeviceNode: func() bool { return true },
		runCmd: func(cmdCtx context.Context, args ...string) ([]byte, error) {
			if args[0] == "scan" {
				return []byte("[-] Device 0000:01:00.0\n"), nil
			}
			select {
			case <-cmdStarted:
			default:
				close(cmdStarted)
			}
			<-cmdCtx.Done()
			select {
			case <-cmdCanceled:
			default:
				close(cmdCanceled)
			}
			return nil, cmdCtx.Err()
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		observeNPUHailoWithHooks(ctx, &capturePublisher{}, "node-hailo-pause", hooks)
	}()

	select {
	case <-cmdStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Hailo command start")
	}

	release := pc.Acquire()
	select {
	case <-cmdCanceled:
	case <-time.After(time.Second):
		t.Fatal("expected ContextUntilPaused to cancel in-flight Hailo subprocess on pause")
	}
	release()

	cancel()
	<-done
}

func TestAMDSysfsScalingFaultAndCacheRefresh(t *testing.T) {
	sysRoot := t.TempDir()
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/vendor", "0x1002\n")
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/gpu_busy_percent", "35\n")
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/hwmon/hwmon0/temp1_input", "-12500\n")

	now := time.Unix(1_700_000_000, 0)
	var nowMu sync.Mutex
	cache := &amdSysfsCache{
		now: func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return now
		},
		entries: make(map[string]*amdSysfsEntry),
	}

	payloads, readErr := cache.collectSamples(sysRoot, "node-amd-sysfs")
	if readErr || len(payloads) != 1 {
		t.Fatalf("expected 1 AMD sysfs payload without readErr, got len=%d readErr=%v", len(payloads), readErr)
	}
	if got := payloads[0].Metrics["temperature_c"]; got != -12.5 {
		t.Fatalf("expected sub-zero millidegree temp -12.5 C, got %v", got)
	}

	// temp1_fault == 1 must suppress temperature_c while retaining utilization_pct.
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/hwmon/hwmon0/temp1_fault", "1\n")
	payloads, _ = cache.collectSamples(sysRoot, "node-amd-sysfs")
	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload with utilization_pct, got %d", len(payloads))
	}
	if _, hasTemp := payloads[0].Metrics["temperature_c"]; hasTemp {
		t.Fatalf("expected temperature_c omitted when temp1_fault == 1, got %v", payloads[0].Metrics["temperature_c"])
	}

	// Out-of-range temperature (>150 C) must be rejected; within TTL, new card1 is not discovered yet.
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/hwmon/hwmon0/temp1_fault", "0\n")
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/hwmon/hwmon0/temp1_input", "160000\n")
	writeSysfsTestFile(t, sysRoot, "class/drm/card1/device/vendor", "0x1002\n")
	writeSysfsTestFile(t, sysRoot, "class/drm/card1/device/gpu_busy_percent", "77\n")

	payloads, _ = cache.collectSamples(sysRoot, "node-amd-sysfs")
	if len(payloads) != 1 {
		t.Fatalf("expected cached card0 only within 5m TTL, got %d payloads", len(payloads))
	}
	if _, hasTemp := payloads[0].Metrics["temperature_c"]; hasTemp {
		t.Fatalf("expected out-of-range 160C to be rejected, got %v", payloads[0].Metrics["temperature_c"])
	}

	// Advance clock >= 5m -> cache refreshes and picks up card1.
	nowMu.Lock()
	now = now.Add(5*time.Minute + time.Second)
	nowMu.Unlock()
	payloads, _ = cache.collectSamples(sysRoot, "node-amd-sysfs")
	if len(payloads) != 2 {
		t.Fatalf("expected 2 cards after 5m TTL expiry, got %d", len(payloads))
	}

	// Deleting cached gpu_busy_percent on card0 causes read error and refreshes cache.
	// Restore valid temp on card0 so all 3 cards emit telemetry after cache refresh.
	writeSysfsTestFile(t, sysRoot, "class/drm/card0/device/hwmon/hwmon0/temp1_input", "55000\n")
	if err := os.Remove(filepath.Join(sysRoot, "class/drm/card0/device/gpu_busy_percent")); err != nil {
		t.Fatalf("remove gpu_busy_percent: %v", err)
	}
	writeSysfsTestFile(t, sysRoot, "class/drm/card2/device/vendor", "0x1002\n")
	writeSysfsTestFile(t, sysRoot, "class/drm/card2/device/gpu_busy_percent", "99\n")

	payloads, _ = cache.collectSamples(sysRoot, "node-amd-sysfs")
	if len(payloads) != 3 {
		t.Fatalf("expected read error to refresh cache and discover card2 (3 payloads), got %d", len(payloads))
	}
}

func writeSysfsTestFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", full, err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
}
