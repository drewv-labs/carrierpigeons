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
	"weak"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"golang.org/x/sys/unix"
)

var (
	darwinSMCFanRe = regexp.MustCompile(`(?i)Fan(?:\s*(\d+))?\s*:\s*([\d\.]+)\s*rpm`)
	darwinTempRe   = regexp.MustCompile(`(?i)(?:CPU die temperature|SMC.*temp).*?:\s*([\d\.]+)\s*C`)

	defaultDarwinPM         = newDarwinPMSingleflight()
	defaultDarwinPMRegistry = &darwinPMRegistry{}
)

type darwinPMRegistry struct {
	mu   sync.Mutex
	byPC map[weak.Pointer[observer.PauseController]]*darwinPMSingleflight
}

func (r *darwinPMRegistry) forController(pc *observer.PauseController) *darwinPMSingleflight {
	if pc == nil {
		return defaultDarwinPM
	}
	key := weak.Make(pc)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byPC == nil {
		r.byPC = make(map[weak.Pointer[observer.PauseController]]*darwinPMSingleflight)
	}
	if sf, ok := r.byPC[key]; ok {
		return sf
	}
	sf := newDarwinPMSingleflight()
	r.byPC[key] = sf
	runtime.AddCleanup(pc, func(k weak.Pointer[observer.PauseController]) {
		r.mu.Lock()
		delete(r.byPC, k)
		r.mu.Unlock()
	}, key)
	return sf
}

func darwinPMSingleflightForContext(ctx context.Context) *darwinPMSingleflight {
	return defaultDarwinPMRegistry.forController(observer.PauseControllerFromContext(ctx))
}

type darwinPMFlight struct {
	mu      sync.Mutex
	waiters int
	cancel  context.CancelFunc
	done    chan struct{}
	out     string
	err     error
}

type darwinPMSingleflight struct {
	mu       sync.Mutex
	now      func() time.Time
	cacheAt  time.Time
	cacheVal string
	inFlight *darwinPMFlight
}

func newDarwinPMSingleflight() *darwinPMSingleflight {
	return &darwinPMSingleflight{now: time.Now}
}

func (s *darwinPMSingleflight) sample(ctx context.Context, samplers string, execFn func(context.Context, string) (string, error)) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if observer.IsPaused(ctx) {
		return "", context.Canceled
	}

	ttl := min(pollInterval(ctx)/2, 2*time.Second)
	if ttl < 500*time.Millisecond {
		ttl = 500 * time.Millisecond
	}

	s.mu.Lock()
	if s.cacheVal != "" && s.now().Sub(s.cacheAt) < ttl {
		cached := s.cacheVal
		s.mu.Unlock()
		return cached, nil
	}

	flight := s.inFlight
	if flight != nil {
		flight.mu.Lock()
		flight.waiters++
		flight.mu.Unlock()
		s.mu.Unlock()
	} else {
		baseCtx, cancelWaiters := context.WithCancel(context.WithoutCancel(ctx))
		cmdCtx, cancelPause := observer.ContextUntilPaused(baseCtx)
		flight = &darwinPMFlight{
			waiters: 1,
			cancel:  cancelWaiters,
			done:    make(chan struct{}),
		}
		s.inFlight = flight
		s.mu.Unlock()

		go func() {
			out, err := execFn(cmdCtx, samplers)
			cancelPause()
			cancelWaiters()

			s.mu.Lock()
			if err == nil {
				s.cacheVal = out
				s.cacheAt = s.now()
			}
			if s.inFlight == flight {
				s.inFlight = nil
			}
			s.mu.Unlock()

			flight.out = out
			flight.err = err
			close(flight.done)
		}()
	}

	select {
	case <-ctx.Done():
		s.mu.Lock()
		flight.mu.Lock()
		flight.waiters--
		shouldCancel := flight.waiters == 0
		cancelFn := flight.cancel
		if shouldCancel && s.inFlight == flight {
			s.inFlight = nil
		}
		flight.mu.Unlock()
		s.mu.Unlock()
		if shouldCancel && cancelFn != nil {
			cancelFn()
		}
		return "", ctx.Err()
	case <-flight.done:
		return flight.out, flight.err
	}
}

func runDarwinPowermetricsCmd(ctx context.Context, samplers string) (string, error) {
	out, err := runCommand(ctx, defaultCmdTimeout, "powermetrics", "--samplers", samplers, "-n", "1", "-i", "100")
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// sampleDarwinPowermetrics consolidates SMC, GPU, and ANE `powermetrics` queries into a singleflight cached subprocess call per interval,
// scoped per PauseController so independent in-process SDK runtimes cannot cancel each other's sampling flights.
func sampleDarwinPowermetrics(ctx context.Context) (string, error) {
	samplers := "smc,gpu_power"
	if runtime.GOARCH == "arm64" {
		samplers = "smc,gpu_power,ane_power"
	}
	return darwinPMSingleflightForContext(ctx).sample(ctx, samplers, runDarwinPowermetricsCmd)
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
