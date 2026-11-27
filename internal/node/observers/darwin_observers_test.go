//go:build darwin

package observers

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"golang.org/x/sys/unix"
)

type darwinTestPublisher struct {
	mu     sync.Mutex
	events []*core.CTDPayload
}

func (p *darwinTestPublisher) Publish(ev *core.CTDPayload) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func TestDarwinZeroForkNetAndMemoryObservers(t *testing.T) {
	counters, err := collectDarwinNetCounters()
	if err != nil {
		t.Fatalf("collectDarwinNetCounters failed: %v", err)
	}
	if len(counters) == 0 {
		t.Fatal("expected at least one active Darwin network interface from NET_RT_IFLIST2")
	}

	for iface, c := range counters {
		// Detect 16-bit/struct-padding misalignment producing absurd PB-scale byte counts.
		const maxSaneBytes = uint64(1) << 50 // 1 PiB
		if c.rxBytes > maxSaneBytes || c.txBytes > maxSaneBytes {
			t.Fatalf("counter ABI misalignment suspected on %s: rxBytes=%d txBytes=%d", iface, c.rxBytes, c.txBytes)
		}
		if c.rxPackets > 0 && c.rxBytes < c.rxPackets {
			t.Fatalf("counter sanity failed on %s: rxBytes=%d < rxPackets=%d", iface, c.rxBytes, c.rxPackets)
		}
		if c.txPackets > 0 && c.txBytes < c.txPackets {
			t.Fatalf("counter sanity failed on %s: txBytes=%d < txPackets=%d", iface, c.txBytes, c.txPackets)
		}
	}

	availMB, ok := readDarwinAvailableMemoryMB()
	if !ok || availMB <= 0 {
		t.Fatalf("expected positive macOS available memory via sysctl, got ok=%v availMB=%.1f", ok, availMB)
	}
}

func TestDarwinIfMsghdr2WireABIDecoding(t *testing.T) {
	if got, want := unsafe.Sizeof(wireIfMsghdr2{}), unsafe.Sizeof(unix.IfMsghdr2{}); got != want {
		t.Fatalf("wireIfMsghdr2 size=%d != unix.IfMsghdr2 size=%d", got, want)
	}
	if got, want := binary.Size(wireIfMsghdr2{}), int(unsafe.Sizeof(unix.IfMsghdr2{})); got != want {
		t.Fatalf("binary.Size(wireIfMsghdr2)=%d != sizeof=%d", got, want)
	}
	if got, want := unsafe.Offsetof(wireIfMsghdr2{}.Data), unsafe.Offsetof(unix.IfMsghdr2{}.Data); got != want {
		t.Fatalf("wireIfMsghdr2.Data offset=%d != unix.IfMsghdr2.Data offset=%d", got, want)
	}

	// Build a synthetic RTM_IFINFO2 message with explicit non-zero padding bytes at offsets 14..16.
	const (
		wantRxPkts = uint64(12345)
		wantTxPkts = uint64(67890)
		wantRxByt  = uint64(9876543210)
		wantTxByt  = uint64(1234567890)
	)

	ifName := "en0"
	sdl := make([]byte, 8+len(ifName))
	sdl[0] = byte(len(sdl))
	sdl[1] = unix.AF_LINK
	binary.LittleEndian.PutUint16(sdl[2:4], 7)
	sdl[5] = byte(len(ifName))
	copy(sdl[8:], ifName)

	hdrSize := int(unsafe.Sizeof(wireIfMsghdr2{}))
	msgLen := hdrSize + len(sdl)
	buf := make([]byte, msgLen)

	binary.LittleEndian.PutUint16(buf[0:2], uint16(msgLen))
	buf[2] = unix.RTM_VERSION
	buf[3] = unix.RTM_IFINFO2
	binary.LittleEndian.PutUint32(buf[4:8], unix.RTA_IFP)
	binary.LittleEndian.PutUint16(buf[12:14], 7)
	// Inject non-zero sentinel bytes in the 2-byte alignment hole between Index and Snd_len.
	buf[14] = 0xAA
	buf[15] = 0xBB

	dataOff := int(unsafe.Offsetof(wireIfMsghdr2{}.Data))
	data := unix.IfData64{
		Ipackets: wantRxPkts,
		Opackets: wantTxPkts,
		Ibytes:   wantRxByt,
		Obytes:   wantTxByt,
	}
	var dataBuf bytes.Buffer
	if err := binary.Write(&dataBuf, binary.LittleEndian, data); err != nil {
		t.Fatalf("encode IfData64: %v", err)
	}
	copy(buf[dataOff:hdrSize], dataBuf.Bytes())
	copy(buf[hdrSize:], sdl)

	seen := parseDarwinRIB(buf, nil)
	got, ok := seen["en0"]
	if !ok {
		t.Fatalf("expected en0 in parsed RIB, got %+v", seen)
	}
	if got.rxBytes != wantRxByt || got.txBytes != wantTxByt || got.rxPackets != wantRxPkts || got.txPackets != wantTxPkts {
		t.Fatalf("decoded counters mismatch: got=%+v, want rxB=%d txB=%d rxP=%d txP=%d",
			got, wantRxByt, wantTxByt, wantRxPkts, wantTxPkts)
	}

	// Verify index fallback when sockaddr_dl is absent.
	bufNoSDL := make([]byte, hdrSize)
	copy(bufNoSDL, buf[:hdrSize])
	binary.LittleEndian.PutUint16(bufNoSDL[0:2], uint16(hdrSize))
	seenFallback := parseDarwinRIB(bufNoSDL, func(idx int) string {
		if idx == 7 {
			return "utun7"
		}
		return ""
	})
	if _, ok := seenFallback["utun7"]; !ok {
		t.Fatalf("expected index fallback to resolve utun7, got %+v", seenFallback)
	}
}

func TestDarwinPowermetricsSingleflightCachingAndPause(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var nowMu sync.Mutex
	sf := &darwinPMSingleflight{
		now: func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return now
		},
	}

	var calls atomic.Int64
	gate := make(chan struct{})
	started := make(chan struct{}, 1)

	execFn := func(ctx context.Context, samplers string) (string, error) {
		if calls.Add(1) == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			<-gate
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "GPU Power: 4200 mW\nANE Power: 300 mW\n", nil
	}

	const workers = 8
	results := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = sf.sample(context.Background(), "smc,gpu_power,ane_power", execFn)
		}(i)
	}

	<-started
	time.Sleep(15 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		close(gate)
		wg.Wait()
		t.Fatalf("expected singleflight to coalesce concurrent callers into 1 call, got %d", got)
	}
	close(gate)
	wg.Wait()

	for i := range workers {
		if errs[i] != nil || results[i] == "" {
			t.Fatalf("worker %d failed: err=%v out=%q", i, errs[i], results[i])
		}
	}

	// Cache hit within TTL should not invoke execFn again.
	out, err := sf.sample(context.Background(), "smc,gpu_power,ane_power", execFn)
	if err != nil || out == "" || calls.Load() != 1 {
		t.Fatalf("expected cached hit with 1 total call, got calls=%d err=%v", calls.Load(), err)
	}

	// Advance clock past TTL (> 2s) and verify refresh.
	nowMu.Lock()
	now = now.Add(3 * time.Second)
	nowMu.Unlock()
	if _, err := sf.sample(context.Background(), "smc,gpu_power,ane_power", execFn); err != nil || calls.Load() != 2 {
		t.Fatalf("expected refresh after TTL expiry (calls=2), got calls=%d err=%v", calls.Load(), err)
	}

	// Verify PauseController cancels an in-flight call and does not cache the failure.
	nowMu.Lock()
	now = now.Add(3 * time.Second)
	nowMu.Unlock()
	pc := observer.NewPauseController()
	pausedCtx := observer.WithPauseController(context.Background(), pc)

	inFlightStarted := make(chan struct{})
	cancelObserved := make(chan struct{})
	pausedExec := func(ctx context.Context, samplers string) (string, error) {
		close(inFlightStarted)
		<-ctx.Done()
		close(cancelObserved)
		return "", ctx.Err()
	}

	var pausedErr error
	var pauseWG sync.WaitGroup
	pauseWG.Add(1)
	go func() {
		defer pauseWG.Done()
		_, pausedErr = sf.sample(pausedCtx, "smc,gpu_power", pausedExec)
	}()

	<-inFlightStarted
	release := pc.Acquire()
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pause to cancel in-flight powermetrics call")
	}
	pauseWG.Wait()
	release()

	if !errors.Is(pausedErr, context.Canceled) {
		t.Fatalf("expected context.Canceled on paused powermetrics flight, got %v", pausedErr)
	}

	// Verify leader ctx cancel does not abort another healthy follower waiting on the same flight.
	nowMu.Lock()
	now = now.Add(3 * time.Second)
	nowMu.Unlock()

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	flightReady := make(chan struct{})
	releaseFlight := make(chan struct{})
	sharedExec := func(ctx context.Context, _ string) (string, error) {
		close(flightReady)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-releaseFlight:
			return "ok-shared", nil
		}
	}

	var leaderErr, followerErr error
	var followerOut string
	var sfWG sync.WaitGroup
	sfWG.Add(2)
	go func() {
		defer sfWG.Done()
		_, leaderErr = sf.sample(leaderCtx, "smc,gpu_power", sharedExec)
	}()
	<-flightReady
	go func() {
		defer sfWG.Done()
		followerOut, followerErr = sf.sample(context.Background(), "smc,gpu_power", sharedExec)
	}()
	time.Sleep(15 * time.Millisecond)
	cancelLeader()
	time.Sleep(15 * time.Millisecond)
	close(releaseFlight)
	sfWG.Wait()

	if !errors.Is(leaderErr, context.Canceled) {
		t.Fatalf("expected leaderErr=context.Canceled, got %v", leaderErr)
	}
	if followerErr != nil || followerOut != "ok-shared" {
		t.Fatalf("expected follower to complete cleanly despite leader cancel, got out=%q err=%v", followerOut, followerErr)
	}
}

func TestDarwinSingleflightScopedPerPauseControllerAndClearsInFlightOnSoleCancel(t *testing.T) {
	pcA := observer.NewPauseController()
	pcB := observer.NewPauseController()
	ctxA := observer.WithPauseController(context.Background(), pcA)
	ctxB := observer.WithPauseController(context.Background(), pcB)

	sfA1 := darwinPMSingleflightForContext(ctxA)
	sfA2 := darwinPMSingleflightForContext(ctxA)
	sfB := darwinPMSingleflightForContext(ctxB)
	if sfA1 != sfA2 {
		t.Fatal("expected identical PauseController to share the same darwinPMSingleflight")
	}
	if sfA1 == sfB || sfA1 == defaultDarwinPM {
		t.Fatal("expected distinct PauseControllers to isolate darwinPMSingleflight instances")
	}

	// Sole waiter canceling must clear s.inFlight BEFORE cancelFn runs so an immediate next caller starts a fresh flight.
	sf := newDarwinPMSingleflight()
	soleCtx, cancelSole := context.WithCancel(context.Background())
	firstStarted := make(chan struct{})
	blockFirstAfterCancel := make(chan struct{})
	var calls atomic.Int64

	execFn := func(ctx context.Context, _ string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			close(firstStarted)
			<-ctx.Done()
			<-blockFirstAfterCancel
			return "", ctx.Err()
		}
		return "fresh-flight-ok", nil
	}

	soleDone := make(chan error, 1)
	go func() {
		_, err := sf.sample(soleCtx, "smc", execFn)
		soleDone <- err
	}()
	<-firstStarted
	cancelSole()
	if err := <-soleDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected sole waiter cancel error, got %v", err)
	}

	// While first flight's goroutine is still stalled in cleanup (blockFirstAfterCancel open),
	// a new caller must NOT attach to the canceled flight—it must launch call #2 immediately!
	out, err := sf.sample(context.Background(), "smc", execFn)
	close(blockFirstAfterCancel)
	if err != nil || out != "fresh-flight-ok" || calls.Load() != 2 {
		t.Fatalf("expected cleared s.inFlight to start fresh flight #2, got out=%q err=%v calls=%d", out, err, calls.Load())
	}
}

func TestDarwinSynchronousPlatformWaitGroupContract(t *testing.T) {
	ctx, cancel := context.WithCancel(WithPollInterval(context.Background(), 20*time.Millisecond))
	pub := &darwinTestPublisher{}

	sampleFn := func(_ context.Context) (string, error) {
		return "GPU Power: 3500 mW\nGPU active residency: 25.0 %\nANE Power: 250 mW\n", nil
	}

	var wg sync.WaitGroup
	wg.Add(2)

	gpuExited := atomic.Bool{}
	npuExited := atomic.Bool{}

	go func() {
		defer wg.Done()
		defer gpuExited.Store(true)
		observeGPUDarwinWithSample(ctx, pub, "darwin-test", sampleFn)
	}()
	go func() {
		defer wg.Done()
		defer npuExited.Store(true)
		observeNPUDarwinWithSample(ctx, pub, "darwin-test", sampleFn)
	}()

	time.Sleep(40 * time.Millisecond)
	if gpuExited.Load() || npuExited.Load() {
		cancel()
		wg.Wait()
		t.Fatal("observeGPUDarwin/observeNPUDarwin must block synchronously until ctx is canceled")
	}

	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for synchronous Darwin GPU/NPU observers to exit on cancel")
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) < 2 {
		t.Fatalf("expected GPU and NPU events before cancellation, got %d", len(pub.events))
	}
}
