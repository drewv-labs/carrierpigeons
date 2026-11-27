package observer

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type pauseControllerKey struct{}

// PauseController coordinates zero-overhead pausing and resumption of Observer loops and PillarObserver publishing.
// Each Edge Agent or SDK runtime owns an isolated PauseController instance.
type PauseController struct {
	mu           sync.Mutex
	paused       atomic.Bool
	manualPaused bool
	holds        int
	resumeCh     chan struct{}
	pauseCh      chan struct{}
}

// NewPauseController constructs an unpaused PauseController.
func NewPauseController() *PauseController {
	return &PauseController{
		pauseCh: make(chan struct{}),
	}
}

func (p *PauseController) refreshLocked() {
	shouldPause := p.manualPaused || p.holds > 0
	wasPaused := p.paused.Load()

	if shouldPause && !wasPaused {
		p.resumeCh = make(chan struct{})
		p.paused.Store(true)
		if p.pauseCh != nil {
			close(p.pauseCh)
			p.pauseCh = nil
		}
		return
	}

	if !shouldPause && wasPaused {
		p.paused.Store(false)
		if p.resumeCh != nil {
			close(p.resumeCh)
			p.resumeCh = nil
		}
		p.pauseCh = make(chan struct{})
	}
}

// Acquire places a reference-counted pause hold on observers and returns an idempotent release function.
func (p *PauseController) Acquire() func() {
	if p == nil {
		return func() {}
	}
	p.mu.Lock()
	p.holds++
	p.refreshLocked()
	p.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			if p.holds > 0 {
				p.holds--
			}
			p.refreshLocked()
			p.mu.Unlock()
		})
	}
}

// Pause engages an explicit manual pause state until Resume is called (and all Acquire holds release).
func (p *PauseController) Pause() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.manualPaused = true
	p.refreshLocked()
	p.mu.Unlock()
}

// Resume clears any manual Pause state. If Acquire holds remain active, observers stay paused until those release.
func (p *PauseController) Resume() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.manualPaused = false
	p.refreshLocked()
	p.mu.Unlock()
}

// IsPaused reports whether observer execution and PillarObserver publishing are currently paused.
func (p *PauseController) IsPaused() bool {
	if p == nil {
		return false
	}
	return p.paused.Load()
}

// WaitIfPaused blocks while p is paused, returning false only if ctx is canceled before resumption.
func (p *PauseController) WaitIfPaused(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || !p.paused.Load() {
		return ctx.Err() == nil
	}

	for {
		p.mu.Lock()
		if !p.paused.Load() {
			p.mu.Unlock()
			return ctx.Err() == nil
		}
		ch := p.resumeCh
		p.mu.Unlock()

		if ch == nil {
			return ctx.Err() == nil
		}

		select {
		case <-ctx.Done():
			return false
		case <-ch:
		}
	}
}

// ContextUntilPaused derives a child context that cancels as soon as p enters a paused state or parent ctx finishes.
// Callers must always invoke the returned CancelFunc to release watcher resources.
func (p *PauseController) ContextUntilPaused(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil {
		return context.WithCancel(ctx)
	}

	p.mu.Lock()
	if p.paused.Load() {
		p.mu.Unlock()
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		return cctx, cancel
	}
	if p.pauseCh == nil {
		p.pauseCh = make(chan struct{})
	}
	pauseSig := p.pauseCh
	p.mu.Unlock()

	cctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-cctx.Done():
		case <-pauseSig:
			cancel()
		}
	}()
	return cctx, cancel
}

// WithPauseController attaches pc to ctx for observers and runners sharing a node runtime.
func WithPauseController(ctx context.Context, pc *PauseController) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if pc == nil {
		return ctx
	}
	return context.WithValue(ctx, pauseControllerKey{}, pc)
}

// PauseControllerFromContext retrieves the PauseController bound to ctx, or nil if none is attached.
func PauseControllerFromContext(ctx context.Context) *PauseController {
	if ctx == nil {
		return nil
	}
	pc, _ := ctx.Value(pauseControllerKey{}).(*PauseController)
	return pc
}

// IsPaused reports whether ctx carries an actively paused PauseController.
func IsPaused(ctx context.Context) bool {
	return PauseControllerFromContext(ctx).IsPaused()
}

// WaitIfPaused blocks while ctx's PauseController is paused, returning false if ctx is canceled.
func WaitIfPaused(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	pc := PauseControllerFromContext(ctx)
	if pc == nil {
		return ctx.Err() == nil
	}
	return pc.WaitIfPaused(ctx)
}

// ContextUntilPaused derives a child context that cancels if ctx's PauseController pauses or ctx finishes.
func ContextUntilPaused(ctx context.Context) (context.Context, context.CancelFunc) {
	if pc := PauseControllerFromContext(ctx); pc != nil {
		return pc.ContextUntilPaused(ctx)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithCancel(ctx)
}

type gatedPublisher struct {
	next core.TelemetryPublisher
	pc   *PauseController
}

// NewGatedPublisher wraps next so PillarObserver (and default-pillar) CTD events are suppressed while pc is paused,
// while PillarRunner and PillarTracer events always pass through unimpeded.
func NewGatedPublisher(next core.TelemetryPublisher, pc *PauseController) core.TelemetryPublisher {
	if next == nil || pc == nil {
		return next
	}
	if existing, ok := next.(*gatedPublisher); ok && existing.pc == pc {
		return next
	}
	return &gatedPublisher{
		next: next,
		pc:   pc,
	}
}

func (g *gatedPublisher) Publish(ev *core.CTDPayload) error {
	if ev == nil || g.next == nil {
		return nil
	}
	if g.pc.IsPaused() && (ev.Pillar == "" || ev.Pillar == core.PillarObserver) {
		return nil
	}
	return g.next.Publish(ev)
}
