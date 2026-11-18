package observer

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// Observer represents a passive or periodic telemetry sensor emitting PillarObserver CTD payloads.
type Observer interface {
	Name() string
	Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher)
}

// Registry provides concurrency-safe observer registration and lookup for multi-instance SDK callers.
type Registry struct {
	mu    sync.RWMutex
	items map[string]Observer
}

// NewRegistry creates an empty Observer Registry.
func NewRegistry() *Registry {
	return &Registry{
		items: make(map[string]Observer),
	}
}

// Clone copies all currently registered observers into an independent Registry instance.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make(map[string]Observer, len(r.items))
	maps.Copy(cp, r.items)
	return &Registry{items: cp}
}

// Register binds an Observer implementation into r.
func (r *Registry) Register(o Observer) {
	if o == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[o.Name()] = o
}

// Get retrieves a registered Observer by name.
func (r *Registry) Get(name string) (Observer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	o, ok := r.items[name]
	return o, ok
}

// Names returns a sorted list of all registered observer names in r.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.items))
	for name := range r.items {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// StartAll launches every registered Observer in r concurrently under ctx.
func (r *Registry) StartAll(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	r.mu.RLock()
	list := make([]Observer, 0, len(r.items))
	for _, o := range r.items {
		list = append(list, o)
	}
	r.mu.RUnlock()

	for _, o := range list {
		obs := o
		go obs.Observe(ctx, nodeID, pub)
	}
}

var defaultRegistry = NewRegistry()

// CloneDefaultRegistry returns an isolated copy of the process-wide default Observer Registry.
func CloneDefaultRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Register binds an Observer implementation to the default runtime registry.
func Register(o Observer) {
	defaultRegistry.Register(o)
}

// Get retrieves a registered Observer by name from the default runtime registry.
func Get(name string) (Observer, bool) {
	return defaultRegistry.Get(name)
}

// RegistryNames returns a sorted list of all currently registered observer names.
func RegistryNames() []string {
	return defaultRegistry.Names()
}

// StartAll launches every registered Observer concurrently under ctx.
func StartAll(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	defaultRegistry.StartAll(ctx, nodeID, pub)
}
