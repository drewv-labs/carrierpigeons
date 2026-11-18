package observer

import (
	"context"
	"slices"
	"sync"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// Observer represents a passive or periodic telemetry sensor emitting PillarObserver CTD payloads.
type Observer interface {
	Name() string
	Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher)
}

var (
	mu       sync.RWMutex
	registry = make(map[string]Observer)
)

// Register binds an Observer implementation to the runtime registry.
func Register(o Observer) {
	if o == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	registry[o.Name()] = o
}

// Get retrieves a registered Observer by name.
func Get(name string) (Observer, bool) {
	mu.RLock()
	defer mu.RUnlock()
	o, ok := registry[name]
	return o, ok
}

// RegistryNames returns a sorted list of all currently registered observer names.
func RegistryNames() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// StartAll launches every registered Observer concurrently under ctx.
func StartAll(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	mu.RLock()
	list := make([]Observer, 0, len(registry))
	for _, o := range registry {
		list = append(list, o)
	}
	mu.RUnlock()

	for _, o := range list {
		obs := o
		go obs.Observe(ctx, nodeID, pub)
	}
}
