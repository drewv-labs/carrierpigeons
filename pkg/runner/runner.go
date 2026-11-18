package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// Runner represents any targeted remote execution task.
type Runner interface {
	Name() string
	Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error
}

// ExecutionPolicy governs node-local sandbox rules for runners (especially ad-hoc ephemeral scripts).
type ExecutionPolicy struct {
	AllowEphemeral      bool
	AllowedInterpreters []string
	MaxTimeoutSec       int
}

type policyContextKey struct{}

// DefaultExecutionPolicy returns safe out-of-the-box runner execution limits.
func DefaultExecutionPolicy() ExecutionPolicy {
	return ExecutionPolicy{
		AllowEphemeral:      true,
		AllowedInterpreters: []string{"sh", "bash", "python3"},
		MaxTimeoutSec:       300,
	}
}

// WithExecutionPolicy attaches node-local runner execution constraints to ctx.
func WithExecutionPolicy(ctx context.Context, p ExecutionPolicy) context.Context {
	return context.WithValue(ctx, policyContextKey{}, p)
}

// PolicyFromContext extracts node-local runner execution constraints or returns DefaultExecutionPolicy().
func PolicyFromContext(ctx context.Context) ExecutionPolicy {
	if ctx != nil {
		if p, ok := ctx.Value(policyContextKey{}).(ExecutionPolicy); ok {
			return p
		}
	}
	return DefaultExecutionPolicy()
}

var (
	mu       sync.RWMutex
	registry = make(map[string]Runner)
)

// Register is called in init() functions or SDK setup to bind a runner to the node runtime.
func Register(r Runner) {
	if r == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	registry[r.Name()] = r
}

// RegisterAlias binds an additional lookup alias (e.g., "SyntheticComputeRnr") to an existing Runner.
func RegisterAlias(alias string, r Runner) {
	if r == nil || strings.TrimSpace(alias) == "" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	registry[strings.TrimSpace(alias)] = r
}

// Get safely retrieves a registered runner by name (including case-insensitive alias lookup).
func Get(name string) (Runner, bool) {
	mu.RLock()
	defer mu.RUnlock()
	if r, ok := registry[name]; ok {
		return r, true
	}
	lower := strings.ToLower(strings.TrimSpace(name))
	for k, r := range registry {
		if strings.ToLower(k) == lower {
			return r, true
		}
	}
	return nil, false
}

// RegistryNames returns a sorted list of all currently registered runners.
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

// Envelope defines the wire protocol for MQTT control dispatches to an individual node.
type Envelope struct {
	RequestID string         `json:"request_id,omitempty"`
	Action    string         `json:"action"` // "start" or "abort"
	Runner    string         `json:"runner"`
	Params    map[string]any `json:"params,omitempty"`
}

// Validate ensures the payload has the minimum required routing data.
func (e Envelope) Validate() error {
	if strings.TrimSpace(e.Runner) == "" {
		return fmt.Errorf("missing runner execution name")
	}
	if e.Action != "" && e.Action != "start" && e.Action != "abort" {
		return fmt.Errorf("invalid action: %s", e.Action)
	}
	return nil
}

// EphemeralSpec describes an ad-hoc script workload synthesized by PigeonCare or an operator.
type EphemeralSpec struct {
	Interpreter string            `json:"interpreter"`
	ScriptBody  string            `json:"script_body"`
	SHA256      string            `json:"sha256"`
	TimeoutSec  int               `json:"timeout_sec,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
}

// ComputeScriptSHA256 returns the lowercase hex SHA-256 digest of a script body.
func ComputeScriptSHA256(scriptBody string) string {
	sum := sha256.Sum256([]byte(scriptBody))
	return hex.EncodeToString(sum[:])
}

// ToParams converts an EphemeralSpec into a runner Envelope Params map.
func (s EphemeralSpec) ToParams() map[string]any {
	hash := s.SHA256
	if hash == "" {
		hash = ComputeScriptSHA256(s.ScriptBody)
	}
	timeout := s.TimeoutSec
	if timeout <= 0 {
		timeout = 30
	}
	interp := strings.TrimSpace(s.Interpreter)
	if interp == "" {
		interp = "sh"
	}
	out := map[string]any{
		"interpreter": interp,
		"script_body": s.ScriptBody,
		"sha256":      hash,
		"timeout_sec": timeout,
	}
	if len(s.Env) > 0 {
		out["env"] = s.Env
	}
	return out
}
