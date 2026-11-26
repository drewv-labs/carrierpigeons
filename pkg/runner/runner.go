package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
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
	AllowedScriptHashes []string
	MaxTimeoutSec       int
}

type policyContextKey struct{}

// DefaultExecutionPolicy returns safe-by-default runner execution limits (ephemeral scripts disabled unless explicitly opted in).
func DefaultExecutionPolicy() ExecutionPolicy {
	return ExecutionPolicy{
		AllowEphemeral:      false,
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

// Registry provides concurrency-safe runner registration and alias resolution for SDK instances.
type Registry struct {
	mu    sync.RWMutex
	items map[string]Runner
}

// NewRegistry constructs an empty Runner Registry.
func NewRegistry() *Registry {
	return &Registry{
		items: make(map[string]Runner),
	}
}

// Clone returns an independent copy of r containing all registered runners and aliases.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make(map[string]Runner, len(r.items))
	maps.Copy(cp, r.items)
	return &Registry{items: cp}
}

// Register binds a runner by its canonical Name().
func (r *Registry) Register(task Runner) {
	if task == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[task.Name()] = task
}

// RegisterAlias binds an additional lookup alias (e.g., "SyntheticComputeRnr") to an existing Runner.
func (r *Registry) RegisterAlias(alias string, task Runner) {
	if task == nil || strings.TrimSpace(alias) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[strings.TrimSpace(alias)] = task
}

// Get safely retrieves a registered runner by name or case-insensitive alias.
func (r *Registry) Get(name string) (Runner, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if task, ok := r.items[name]; ok {
		return task, true
	}
	lower := strings.ToLower(strings.TrimSpace(name))
	for k, task := range r.items {
		if strings.ToLower(k) == lower {
			return task, true
		}
	}
	return nil, false
}

// CanonicalKey returns the canonical concurrency key (`strings.ToLower(task.Name())`) for any runner or alias.
func (r *Registry) CanonicalKey(name string) string {
	if task, ok := r.Get(name); ok {
		return strings.ToLower(strings.TrimSpace(task.Name()))
	}
	return strings.ToLower(strings.TrimSpace(name))
}

// Names returns a sorted list of all registered runner keys and aliases in r.
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

var defaultRegistry = NewRegistry()

// CloneDefaultRegistry returns an isolated copy of the process-wide default Runner Registry.
func CloneDefaultRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Register is called in init() functions or SDK setup to bind a runner to the default runtime registry.
func Register(r Runner) {
	defaultRegistry.Register(r)
}

// RegisterAlias binds an additional lookup alias to an existing Runner in the default registry.
func RegisterAlias(alias string, r Runner) {
	defaultRegistry.RegisterAlias(alias, r)
}

// Get safely retrieves a registered runner by name (including case-insensitive alias lookup).
func Get(name string) (Runner, bool) {
	return defaultRegistry.Get(name)
}

// CanonicalKey returns the canonical concurrency key for a runner or alias in the default registry.
func CanonicalKey(name string) string {
	return defaultRegistry.CanonicalKey(name)
}

// RegistryNames returns a sorted list of all currently registered runners.
func RegistryNames() []string {
	return defaultRegistry.Names()
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
	if err := core.ValidateTopicToken(e.Runner); err != nil {
		return fmt.Errorf("invalid runner execution name: %w", err)
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

// NormalizeInterpreter normalizes an ephemeral interpreter token (defaulting empty to "sh")
// and returns "" if the token is syntactically invalid (e.g., "." or "..").
func NormalizeInterpreter(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "sh"
	}
	base := strings.ToLower(strings.TrimSpace(filepath.Base(trimmed)))
	if base == "" || base == "." || base == ".." || base == string(filepath.Separator) {
		return ""
	}
	return base
}

// ComputeScriptSHA256 returns the lowercase hex SHA-256 digest of a script body.
func ComputeScriptSHA256(scriptBody string) string {
	sum := sha256.Sum256([]byte(scriptBody))
	return hex.EncodeToString(sum[:])
}

// ComputeBoundScriptSHA256 returns the lowercase hex SHA-256 digest binding interpreter and script body
// (`sha256(interpreter + "\x00" + scriptBody)`), preventing cross-interpreter hash reuse.
func ComputeBoundScriptSHA256(interpreter, scriptBody string) string {
	norm := NormalizeInterpreter(interpreter)
	if norm == "" {
		norm = strings.ToLower(strings.TrimSpace(interpreter))
	}
	h := sha256.New()
	_, _ = h.Write([]byte(norm))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(scriptBody))
	return hex.EncodeToString(h.Sum(nil))
}

// NormalizeAllowedScriptHashEntry canonicalizes an allowed_script_hashes policy entry.
// Prefixed entries ("<interpreter>:<sha256>") trim whitespace around ":" and normalize non-empty
// interpreters via NormalizeInterpreter (e.g. "/usr/bin/sh : <HASH>" -> "sh:<hash>").
func NormalizeAllowedScriptHashEntry(raw string) (string, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return "", false
	}
	if rawInterp, rawHash, hasColon := strings.Cut(trimmed, ":"); hasColon {
		rawInterp = strings.TrimSpace(rawInterp)
		normInterp := NormalizeInterpreter(rawInterp)
		if rawInterp == "" || normInterp == "" {
			return "", false
		}
		hash := strings.TrimSpace(rawHash)
		if hash == "" {
			return "", false
		}
		return normInterp + ":" + hash, true
	}
	return trimmed, true
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
