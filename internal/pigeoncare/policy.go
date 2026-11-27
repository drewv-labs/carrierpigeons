package pigeoncare

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

// PermissionMode emulates Google Antigravity / Zed agent autonomy tiers.
type PermissionMode string

const (
	ModeDefault  PermissionMode = "default"
	ModeAutoEdit PermissionMode = "auto_edit"
	ModeYOLO     PermissionMode = "yolo"
)

// ParseMode normalizes user/config mode strings into a canonical PermissionMode.
func ParseMode(raw string) PermissionMode {
	if mode, err := ParseModeStrict(raw); err == nil {
		return mode
	}
	return ModeDefault
}

// ParseModeStrict parses a non-empty permission mode string and rejects unrecognized values.
func ParseModeStrict(raw string) (PermissionMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default":
		return ModeDefault, nil
	case "yolo":
		return ModeYOLO, nil
	case "auto_edit", "auto-edit", "autoedit", "auto":
		return ModeAutoEdit, nil
	default:
		return "", fmt.Errorf("invalid permission mode %q (expected 'default', 'auto_edit', or 'yolo')", raw)
	}
}

// DisplayLabel returns the human-friendly badge label for TUI rendering.
func (m PermissionMode) DisplayLabel() string {
	switch m {
	case ModeYOLO:
		return "YOLO"
	case ModeAutoEdit:
		return "Auto Edit"
	default:
		return "Default"
	}
}

// Next cycles Default -> Auto Edit -> YOLO -> Default.
func (m PermissionMode) Next() PermissionMode {
	switch m {
	case ModeDefault:
		return ModeAutoEdit
	case ModeAutoEdit:
		return ModeYOLO
	default:
		return ModeDefault
	}
}

// ConfirmationDecision represents an operator's interactive response to an agent action prompt.
type ConfirmationDecision string

const (
	DecisionAllow       ConfirmationDecision = "allow"
	DecisionAlwaysAllow ConfirmationDecision = "always_allow"
	DecisionDeny        ConfirmationDecision = "deny"
)

// ActionKind classifies gated side-effect proposals from PigeonCare.
type ActionKind string

const (
	ActionEphemeralRunner ActionKind = "ephemeral_runner"
	ActionStandardRunner  ActionKind = "standard_runner"
	ActionCreatePMV       ActionKind = "create_pmv"
)

// ActionProposal describes a pending runner dispatch or PigeonMath view creation awaiting PolicyGate evaluation.
type ActionProposal struct {
	Kind        ActionKind          `json:"kind"`
	Title       string              `json:"title"`
	Runner      string              `json:"runner,omitempty"`
	Selectors   core.RunnerSelector `json:"selectors,omitempty"`
	Interpreter string              `json:"interpreter,omitempty"`
	ScriptBody  string              `json:"script_body,omitempty"`
	SHA256      string              `json:"sha256,omitempty"`
	TimeoutSec  int                 `json:"timeout_sec,omitempty"`
	Env         map[string]string   `json:"env,omitempty"`
	ViewName    string              `json:"view_name,omitempty"`
	RawSQL      string              `json:"raw_sql,omitempty"`
}

// Prompter asks the human operator `[Allow, Always Allow, Deny]` when PolicyGate requires confirmation.
type Prompter func(ctx context.Context, proposal ActionProposal) (ConfirmationDecision, error)

type ephemeralApprovalKey struct {
	interpreter string
	sha256      string
	selectors   string
}

type canonicalSelector struct {
	All   bool     `json:"all"`
	Nodes []string `json:"nodes,omitempty"`
	Tags  []string `json:"tags,omitempty"`
}

func canonicalizeSelectors(sel core.RunnerSelector) string {
	var nodes []string
	if len(sel.Nodes) > 0 {
		seen := make(map[string]struct{}, len(sel.Nodes))
		nodes = make([]string, 0, len(sel.Nodes))
		for _, raw := range sel.Nodes {
			n := strings.TrimSpace(raw)
			if n == "" {
				continue
			}
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				nodes = append(nodes, n)
			}
		}
		slices.Sort(nodes)
	}
	tags := core.NormalizeTags(sel.Tags)
	if len(tags) == 0 {
		tags = nil
	}
	raw, _ := json.Marshal(canonicalSelector{
		All:   sel.All,
		Nodes: nodes,
		Tags:  tags,
	})
	return string(raw)
}

func isValidSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func normalizeProposalScript(proposal ActionProposal) (interp string, bodyHash string, boundHash string, key ephemeralApprovalKey, valid bool) {
	interp = runner.NormalizeInterpreter(proposal.Interpreter)
	if interp == "" {
		return "", "", "", ephemeralApprovalKey{}, false
	}
	if strings.TrimSpace(proposal.ScriptBody) == "" {
		return "", "", "", ephemeralApprovalKey{}, false
	}

	sha := strings.ToLower(strings.TrimSpace(proposal.SHA256))
	computedBody := runner.ComputeScriptSHA256(proposal.ScriptBody)
	if sha == "" {
		sha = computedBody
	} else if sha != computedBody {
		return "", "", "", ephemeralApprovalKey{}, false
	}
	boundHash = runner.ComputeBoundScriptSHA256(interp, proposal.ScriptBody)
	if !isValidSHA256Hex(sha) {
		return "", "", "", ephemeralApprovalKey{}, false
	}

	return interp, sha, boundHash, ephemeralApprovalKey{
		interpreter: interp,
		sha256:      sha,
		selectors:   canonicalizeSelectors(proposal.Selectors),
	}, true
}

func normalizeAllowedViewKey(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if norm, err := db.NormalizePMVName(trimmed); err == nil {
		return norm
	}
	return strings.ToLower(trimmed)
}

// PolicyGate enforces `[Default, Auto Edit, YOLO]` modes and session/config allowlists.
type PolicyGate struct {
	mu                   sync.RWMutex
	mode                 PermissionMode
	allowedRunners       map[string]struct{}
	allowedScriptHashes  map[string]struct{}
	alwaysAllowedScripts map[ephemeralApprovalKey]struct{}
	allowedViews         map[string]struct{}
	prompter             Prompter
}

// NewPolicyGate initializes a PolicyGate from WorkstationConfig permission settings.
func NewPolicyGate(cfg config.PermissionConfig, prompter Prompter) *PolicyGate {
	g := &PolicyGate{
		mode:                 ParseMode(cfg.Mode),
		allowedRunners:       make(map[string]struct{}),
		allowedScriptHashes:  make(map[string]struct{}),
		alwaysAllowedScripts: make(map[ephemeralApprovalKey]struct{}),
		allowedViews:         make(map[string]struct{}),
		prompter:             prompter,
	}
	for _, r := range cfg.AllowedRunners {
		if trimmed := strings.ToLower(strings.TrimSpace(r)); trimmed != "" {
			g.allowedRunners[trimmed] = struct{}{}
		}
	}
	for _, h := range cfg.AllowedScriptHashes {
		if norm, ok := runner.NormalizeAllowedScriptHashEntry(h); ok {
			g.allowedScriptHashes[norm] = struct{}{}
		}
	}
	return g
}

func (g *PolicyGate) Mode() PermissionMode {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.mode
}

func (g *PolicyGate) SetMode(m PermissionMode) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mode = m
}

func (g *PolicyGate) CycleMode() PermissionMode {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mode = g.mode.Next()
	return g.mode
}

func (g *PolicyGate) SetPrompter(p Prompter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prompter = p
}

func (g *PolicyGate) isConfigScriptHashAllowedLocked(interp, bodyHash, boundHash string) bool {
	if _, ok := g.allowedScriptHashes[interp+":"+bodyHash]; ok {
		return true
	}
	if boundHash != "" {
		if _, ok := g.allowedScriptHashes[interp+":"+boundHash]; ok {
			return true
		}
		if _, ok := g.allowedScriptHashes[boundHash]; ok {
			return true
		}
	}
	return false
}

// Authorize evaluates whether `proposal` may execute immediately or requires `[Allow / Always Allow / Deny]`.
func (g *PolicyGate) Authorize(ctx context.Context, proposal ActionProposal) error {
	switch proposal.Kind {
	case ActionEphemeralRunner, ActionStandardRunner, ActionCreatePMV:
	default:
		return fmt.Errorf("unsupported action proposal kind %q", proposal.Kind)
	}

	var (
		interp    string
		bodyHash  string
		boundHash string
		scriptKey ephemeralApprovalKey
		scriptOK  bool
	)
	if proposal.Kind == ActionEphemeralRunner {
		interp, bodyHash, boundHash, scriptKey, scriptOK = normalizeProposalScript(proposal)
		if !scriptOK {
			return fmt.Errorf("ephemeral runner proposal %q failed script/hash validation", proposal.Title)
		}
	}

	runnerKey := strings.ToLower(strings.TrimSpace(proposal.Runner))
	viewKey := normalizeAllowedViewKey(proposal.ViewName)

	g.mu.RLock()
	mode := g.mode
	prompter := g.prompter
	_, runnerAllowed := g.allowedRunners[runnerKey]
	scriptAllowed := false
	if scriptOK {
		if _, ok := g.alwaysAllowedScripts[scriptKey]; ok {
			scriptAllowed = true
		} else if g.isConfigScriptHashAllowedLocked(interp, bodyHash, boundHash) {
			scriptAllowed = true
		}
	}
	_, viewAllowed := g.allowedViews[viewKey]
	g.mu.RUnlock()

	// 1. YOLO mode grants autonomous execution for validated ephemeral runners, standard runners, and PMV creation.
	if mode == ModeYOLO {
		return nil
	}

	// 2. Check existing session / config allowlists.
	switch proposal.Kind {
	case ActionEphemeralRunner:
		if scriptAllowed {
			return nil
		}
	case ActionStandardRunner:
		if runnerAllowed {
			return nil
		}
	case ActionCreatePMV:
		if mode == ModeAutoEdit || viewAllowed {
			return nil
		}
	}

	// 3. Prompt human operator `[Allow, Always Allow, Deny]`.
	if prompter == nil {
		return fmt.Errorf("action %q blocked under %s mode (no interactive confirmation prompter attached)",
			proposal.Title, mode.DisplayLabel())
	}

	decision, err := prompter(ctx, proposal)
	if err != nil {
		return fmt.Errorf("confirmation aborted: %w", err)
	}

	switch decision {
	case DecisionAllow:
		return nil
	case DecisionAlwaysAllow:
		g.mu.Lock()
		switch proposal.Kind {
		case ActionEphemeralRunner:
			if scriptOK {
				g.alwaysAllowedScripts[scriptKey] = struct{}{}
			}
		case ActionStandardRunner:
			if runnerKey != "" {
				g.allowedRunners[runnerKey] = struct{}{}
			}
		case ActionCreatePMV:
			if viewKey != "" {
				g.allowedViews[viewKey] = struct{}{}
			}
		}
		g.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("operator denied %s", proposal.Title)
	}
}
