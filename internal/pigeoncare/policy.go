package pigeoncare

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/drewv-labs/carrier-pigeons/pkg/config"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
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
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "yolo":
		return ModeYOLO
	case "auto_edit", "auto-edit", "autoedit", "auto":
		return ModeAutoEdit
	default:
		return ModeDefault
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
	ViewName    string              `json:"view_name,omitempty"`
	RawSQL      string              `json:"raw_sql,omitempty"`
}

// Prompter asks the human operator `[Allow, Always Allow, Deny]` when PolicyGate requires confirmation.
type Prompter func(ctx context.Context, proposal ActionProposal) (ConfirmationDecision, error)

// PolicyGate enforces `[Default, Auto Edit, YOLO]` modes and session/config allowlists.
type PolicyGate struct {
	mu                  sync.RWMutex
	mode                PermissionMode
	allowedRunners      map[string]struct{}
	allowedScriptHashes map[string]struct{}
	allowedViews        map[string]struct{}
	prompter            Prompter
}

// NewPolicyGate initializes a PolicyGate from WorkstationConfig permission settings.
func NewPolicyGate(cfg config.PermissionConfig, prompter Prompter) *PolicyGate {
	g := &PolicyGate{
		mode:                ParseMode(cfg.Mode),
		allowedRunners:      make(map[string]struct{}),
		allowedScriptHashes: make(map[string]struct{}),
		allowedViews:        make(map[string]struct{}),
		prompter:            prompter,
	}
	for _, r := range cfg.AllowedRunners {
		if trimmed := strings.ToLower(strings.TrimSpace(r)); trimmed != "" {
			g.allowedRunners[trimmed] = struct{}{}
		}
	}
	for _, h := range cfg.AllowedScriptHashes {
		if trimmed := strings.ToLower(strings.TrimSpace(h)); trimmed != "" {
			g.allowedScriptHashes[trimmed] = struct{}{}
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

// Authorize evaluates whether `proposal` may execute immediately or requires `[Allow / Always Allow / Deny]`.
func (g *PolicyGate) Authorize(ctx context.Context, proposal ActionProposal) error {
	g.mu.RLock()
	mode := g.mode
	prompter := g.prompter
	_, runnerAllowed := g.allowedRunners[strings.ToLower(strings.TrimSpace(proposal.Runner))]
	_, hashAllowed := g.allowedScriptHashes[strings.ToLower(strings.TrimSpace(proposal.SHA256))]
	_, viewAllowed := g.allowedViews[strings.ToLower(strings.TrimSpace(proposal.ViewName))]
	g.mu.RUnlock()

	// 1. YOLO mode grants autonomous execution for ephemeral runners, standard runners, and PMV creation.
	if mode == ModeYOLO {
		return nil
	}

	// 2. Check existing session / config allowlists.
	switch proposal.Kind {
	case ActionEphemeralRunner:
		if hashAllowed && proposal.SHA256 != "" {
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
			if proposal.SHA256 != "" {
				g.allowedScriptHashes[strings.ToLower(proposal.SHA256)] = struct{}{}
			}
		case ActionStandardRunner:
			if proposal.Runner != "" {
				g.allowedRunners[strings.ToLower(proposal.Runner)] = struct{}{}
			}
		case ActionCreatePMV:
			if proposal.ViewName != "" {
				g.allowedViews[strings.ToLower(proposal.ViewName)] = struct{}{}
			}
		}
		g.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("operator denied %s", proposal.Title)
	}
}
