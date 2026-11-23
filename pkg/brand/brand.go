package brand

import (
	"fmt"
	"strings"
	"sync"
)

// Spec defines the whitelabel identity and namespace parameters for CarrierPigeons
// or any downstream appliance/SDK wrapper built on top of it.
type Spec struct {
	ProductName    string `toml:"product_name" json:"product_name"`
	CoopName       string `toml:"coop_name" json:"coop_name"`
	EdgeName       string `toml:"edge_name" json:"edge_name"`
	CLIName        string `toml:"cli_name" json:"cli_name"`
	AgentName      string `toml:"agent_name" json:"agent_name"`
	MCPName        string `toml:"mcp_name" json:"mcp_name"`
	MathName       string `toml:"math_name" json:"math_name"`
	TopicRoot      string `toml:"topic_root" json:"topic_root"`
	EnvPrefix      string `toml:"env_prefix" json:"env_prefix"`
	MathSchema     string `toml:"math_schema" json:"math_schema"`
	AccentColor    string `toml:"accent_color" json:"accent_color"`
	SecondaryColor string `toml:"secondary_color" json:"secondary_color"`
	MutedColor     string `toml:"muted_color" json:"muted_color"`
}

// Default returns the upstream CarrierPigeons identity specification.
func Default() Spec {
	return Spec{
		ProductName:    "CarrierPigeons",
		CoopName:       "PigeonCoop",
		EdgeName:       "Pigeoneer",
		CLIName:        "pigeons",
		AgentName:      "PigeonCare",
		MCPName:        "PigeonMCP",
		MathName:       "PigeonMath",
		TopicRoot:      "pigeons",
		EnvPrefix:      "PIGEONS",
		MathSchema:     "pigeonmath",
		AccentColor:    "86",
		SecondaryColor: "205",
		MutedColor:     "240",
	}
}

// MergeWithDefaults fills any blank fields in s using Default().
func (s Spec) MergeWithDefaults() Spec {
	d := Default()
	if strings.TrimSpace(s.ProductName) == "" {
		s.ProductName = d.ProductName
	}
	if strings.TrimSpace(s.CoopName) == "" {
		s.CoopName = d.CoopName
	}
	if strings.TrimSpace(s.EdgeName) == "" {
		s.EdgeName = d.EdgeName
	}
	if strings.TrimSpace(s.CLIName) == "" {
		s.CLIName = d.CLIName
	}
	if strings.TrimSpace(s.AgentName) == "" {
		s.AgentName = d.AgentName
	}
	if strings.TrimSpace(s.MCPName) == "" {
		s.MCPName = d.MCPName
	}
	if strings.TrimSpace(s.MathName) == "" {
		s.MathName = d.MathName
	}
	s.TopicRoot = strings.Trim(strings.TrimSpace(s.TopicRoot), "/")
	if s.TopicRoot == "" {
		s.TopicRoot = d.TopicRoot
	}
	s.EnvPrefix = strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(s.EnvPrefix)), "_")
	if s.EnvPrefix == "" {
		s.EnvPrefix = d.EnvPrefix
	}
	if strings.TrimSpace(s.MathSchema) == "" {
		s.MathSchema = d.MathSchema
	}
	if strings.TrimSpace(s.AccentColor) == "" {
		s.AccentColor = d.AccentColor
	}
	if strings.TrimSpace(s.SecondaryColor) == "" {
		s.SecondaryColor = d.SecondaryColor
	}
	if strings.TrimSpace(s.MutedColor) == "" {
		s.MutedColor = d.MutedColor
	}
	return s
}

func (s Spec) CTDPrefix() string {
	return s.MergeWithDefaults().TopicRoot + "/ctd"
}

func (s Spec) TelemetryWildcard() string {
	return s.CTDPrefix() + "/#"
}

func (s Spec) MeshTopic() string {
	return fmt.Sprintf("%s/registry/mesh", s.MergeWithDefaults().TopicRoot)
}

func (s Spec) RegistryTopic(nodeID string) string {
	return fmt.Sprintf("%s/registry/%s/status", s.MergeWithDefaults().TopicRoot, nodeID)
}

func (s Spec) RegistryWildcard() string {
	return fmt.Sprintf("%s/registry/+/status", s.MergeWithDefaults().TopicRoot)
}

func (s Spec) HelloTopic(nodeID string) string {
	return fmt.Sprintf("%s/registry/%s/hello", s.MergeWithDefaults().TopicRoot, nodeID)
}

func (s Spec) HelloWildcard() string {
	return fmt.Sprintf("%s/registry/+/hello", s.MergeWithDefaults().TopicRoot)
}

func (s Spec) ControlTopic(nodeID string) string {
	return fmt.Sprintf("%s/control/%s", s.MergeWithDefaults().TopicRoot, nodeID)
}

func (s Spec) RunnerRequestTopic() string {
	return fmt.Sprintf("%s/requests/runner", s.MergeWithDefaults().TopicRoot)
}

var (
	activeMu   sync.RWMutex
	activeSpec = Default()
)

// Active returns the currently active process-wide BrandSpec.
//
// Deprecated: Prefer passing an instance-scoped brand.Spec into coop.New, edge.New,
// controlplane, and mcpserver.New for multi-tenant isolation.
func Active() Spec {
	activeMu.RLock()
	defer activeMu.RUnlock()
	return activeSpec
}

// SetActive updates the process-wide BrandSpec for standalone CLI binaries.
//
// Deprecated: Avoid mutating global brand state in library paths; inject brand.Spec per instance instead.
func SetActive(spec Spec) {
	activeMu.Lock()
	defer activeMu.Unlock()
	activeSpec = spec.MergeWithDefaults()
}
