// File: pkg/core/ctd.go
package core

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
)

const (
	// DefaultTopicPrefix is the canonical MQTT namespace for CTD wide events.
	DefaultTopicPrefix = "pigeons/ctd"
	// DefaultTelemetryWildcard subscribes to all pillars, nodes, and components.
	DefaultTelemetryWildcard = "pigeons/ctd/#"
	// RegistryWildcard subscribes to LWT / presence state changes across all nodes.
	RegistryWildcard = "pigeons/registry/+/status"
	// HelloWildcard subscribes to boot metadata & tag announcements across all nodes.
	HelloWildcard = "pigeons/registry/+/hello"
	// DefaultRunnerRequestTopic is the Coop control-plane ingress topic for RunnerRequestCTD.
	DefaultRunnerRequestTopic = "pigeons/requests/runner"
)

// RegistryTopic returns the MQTT presence/LWT topic for a given edge node.
func RegistryTopic(nodeID string) string {
	return brand.Active().RegistryTopic(nodeID)
}

// HelloTopic returns the MQTT metadata & boot-tag announcement topic for a given node.
func HelloTopic(nodeID string) string {
	return brand.Active().HelloTopic(nodeID)
}

// ControlTopic returns the MQTT runner control dispatch topic for a given edge node.
func ControlTopic(nodeID string) string {
	return brand.Active().ControlTopic(nodeID)
}

// RunnerRequestTopic returns the MQTT topic where CLI/Agent clients submit RunnerRequestCTD payloads to Coop.
func RunnerRequestTopic() string {
	return brand.Active().RunnerRequestTopic()
}

// NormalizeTags trims, lowercases, deduplicates, and sorts a tag slice deterministically.
func NormalizeTags(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, raw := range tags {
		t := strings.ToLower(strings.TrimSpace(raw))
		if t == "" {
			continue
		}
		if _, exists := seen[t]; !exists {
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	slices.Sort(out)
	return out
}

// NodeHello announces node hardware capabilities and config-defined boot tags upon connection.
type NodeHello struct {
	NodeID    string            `json:"node_id"`
	Arch      string            `json:"arch"`
	OS        string            `json:"os"`
	NumCPU    int               `json:"num_cpu"`
	BootTags  []string          `json:"boot_tags"`
	Meta      map[string]string `json:"meta,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// RunnerSelector scopes which nodes in the mesh should execute or abort a RunnerRequestCTD.
type RunnerSelector struct {
	All   bool     `json:"all,omitempty"`
	Nodes []string `json:"nodes,omitempty"`
	Tags  []string `json:"tags,omitempty"`
}

func (s RunnerSelector) IsEmpty() bool {
	return !s.All && len(s.Nodes) == 0 && len(s.Tags) == 0
}

// RunnerRequestCTD is submitted by Pigeons CLI/TUI or PigeonCare to PigeonCoop
// to resolve node/tag targets, record lineage, and fan out runner execution.
type RunnerRequestCTD struct {
	RequestID   string         `json:"request_id"`
	Action      string         `json:"action"` // "start" or "abort"
	Runner      string         `json:"runner"`
	Selectors   RunnerSelector `json:"selectors"`
	Params      map[string]any `json:"params,omitempty"`
	RequestedBy string         `json:"requested_by,omitempty"`
	RequestedAt time.Time      `json:"requested_at"`
}

func (r RunnerRequestCTD) Validate() error {
	if strings.TrimSpace(r.RequestID) == "" {
		return fmt.Errorf("missing request_id")
	}
	if strings.TrimSpace(r.Runner) == "" {
		return fmt.Errorf("missing runner name")
	}
	if r.Action != "" && r.Action != "start" && r.Action != "abort" {
		return fmt.Errorf("invalid runner request action: %s", r.Action)
	}
	if r.Selectors.IsEmpty() {
		return fmt.Errorf("runner request requires at least one selector (--all, --node, or --tag)")
	}
	return nil
}

// Pillar defines the core SDK primitive type emitting the event.
type Pillar string

const (
	PillarObserver Pillar = "observer"
	PillarRunner   Pillar = "runner"
	PillarTracer   Pillar = "tracer"
)

// Artifact represents any tangible Noun (hardware node, dataset, telemetry snapshot, script, PMV) in the DAG.
type Artifact struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"` // e.g., "node", "dataset", "ephemeral_runner", "pmv"
	NodeID    string            `json:"node_id,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// TraceEdge represents a directed Verb connecting two Artifacts across time.
type TraceEdge struct {
	SourceID  string            `json:"source_id"`
	Verb      string            `json:"verb"` // e.g., "dispatched_to", "certified_as", "transformed_into"
	TargetID  string            `json:"target_id"`
	NodeID    string            `json:"node_id,omitempty"`
	RunID     string            `json:"run_id,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// CTDPayload is the immutable wire envelope transmitted over MQTT and stored in the Ledger.
type CTDPayload struct {
	EventID        string            `json:"event_id,omitempty"`
	NodeID         string            `json:"node_id"`
	Pillar         Pillar            `json:"pillar"`
	Component      string            `json:"component"`
	Status         string            `json:"status"`
	EventTimestamp time.Time         `json:"event_timestamp"`
	Metrics        map[string]any    `json:"metrics,omitempty"`
	Params         map[string]string `json:"params,omitempty"`
	Artifact       *Artifact         `json:"artifact,omitempty"`
	Trace          *TraceEdge        `json:"trace,omitempty"`
}

// NewCTDPayload initializes a standard Observer/Runner payload.
func NewCTDPayload(nodeID, component, status string) *CTDPayload {
	return &CTDPayload{
		NodeID:         nodeID,
		Pillar:         PillarObserver,
		Component:      component,
		Status:         status,
		EventTimestamp: time.Now().UTC(),
		Metrics:        make(map[string]any),
		Params:         make(map[string]string),
	}
}

func (c *CTDPayload) WithPillar(p Pillar) *CTDPayload {
	c.Pillar = p
	return c
}

func (c *CTDPayload) WithArtifact(a Artifact) *CTDPayload {
	if a.CreatedAt.IsZero() {
		a.CreatedAt = c.EventTimestamp
	}
	c.Artifact = &a
	return c
}

func (c *CTDPayload) WithTrace(sourceID, verb, targetID string, meta map[string]string) *CTDPayload {
	c.Pillar = PillarTracer
	c.Trace = &TraceEdge{
		SourceID:  sourceID,
		Verb:      verb,
		TargetID:  targetID,
		NodeID:    c.NodeID,
		Meta:      meta,
		Timestamp: c.EventTimestamp,
	}
	return c
}

func (c *CTDPayload) AddMetric(key string, val any) *CTDPayload {
	if c.Metrics == nil {
		c.Metrics = make(map[string]any)
	}
	c.Metrics[key] = val
	return c
}

func (c *CTDPayload) AddParam(key, val string) *CTDPayload {
	if c.Params == nil {
		c.Params = make(map[string]string)
	}
	c.Params[key] = val
	return c
}

func (c *CTDPayload) UpdateStatus(status string) *CTDPayload {
	c.Status = status
	c.EventTimestamp = time.Now().UTC()
	return c
}

func (c *CTDPayload) RoutingTopic(prefix string) string {
	if prefix == "" {
		prefix = brand.Active().CTDPrefix()
	}
	pillar := c.Pillar
	if pillar == "" {
		pillar = PillarObserver
	}
	return fmt.Sprintf("%s/%s/%s/%s", prefix, pillar, c.NodeID, c.Component)
}

func (c *CTDPayload) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}
