// File: pkg/core/ctd.go
package core

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
)

var (
	validTopicTokenRe = regexp.MustCompile(`^[a-zA-Z0-9._:-]{1,128}$`)
	validMeshNameRe   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// DefaultMeshName is the default Mesh Name System (MNS) identifier when mesh_name is omitted.
const DefaultMeshName = "default"

// ValidateMeshName enforces RFC 1123 lowercase kebab-case DNS hostname label rules (1..63 bytes,
// strictly [a-z0-9-], starting and ending with an alphanumeric character).
func ValidateMeshName(name string) error {
	if name == "" {
		return fmt.Errorf("mesh name cannot be empty")
	}
	if len(name) > 63 {
		return fmt.Errorf("mesh name %q exceeds 63-byte RFC 1123 hostname label limit (%d bytes)", name, len(name))
	}
	if !validMeshNameRe.MatchString(name) {
		return fmt.Errorf("mesh name %q must be lowercase kebab-case RFC 1123 hostname label (^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$)", name)
	}
	return nil
}

// MeshIdentity represents the registered Mesh Name System (MNS) record advertised by PigeonCoop
// in the PostgreSQL CTD Ledger and on the retained MQTT mesh registry topic.
type MeshIdentity struct {
	MeshName   string    `json:"mesh_name"`
	CoopNodeID string    `json:"coop_node_id"`
	TopicRoot  string    `json:"topic_root,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ValidateTopicToken enforces safe MQTT topic-segment identifiers (node IDs, pillars, components, tags, runner names).
// Explicitly rejects wildcards (+, #), path separators (/), NUL bytes, whitespace, and control characters.
func ValidateTopicToken(token string) error {
	if token == "" {
		return fmt.Errorf("topic token cannot be empty")
	}
	for i := 0; i < len(token); i++ {
		b := token[i]
		if b == '/' || b == '+' || b == '#' || b == 0x00 || b <= 0x20 || b == 0x7f {
			return fmt.Errorf("topic token %q contains forbidden character %q", token, b)
		}
	}
	if !validTopicTokenRe.MatchString(token) {
		return fmt.Errorf("topic token %q must match ^[a-zA-Z0-9._:-]{1,128}$", token)
	}
	return nil
}

const (
	// DefaultTopicPrefix is the canonical MQTT namespace for CTD wide events.
	DefaultTopicPrefix = "pigeons/ctd"
	// DefaultTelemetryWildcard subscribes to all pillars, nodes, and components.
	DefaultTelemetryWildcard = "pigeons/ctd/#"
	// DefaultMeshTopic is the retained MQTT topic advertising PigeonCoop's MNS identity.
	DefaultMeshTopic = "pigeons/registry/mesh"
	// RegistryWildcard subscribes to LWT / presence state changes across all nodes.
	RegistryWildcard = "pigeons/registry/+/status"
	// HelloWildcard subscribes to boot metadata & tag announcements across all nodes.
	HelloWildcard = "pigeons/registry/+/hello"
	// DefaultRunnerRequestTopic is the Coop control-plane ingress topic for RunnerRequestCTD.
	DefaultRunnerRequestTopic = "pigeons/requests/runner"
)

// MeshTopic returns the retained MQTT topic advertising PigeonCoop's MNS identity.
func MeshTopic() string {
	return brand.Active().MeshTopic()
}

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
	RequestID       string         `json:"request_id"`
	TargetRequestID string         `json:"target_request_id,omitempty"`
	Action          string         `json:"action"` // "start" or "abort"
	Runner          string         `json:"runner"`
	Selectors       RunnerSelector `json:"selectors"`
	Params          map[string]any `json:"params,omitempty"`
	RequestedBy     string         `json:"requested_by,omitempty"`
	RequestedAt     time.Time      `json:"requested_at"`
}

func (r RunnerRequestCTD) Validate() error {
	if strings.TrimSpace(r.RequestID) == "" {
		return fmt.Errorf("missing request_id")
	}
	if err := ValidateTopicToken(r.Runner); err != nil {
		return fmt.Errorf("invalid runner name: %w", err)
	}
	if r.RequestedBy != "" {
		if err := ValidateTopicToken(r.RequestedBy); err != nil {
			return fmt.Errorf("invalid requested_by: %w", err)
		}
	}
	if r.Action != "" && r.Action != "start" && r.Action != "abort" {
		return fmt.Errorf("invalid runner request action: %s", r.Action)
	}
	if r.Selectors.IsEmpty() {
		return fmt.Errorf("runner request requires at least one selector (--all, --node, or --tag)")
	}
	for _, n := range r.Selectors.Nodes {
		if err := ValidateTopicToken(n); err != nil {
			return fmt.Errorf("invalid selector node: %w", err)
		}
	}
	for _, tag := range r.Selectors.Tags {
		if err := ValidateTopicToken(tag); err != nil {
			return fmt.Errorf("invalid selector tag: %w", err)
		}
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

	rawMetrics json.RawMessage
	rawParams  json.RawMessage
}

type wireCTDPayload struct {
	EventID        string          `json:"event_id,omitempty"`
	NodeID         string          `json:"node_id"`
	Pillar         Pillar          `json:"pillar"`
	Component      string          `json:"component"`
	Status         string          `json:"status"`
	EventTimestamp time.Time       `json:"event_timestamp"`
	Metrics        json.RawMessage `json:"metrics,omitempty"`
	Params         json.RawMessage `json:"params,omitempty"`
	Artifact       *Artifact       `json:"artifact,omitempty"`
	Trace          *TraceEdge      `json:"trace,omitempty"`
}

func cloneRawJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return nil, fmt.Errorf("expected JSON object, got %q", trimmed[:min(len(trimmed), 24)])
	}
	cp := make(json.RawMessage, len(trimmed))
	copy(cp, trimmed)
	return cp, nil
}

// UnmarshalJSON decodes a CTDPayload while preserving the raw JSON slices for metrics and params
// so downstream storage sinks can avoid redundant re-marshaling.
func (c *CTDPayload) UnmarshalJSON(data []byte) error {
	var w wireCTDPayload
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	rawMetrics, err := cloneRawJSONObject(w.Metrics)
	if err != nil {
		return fmt.Errorf("decode metrics: %w", err)
	}
	rawParams, err := cloneRawJSONObject(w.Params)
	if err != nil {
		return fmt.Errorf("decode params: %w", err)
	}

	var metrics map[string]any
	if len(rawMetrics) > 0 {
		if err := json.Unmarshal(rawMetrics, &metrics); err != nil {
			return fmt.Errorf("decode metrics map: %w", err)
		}
	}
	var params map[string]string
	if len(rawParams) > 0 {
		if err := json.Unmarshal(rawParams, &params); err != nil {
			return fmt.Errorf("decode params map: %w", err)
		}
	}

	*c = CTDPayload{
		EventID:        w.EventID,
		NodeID:         w.NodeID,
		Pillar:         w.Pillar,
		Component:      w.Component,
		Status:         w.Status,
		EventTimestamp: w.EventTimestamp,
		Metrics:        metrics,
		Params:         params,
		Artifact:       w.Artifact,
		Trace:          w.Trace,
		rawMetrics:     rawMetrics,
		rawParams:      rawParams,
	}
	return nil
}

// DecodeWireCTDPayload parses only the top-level CTD envelope fields (plus optional Artifact/Trace)
// and retains raw metrics/params JSON slices without allocating Go maps on the high-throughput ingest path.
func DecodeWireCTDPayload(data []byte) (*CTDPayload, error) {
	var w wireCTDPayload
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	rawMetrics, err := cloneRawJSONObject(w.Metrics)
	if err != nil {
		return nil, fmt.Errorf("decode metrics: %w", err)
	}
	rawParams, err := cloneRawJSONObject(w.Params)
	if err != nil {
		return nil, fmt.Errorf("decode params: %w", err)
	}
	return &CTDPayload{
		EventID:        w.EventID,
		NodeID:         w.NodeID,
		Pillar:         w.Pillar,
		Component:      w.Component,
		Status:         w.Status,
		EventTimestamp: w.EventTimestamp,
		Artifact:       w.Artifact,
		Trace:          w.Trace,
		rawMetrics:     rawMetrics,
		rawParams:      rawParams,
	}, nil
}

// RawMetrics returns the preserved wire JSON bytes for metrics when available and unmutated.
func (c *CTDPayload) RawMetrics() []byte {
	if c == nil || len(c.rawMetrics) == 0 {
		return nil
	}
	return c.rawMetrics
}

// RawParams returns the preserved wire JSON bytes for params when available and unmutated.
func (c *CTDPayload) RawParams() []byte {
	if c == nil || len(c.rawParams) == 0 {
		return nil
	}
	return c.rawParams
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
	c.rawMetrics = nil
	return c
}

func (c *CTDPayload) AddParam(key, val string) *CTDPayload {
	if c.Params == nil {
		c.Params = make(map[string]string)
	}
	c.Params[key] = val
	c.rawParams = nil
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
