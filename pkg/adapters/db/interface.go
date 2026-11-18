// File: pkg/adapters/db/interface.go
package db

import (
	"context"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// NodeSummary represents the materialized state of an edge or hub node for CLI, TUI, and MCP.
type NodeSummary struct {
	NodeID        string         `json:"node_id"`
	Arch          string         `json:"arch,omitempty"`
	OS            string         `json:"os,omitempty"`
	Tags          []string       `json:"tags"`
	Component     string         `json:"component"`
	LastSeen      time.Time      `json:"last_seen"`
	Status        string         `json:"status"`
	LatestMetrics map[string]any `json:"latest_metrics"`
	IsHealthy     bool           `json:"is_healthy"`
}

// DAG represents a reconstructed Artifact & Tracer graph for PigeonMath / lineage authentication.
type DAG struct {
	RootID    string           `json:"root_id"`
	Artifacts []core.Artifact  `json:"artifacts"`
	Edges     []core.TraceEdge `json:"edges"`
}

// UnloadFilter scopes which rows are pulled when exporting to Snappy Parquet.
type UnloadFilter struct {
	NodeIDPattern string
	Pillar        string
	Since         time.Time
	Until         time.Time
	Limit         int
}

// LedgerActivity summarizes actor write/update attribution on the CTD Ledger.
type LedgerActivity struct {
	SourceTable string    `json:"source_table"`
	Action      string    `json:"action"`
	DBActor     string    `json:"db_actor"`
	AppActor    string    `json:"app_actor"`
	Target      string    `json:"target"`
	RecordCount int64     `json:"record_count"`
	LastEventAt time.Time `json:"last_event_at"`
}

// PMVRecord represents a cached & certified PigeonMath analytical view (`pigeonmath.pmv_*`).
type PMVRecord struct {
	ViewName         string         `json:"view_name"`
	SubmittedBy      string         `json:"submitted_by"`
	RawIntentSQL     string         `json:"raw_intent_sql"`
	OptimizedSQL     string         `json:"optimized_sql"`
	Status           string         `json:"status"` // draft | optimizing | validating | certified | failed
	Version          int            `json:"version"`
	AgentNotes       string         `json:"agent_notes"`
	ValidationReport map[string]any `json:"validation_report"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

// ReadOnlyDB is the strictly read-only CTD Ledger contract exposed to PigeonCare and PigeonMCP.
// Implementations MUST enforce PostgreSQL READ ONLY transaction semantics against public tables.
type ReadOnlyDB interface {
	GetClusterStatus(ctx context.Context, staleThreshold time.Duration) ([]NodeSummary, error)
	ResolveNodes(ctx context.Context, sel core.RunnerSelector) ([]string, error)
	GetLineageDAG(ctx context.Context, artifactID string, maxDepth int) (*DAG, error)
	GetLedgerActivity(ctx context.Context, limit int) ([]LedgerActivity, error)
	QueryReadOnlyMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error)
	ExplainReadOnly(ctx context.Context, selectSQL string) (string, error)
}

// MathDB defines operations isolated strictly to the `pigeonmath` analytical schema.
type MathDB interface {
	EnsureMathSchema(ctx context.Context) error
	SavePMVDraft(ctx context.Context, rec PMVRecord) (*PMVRecord, error)
	UpdatePMVState(ctx context.Context, rec PMVRecord) error
	GetPMV(ctx context.Context, viewName string) (*PMVRecord, error)
	ListPMVs(ctx context.Context) ([]PMVRecord, error)
	MaterializePMV(ctx context.Context, viewName string, selectSQL string) error
	DropPMV(ctx context.Context, viewName string) error
}

// AgentSafeDB combines strictly read-only CTD Ledger queries with isolated `pigeonmath` view management.
// Under no circumstances does AgentSafeDB expose write methods against public CTD Ledger tables.
type AgentSafeDB interface {
	ReadOnlyDB
	MathDB
	Close()
}

// BaseDB is the full storage adapter contract for PigeonCoop, human CLI administration, and SDK wrappers.
type BaseDB interface {
	ReadOnlyDB
	MathDB

	EnsureSchema(ctx context.Context) error
	ProvisionRoles(ctx context.Context, ingestPassword, readOnlyPassword string) error

	// High-throughput & single-event CTD ingestion
	InsertEvent(ctx context.Context, payload *core.CTDPayload) error
	BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error)
	UpsertArtifact(ctx context.Context, a core.Artifact) error
	InsertTrace(ctx context.Context, t core.TraceEdge) error

	// Node & Hub-authoritative Tag Registry
	UpsertNodeHello(ctx context.Context, hello core.NodeHello) ([]string, error)
	UpdateNodePresence(ctx context.Context, nodeID, status string) error
	UpdateNodePresenceTransition(ctx context.Context, nodeID, status string) (prevStatus string, changed bool, err error)
	AddNodeTags(ctx context.Context, nodeID string, tags []string) ([]string, error)
	RemoveNodeTags(ctx context.Context, nodeID string, tags []string) ([]string, error)

	// Export & admin escape hatches (reserved for Coop / human operators)
	FetchForUnload(ctx context.Context, filter UnloadFilter) ([]core.CTDPayload, error)
	RegisterView(ctx context.Context, viewName string, selectQuery string) error
	ExecRaw(ctx context.Context, sql string, args ...any) error
	QueryMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error)

	Close()
}
