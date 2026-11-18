package db

import (
	"context"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

var _ AgentSafeDB = (*ReadOnlyAgentStore)(nil)

// ReadOnlyAgentStore wraps a PostgresStore behind the narrow AgentSafeDB capability boundary,
// physically withholding all write/mutation methods against the public CTD Ledger.
type ReadOnlyAgentStore struct {
	inner     *PostgresStore
	ownsInner bool
}

// NewReadOnlyAgentStore opens a dedicated connection pool for PigeonCare or PigeonMCP
// stamped with `application_name` and constrained to AgentSafeDB methods.
func NewReadOnlyAgentStore(ctx context.Context, connStr, appActor string) (*ReadOnlyAgentStore, error) {
	if connStr == "" {
		connStr = ResolveReadOnlyDBURL()
	}
	if appActor == "" {
		appActor = "pigeoncare-agent"
	}
	store, err := NewPostgresStoreWithOptions(ctx, connStr, StoreOptions{
		AppActor: appActor,
		MaxConns: 8,
		MinConns: 1,
	})
	if err != nil {
		return nil, err
	}
	return &ReadOnlyAgentStore{inner: store, ownsInner: true}, nil
}

// WrapAgentSafeDB wraps an existing PostgresStore without transferring Close() ownership,
// useful when the operator TUI shares a pool across views while restricting PigeonCare's capability surface.
func WrapAgentSafeDB(store *PostgresStore) *ReadOnlyAgentStore {
	return &ReadOnlyAgentStore{inner: store, ownsInner: false}
}

func (r *ReadOnlyAgentStore) Close() {
	if r.ownsInner && r.inner != nil {
		r.inner.Close()
	}
}

func (r *ReadOnlyAgentStore) GetClusterStatus(ctx context.Context, staleThreshold time.Duration) ([]NodeSummary, error) {
	return r.inner.GetClusterStatus(ctx, staleThreshold)
}

func (r *ReadOnlyAgentStore) ResolveNodes(ctx context.Context, sel core.RunnerSelector) ([]string, error) {
	return r.inner.ResolveNodes(ctx, sel)
}

func (r *ReadOnlyAgentStore) GetLineageDAG(ctx context.Context, artifactID string, maxDepth int) (*DAG, error) {
	return r.inner.GetLineageDAG(ctx, artifactID, maxDepth)
}

func (r *ReadOnlyAgentStore) GetLedgerActivity(ctx context.Context, limit int) ([]LedgerActivity, error) {
	return r.inner.GetLedgerActivity(ctx, limit)
}

func (r *ReadOnlyAgentStore) QueryReadOnlyMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	return r.inner.QueryReadOnlyMaps(ctx, sql, args...)
}

func (r *ReadOnlyAgentStore) ExplainReadOnly(ctx context.Context, selectSQL string) (string, error) {
	return r.inner.ExplainReadOnly(ctx, selectSQL)
}

func (r *ReadOnlyAgentStore) EnsureMathSchema(ctx context.Context) error {
	return r.inner.EnsureMathSchema(ctx)
}

func (r *ReadOnlyAgentStore) SavePMVDraft(ctx context.Context, rec PMVRecord) (*PMVRecord, error) {
	return r.inner.SavePMVDraft(ctx, rec)
}

func (r *ReadOnlyAgentStore) UpdatePMVState(ctx context.Context, rec PMVRecord) error {
	return r.inner.UpdatePMVState(ctx, rec)
}

func (r *ReadOnlyAgentStore) GetPMV(ctx context.Context, viewName string) (*PMVRecord, error) {
	return r.inner.GetPMV(ctx, viewName)
}

func (r *ReadOnlyAgentStore) ListPMVs(ctx context.Context) ([]PMVRecord, error) {
	return r.inner.ListPMVs(ctx)
}

func (r *ReadOnlyAgentStore) MaterializePMV(ctx context.Context, viewName string, selectSQL string) error {
	return r.inner.MaterializePMV(ctx, viewName, selectSQL)
}
