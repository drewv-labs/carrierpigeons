-- CarrierPigeons CTD Ledger Schema (PostgreSQL 16+ / 18+)
-- Aligned with pkg/adapters/db/PostgresStore.EnsureSchema

CREATE TABLE IF NOT EXISTS telemetric_ledger (
	id BIGSERIAL PRIMARY KEY,
	node_id TEXT NOT NULL,
	pillar TEXT NOT NULL DEFAULT 'observer',
	component TEXT NOT NULL,
	status TEXT NOT NULL,
	event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
	params JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_ledger_node_ts ON telemetric_ledger (node_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_pillar ON telemetric_ledger (pillar);
CREATE INDEX IF NOT EXISTS idx_ledger_metrics_gin ON telemetric_ledger USING GIN (metrics);

CREATE TABLE IF NOT EXISTS ctd_artifacts (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	node_id TEXT,
	meta JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_artifacts_kind ON ctd_artifacts (kind);

CREATE TABLE IF NOT EXISTS ctd_traces (
	id BIGSERIAL PRIMARY KEY,
	source_id TEXT NOT NULL,
	verb TEXT NOT NULL,
	target_id TEXT NOT NULL,
	node_id TEXT,
	run_id TEXT,
	meta JSONB NOT NULL DEFAULT '{}'::jsonb,
	event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_traces_source ON ctd_traces (source_id);
CREATE INDEX IF NOT EXISTS idx_traces_target ON ctd_traces (target_id);
