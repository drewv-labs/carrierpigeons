-- CarrierPigeons CTD Ledger, Node Tag Registry, RBAC, & PigeonMath Schema
-- Compatible with PostgreSQL 16+ / 18+ across RISC-V, ARM64, and x86_64

CREATE TABLE IF NOT EXISTS telemetric_ledger (
	id BIGSERIAL PRIMARY KEY,
	node_id TEXT NOT NULL,
	pillar TEXT NOT NULL DEFAULT 'observer',
	component TEXT NOT NULL,
	status TEXT NOT NULL,
	event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
	params JSONB NOT NULL DEFAULT '{}'::jsonb,
	ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	db_actor TEXT NOT NULL DEFAULT SESSION_USER,
	app_actor TEXT NOT NULL DEFAULT COALESCE(NULLIF(current_setting('application_name', true), ''), 'unknown')
);

CREATE INDEX IF NOT EXISTS idx_ledger_node_ts ON telemetric_ledger (node_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_ts_desc ON telemetric_ledger (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_ingested_ts ON telemetric_ledger (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_runner_ts ON telemetric_ledger (event_timestamp DESC) WHERE pillar = 'runner';
CREATE INDEX IF NOT EXISTS idx_ledger_metrics_path_gin ON telemetric_ledger USING GIN (metrics jsonb_path_ops);

CREATE TABLE IF NOT EXISTS ctd_nodes (
	node_id TEXT PRIMARY KEY,
	arch TEXT NOT NULL DEFAULT '',
	os TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'ONLINE',
	boot_tags TEXT[] NOT NULL DEFAULT '{}',
	operator_tags TEXT[] NOT NULL DEFAULT '{}',
	tags TEXT[] NOT NULL DEFAULT '{}',
	last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_by TEXT NOT NULL DEFAULT SESSION_USER,
	meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_ctd_nodes_tags_gin ON ctd_nodes USING GIN (tags);

CREATE TABLE IF NOT EXISTS ctd_artifacts (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	node_id TEXT,
	meta JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_by TEXT NOT NULL DEFAULT SESSION_USER
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
	event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	db_actor TEXT NOT NULL DEFAULT SESSION_USER
);

CREATE INDEX IF NOT EXISTS idx_traces_source ON ctd_traces (source_id);
CREATE INDEX IF NOT EXISTS idx_traces_target ON ctd_traces (target_id);

-- Isolated PigeonMath Analytical Schema & PMV Intent Cache
CREATE SCHEMA IF NOT EXISTS pigeonmath;

CREATE TABLE IF NOT EXISTS pigeonmath.pmv_catalog (
	view_name TEXT PRIMARY KEY CHECK (view_name ~ '^pmv_[a-z0-9_]+$'),
	submitted_by TEXT NOT NULL DEFAULT SESSION_USER,
	raw_intent_sql TEXT NOT NULL,
	optimized_sql TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'draft',
	version INT NOT NULL DEFAULT 1,
	agent_notes TEXT NOT NULL DEFAULT '',
	validation_report JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Least-Privilege Role Separation:
-- 1) ctd_ingestor: PigeonCoop COPY/INSERT ingestion role (NOSUPERUSER)
-- 2) pigeoncare_ro: Read-Only on public CTD Ledger + isolated view management in pigeonmath (NOSUPERUSER)
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ctd_ingestor') THEN
		CREATE ROLE ctd_ingestor NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN PASSWORD 'ctd_ingest_password';
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pigeoncare_ro') THEN
		CREATE ROLE pigeoncare_ro NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN PASSWORD 'pigeoncare_readonly';
	END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO ctd_ingestor;
GRANT SELECT, INSERT ON telemetric_ledger, ctd_traces TO ctd_ingestor;
GRANT SELECT, INSERT, UPDATE ON ctd_nodes, ctd_artifacts TO ctd_ingestor;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ctd_ingestor;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM pigeoncare_ro;
REVOKE CREATE ON SCHEMA public FROM pigeoncare_ro;
GRANT USAGE ON SCHEMA public TO pigeoncare_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pigeoncare_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pigeoncare_ro;

GRANT USAGE, CREATE ON SCHEMA pigeonmath TO pigeoncare_ro;
GRANT SELECT, INSERT, UPDATE, DELETE ON pigeonmath.pmv_catalog TO pigeoncare_ro;
