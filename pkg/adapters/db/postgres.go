// File: pkg/adapters/db/postgres.go
package db

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/jsonutil"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultPostgresURL is the local development fallback DSN matching deployments/compose.
const DefaultPostgresURL = "postgres://pigeoncoop_admin:ctd_dev_password@127.0.0.1:5432/edge_ledger"

const (
	// DefaultReadOnlyMaxRows caps heap materialization for ad-hoc read-only agent/MCP queries.
	DefaultReadOnlyMaxRows = 500
	// DefaultReadOnlyMaxBytes caps total scanned result bytes (2 MiB) for ad-hoc read-only agent/MCP queries.
	DefaultReadOnlyMaxBytes = 2 << 20
	// DefaultLedgerPruneBatchLimit bounds rows deleted per PruneLedgerBefore call.
	DefaultLedgerPruneBatchLimit = 10000
	maxLedgerPruneBatchLimit     = 100000
)

// ErrReadOnlyByteLimitExceeded indicates a read-only query exceeded its configured byte ceiling while scanning rows.
var ErrReadOnlyByteLimitExceeded = errors.New("read-only query result exceeded byte ceiling")

var (
	validIdentifier    = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
	validPMVIdentifier = regexp.MustCompile(`^pmv_[a-z0-9_]{1,59}$`)
	dollarTagStartRe   = regexp.MustCompile(`^\$(?:[a-zA-Z_][a-zA-Z0-9_]*)?\$`)
	forbiddenRowLockRe = regexp.MustCompile(`(?i)\bfor\s+(?:no\s+key\s+update|key\s+share|update|share)\b`)
	forbiddenViewDMLRe = regexp.MustCompile(`(?i)\b(into|insert|update|delete|drop|alter|truncate|grant|revoke|copy|call|do|create|execute|set|set_config|reset|listen|notify|prepare|deallocate|lock|vacuum|analyze|cluster|reindex|refresh|discard|load|security|reassign|pg_sleep|pg_sleep_for|pg_sleep_until|pg_advisory_lock|pg_advisory_lock_shared|pg_try_advisory_lock|pg_try_advisory_lock_shared|pg_advisory_xact_lock|pg_advisory_xact_lock_shared|pg_try_advisory_xact_lock|pg_try_advisory_xact_lock_shared|lo_import|lo_export|lo_unlink|lo_put|lo_from_bytea|pg_read_file|pg_read_binary_file|pg_ls_dir|pg_stat_file|pg_terminate_backend|pg_cancel_backend|pg_reload_conf|pg_rotate_logfile|dblink|dblink_exec|dblink_connect|query_to_xml|query_to_xmlschema|query_to_xml_and_xmlschema|database_to_xml|database_to_xmlschema|database_to_xml_and_xmlschema|schema_to_xml|schema_to_xmlschema|schema_to_xml_and_xmlschema|table_to_xml|table_to_xmlschema|table_to_xml_and_xmlschema|cursor_to_xml|cursor_to_xmlschema|cursor_to_xml_and_xmlschema)\b`)
)

// ValidateSchemaIdentifier enforces PostgreSQL unquoted identifier rules (max 63 bytes) before DDL interpolation.
func ValidateSchemaIdentifier(schema string) error {
	if !validIdentifier.MatchString(schema) {
		return fmt.Errorf("invalid PostgreSQL schema identifier %q (must match ^[a-zA-Z_][a-zA-Z0-9_]{0,62}$)", schema)
	}
	return nil
}

var (
	_ BaseDB       = (*PostgresStore)(nil)
	_ AtomicPMVDB  = (*PostgresStore)(nil)
	_ TraceBatchDB = (*PostgresStore)(nil)
)

// ResolveDBURL checks standard CarrierPigeons environment variables before falling back to DefaultPostgresURL.
func ResolveDBURL() string {
	for _, key := range []string{"PIGEONS_DB_URL", "PIGEONCOOP_DB_URL", "DATABASE_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return DefaultPostgresURL
}

// DeriveReadOnlyURLFromAdmin swaps the DSN credentials to the least-privilege `pigeoncare_ro` role.
func DeriveReadOnlyURLFromAdmin(baseDSN, roPassword string) string {
	u, err := url.Parse(strings.TrimSpace(baseDSN))
	if err != nil || u.Scheme == "" {
		return baseDSN
	}
	if roPassword == "" {
		roPassword = strings.TrimSpace(os.Getenv("PIGEONCARE_RO_PASSWORD"))
	}
	if roPassword == "" {
		roPassword = "pigeoncare_readonly"
	}
	u.User = url.UserPassword("pigeoncare_ro", roPassword)
	return u.String()
}

// ResolveReadOnlyDBURL resolves the dedicated read-only DSN for PigeonCare/PigeonMCP,
// deriving `pigeoncare_ro` on the configured database host if no explicit read-only DSN is set.
func ResolveReadOnlyDBURL() string {
	for _, key := range []string{"PIGEONCARE_DB_URL", "PIGEONS_RO_DB_URL", "PIGEONMCP_DB_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return DeriveReadOnlyURLFromAdmin(ResolveDBURL(), "")
}

// NormalizePMVName ensures any user/agent view name conforms to `pmv_<lowercase_slug>`.
func NormalizePMVName(raw string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(raw))
	if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" {
		return "", fmt.Errorf("empty PigeonMath view name")
	}
	if !strings.HasPrefix(name, "pmv_") {
		name = "pmv_" + name
	}
	if !validPMVIdentifier.MatchString(name) {
		return "", fmt.Errorf("invalid PigeonMath view name %q (must match ^pmv_[a-z0-9_]{1,59}$)", name)
	}
	return name, nil
}

func isSQLIdentStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b >= 0x80
}

func isSQLIdentChar(b byte) bool {
	return isSQLIdentStart(b) || (b >= '0' && b <= '9') || b == '$'
}

// matchDollarQuoteTag matches PostgreSQL dollar-quote opening delimiters:
// ASCII tags via dollarTagStartRe as well as PostgreSQL scan.l non-ASCII dolq_start/dolq_cont (>= 0x80).
func matchDollarQuoteTag(s string) string {
	if tag := dollarTagStartRe.FindString(s); tag != "" {
		return tag
	}
	if len(s) < 3 || s[0] != '$' || !isSQLIdentStart(s[1]) {
		return ""
	}
	j := 2
	for j < len(s) && (isSQLIdentStart(s[j]) || (s[j] >= '0' && s[j] <= '9')) {
		j++
	}
	if j < len(s) && s[j] == '$' {
		return s[:j+1]
	}
	return ""
}

// matchQuoteContinue matches PostgreSQL scan.l {quotecontinue} ({whitespace_with_newline}{quote})
// immediately after a closing single quote so multi-line continued literals preserve escape mode.
func matchQuoteContinue(sql string, afterQuoteIdx int) (int, bool) {
	n := len(sql)
	j := afterQuoteIdx
	// {non_newline_whitespace}* : horizontal spaces or '--...' line comments before the mandatory newline.
	for j < n {
		switch sql[j] {
		case ' ', '\t', '\f', '\v':
			j++
		case '-':
			if j+1 < n && sql[j+1] == '-' {
				j += 2
				for j < n && sql[j] != '\n' && sql[j] != '\r' {
					j++
				}
				continue
			}
			return 0, false
		default:
			goto needNewline
		}
	}
needNewline:
	if j >= n || (sql[j] != '\n' && sql[j] != '\r') {
		return 0, false
	}
	j++
	// {special_whitespace}* : any whitespace or newline-terminated '--...' comments after the first newline.
	for j < n {
		switch sql[j] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			j++
		case '-':
			if j+1 < n && sql[j+1] == '-' {
				k := j + 2
				for k < n && sql[k] != '\n' && sql[k] != '\r' {
					k++
				}
				if k < n && (sql[k] == '\n' || sql[k] == '\r') {
					j = k + 1
					continue
				}
			}
			return 0, false
		case '\'':
			return j + 1, true
		default:
			return 0, false
		}
	}
	return 0, false
}

func consumeSingleQuotedLiteral(sql string, quoteIdx int, backslashEscape bool) (int, error) {
	n := len(sql)
	i := quoteIdx + 1
	for i < n {
		if backslashEscape && sql[i] == '\\' {
			if i+1 >= n {
				return 0, fmt.Errorf("unterminated SQL string literal")
			}
			i += 2
			continue
		}
		if sql[i] == '\'' {
			if i+1 < n && sql[i+1] == '\'' {
				i += 2
				continue
			}
			if nextIdx, continued := matchQuoteContinue(sql, i+1); continued {
				i = nextIdx
				continue
			}
			return i + 1, nil
		}
		i++
	}
	return 0, fmt.Errorf("unterminated SQL string literal")
}

func consumeDoubleQuotedIdent(sql string, quoteIdx int) (string, int, error) {
	n := len(sql)
	i := quoteIdx + 1
	var ident strings.Builder
	for i < n {
		if sql[i] == '"' {
			if i+1 < n && sql[i+1] == '"' {
				ident.WriteByte('"')
				i += 2
				continue
			}
			return ident.String(), i + 1, nil
		}
		ident.WriteByte(sql[i])
		i++
	}
	return "", 0, fmt.Errorf("unterminated SQL quoted identifier")
}

// stripSQLLiteralsAndComments replaces SQL string literals, quoted identifiers, dollar-quoted strings,
// Unicode string literals, and comments with whitespace/placeholders so keyword/semicolon validation never
// misfires on literal string data while rejecting Unicode quoted identifiers, forbidden quoted identifiers,
// and unclosed literal or comment tricks.
func stripSQLLiteralsAndComments(sql string) (string, error) {
	var out strings.Builder
	out.Grow(len(sql))

	n := len(sql)
	for i := 0; i < n; {
		ch := sql[i]

		// Line comment: -- ... \n or \r (PostgreSQL scan.l treats both \n and \r as comment terminators)
		if ch == '-' && i+1 < n && sql[i+1] == '-' {
			out.WriteByte(' ')
			i += 2
			for i < n && sql[i] != '\n' && sql[i] != '\r' {
				i++
			}
			continue
		}

		// Nested block comment: /* ... */
		if ch == '/' && i+1 < n && sql[i+1] == '*' {
			out.WriteByte(' ')
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if sql[i] == '/' && i+1 < n && sql[i+1] == '*' {
					depth++
					i += 2
				} else if sql[i] == '*' && i+1 < n && sql[i+1] == '/' {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return "", fmt.Errorf("unterminated SQL block comment")
			}
			continue
		}

		// PostgreSQL escape string literal: E'...' or e'...' (only at token start)
		if (ch == 'e' || ch == 'E') && i+1 < n && sql[i+1] == '\'' {
			nextIdx, err := consumeSingleQuotedLiteral(sql, i+1, true)
			if err != nil {
				return "", err
			}
			out.WriteString(" '' ")
			i = nextIdx
			continue
		}

		// PostgreSQL Unicode escape string literal (U&'...') and rejected Unicode quoted identifier (U&"...")
		if (ch == 'u' || ch == 'U') && i+2 < n && sql[i+1] == '&' {
			if sql[i+2] == '\'' {
				nextIdx, err := consumeSingleQuotedLiteral(sql, i+2, false)
				if err != nil {
					return "", err
				}
				out.WriteString(" '' ")
				i = nextIdx
				continue
			}
			if sql[i+2] == '"' {
				return "", fmt.Errorf("Unicode quoted SQL identifiers (U&\"...\") are forbidden")
			}
		}

		// Standard single-quoted string literal ('' SQL escapes only)
		if ch == '\'' {
			nextIdx, err := consumeSingleQuotedLiteral(sql, i, false)
			if err != nil {
				return "", err
			}
			out.WriteString(" '' ")
			i = nextIdx
			continue
		}

		// Double-quoted SQL identifier: "..." (unescape and inspect directly so quoted dangerous functions cannot hide)
		if ch == '"' {
			ident, nextIdx, err := consumeDoubleQuotedIdent(sql, i)
			if err != nil {
				return "", err
			}
			if match := forbiddenRowLockRe.FindString(ident); match != "" {
				return "", fmt.Errorf("forbidden row-locking clause %q in quoted identifier", match)
			}
			if match := forbiddenViewDMLRe.FindString(ident); match != "" {
				return "", fmt.Errorf("forbidden mutating or session-altering keyword %q in quoted identifier", match)
			}
			out.WriteString(" _ident ")
			i = nextIdx
			continue
		}

		// PostgreSQL dollar-quoted string: $$...$$, $tag$...$tag$, or non-ASCII $α$...$α$
		if ch == '$' {
			if tag := matchDollarQuoteTag(sql[i:]); tag != "" {
				out.WriteString(" '' ")
				i += len(tag)
				endIdx := strings.Index(sql[i:], tag)
				if endIdx < 0 {
					return "", fmt.Errorf("unterminated dollar-quoted SQL literal")
				}
				i += endIdx + len(tag)
				continue
			}
		}

		// Consume full unquoted identifiers in one token so embedded '$', 'e', or 'u' cannot trigger mid-identifier.
		if isSQLIdentStart(ch) {
			start := i
			i++
			for i < n && isSQLIdentChar(sql[i]) {
				i++
			}
			out.WriteString(sql[start:i])
			continue
		}

		out.WriteByte(ch)
		i++
	}

	return out.String(), nil
}

// ValidateSelectOnlySQL enforces single-statement read-only SELECT/WITH syntax before view creation or execution.
func ValidateSelectOnlySQL(rawSQL string) (string, error) {
	cleaned := strings.TrimSpace(rawSQL)
	cleaned = strings.TrimSuffix(cleaned, ";")
	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "" {
		return "", fmt.Errorf("SQL query cannot be empty")
	}

	stripped, err := stripSQLLiteralsAndComments(cleaned)
	if err != nil {
		return "", err
	}
	stripped = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stripped), ";"))
	if stripped == "" {
		return "", fmt.Errorf("SQL query cannot contain only comments")
	}
	if strings.Contains(stripped, ";") {
		return "", fmt.Errorf("multi-statement SQL is forbidden")
	}
	lower := strings.ToLower(stripped)
	if !strings.HasPrefix(lower, "select") && !strings.HasPrefix(lower, "with") {
		return "", fmt.Errorf("SQL must start with SELECT or WITH")
	}
	if match := forbiddenRowLockRe.FindString(stripped); match != "" {
		return "", fmt.Errorf("forbidden row-locking clause %q in analytical query", match)
	}
	if match := forbiddenViewDMLRe.FindString(stripped); match != "" {
		return "", fmt.Errorf("forbidden mutating or session-altering keyword %q in analytical query", match)
	}
	return cleaned, nil
}

// StoreOptions customizes connection pool sizing, session timeouts, and RBAC safety checks.
type StoreOptions struct {
	AppActor            string
	MaxConns            int32
	MinConns            int32
	StatementTimeoutMs  int
	EnforceNonSuperuser bool
	MathSchema          string
	ReadOnlyMaxBytes    int
}

type pgSchemaConn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type PostgresStore struct {
	pool             *pgxpool.Pool
	mathSchema       string
	readOnlyMaxBytes int

	// Optional unexported test seams for unit testing without a live database.
	copyBatchFn   func(ctx context.Context, copyRows [][]any, nodeLastSeen map[string]time.Time, artifacts []*core.Artifact, traces []*core.TraceEdge) (int64, error)
	copyTracesFn  func(ctx context.Context, copyRows [][]any) (int64, error)
	insertEventFn func(ctx context.Context, p *core.CTDPayload) error
	pruneExecFn   func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func redactDSNParseError(connStr string, err error) error {
	if err == nil {
		return nil
	}
	var extra []string
	trimmed := strings.TrimSpace(connStr)
	if trimmed != "" && !strings.Contains(trimmed, "://") && !strings.Contains(trimmed, "=") {
		if atIdx := strings.LastIndexByte(trimmed, '@'); atIdx > 0 {
			if colonIdx := strings.IndexByte(trimmed[:atIdx], ':'); colonIdx >= 0 {
				if secret := trimmed[colonIdx+1 : atIdx]; secret != "" {
					extra = append(extra, secret)
				}
			}
		} else {
			extra = append(extra, trimmed)
		}
	}
	return jsonutil.RedactError(err, extra...)
}

// NewPostgresStore initializes a connection pool to the PostgreSQL CTD Ledger.
func NewPostgresStore(ctx context.Context, connStr string) (*PostgresStore, error) {
	return NewPostgresStoreWithOptions(ctx, connStr, StoreOptions{
		AppActor: "carrierpigeons",
		MaxConns: 16,
		MinConns: 2,
	})
}

// NewPostgresStoreWithOptions initializes a connection pool stamped with `application_name` for ledger attribution.
func NewPostgresStoreWithOptions(ctx context.Context, connStr string, opts StoreOptions) (*PostgresStore, error) {
	if strings.TrimSpace(connStr) == "" {
		connStr = ResolveDBURL()
	}

	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pgx config: %w", redactDSNParseError(connStr, err))
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	} else {
		cfg.MaxConns = 16
	}
	if opts.MinConns > 0 {
		cfg.MinConns = opts.MinConns
	} else {
		cfg.MinConns = 2
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if strings.TrimSpace(opts.AppActor) != "" {
		if _, exists := cfg.ConnConfig.RuntimeParams["application_name"]; !exists {
			cfg.ConnConfig.RuntimeParams["application_name"] = strings.TrimSpace(opts.AppActor)
		}
	}
	if opts.StatementTimeoutMs > 0 {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.Itoa(opts.StatementTimeoutMs)
	}

	var extraSecrets []string
	if cfg.ConnConfig != nil && cfg.ConnConfig.Password != "" {
		extraSecrets = append(extraSecrets, cfg.ConnConfig.Password)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", jsonutil.RedactError(err, extraSecrets...))
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", jsonutil.RedactError(err, extraSecrets...))
	}

	if opts.EnforceNonSuperuser {
		if err := verifyNonSuperuserPool(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
	}

	schema := strings.TrimSpace(opts.MathSchema)
	if schema == "" {
		schema = brand.Active().MathSchema
	}
	if schema == "" {
		schema = "pigeonmath"
	}
	if err := ValidateSchemaIdentifier(schema); err != nil {
		pool.Close()
		return nil, err
	}

	readOnlyMaxBytes := opts.ReadOnlyMaxBytes
	if readOnlyMaxBytes <= 0 {
		readOnlyMaxBytes = DefaultReadOnlyMaxBytes
	}

	return &PostgresStore{pool: pool, mathSchema: schema, readOnlyMaxBytes: readOnlyMaxBytes}, nil
}

// Pool exposes the underlying pgxpool.Pool for advanced callers.
func (s *PostgresStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *PostgresStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

const enforceNonSuperuserQuery = `
	SELECT
		SESSION_USER,
		COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = SESSION_USER), true) AS is_super,
		COALESCE(has_schema_privilege(SESSION_USER, 'public', 'CREATE'), true) AS can_create_public,
		(to_regclass('public.telemetric_ledger') IS NULL) AS ledger_missing,
		(
			SELECT COALESCE(bool_or(has_table_privilege(SESSION_USER, tbl, 'INSERT, UPDATE, DELETE, TRUNCATE')), false)
			FROM unnest(ARRAY[
				'public.telemetric_ledger',
				'public.ctd_nodes',
				'public.ctd_artifacts',
				'public.ctd_traces',
				'public.ctd_mesh'
			]) AS tbl
			WHERE to_regclass(tbl) IS NOT NULL
		) AS can_mutate_core
`

type roBoundaryStatus struct {
	SessionUser     string
	IsSuper         bool
	CanCreatePublic bool
	LedgerMissing   bool
	CanMutateCore   bool
}

func evaluateROBoundary(st roBoundaryStatus) error {
	if st.LedgerMissing {
		return fmt.Errorf(
			"hard read-only boundary violation: core table public.telemetric_ledger does not exist yet (refusing fail-open read-only connection as %q)",
			st.SessionUser,
		)
	}
	if st.IsSuper || st.CanCreatePublic || st.CanMutateCore {
		return fmt.Errorf(
			"hard read-only boundary violation: connected as privileged/writable PostgreSQL role %q (super=%v, create_public=%v, mutate_core=%v; provision least-privilege roles via `pigeons coop provision-roles` and connect as `pigeoncare_ro`, or set PIGEONS_ALLOW_SUPERUSER_RO=true only for isolated local testing)",
			st.SessionUser, st.IsSuper, st.CanCreatePublic, st.CanMutateCore,
		)
	}
	return nil
}

func verifyNonSuperuserPool(ctx context.Context, pool *pgxpool.Pool) error {
	var st roBoundaryStatus
	err := pool.QueryRow(ctx, enforceNonSuperuserQuery).Scan(
		&st.SessionUser,
		&st.IsSuper,
		&st.CanCreatePublic,
		&st.LedgerMissing,
		&st.CanMutateCore,
	)
	if err != nil {
		return fmt.Errorf("verify read-only PostgreSQL role boundary: %w", err)
	}
	return evaluateROBoundary(st)
}

const ensureSchemaReadyQuery = `
	SELECT
		(
			SELECT COUNT(*) = 5
			FROM information_schema.tables
			WHERE table_schema = 'public'
			  AND table_name IN ('telemetric_ledger', 'ctd_nodes', 'ctd_artifacts', 'ctd_traces', 'ctd_mesh')
		)
		AND
		(
			SELECT COUNT(*) = 3
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'telemetric_ledger'
			  AND column_name IN ('ingested_at', 'db_actor', 'app_actor')
		)
		AND to_regclass('public.idx_ledger_metrics_path_gin') IS NULL
		AND to_regclass('public.idx_ledger_ts_desc') IS NULL
		AND to_regclass('public.idx_ledger_ingested_ts') IS NULL
		AND to_regclass('public.idx_ledger_ts_brin') IS NOT NULL
		AND to_regclass('public.idx_ledger_ingested_brin') IS NOT NULL
		AND to_regclass('public.idx_ledger_pause_node_ts') IS NOT NULL
`

const ensureSchemaDDL = `
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

	ALTER TABLE telemetric_ledger ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
	ALTER TABLE telemetric_ledger ADD COLUMN IF NOT EXISTS db_actor TEXT NOT NULL DEFAULT SESSION_USER;
	ALTER TABLE telemetric_ledger ADD COLUMN IF NOT EXISTS app_actor TEXT NOT NULL DEFAULT COALESCE(NULLIF(current_setting('application_name', true), ''), 'unknown');

	DROP INDEX IF EXISTS idx_ledger_pillar;
	DROP INDEX IF EXISTS idx_ledger_metrics_gin;
	DROP INDEX IF EXISTS idx_ledger_metrics_path_gin;
	DROP INDEX IF EXISTS idx_ledger_ts_desc;
	DROP INDEX IF EXISTS idx_ledger_ingested_ts;
	CREATE INDEX IF NOT EXISTS idx_ledger_node_ts ON telemetric_ledger (node_id, event_timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_ledger_node_comp_ts ON telemetric_ledger (node_id, component, event_timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_ledger_ts_brin ON telemetric_ledger USING BRIN (event_timestamp);
	CREATE INDEX IF NOT EXISTS idx_ledger_ingested_brin ON telemetric_ledger USING BRIN (ingested_at);
	CREATE INDEX IF NOT EXISTS idx_ledger_runner_ts ON telemetric_ledger (event_timestamp DESC) WHERE pillar = 'runner';
	CREATE INDEX IF NOT EXISTS idx_ledger_pause_node_ts
		ON telemetric_ledger (node_id, event_timestamp DESC)
		WHERE pillar = 'runner' AND component IN ('runner:PauseRnr', 'runner:pause', 'runner:pausernr');

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

	ALTER TABLE ctd_artifacts ADD COLUMN IF NOT EXISTS updated_by TEXT NOT NULL DEFAULT SESSION_USER;
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

	ALTER TABLE ctd_traces ADD COLUMN IF NOT EXISTS db_actor TEXT NOT NULL DEFAULT SESSION_USER;
	CREATE INDEX IF NOT EXISTS idx_traces_source ON ctd_traces (source_id);
	CREATE INDEX IF NOT EXISTS idx_traces_target ON ctd_traces (target_id);

	CREATE TABLE IF NOT EXISTS ctd_mesh (
		singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
		mesh_name TEXT NOT NULL CHECK (mesh_name ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
		coop_node_id TEXT NOT NULL DEFAULT '',
		topic_root TEXT NOT NULL DEFAULT 'pigeons',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_by TEXT NOT NULL DEFAULT SESSION_USER
	);
`

// EnsureSchema migrates the core CTD Ledger tables, Node/Tag Registry, MNS singleton, attribution columns, and PigeonMath schema.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	var ready bool
	if err := s.pool.QueryRow(ctx, ensureSchemaReadyQuery).Scan(&ready); err == nil && ready {
		return s.EnsureMathSchema(ctx)
	}

	if _, err := s.pool.Exec(ctx, ensureSchemaDDL); err != nil {
		return err
	}
	return s.EnsureMathSchema(ctx)
}

// UpsertMeshIdentity persists PigeonCoop's registered Mesh Name System (MNS) singleton record in `ctd_mesh`.
func (s *PostgresStore) UpsertMeshIdentity(ctx context.Context, identity core.MeshIdentity) error {
	if err := core.ValidateMeshName(identity.MeshName); err != nil {
		return fmt.Errorf("invalid mesh_name: %w", err)
	}
	topicRoot := strings.Trim(strings.TrimSpace(identity.TopicRoot), "/")
	if topicRoot == "" {
		topicRoot = "pigeons"
	}
	ts := identity.UpdatedAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ctd_mesh (singleton, mesh_name, coop_node_id, topic_root, updated_at, updated_by)
		VALUES (TRUE, $1, $2, $3, $4, SESSION_USER)
		ON CONFLICT (singleton) DO UPDATE SET
			mesh_name    = EXCLUDED.mesh_name,
			coop_node_id = EXCLUDED.coop_node_id,
			topic_root   = EXCLUDED.topic_root,
			updated_at   = EXCLUDED.updated_at,
			updated_by   = SESSION_USER
	`, identity.MeshName, strings.TrimSpace(identity.CoopNodeID), topicRoot, ts)
	return err
}

// GetMeshIdentity queries the registered MNS singleton record inside a PostgreSQL READ ONLY transaction.
func (s *PostgresStore) GetMeshIdentity(ctx context.Context) (*core.MeshIdentity, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var out core.MeshIdentity
	err = tx.QueryRow(ctx, `
		SELECT mesh_name, coop_node_id, topic_root, updated_at
		FROM ctd_mesh
		WHERE singleton = TRUE
		LIMIT 1
	`).Scan(&out.MeshName, &out.CoopNodeID, &out.TopicRoot, &out.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}

const ensureMathSchemaExistsQuery = `
	SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = 'pmv_catalog'
	)
`

func (s *PostgresStore) ensureMathSchemaWithConn(ctx context.Context, conn pgSchemaConn) error {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return err
	}
	if conn == nil {
		return fmt.Errorf("postgres store pool is not initialized")
	}
	var exists bool
	if err := conn.QueryRow(ctx, ensureMathSchemaExistsQuery, s.mathSchema).Scan(&exists); err != nil {
		return fmt.Errorf("check %s.pmv_catalog existence: %w", s.mathSchema, err)
	}
	if exists {
		return nil
	}
	ddl := fmt.Sprintf(`
	CREATE SCHEMA IF NOT EXISTS %[1]s;

	CREATE TABLE IF NOT EXISTS %[1]s.pmv_catalog (
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
	`, s.mathSchema)
	_, err := conn.Exec(ctx, ddl)
	return err
}

// EnsureMathSchema creates the isolated `pigeonmath` analytical schema and `pmv_catalog` intent cache.
// Existence is checked via `pg_catalog` (world-readable) rather than `information_schema.tables` (filtered by
// table privileges), so least-privilege `ctd_ingestor` connections return `nil` immediately when `pmv_catalog`
// already exists without attempting `CREATE SCHEMA`.
func (s *PostgresStore) EnsureMathSchema(ctx context.Context) error {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return err
	}
	if s.pool == nil {
		return fmt.Errorf("postgres store pool is not initialized")
	}
	return s.ensureMathSchemaWithConn(ctx, s.pool)
}

func isLoopbackPGHost(host string) bool {
	h := strings.Trim(strings.TrimSpace(host), "[]")
	if h == "" || strings.HasPrefix(h, "/") || strings.EqualFold(h, "localhost") {
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func isDefaultRolePassword(pw string) bool {
	switch pw {
	case "ctd_dev_password", "ctd_ingest_password", "pigeoncare_readonly":
		return true
	default:
		return false
	}
}

func validateProvisionRolePasswords(host, ingestPassword, readOnlyPassword string) (string, string, error) {
	if ingestPassword == "" {
		ingestPassword = strings.TrimSpace(os.Getenv("CTD_INGESTOR_PASSWORD"))
	}
	if readOnlyPassword == "" {
		readOnlyPassword = strings.TrimSpace(os.Getenv("PIGEONCARE_RO_PASSWORD"))
	}

	if !isLoopbackPGHost(host) {
		if ingestPassword == "" || isDefaultRolePassword(ingestPassword) {
			return "", "", fmt.Errorf("refusing to provision ctd_ingestor with empty or default password on non-loopback PostgreSQL host %q", host)
		}
		if readOnlyPassword == "" || isDefaultRolePassword(readOnlyPassword) {
			return "", "", fmt.Errorf("refusing to provision pigeoncare_ro with empty or default password on non-loopback PostgreSQL host %q", host)
		}
		return ingestPassword, readOnlyPassword, nil
	}

	if ingestPassword == "" {
		ingestPassword = "ctd_ingest_password"
	}
	if readOnlyPassword == "" {
		readOnlyPassword = "pigeoncare_readonly"
	}
	return ingestPassword, readOnlyPassword, nil
}

const (
	scramSHA256SaltBytes  = 16
	scramSHA256Iterations = 4096
)

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// buildSCRAMSHA256VerifierWithSalt computes a PostgreSQL-compatible SCRAM-SHA-256 verifier
// per RFC 7677 / RFC 5802 (`SCRAM-SHA-256$<iter>:<base64(salt)>$<base64(StoredKey)>:<base64(ServerKey)>`).
func buildSCRAMSHA256VerifierWithSalt(password string, salt []byte, iterations int) (string, error) {
	if password == "" {
		return "", fmt.Errorf("SCRAM-SHA-256 password cannot be empty")
	}
	if strings.ContainsRune(password, '\x00') {
		return "", fmt.Errorf("SCRAM-SHA-256 password cannot contain NUL byte")
	}
	if len(salt) == 0 {
		return "", fmt.Errorf("SCRAM-SHA-256 salt cannot be empty")
	}
	if iterations <= 0 {
		return "", fmt.Errorf("SCRAM-SHA-256 iterations must be positive, got %d", iterations)
	}

	saltedPassword, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return "", fmt.Errorf("derive SCRAM-SHA-256 salted password: %w", err)
	}
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	return fmt.Sprintf(
		"SCRAM-SHA-256$%d:%s$%s:%s",
		iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(storedKey[:]),
		base64.StdEncoding.EncodeToString(serverKey),
	), nil
}

// buildSCRAMSHA256Verifier derives a PostgreSQL SCRAM-SHA-256 verifier using a CSPRNG 16-byte salt
// so plaintext role passwords never leave the Go process in DDL SQL.
func buildSCRAMSHA256Verifier(password string) (string, error) {
	salt := make([]byte, scramSHA256SaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate SCRAM-SHA-256 salt: %w", err)
	}
	return buildSCRAMSHA256VerifierWithSalt(password, salt, scramSHA256Iterations)
}

func buildProvisionRolesSQL(mathSchema, ingestVerifier, readOnlyVerifier string) string {
	escapeLit := func(v string) string {
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return fmt.Sprintf(`
	DO $provision$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ctd_ingestor') THEN
			EXECUTE 'CREATE ROLE ctd_ingestor NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN PASSWORD ' || quote_literal(%[1]s);
		ELSE
			EXECUTE 'ALTER ROLE ctd_ingestor WITH LOGIN PASSWORD ' || quote_literal(%[1]s);
		END IF;
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pigeoncare_ro') THEN
			EXECUTE 'CREATE ROLE pigeoncare_ro NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN PASSWORD ' || quote_literal(%[2]s);
		ELSE
			EXECUTE 'ALTER ROLE pigeoncare_ro WITH LOGIN PASSWORD ' || quote_literal(%[2]s);
		END IF;
	END
	$provision$;

	GRANT USAGE ON SCHEMA public TO ctd_ingestor;
	GRANT SELECT, INSERT ON telemetric_ledger, ctd_traces TO ctd_ingestor;
	GRANT SELECT, INSERT, UPDATE ON ctd_nodes, ctd_artifacts, ctd_mesh TO ctd_ingestor;
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ctd_ingestor;

	REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM PUBLIC, pigeoncare_ro;
	REVOKE CREATE ON SCHEMA public FROM PUBLIC, pigeoncare_ro;
	GRANT USAGE ON SCHEMA public TO pigeoncare_ro;
	GRANT SELECT ON ALL TABLES IN SCHEMA public TO pigeoncare_ro;
	ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pigeoncare_ro;

	GRANT USAGE, CREATE ON SCHEMA %[3]s TO pigeoncare_ro;
	GRANT SELECT, INSERT, UPDATE, DELETE ON %[3]s.pmv_catalog TO pigeoncare_ro;
	`, escapeLit(ingestVerifier), escapeLit(readOnlyVerifier), mathSchema)
}

// ProvisionRoles idempotently configures least-privilege PostgreSQL roles:
// - `ctd_ingestor`: Write/COPY access for PigeonCoop ingestion.
// - `pigeoncare_ro`: Strictly Read-Only on `public.*` + isolated view creation in `pigeonmath`.
func (s *PostgresStore) ProvisionRoles(ctx context.Context, ingestPassword, readOnlyPassword string) error {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return err
	}
	host := ""
	if s.pool != nil && s.pool.Config() != nil && s.pool.Config().ConnConfig != nil {
		host = s.pool.Config().ConnConfig.Host
	}
	var err error
	ingestPassword, readOnlyPassword, err = validateProvisionRolePasswords(host, ingestPassword, readOnlyPassword)
	if err != nil {
		return err
	}
	ingestVerifier, err := buildSCRAMSHA256Verifier(ingestPassword)
	if err != nil {
		return fmt.Errorf("build ctd_ingestor verifier: %w", err)
	}
	readOnlyVerifier, err := buildSCRAMSHA256Verifier(readOnlyPassword)
	if err != nil {
		return fmt.Errorf("build pigeoncare_ro verifier: %w", err)
	}

	sql := buildProvisionRolesSQL(s.mathSchema, ingestVerifier, readOnlyVerifier)
	_, err = s.pool.Exec(ctx, sql)
	return err
}

func marshalObjectOrEmpty[K comparable, V any](m map[K]V) ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

func marshalValidationReport(report map[string]any) ([]byte, map[string]any, error) {
	if len(report) == 0 {
		return []byte("{}"), report, nil
	}
	sanitizedAny := jsonutil.SanitizeValue(report)
	sanitizedMap, _ := sanitizedAny.(map[string]any)
	raw, err := json.Marshal(sanitizedMap)
	if err != nil {
		return nil, nil, err
	}
	return raw, sanitizedMap, nil
}

func payloadMetricsJSON(p *core.CTDPayload) ([]byte, error) {
	if raw := p.RawMetrics(); len(raw) > 0 {
		return raw, nil
	}
	return marshalObjectOrEmpty(p.Metrics)
}

func payloadParamsJSON(p *core.CTDPayload) ([]byte, error) {
	if raw := p.RawParams(); len(raw) > 0 {
		return raw, nil
	}
	return marshalObjectOrEmpty(p.Params)
}

// isBisectablePGError reports whether a PostgreSQL batch error stems from a row-level data or
// constraint violation (SQLSTATE 22xxx / 23xxx) where binary subdivision can isolate a poison row.
// Connection failures (08xxx), cancellations/operator intervention (57xxx), serialization failures
// (40001), and deadlocks (40P01) must abort immediately without triggering bisection storms.
func isBisectablePGError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	code := pgErr.Code
	if strings.HasPrefix(code, "08") || strings.HasPrefix(code, "57") || code == "40001" || code == "40P01" {
		return false
	}
	return strings.HasPrefix(code, "22") || strings.HasPrefix(code, "23")
}

const upsertNodeLastSeenSQL = `
	INSERT INTO ctd_nodes (node_id, last_seen, updated_at)
	VALUES ($1, $2, $2)
	ON CONFLICT (node_id) DO UPDATE
	SET last_seen = GREATEST(ctd_nodes.last_seen, EXCLUDED.last_seen),
	    updated_at = EXCLUDED.updated_at
	WHERE ctd_nodes.last_seen IS NULL
	   OR ctd_nodes.last_seen < EXCLUDED.last_seen - INTERVAL '5 seconds'
`

func prepareEventJSON(p *core.CTDPayload) ([]byte, []byte, error) {
	metricsJSON, err := payloadMetricsJSON(p)
	if err != nil {
		log.Printf("[Postgres] Failed marshaling metrics for node=%q component=%q: %v", p.NodeID, p.Component, err)
		return nil, nil, fmt.Errorf("marshal metrics (node=%q component=%q): %w", p.NodeID, p.Component, err)
	}
	paramsJSON, err := payloadParamsJSON(p)
	if err != nil {
		log.Printf("[Postgres] Failed marshaling params for node=%q component=%q: %v", p.NodeID, p.Component, err)
		return nil, nil, fmt.Errorf("marshal params (node=%q component=%q): %w", p.NodeID, p.Component, err)
	}
	return metricsJSON, paramsJSON, nil
}

// InsertEvent writes a single CTD envelope, throttles `ctd_nodes.last_seen`, and shreds embedded Artifacts/Traces.
func (s *PostgresStore) InsertEvent(ctx context.Context, p *core.CTDPayload) error {
	if p == nil {
		return fmt.Errorf("cannot insert nil CTDPayload")
	}
	metricsJSON, paramsJSON, err := prepareEventJSON(p)
	if err != nil {
		return err
	}
	if s.insertEventFn != nil {
		return s.insertEventFn(ctx, p)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := s.insertPreparedEventWithTx(ctx, tx, p, metricsJSON, paramsJSON); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) insertPreparedEventWithTx(ctx context.Context, tx pgx.Tx, p *core.CTDPayload, metricsJSON, paramsJSON []byte) error {
	pillar := p.Pillar
	if pillar == "" {
		pillar = core.PillarObserver
	}
	ts := p.EventTimestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO telemetric_ledger (node_id, pillar, component, status, event_timestamp, metrics, params)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, p.NodeID, string(pillar), p.Component, p.Status, ts, metricsJSON, paramsJSON)
	if err != nil {
		return fmt.Errorf("insert telemetric_ledger: %w", err)
	}

	if strings.TrimSpace(p.NodeID) != "" {
		if _, err := tx.Exec(ctx, upsertNodeLastSeenSQL, p.NodeID, ts); err != nil {
			return fmt.Errorf("upsert ctd_nodes last_seen: %w", err)
		}
	}

	if err := s.upsertArtifactTx(ctx, tx, p.Artifact, ts); err != nil {
		return err
	}
	if err := s.insertTraceTx(ctx, tx, p.Trace, ts); err != nil {
		return err
	}
	return nil
}

// BatchInsertEvents streams wide events into `telemetric_ledger` using PostgreSQL's binary
// `COPY FROM STDIN` protocol (`pgx.CopyFrom`) and flushes any attached Artifacts/Traces in a single pipelined batch.
// If binary COPY fails due to a row-level data/constraint violation, it recursively bisects (`O(log B)`) to isolate poison rows.
func (s *PostgresStore) BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error) {
	valid := make([]*core.CTDPayload, 0, len(payloads))
	for _, p := range payloads {
		if p != nil && strings.TrimSpace(p.NodeID) != "" {
			valid = append(valid, p)
		}
	}
	if len(valid) == 0 {
		return 0, nil
	}
	return s.batchInsertDivideAndConquer(ctx, valid)
}

func (s *PostgresStore) runCopyBatch(
	ctx context.Context,
	copyRows [][]any,
	nodeLastSeen map[string]time.Time,
	artifacts []*core.Artifact,
	traces []*core.TraceEdge,
) (int64, error) {
	if s.copyBatchFn != nil {
		return s.copyBatchFn(ctx, copyRows, nodeLastSeen, artifacts, traces)
	}
	return s.execCopyBatch(ctx, copyRows, nodeLastSeen, artifacts, traces)
}

func (s *PostgresStore) batchInsertDivideAndConquer(ctx context.Context, payloads []*core.CTDPayload) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(payloads) == 0 {
		return 0, nil
	}
	if len(payloads) == 1 {
		if err := s.InsertEvent(ctx, payloads[0]); err != nil {
			return 0, err
		}
		return 1, nil
	}

	now := time.Now().UTC()
	alignedPayloads := make([]*core.CTDPayload, 0, len(payloads))
	copyRows := make([][]any, 0, len(payloads))
	nodeLastSeen := make(map[string]time.Time, 16)
	var artifacts []*core.Artifact
	var traces []*core.TraceEdge
	var marshalErrs []error

	for _, p := range payloads {
		pillar := p.Pillar
		if pillar == "" {
			pillar = core.PillarObserver
		}
		ts := p.EventTimestamp
		if ts.IsZero() {
			ts = now
		}
		metricsJSON, paramsJSON, err := prepareEventJSON(p)
		if err != nil {
			marshalErrs = append(marshalErrs, err)
			continue
		}

		alignedPayloads = append(alignedPayloads, p)
		copyRows = append(copyRows, []any{
			p.NodeID,
			string(pillar),
			p.Component,
			p.Status,
			ts,
			metricsJSON,
			paramsJSON,
		})

		if prev, ok := nodeLastSeen[p.NodeID]; !ok || ts.After(prev) {
			nodeLastSeen[p.NodeID] = ts
		}
		if p.Artifact != nil {
			a := *p.Artifact
			if a.CreatedAt.IsZero() {
				a.CreatedAt = ts
			}
			artifacts = append(artifacts, &a)
		}
		if p.Trace != nil {
			t := *p.Trace
			if t.Timestamp.IsZero() {
				t.Timestamp = ts
			}
			traces = append(traces, &t)
		}
	}

	if len(copyRows) == 0 {
		return 0, errors.Join(marshalErrs...)
	}

	copied, err := s.runCopyBatch(ctx, copyRows, nodeLastSeen, artifacts, traces)
	if err == nil {
		return copied, errors.Join(marshalErrs...)
	}
	if ctx.Err() != nil {
		return 0, errors.Join(append(marshalErrs, ctx.Err())...)
	}
	if !isBisectablePGError(err) {
		return 0, errors.Join(append(marshalErrs, err)...)
	}
	if len(alignedPayloads) == 1 {
		return 0, errors.Join(append(marshalErrs, err)...)
	}

	// Binary-bisect poison-row isolation (O(log B) roundtrips instead of O(B) sequential transactions).
	mid := len(alignedPayloads) / 2
	leftCount, leftErr := s.batchInsertDivideAndConquer(ctx, alignedPayloads[:mid])
	rightCount, rightErr := s.batchInsertDivideAndConquer(ctx, alignedPayloads[mid:])
	return leftCount + rightCount, errors.Join(append(marshalErrs, leftErr, rightErr)...)
}

const upsertBatchNodeLastSeenSQL = `
	INSERT INTO ctd_nodes (node_id, last_seen, updated_at)
	SELECT * FROM UNNEST($1::text[], $2::timestamptz[], $2::timestamptz[])
	ON CONFLICT (node_id) DO UPDATE
	SET last_seen = GREATEST(ctd_nodes.last_seen, EXCLUDED.last_seen),
	    updated_at = EXCLUDED.updated_at
	WHERE ctd_nodes.last_seen IS NULL
	   OR ctd_nodes.last_seen < EXCLUDED.last_seen - INTERVAL '5 seconds'
`

func (s *PostgresStore) execCopyBatch(
	ctx context.Context,
	copyRows [][]any,
	nodeLastSeen map[string]time.Time,
	artifacts []*core.Artifact,
	traces []*core.TraceEdge,
) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	copied, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"telemetric_ledger"},
		[]string{"node_id", "pillar", "component", "status", "event_timestamp", "metrics", "params"},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return 0, fmt.Errorf("copy telemetric_ledger: %w", err)
	}

	var batch pgx.Batch
	queued := 0

	if len(nodeLastSeen) > 0 {
		nodeIDs := make([]string, 0, len(nodeLastSeen))
		for nodeID := range nodeLastSeen {
			nodeIDs = append(nodeIDs, nodeID)
		}
		slices.Sort(nodeIDs)
		timestamps := make([]time.Time, len(nodeIDs))
		for i, nodeID := range nodeIDs {
			timestamps[i] = nodeLastSeen[nodeID]
		}
		batch.Queue(upsertBatchNodeLastSeenSQL, nodeIDs, timestamps)
		queued++
	}

	if len(artifacts) > 0 {
		dedupArtifacts := make(map[string]*core.Artifact, len(artifacts))
		artifactIDs := make([]string, 0, len(artifacts))
		for _, a := range artifacts {
			if a == nil || a.ID == "" {
				continue
			}
			if existing, ok := dedupArtifacts[a.ID]; ok {
				if a.Kind != "" {
					existing.Kind = a.Kind
				}
				if a.NodeID != "" {
					existing.NodeID = a.NodeID
				}
				if len(a.Meta) > 0 {
					if existing.Meta == nil {
						existing.Meta = make(map[string]string, len(a.Meta))
					}
					for k, v := range a.Meta {
						existing.Meta[k] = v
					}
				}
			} else {
				cp := *a
				if len(a.Meta) > 0 {
					cp.Meta = make(map[string]string, len(a.Meta))
					for k, v := range a.Meta {
						cp.Meta[k] = v
					}
				}
				dedupArtifacts[a.ID] = &cp
				artifactIDs = append(artifactIDs, a.ID)
			}
		}
		slices.Sort(artifactIDs)
		for _, id := range artifactIDs {
			a := dedupArtifacts[id]
			metaJSON, mErr := marshalObjectOrEmpty(a.Meta)
			if mErr != nil {
				return 0, fmt.Errorf("marshal artifact meta: %w", mErr)
			}
			batch.Queue(`
				INSERT INTO ctd_artifacts (id, kind, node_id, meta, created_at, updated_at, updated_by)
				VALUES ($1, $2, NULLIF($3, ''), $4, $5, NOW(), SESSION_USER)
				ON CONFLICT (id) DO UPDATE SET
					kind = EXCLUDED.kind,
					node_id = COALESCE(EXCLUDED.node_id, ctd_artifacts.node_id),
					meta = ctd_artifacts.meta || EXCLUDED.meta,
					updated_at = NOW(),
					updated_by = SESSION_USER
			`, a.ID, a.Kind, a.NodeID, metaJSON, a.CreatedAt)
			queued++
		}
	}

	for _, t := range traces {
		if t == nil {
			continue
		}
		traceMetaJSON, mErr := marshalObjectOrEmpty(t.Meta)
		if mErr != nil {
			return 0, fmt.Errorf("marshal trace meta: %w", mErr)
		}
		batch.Queue(`
			INSERT INTO ctd_traces (source_id, verb, target_id, node_id, run_id, meta, event_timestamp)
			VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7)
		`, t.SourceID, t.Verb, t.TargetID, t.NodeID, t.RunID, traceMetaJSON, t.Timestamp)
		queued++
	}

	if queued > 0 {
		br := tx.SendBatch(ctx, &batch)
		for range queued {
			if _, execErr := br.Exec(); execErr != nil {
				_ = br.Close()
				return 0, fmt.Errorf("batch metadata upsert: %w", execErr)
			}
		}
		if closeErr := br.Close(); closeErr != nil {
			return 0, closeErr
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return copied, nil
}

func (s *PostgresStore) upsertArtifactTx(ctx context.Context, tx pgx.Tx, a *core.Artifact, fallbackTS time.Time) error {
	if a == nil {
		return nil
	}
	metaJSON, err := marshalObjectOrEmpty(a.Meta)
	if err != nil {
		return fmt.Errorf("marshal artifact meta: %w", err)
	}
	createdAt := a.CreatedAt
	if createdAt.IsZero() {
		createdAt = fallbackTS
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO ctd_artifacts (id, kind, node_id, meta, created_at, updated_at, updated_by)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, NOW(), SESSION_USER)
		ON CONFLICT (id) DO UPDATE SET
			kind = EXCLUDED.kind,
			node_id = COALESCE(EXCLUDED.node_id, ctd_artifacts.node_id),
			meta = ctd_artifacts.meta || EXCLUDED.meta,
			updated_at = NOW(),
			updated_by = SESSION_USER
	`, a.ID, a.Kind, a.NodeID, metaJSON, createdAt)
	if err != nil {
		return fmt.Errorf("upsert artifact: %w", err)
	}
	return nil
}

func (s *PostgresStore) insertTraceTx(ctx context.Context, tx pgx.Tx, t *core.TraceEdge, fallbackTS time.Time) error {
	if t == nil {
		return nil
	}
	traceMetaJSON, err := marshalObjectOrEmpty(t.Meta)
	if err != nil {
		return fmt.Errorf("marshal trace meta: %w", err)
	}
	traceTS := t.Timestamp
	if traceTS.IsZero() {
		traceTS = fallbackTS
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO ctd_traces (source_id, verb, target_id, node_id, run_id, meta, event_timestamp)
		VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7)
	`, t.SourceID, t.Verb, t.TargetID, t.NodeID, t.RunID, traceMetaJSON, traceTS)
	if err != nil {
		return fmt.Errorf("insert trace: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertArtifact(ctx context.Context, a core.Artifact) error {
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	metaJSON, err := marshalObjectOrEmpty(a.Meta)
	if err != nil {
		return fmt.Errorf("marshal artifact meta: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO ctd_artifacts (id, kind, node_id, meta, created_at, updated_at, updated_by)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, NOW(), SESSION_USER)
		ON CONFLICT (id) DO UPDATE SET
			kind = EXCLUDED.kind,
			node_id = COALESCE(EXCLUDED.node_id, ctd_artifacts.node_id),
			meta = ctd_artifacts.meta || EXCLUDED.meta,
			updated_at = NOW(),
			updated_by = SESSION_USER
	`, a.ID, a.Kind, a.NodeID, metaJSON, a.CreatedAt)
	return err
}

func (s *PostgresStore) InsertTrace(ctx context.Context, t core.TraceEdge) error {
	if t.Timestamp.IsZero() {
		t.Timestamp = time.Now().UTC()
	}
	metaJSON, err := marshalObjectOrEmpty(t.Meta)
	if err != nil {
		return fmt.Errorf("marshal trace meta: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO ctd_traces (source_id, verb, target_id, node_id, run_id, meta, event_timestamp)
		VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7)
	`, t.SourceID, t.Verb, t.TargetID, t.NodeID, t.RunID, metaJSON, t.Timestamp)
	return err
}

var ctdTracesCopyColumns = []string{
	"source_id",
	"verb",
	"target_id",
	"node_id",
	"run_id",
	"meta",
	"event_timestamp",
}

// InsertTraces streams multiple TraceEdge records into `ctd_traces` using `pgx.CopyFrom`
// so multi-node runner dispatches avoid per-edge INSERT transactions.
func (s *PostgresStore) InsertTraces(ctx context.Context, edges []core.TraceEdge) error {
	if len(edges) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now().UTC()
	copyRows := make([][]any, 0, len(edges))
	for _, t := range edges {
		ts := t.Timestamp
		if ts.IsZero() {
			ts = now
		}
		metaJSON, err := marshalObjectOrEmpty(t.Meta)
		if err != nil {
			return fmt.Errorf("marshal trace meta: %w", err)
		}
		var nodeID any
		if t.NodeID != "" {
			nodeID = t.NodeID
		}
		var runID any
		if t.RunID != "" {
			runID = t.RunID
		}
		copyRows = append(copyRows, []any{
			t.SourceID,
			t.Verb,
			t.TargetID,
			nodeID,
			runID,
			metaJSON,
			ts,
		})
	}
	if s.copyTracesFn != nil {
		_, err := s.copyTracesFn(ctx, copyRows)
		return err
	}
	if s.pool == nil {
		return fmt.Errorf("postgres store pool is not initialized")
	}
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"ctd_traces"},
		ctdTracesCopyColumns,
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return fmt.Errorf("copy ctd_traces: %w", err)
	}
	return nil
}

// UpsertNodeHello registers a connecting node's hardware metadata & boot tags while preserving operator tags.
func (s *PostgresStore) UpsertNodeHello(ctx context.Context, h core.NodeHello) ([]string, error) {
	bootTags := core.NormalizeTags(h.BootTags)
	metaJSON, err := marshalObjectOrEmpty(h.Meta)
	if err != nil {
		return nil, err
	}
	ts := h.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	var merged []string
	err = s.pool.QueryRow(ctx, `
		INSERT INTO ctd_nodes (node_id, arch, os, status, boot_tags, operator_tags, tags, last_seen, updated_at, updated_by, meta)
		VALUES ($1, $2, $3, 'ONLINE', $4, '{}', $4, $5, NOW(), SESSION_USER, $6)
		ON CONFLICT (node_id) DO UPDATE SET
			arch = CASE WHEN EXCLUDED.arch <> '' THEN EXCLUDED.arch ELSE ctd_nodes.arch END,
			os = CASE WHEN EXCLUDED.os <> '' THEN EXCLUDED.os ELSE ctd_nodes.os END,
			status = 'ONLINE',
			boot_tags = EXCLUDED.boot_tags,
			tags = ARRAY(
				SELECT DISTINCT unnest(EXCLUDED.boot_tags || ctd_nodes.operator_tags) ORDER BY 1
			),
			last_seen = GREATEST(ctd_nodes.last_seen, EXCLUDED.last_seen),
			updated_at = NOW(),
			updated_by = SESSION_USER,
			meta = ctd_nodes.meta || EXCLUDED.meta
		RETURNING tags
	`, h.NodeID, h.Arch, h.OS, bootTags, ts, metaJSON).Scan(&merged)
	return merged, err
}

// UpdateNodePresence updates a node's LWT status (`ONLINE` / `OFFLINE`) in `ctd_nodes`.
func (s *PostgresStore) UpdateNodePresence(ctx context.Context, nodeID, status string) error {
	_, _, err := s.UpdateNodePresenceTransition(ctx, nodeID, status)
	return err
}

// UpdateNodePresenceTransition atomically records a node's presence state and reports whether the status changed,
// allowing PigeonCoop to suppress duplicate Ntfy alerts on retained MQTT message replay.
func (s *PostgresStore) UpdateNodePresenceTransition(ctx context.Context, nodeID, status string) (string, bool, error) {
	var prevStatus string
	err := s.pool.QueryRow(ctx, `
		WITH prior AS (
			SELECT status FROM ctd_nodes WHERE node_id = $1 FOR UPDATE
		),
		upserted AS (
			INSERT INTO ctd_nodes (node_id, status, last_seen, updated_at, updated_by)
			VALUES ($1, $2, NOW(), NOW(), SESSION_USER)
			ON CONFLICT (node_id) DO UPDATE SET
				status = EXCLUDED.status,
				last_seen = CASE WHEN EXCLUDED.status = 'ONLINE' THEN NOW() ELSE ctd_nodes.last_seen END,
				updated_at = NOW(),
				updated_by = SESSION_USER
			RETURNING status
		)
		SELECT COALESCE((SELECT status FROM prior), '') FROM upserted
	`, nodeID, status).Scan(&prevStatus)
	if err != nil {
		return "", false, err
	}
	changed := (prevStatus != "" && prevStatus != status) || (prevStatus == "" && status == "OFFLINE")
	return prevStatus, changed, nil
}

// AddNodeTags appends operator-defined tags to a node and returns the merged effective tag set.
func (s *PostgresStore) AddNodeTags(ctx context.Context, nodeID string, add []string) ([]string, error) {
	norm := core.NormalizeTags(add)
	if len(norm) == 0 {
		return nil, fmt.Errorf("no valid tags provided")
	}
	var merged []string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO ctd_nodes (node_id, operator_tags, tags, updated_at, updated_by)
		VALUES ($1, $2, $2, NOW(), SESSION_USER)
		ON CONFLICT (node_id) DO UPDATE SET
			operator_tags = ARRAY(
				SELECT DISTINCT unnest(ctd_nodes.operator_tags || EXCLUDED.operator_tags) ORDER BY 1
			),
			tags = ARRAY(
				SELECT DISTINCT unnest(ctd_nodes.boot_tags || ctd_nodes.operator_tags || EXCLUDED.operator_tags) ORDER BY 1
			),
			updated_at = NOW(),
			updated_by = SESSION_USER
		RETURNING tags
	`, nodeID, norm).Scan(&merged)
	return merged, err
}

// RemoveNodeTags removes tags from a node's operator_tags (and boot_tags cache) and returns the remaining effective tags.
func (s *PostgresStore) RemoveNodeTags(ctx context.Context, nodeID string, remove []string) ([]string, error) {
	norm := core.NormalizeTags(remove)
	if len(norm) == 0 {
		return nil, fmt.Errorf("no valid tags provided")
	}
	var merged []string
	err := s.pool.QueryRow(ctx, `
		UPDATE ctd_nodes
		SET
			operator_tags = ARRAY(
				SELECT t FROM unnest(ctd_nodes.operator_tags) AS t WHERE NOT (t = ANY($2)) ORDER BY 1
			),
			boot_tags = ARRAY(
				SELECT t FROM unnest(ctd_nodes.boot_tags) AS t WHERE NOT (t = ANY($2)) ORDER BY 1
			),
			tags = ARRAY(
				SELECT DISTINCT t FROM unnest(ctd_nodes.boot_tags || ctd_nodes.operator_tags) AS t WHERE NOT (t = ANY($2)) ORDER BY 1
			),
			updated_at = NOW(),
			updated_by = SESSION_USER
		WHERE node_id = $1
		RETURNING tags
	`, nodeID, norm).Scan(&merged)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("node %q not found in registry", nodeID)
	}
	return merged, err
}

const resolveNodesAllQuery = `
	SELECT DISTINCT node_id FROM (
		SELECT node_id FROM ctd_nodes
		WHERE status <> 'OFFLINE'
		  AND last_seen >= NOW() - INTERVAL '15 minutes'
		UNION
		SELECT DISTINCT node_id FROM telemetric_ledger
		WHERE event_timestamp >= NOW() - INTERVAL '15 minutes'
	) active_nodes
	ORDER BY node_id ASC
`

// ResolveNodes evaluates a RunnerSelector (`--all`, `--node`, `--tag`) against `ctd_nodes` and `telemetric_ledger`.
func (s *PostgresStore) ResolveNodes(ctx context.Context, sel core.RunnerSelector) ([]string, error) {
	if sel.IsEmpty() {
		return nil, nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	return s.resolveNodesWithTx(ctx, tx, sel)
}

func (s *PostgresStore) resolveNodesWithTx(ctx context.Context, tx pgx.Tx, sel core.RunnerSelector) ([]string, error) {
	if sel.IsEmpty() {
		return nil, nil
	}

	if sel.All {
		rows, err := tx.Query(ctx, resolveNodesAllQuery)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			out = append(out, id)
		}
		return out, rows.Err()
	}

	resolved := make(map[string]struct{})
	for _, n := range sel.Nodes {
		trimmed := strings.TrimSpace(n)
		if trimmed != "" {
			resolved[trimmed] = struct{}{}
		}
	}

	normTags := core.NormalizeTags(sel.Tags)
	if len(normTags) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT node_id
			FROM ctd_nodes
			WHERE tags && $1
			ORDER BY node_id ASC
		`, normTags)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			resolved[id] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	out := make([]string, 0, len(resolved))
	for id := range resolved {
		out = append(out, id)
	}
	slices.Sort(out)
	return out, nil
}

func isAbnormalStatus(status string) bool {
	s := strings.ToLower(status)
	return strings.Contains(s, "error") ||
		strings.Contains(s, "critical") ||
		strings.Contains(s, "fail") ||
		s == "offline"
}

const getClusterStatusQuery = `
	WITH known_nodes AS (
		SELECT node_id FROM ctd_nodes
		UNION
		SELECT DISTINCT node_id
		FROM telemetric_ledger
		WHERE event_timestamp >= NOW() - INTERVAL '15 minutes'
	)
	SELECT
		k.node_id,
		COALESCE(n.arch, '') AS arch,
		COALESCE(n.os, '') AS os,
		COALESCE(n.tags, '{}') AS tags,
		COALESCE(os_obs.component, l.component, 'registry') AS component,
		GREATEST(COALESCE(l.event_timestamp, TIMESTAMPTZ 'epoch'), COALESCE(n.last_seen, TIMESTAMPTZ 'epoch')) AS last_seen,
		CASE
			WHEN n.status = 'OFFLINE' THEN 'OFFLINE'
			WHEN pause_rnr.status = 'runner-start' THEN 'paused'
			ELSE COALESCE(l.status, n.status, 'unknown')
		END AS status,
		COALESCE(os_obs.metrics, '{}'::jsonb) || COALESCE(l.metrics, '{}'::jsonb) AS metrics
	FROM known_nodes k
	LEFT JOIN ctd_nodes n ON n.node_id = k.node_id
	LEFT JOIN LATERAL (
		SELECT component, event_timestamp, status, metrics
		FROM telemetric_ledger tl
		WHERE tl.node_id = k.node_id
		ORDER BY tl.event_timestamp DESC
		LIMIT 1
	) l ON true
	LEFT JOIN LATERAL (
		SELECT component, metrics
		FROM telemetric_ledger h
		WHERE h.node_id = k.node_id AND h.component = 'host-os'
		ORDER BY h.event_timestamp DESC
		LIMIT 1
	) os_obs ON true
	LEFT JOIN LATERAL (
		SELECT status
		FROM telemetric_ledger p
		WHERE p.node_id = k.node_id
		  AND p.pillar = 'runner'
		  AND p.component IN ('runner:PauseRnr', 'runner:pause', 'runner:pausernr')
		ORDER BY p.event_timestamp DESC
		LIMIT 1
	) pause_rnr ON true
	ORDER BY k.node_id ASC
`

// GetClusterStatus returns the latest state, hardware arch/OS, and tags per node using an O(N_nodes * log N_rows)
// LATERAL index probe on `idx_ledger_node_ts` and `idx_ledger_pause_node_ts` instead of scanning the entire historical ledger.
func (s *PostgresStore) GetClusterStatus(ctx context.Context, staleThreshold time.Duration) ([]NodeSummary, error) {
	if staleThreshold <= 0 {
		staleThreshold = 3 * time.Minute
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, getClusterStatusQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []NodeSummary
	cutoff := time.Now().UTC().Add(-staleThreshold)

	for rows.Next() {
		var ns NodeSummary
		var metricsBytes []byte
		if err := rows.Scan(&ns.NodeID, &ns.Arch, &ns.OS, &ns.Tags, &ns.Component, &ns.LastSeen, &ns.Status, &metricsBytes); err != nil {
			return nil, err
		}
		if ns.Tags == nil {
			ns.Tags = []string{}
		}
		if len(metricsBytes) > 0 {
			_ = json.Unmarshal(metricsBytes, &ns.LatestMetrics)
		}
		ns.IsHealthy = (strings.EqualFold(ns.Status, "paused") || ns.LastSeen.After(cutoff)) && !isAbnormalStatus(ns.Status)
		summaries = append(summaries, ns)
	}
	return summaries, rows.Err()
}

// GetLedgerActivity returns actor write/update attribution summaries over the recent 24-hour window,
// leveraging `idx_ledger_ingested_brin` rather than scanning historical cold partitions.
func (s *PostgresStore) GetLedgerActivity(ctx context.Context, limit int) ([]LedgerActivity, error) {
	if limit <= 0 {
		limit = 25
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH activity AS (
			SELECT
				'telemetric_ledger' AS source_table,
				'INSERT/COPY' AS action,
				db_actor,
				app_actor,
				node_id || '/' || pillar AS target,
				COUNT(*)::bigint AS record_count,
				MAX(ingested_at) AS last_event_at
			FROM telemetric_ledger
			WHERE ingested_at >= NOW() - INTERVAL '24 hours'
			GROUP BY db_actor, app_actor, node_id, pillar

			UNION ALL

			SELECT
				'ctd_nodes' AS source_table,
				'UPSERT/TAG' AS action,
				updated_by AS db_actor,
				'registry' AS app_actor,
				node_id AS target,
				1::bigint AS record_count,
				updated_at AS last_event_at
			FROM ctd_nodes

			UNION ALL

			SELECT
				'ctd_artifacts' AS source_table,
				'UPSERT' AS action,
				updated_by AS db_actor,
				kind AS app_actor,
				id AS target,
				1::bigint AS record_count,
				updated_at AS last_event_at
			FROM ctd_artifacts
		)
		SELECT source_table, action, db_actor, app_actor, target, record_count, last_event_at
		FROM activity
		ORDER BY last_event_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LedgerActivity
	for rows.Next() {
		var a LedgerActivity
		if err := rows.Scan(&a.SourceTable, &a.Action, &a.DBActor, &a.AppActor, &a.Target, &a.RecordCount, &a.LastEventAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

const (
	defaultLineageMaxDepth  = 10
	maxLineageDepth         = 32
	defaultLineageHopLimit  = 250
	defaultLineageTotalRows = 2500
)

const getLineageDAGQuery = `
	WITH RECURSIVE lineage (
		id, source_id, verb, target_id, node_id, run_id, meta, event_timestamp, depth
	) AS (
		SELECT base.id, base.source_id, base.verb, base.target_id, base.node_id, base.run_id, base.meta, base.event_timestamp, 1 AS depth
		FROM (
			SELECT id, source_id, verb, target_id,
			       COALESCE(node_id, '') AS node_id,
			       COALESCE(run_id, '') AS run_id,
			       meta, event_timestamp
			FROM ctd_traces
			WHERE source_id = $1 OR target_id = $1
			ORDER BY event_timestamp DESC, id DESC
			LIMIT $3
		) base
		UNION ALL
		SELECT t.id, t.source_id, t.verb, t.target_id, t.node_id, t.run_id, t.meta, t.event_timestamp, l.depth + 1
		FROM lineage l
		CROSS JOIN LATERAL (
			SELECT tr.id, tr.source_id, tr.verb, tr.target_id,
			       COALESCE(tr.node_id, '') AS node_id,
			       COALESCE(tr.run_id, '') AS run_id,
			       tr.meta, tr.event_timestamp
			FROM ctd_traces tr
			WHERE tr.source_id = l.target_id OR tr.target_id = l.source_id
			ORDER BY tr.event_timestamp DESC, tr.id DESC
			LIMIT $3
		) t
		WHERE l.depth < $2
	) CYCLE id SET is_cycle USING path
	SELECT source_id, verb, target_id, node_id, run_id, meta, event_timestamp
	FROM (
		SELECT DISTINCT ON (id) id, source_id, verb, target_id, node_id, run_id, meta, event_timestamp
		FROM (
			SELECT id, source_id, verb, target_id, node_id, run_id, meta, event_timestamp
			FROM lineage
			WHERE NOT is_cycle
			LIMIT $4
		) capped
		ORDER BY id
	) bounded
	ORDER BY event_timestamp ASC
`

// GetLineageDAG walks upstream and downstream edges from any Artifact ID using a bounded recursive CTE.
func (s *PostgresStore) GetLineageDAG(ctx context.Context, artifactID string, maxDepth int) (*DAG, error) {
	if maxDepth <= 0 {
		maxDepth = defaultLineageMaxDepth
	} else if maxDepth > maxLineageDepth {
		maxDepth = maxLineageDepth
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	return s.getLineageDAGWithTx(ctx, tx, artifactID, maxDepth)
}

func (s *PostgresStore) getLineageDAGWithTx(ctx context.Context, tx pgx.Tx, artifactID string, maxDepth int) (*DAG, error) {
	if maxDepth <= 0 {
		maxDepth = defaultLineageMaxDepth
	} else if maxDepth > maxLineageDepth {
		maxDepth = maxLineageDepth
	}

	rows, err := tx.Query(ctx, getLineageDAGQuery, artifactID, maxDepth, defaultLineageHopLimit, defaultLineageTotalRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dag := &DAG{RootID: artifactID}
	artifactSet := map[string]bool{artifactID: true}

	for rows.Next() {
		var edge core.TraceEdge
		var metaBytes []byte
		if err := rows.Scan(&edge.SourceID, &edge.Verb, &edge.TargetID, &edge.NodeID, &edge.RunID, &metaBytes, &edge.Timestamp); err != nil {
			return nil, err
		}
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &edge.Meta)
		}
		dag.Edges = append(dag.Edges, edge)
		artifactSet[edge.SourceID] = true
		artifactSet[edge.TargetID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(artifactSet))
	for id := range artifactSet {
		ids = append(ids, id)
	}

	aRows, err := tx.Query(ctx, `
		SELECT id, kind, COALESCE(node_id, ''), meta, created_at
		FROM ctd_artifacts
		WHERE id = ANY($1)
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("query lineage artifacts: %w", err)
	}
	defer aRows.Close()

	for aRows.Next() {
		var a core.Artifact
		var metaBytes []byte
		if err := aRows.Scan(&a.ID, &a.Kind, &a.NodeID, &metaBytes, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan lineage artifact: %w", err)
		}
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &a.Meta)
		}
		dag.Artifacts = append(dag.Artifacts, a)
	}
	if err := aRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lineage artifacts: %w", err)
	}

	return dag, nil
}

// FetchForUnload pulls filtered ledger events ready to be shredded into Snappy Parquet.
func (s *PostgresStore) FetchForUnload(ctx context.Context, f UnloadFilter) ([]core.CTDPayload, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100000
	}
	nodePattern := f.NodeIDPattern
	if nodePattern == "" {
		nodePattern = "%"
	}

	var sincePtr, untilPtr *time.Time
	if !f.Since.IsZero() {
		t := f.Since.UTC()
		sincePtr = &t
	}
	if !f.Until.IsZero() {
		t := f.Until.UTC()
		untilPtr = &t
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id::text, node_id, pillar, component, status, event_timestamp, metrics, params
		FROM telemetric_ledger
		WHERE node_id LIKE $1
		  AND ($2 = '' OR pillar = $2)
		  AND ($3::timestamptz IS NULL OR event_timestamp >= $3)
		  AND ($4::timestamptz IS NULL OR event_timestamp <= $4)
		ORDER BY event_timestamp ASC
		LIMIT $5
	`, nodePattern, f.Pillar, sincePtr, untilPtr, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []core.CTDPayload
	for rows.Next() {
		var p core.CTDPayload
		var pillarStr string
		var metricsBytes, paramsBytes []byte
		if err := rows.Scan(&p.EventID, &p.NodeID, &pillarStr, &p.Component, &p.Status, &p.EventTimestamp, &metricsBytes, &paramsBytes); err != nil {
			return nil, err
		}
		p.Pillar = core.Pillar(pillarStr)
		if len(metricsBytes) > 0 {
			_ = json.Unmarshal(metricsBytes, &p.Metrics)
		}
		if len(paramsBytes) > 0 {
			_ = json.Unmarshal(paramsBytes, &p.Params)
		}
		results = append(results, p)
	}
	return results, rows.Err()
}

const pruneLedgerBeforeSQL = `
	WITH doomed AS (
		SELECT id
		FROM telemetric_ledger
		WHERE event_timestamp < $1
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	)
	DELETE FROM telemetric_ledger t
	USING doomed d
	WHERE t.id = d.id
`

// PruneLedgerBefore deletes up to batchLimit telemetric_ledger rows older than cutoff in a single bounded transaction batch.
func (s *PostgresStore) PruneLedgerBefore(ctx context.Context, cutoff time.Time, batchLimit int) (int64, error) {
	if cutoff.IsZero() {
		return 0, fmt.Errorf("prune cutoff timestamp cannot be zero")
	}
	if batchLimit <= 0 {
		batchLimit = DefaultLedgerPruneBatchLimit
	} else if batchLimit > maxLedgerPruneBatchLimit {
		batchLimit = maxLedgerPruneBatchLimit
	}

	cutoffUTC := cutoff.UTC()
	if s.pruneExecFn != nil {
		tag, err := s.pruneExecFn(ctx, pruneLedgerBeforeSQL, cutoffUTC, batchLimit)
		if err != nil {
			return 0, err
		}
		return tag.RowsAffected(), nil
	}
	if s.pool == nil {
		return 0, fmt.Errorf("postgres store pool is not initialized")
	}
	tag, err := s.pool.Exec(ctx, pruneLedgerBeforeSQL, cutoffUTC, batchLimit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func rawRowBytes(rawVals [][]byte) int {
	var total int
	for _, raw := range rawVals {
		total += len(raw)
	}
	return total
}

func estimateValueBytes(v any) int {
	switch val := v.(type) {
	case nil:
		return 8
	case string:
		return len(val)
	case []byte:
		return len(val)
	case []any:
		total := 24
		for _, item := range val {
			total += estimateValueBytes(item)
		}
		return total
	case map[string]any:
		return estimateRowMapBytes(val)
	case []string:
		total := 24
		for _, s := range val {
			total += len(s)
		}
		return total
	default:
		return 16
	}
}

func estimateRowMapBytes(m map[string]any) int {
	total := 16
	for k, v := range m {
		total += len(k) + estimateValueBytes(v)
	}
	return total
}

func scanReadOnlyRows(rows pgx.Rows, maxRows, maxBytes int) ([]map[string]any, error) {
	if maxRows <= 0 {
		maxRows = DefaultReadOnlyMaxRows
	}
	if maxBytes <= 0 {
		maxBytes = DefaultReadOnlyMaxBytes
	}

	out := make([]map[string]any, 0, min(maxRows, 64))
	totalBytes := 0
	for len(out) < maxRows && rows.Next() {
		rawBytes := rawRowBytes(rows.RawValues())
		if rawBytes > 0 && totalBytes+rawBytes > maxBytes {
			return nil, fmt.Errorf("%w: scanned %d bytes (limit %d)", ErrReadOnlyByteLimitExceeded, totalBytes+rawBytes, maxBytes)
		}
		m, err := pgx.RowToMap(rows)
		if err != nil {
			return nil, err
		}
		rowBytes := max(rawBytes, estimateRowMapBytes(m))
		if totalBytes+rowBytes > maxBytes {
			return nil, fmt.Errorf("%w: scanned %d bytes (limit %d)", ErrReadOnlyByteLimitExceeded, totalBytes+rowBytes, maxBytes)
		}
		totalBytes += rowBytes
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// QueryReadOnlyMaps executes arbitrary read queries strictly inside a PostgreSQL `READ ONLY` transaction
// with bounded statement timeout, bounded row count (`DefaultReadOnlyMaxRows`), and bounded heap bytes (`DefaultReadOnlyMaxBytes`).
func (s *PostgresStore) QueryReadOnlyMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	return s.QueryReadOnlyMapsBounded(ctx, DefaultReadOnlyMaxRows, sql, args...)
}

// QueryReadOnlyMapsBounded executes a validated read-only SELECT inside a PostgreSQL `READ ONLY` transaction,
// enforcing `SET LOCAL statement_timeout = '5000ms'`, stopping stream consumption at `maxRows`, and enforcing the store byte ceiling.
func (s *PostgresStore) QueryReadOnlyMapsBounded(ctx context.Context, maxRows int, sql string, args ...any) ([]map[string]any, error) {
	maxBytes := DefaultReadOnlyMaxBytes
	if s != nil && s.readOnlyMaxBytes > 0 {
		maxBytes = s.readOnlyMaxBytes
	}
	return s.QueryReadOnlyMapsBoundedBytes(ctx, maxRows, maxBytes, sql, args...)
}

// QueryReadOnlyMapsBoundedBytes executes a validated read-only SELECT inside a PostgreSQL `READ ONLY` transaction
// with explicit row and total byte ceilings.
func (s *PostgresStore) QueryReadOnlyMapsBoundedBytes(ctx context.Context, maxRows, maxBytes int, sql string, args ...any) ([]map[string]any, error) {
	cleaned, err := ValidateSelectOnlySQL(sql)
	if err != nil {
		return nil, err
	}
	if maxRows <= 0 {
		maxRows = DefaultReadOnlyMaxRows
	}
	if maxBytes <= 0 {
		if s != nil && s.readOnlyMaxBytes > 0 {
			maxBytes = s.readOnlyMaxBytes
		} else {
			maxBytes = DefaultReadOnlyMaxBytes
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	return s.queryReadOnlyMapsWithTx(ctx, tx, maxRows, maxBytes, cleaned, args...)
}

func (s *PostgresStore) queryReadOnlyMapsWithTx(ctx context.Context, tx pgx.Tx, maxRows, maxBytes int, cleanedSQL string, args ...any) ([]map[string]any, error) {
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '5000ms'"); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, cleanedSQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanReadOnlyRows(rows, maxRows, maxBytes)
}

// ExplainReadOnly validates syntax and planner structure of a candidate SELECT query inside a rolled-back READ ONLY transaction.
func (s *PostgresStore) ExplainReadOnly(ctx context.Context, selectSQL string) (string, error) {
	cleaned, err := ValidateSelectOnlySQL(selectSQL)
	if err != nil {
		return "", err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '5000ms'"); err != nil {
		return "", err
	}

	var planJSON string
	if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+cleaned).Scan(&planJSON); err != nil {
		return "", err
	}
	return planJSON, nil
}

const pmvAdvisoryLockSQL = "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))"

func buildSavePMVDraftSQL(mathSchema string) string {
	return fmt.Sprintf(`
		WITH upserted AS (
			INSERT INTO %[1]s.pmv_catalog (view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at)
			VALUES ($1, COALESCE(NULLIF($2, ''), SESSION_USER), $3, $4, 'draft', 1, $5, $6, NOW(), NOW())
			ON CONFLICT (view_name) DO UPDATE SET
				submitted_by = COALESCE(NULLIF(EXCLUDED.submitted_by, ''), %[1]s.pmv_catalog.submitted_by),
				raw_intent_sql = EXCLUDED.raw_intent_sql,
				optimized_sql = EXCLUDED.optimized_sql,
				status = 'draft',
				version = %[1]s.pmv_catalog.version + 1,
				agent_notes = EXCLUDED.agent_notes,
				validation_report = EXCLUDED.validation_report,
				updated_at = NOW()
			WHERE %[1]s.pmv_catalog.status <> 'certified'
			RETURNING view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at
		)
		SELECT view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at
		FROM upserted
		UNION ALL
		SELECT view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at
		FROM %[1]s.pmv_catalog
		WHERE view_name = $1 AND NOT EXISTS (SELECT 1 FROM upserted)
		LIMIT 1
	`, mathSchema)
}

func buildUpdatePMVStateSQL(mathSchema string) string {
	return fmt.Sprintf(`
		UPDATE %[1]s.pmv_catalog
		SET
			optimized_sql = $2,
			status = $3,
			agent_notes = $4,
			validation_report = $5,
			updated_at = NOW()
		WHERE view_name = $1
		  AND ($3 = 'certified' OR status <> 'certified')
	`, mathSchema)
}

func buildCertifyPMVCatalogSQL(mathSchema string) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]s.pmv_catalog (view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at)
		VALUES ($1, COALESCE(NULLIF($2, ''), SESSION_USER), $3, $4, 'certified', 1, $5, $6, NOW(), NOW())
		ON CONFLICT (view_name) DO UPDATE SET
			submitted_by = COALESCE(NULLIF(EXCLUDED.submitted_by, ''), %[1]s.pmv_catalog.submitted_by),
			raw_intent_sql = EXCLUDED.raw_intent_sql,
			optimized_sql = EXCLUDED.optimized_sql,
			status = 'certified',
			version = CASE
				WHEN %[1]s.pmv_catalog.status = 'certified' THEN %[1]s.pmv_catalog.version + 1
				ELSE %[1]s.pmv_catalog.version
			END,
			agent_notes = EXCLUDED.agent_notes,
			validation_report = EXCLUDED.validation_report,
			updated_at = NOW()
		RETURNING view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at
	`, mathSchema)
}

// SavePMVDraft caches the actor's raw SQL intent in `pigeonmath.pmv_catalog` before optimization.
// Already-certified views are never demoted to `status = 'draft'` by lagging concurrent draft writers.
func (s *PostgresStore) SavePMVDraft(ctx context.Context, rec PMVRecord) (*PMVRecord, error) {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return nil, err
	}
	normName, err := NormalizePMVName(rec.ViewName)
	if err != nil {
		return nil, err
	}
	cleanedRaw, err := ValidateSelectOnlySQL(rec.RawIntentSQL)
	if err != nil {
		return nil, fmt.Errorf("invalid raw SQL intent: %w", err)
	}
	reportJSON, sanitizedReport, err := marshalValidationReport(rec.ValidationReport)
	if err != nil {
		return nil, err
	}
	if s.pool == nil {
		return nil, fmt.Errorf("postgres store pool is not initialized")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	out, err := s.savePMVDraftWithTx(ctx, tx, normName, cleanedRaw, rec, reportJSON, sanitizedReport)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgresStore) savePMVDraftWithTx(
	ctx context.Context,
	tx pgx.Tx,
	normName, cleanedRaw string,
	rec PMVRecord,
	reportJSON []byte,
	sanitizedReport map[string]any,
) (*PMVRecord, error) {
	if _, err := tx.Exec(ctx, pmvAdvisoryLockSQL, s.mathSchema, normName); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, buildSavePMVDraftSQL(s.mathSchema), normName, rec.SubmittedBy, cleanedRaw, rec.OptimizedSQL, rec.AgentNotes, reportJSON)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, pgx.ErrNoRows
	}
	var out PMVRecord
	var reportBytes []byte
	if err := rows.Scan(
		&out.ViewName, &out.SubmittedBy, &out.RawIntentSQL, &out.OptimizedSQL,
		&out.Status, &out.Version, &out.AgentNotes, &reportBytes, &out.CreatedAt, &out.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(reportBytes) > 0 {
		_ = json.Unmarshal(reportBytes, &out.ValidationReport)
	} else {
		out.ValidationReport = sanitizedReport
	}
	return &out, nil
}

// UpdatePMVState updates optimization notes, status, and validation report for a cached PMV.
// Non-certified state updates are guarded with `WHERE status <> 'certified'` so lagging failure/draft updates
// cannot overwrite an already-certified view.
func (s *PostgresStore) UpdatePMVState(ctx context.Context, rec PMVRecord) error {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return err
	}
	normName, err := NormalizePMVName(rec.ViewName)
	if err != nil {
		return err
	}
	reportJSON, _, err := marshalValidationReport(rec.ValidationReport)
	if err != nil {
		return err
	}
	if s.pool == nil {
		return fmt.Errorf("postgres store pool is not initialized")
	}
	return s.updatePMVStateWithConn(ctx, s.pool, normName, rec, reportJSON)
}

func (s *PostgresStore) updatePMVStateWithConn(ctx context.Context, conn pgSchemaConn, normName string, rec PMVRecord, reportJSON []byte) error {
	_, err := conn.Exec(ctx, buildUpdatePMVStateSQL(s.mathSchema), normName, rec.OptimizedSQL, rec.Status, rec.AgentNotes, reportJSON)
	return err
}

// GetPMV retrieves a cached/certified PigeonMath view record by name (reloading original raw intent across context flushes).
func (s *PostgresStore) GetPMV(ctx context.Context, viewName string) (*PMVRecord, error) {
	normName, err := NormalizePMVName(viewName)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
		SELECT view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at
		FROM %[1]s.pmv_catalog
		WHERE view_name = $1
	`, s.mathSchema)

	var rec PMVRecord
	var reportBytes []byte
	err = s.pool.QueryRow(ctx, query, normName).Scan(
		&rec.ViewName, &rec.SubmittedBy, &rec.RawIntentSQL, &rec.OptimizedSQL,
		&rec.Status, &rec.Version, &rec.AgentNotes, &reportBytes, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if len(reportBytes) > 0 {
		_ = json.Unmarshal(reportBytes, &rec.ValidationReport)
	}
	return &rec, nil
}

// ListPMVs lists all cached and certified PigeonMath views in `pigeonmath.pmv_catalog`.
func (s *PostgresStore) ListPMVs(ctx context.Context) ([]PMVRecord, error) {
	query := fmt.Sprintf(`
		SELECT view_name, submitted_by, raw_intent_sql, optimized_sql, status, version, agent_notes, validation_report, created_at, updated_at
		FROM %[1]s.pmv_catalog
		ORDER BY updated_at DESC
	`, s.mathSchema)

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PMVRecord
	for rows.Next() {
		var rec PMVRecord
		var reportBytes []byte
		if err := rows.Scan(
			&rec.ViewName, &rec.SubmittedBy, &rec.RawIntentSQL, &rec.OptimizedSQL,
			&rec.Status, &rec.Version, &rec.AgentNotes, &reportBytes, &rec.CreatedAt, &rec.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if len(reportBytes) > 0 {
			_ = json.Unmarshal(reportBytes, &rec.ValidationReport)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *PostgresStore) materializeViewWithTx(ctx context.Context, tx pgx.Tx, normName, cleanedSQL string) error {
	if _, err := tx.Exec(ctx, "SAVEPOINT pmv_replace"); err != nil {
		return err
	}
	replaceSQL := fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s WITH (security_invoker = true) AS %s", s.mathSchema, normName, cleanedSQL)
	if _, err := tx.Exec(ctx, replaceSQL); err != nil {
		// View signature change (e.g., SQLSTATE 42P16): roll back to savepoint and drop+recreate atomically within the same tx.
		if _, rbErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT pmv_replace"); rbErr != nil {
			return fmt.Errorf("replace view failed (%v); savepoint rollback failed: %w", err, rbErr)
		}
		dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", s.mathSchema, normName)
		if _, dropErr := tx.Exec(ctx, dropSQL); dropErr != nil {
			return fmt.Errorf("replace view failed (%v); drop view failed: %w", err, dropErr)
		}
		createSQL := fmt.Sprintf("CREATE VIEW %s.%s WITH (security_invoker = true) AS %s", s.mathSchema, normName, cleanedSQL)
		if _, createErr := tx.Exec(ctx, createSQL); createErr != nil {
			return createErr
		}
	}
	return nil
}

// MaterializePMV atomically creates or replaces `<mathSchema>.pmv_<slug>` with `security_invoker = true`
// inside a bounded-timeout transaction so column schema evolutions succeed without privilege hijacking or partial drops.
func (s *PostgresStore) MaterializePMV(ctx context.Context, viewName string, selectSQL string) error {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return err
	}
	normName, err := NormalizePMVName(viewName)
	if err != nil {
		return err
	}
	cleanedSQL, err := ValidateSelectOnlySQL(selectSQL)
	if err != nil {
		return err
	}
	if s.pool == nil {
		return fmt.Errorf("postgres store pool is not initialized")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '5000ms'"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, pmvAdvisoryLockSQL, s.mathSchema, normName); err != nil {
		return err
	}
	if err := s.materializeViewWithTx(ctx, tx, normName, cleanedSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MaterializeAndCertifyPMV acquires a per-view transaction advisory lock, materializes `<mathSchema>.pmv_<slug>`
// (`CREATE OR REPLACE VIEW ... WITH (security_invoker = true)`), and upserts `pmv_catalog` with `status = 'certified'`
// inside a single atomic transaction so live certified views are never transiently demoted to `status = 'draft'`.
func (s *PostgresStore) MaterializeAndCertifyPMV(ctx context.Context, rec PMVRecord) (*PMVRecord, error) {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return nil, err
	}
	normName, err := NormalizePMVName(rec.ViewName)
	if err != nil {
		return nil, err
	}
	cleanedRaw, err := ValidateSelectOnlySQL(rec.RawIntentSQL)
	if err != nil {
		return nil, fmt.Errorf("invalid raw SQL intent: %w", err)
	}
	selectCandidate := strings.TrimSpace(rec.OptimizedSQL)
	if selectCandidate == "" {
		selectCandidate = cleanedRaw
	}
	cleanedOptimized, err := ValidateSelectOnlySQL(selectCandidate)
	if err != nil {
		return nil, fmt.Errorf("invalid optimized SQL: %w", err)
	}
	reportJSON, sanitizedReport, err := marshalValidationReport(rec.ValidationReport)
	if err != nil {
		return nil, err
	}
	if s.pool == nil {
		return nil, fmt.Errorf("postgres store pool is not initialized")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	out, err := s.materializeAndCertifyPMVWithTx(ctx, tx, normName, cleanedRaw, cleanedOptimized, rec, reportJSON, sanitizedReport)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgresStore) materializeAndCertifyPMVWithTx(
	ctx context.Context,
	tx pgx.Tx,
	normName, cleanedRaw, cleanedOptimized string,
	rec PMVRecord,
	reportJSON []byte,
	sanitizedReport map[string]any,
) (*PMVRecord, error) {
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '5000ms'"); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, pmvAdvisoryLockSQL, s.mathSchema, normName); err != nil {
		return nil, err
	}
	if err := s.materializeViewWithTx(ctx, tx, normName, cleanedOptimized); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, buildCertifyPMVCatalogSQL(s.mathSchema), normName, rec.SubmittedBy, cleanedRaw, cleanedOptimized, rec.AgentNotes, reportJSON)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, pgx.ErrNoRows
	}

	var out PMVRecord
	var reportBytes []byte
	if err := rows.Scan(
		&out.ViewName, &out.SubmittedBy, &out.RawIntentSQL, &out.OptimizedSQL,
		&out.Status, &out.Version, &out.AgentNotes, &reportBytes, &out.CreatedAt, &out.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(reportBytes) > 0 {
		_ = json.Unmarshal(reportBytes, &out.ValidationReport)
	} else {
		out.ValidationReport = sanitizedReport
	}
	return &out, nil
}

// DropPMV removes a view inside the isolated `pigeonmath` schema if present.
func (s *PostgresStore) DropPMV(ctx context.Context, viewName string) error {
	if err := ValidateSchemaIdentifier(s.mathSchema); err != nil {
		return err
	}
	normName, err := NormalizePMVName(viewName)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s.%s", s.mathSchema, normName))
	return err
}

// validateViewIdentifier checks that a view name is a safe unquoted SQL identifier.
func validateViewIdentifier(viewName string) error {
	if !validIdentifier.MatchString(viewName) {
		return fmt.Errorf("invalid SQL view identifier: %s", viewName)
	}
	return nil
}

// RegisterView creates a PigeonMath view inside the isolated `pigeonmath` schema.
func (s *PostgresStore) RegisterView(ctx context.Context, viewName string, selectQuery string) error {
	if err := validateViewIdentifier(viewName); err != nil {
		return err
	}
	return s.MaterializePMV(ctx, viewName, selectQuery)
}

func (s *PostgresStore) ExecRaw(ctx context.Context, sql string, args ...any) error {
	_, err := s.pool.Exec(ctx, sql, args...)
	return err
}

// QueryMaps executes read/admin queries for human CLI callers and returns a driver-agnostic slice of maps.
func (s *PostgresStore) QueryMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToMap)
}
