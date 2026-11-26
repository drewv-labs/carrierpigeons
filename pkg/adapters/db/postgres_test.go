package db

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestValidateViewIdentifier(t *testing.T) {
	valid := []string{"benchmark_telemetry", "_internal_view", "v1_storage_stats"}
	for _, name := range valid {
		if err := validateViewIdentifier(name); err != nil {
			t.Fatalf("expected valid view identifier %q, got %v", name, err)
		}
	}

	invalid := []string{
		"",
		"123view",
		"bad-name",
		"v; DROP TABLE telemetric_ledger; --",
		"public.view_name",
	}
	for _, name := range invalid {
		if err := validateViewIdentifier(name); err == nil {
			t.Fatalf("expected error for invalid view identifier %q", name)
		}
	}
}

func TestNormalizePMVNameAndValidateSelectOnlySQL(t *testing.T) {
	got, err := NormalizePMVName("thermal_rollup")
	if err != nil || got != "pmv_thermal_rollup" {
		t.Fatalf("expected pmv_thermal_rollup, got %q (err=%v)", got, err)
	}
	got, err = NormalizePMVName("pigeonmath.PMV_Node_Health")
	if err != nil || got != "pmv_node_health" {
		t.Fatalf("expected pmv_node_health, got %q (err=%v)", got, err)
	}
	if _, err := NormalizePMVName("bad-name!"); err == nil {
		t.Fatal("expected invalid PMV name to fail")
	}
	if _, err := NormalizePMVName("pmv_"); err == nil {
		t.Fatal("expected empty pmv_ suffix to fail")
	}
	maxLenSlug := strings.Repeat("a", 59)
	got, err = NormalizePMVName(maxLenSlug)
	if err != nil || len(got) != 63 {
		t.Fatalf("expected 63-byte PMV identifier to pass, got %q (len=%d, err=%v)", got, len(got), err)
	}
	if _, err := NormalizePMVName(strings.Repeat("a", 60)); err == nil {
		t.Fatal("expected 64-byte PMV identifier (>63 bytes) to be rejected")
	}
	if _, err := NormalizePMVName("pmv_" + strings.Repeat("b", 60)); err == nil {
		t.Fatal("expected >63-byte prefixed PMV identifier to be rejected")
	}

	validQueries := []string{
		"SELECT node_id, created_at, updated_at FROM ctd_nodes;",
		"WITH recent AS (SELECT node_id, metrics FROM telemetric_ledger) SELECT * FROM recent",
		"SELECT 'drop table telemetric_ledger; update ctd_nodes' AS literal_note FROM ctd_nodes",
		"SELECT $$delete from telemetric_ledger$$ AS dollar_literal /* nested /* drop */ comment */ FROM ctd_nodes",
		"SELECT node_id -- trailing comment with delete keyword\nFROM ctd_nodes",
		"SELECT node_id FROM ctd_nodes -- trailing line comment at EOF",
		"SELECT E'escaped\\'quote; drop table x' AS esc_lit FROM ctd_nodes",
		"SELECT 'hello '\n'world' AS multiline_concat FROM ctd_nodes",
		"SELECT $α$safe_payload$α$ AS non_ascii_dollar FROM ctd_nodes",
		"SELECT U&'d\\0061t\\+000061' AS u_lit FROM ctd_nodes",
		"SELECT \"node_id\", \"foo\"\"bar\" AS \"CPU; Temp\" FROM \"ctd_nodes\"",
		"SELECT true, 'not an E string\\' FROM ctd_nodes",
	}
	for _, q := range validQueries {
		if _, err := ValidateSelectOnlySQL(q); err != nil {
			t.Fatalf("expected query %q to pass validation, got %v", q, err)
		}
	}

	blockedQueries := []string{
		"",
		"DELETE FROM telemetric_ledger",
		"SELECT 1; DROP TABLE telemetric_ledger",
		"WITH evil AS (DELETE FROM telemetric_ledger RETURNING *) SELECT * FROM evil",
		"UPDATE ctd_nodes SET status = 'OFFLINE'",
		"SELECT * INTO sneaky_copy FROM ctd_nodes",
		"SELECT * FROM ctd_nodes FOR UPDATE",
		"SELECT * FROM ctd_nodes FOR NO KEY UPDATE",
		"SELECT * FROM ctd_nodes FOR SHARE",
		"SELECT * FROM ctd_nodes FOR KEY SHARE",
		"SELECT pg_advisory_lock(1)",
		"SELECT pg_advisory_lock_shared(1)",
		"SELECT pg_try_advisory_lock(1)",
		"SELECT pg_sleep(30)",
		"SELECT \"pg_sleep\"(5)",
		"SELECT \"PG_SLEEP\"(5)",
		"SELECT \"x\"\"pg_sleep\"(5)",
		"SELECT pg_catalog.\"pg_read_file\"('/etc/passwd')",
		"SELECT \"pg_catalog\".\"pg_read_file\"('/etc/passwd')",
		"SELECT \"for update\" FROM ctd_nodes",
		"\"SELECT\" * FROM ctd_nodes",
		"SELECT U&\"pg_sleep\"(5)",
		"SELECT u&\"pg_sleep\"(5)",
		"SELECT U&\"\\0070g_sleep\"(5)",
		"SELECT U&\"!0070g_sleep\" UESCAPE '!' (5)",
		"SELECT U&\"col\\0020name\" FROM ctd_nodes",
		"SELECT $1$; DROP TABLE ctd_nodes; --$1$",
		"SELECT foo$tag$; DROP TABLE ctd_nodes; --$tag$",
		"SELECT 1 -- comment\r; DROP TABLE ctd_nodes",
		"SELECT E'a'\n'\\'' || pg_sleep(5)::text --'",
		"SELECT E'a' -- comment\n'\\'' || pg_sleep(5)::text --'",
		"SELECT E'a'\n-- line comment\n'\\'' || pg_sleep(5)::text --'",
		"SELECT $α$';--$α$ || pg_sleep(5)::text --'",
		"SELECT $aα$';--$aα$ || pg_sleep(5)::text --'",
		"SELECT 1$$';--$$ || pg_sleep(5)::text --'",
		"SELECT set_config('statement_timeout', '0', true)",
		"SELECT \"set_config\"('statement_timeout', '0', true)",
		"SELECT query_to_xml('SELECT pg_sleep(30)', true, false, '')",
		"SELECT \"query_to_xml\"('SELECT pg_sleep(30)', true, false, '')",
		"SELECT query_to_xmlschema('SELECT 1', true, false, '')",
		"SELECT query_to_xml_and_xmlschema('SELECT 1', true, false, '')",
		"SELECT database_to_xml(true, false, '')",
		"SELECT database_to_xmlschema(true, false, '')",
		"SELECT database_to_xml_and_xmlschema(true, false, '')",
		"SELECT schema_to_xml('public', true, false, '')",
		"SELECT table_to_xml('ctd_nodes', true, false, '')",
		"SELECT cursor_to_xml('c', 1, true, false, '')",
		"SELECT lo_import('/etc/passwd')",
		"SELECT lo_export(1, '/tmp/x')",
		"SELECT dblink('dbname=x', 'SELECT 1')",
		"SELECT dblink_exec('dbname=x', 'DELETE FROM ctd_nodes')",
		"SELECT pg_read_file('/etc/hostname')",
		"SELECT pg_read_binary_file('/etc/hostname')",
		"SELECT pg_ls_dir('.')",
		"SELECT true'\\'; DROP TABLE ctd_nodes; --'",
	}
	for _, q := range blockedQueries {
		if _, err := ValidateSelectOnlySQL(q); err == nil {
			t.Fatalf("expected mutating or dangerous query %q to be rejected", q)
		}
	}
}

func TestValidateSchemaIdentifierAndProvisionRolePasswordsAndBisectGuard(t *testing.T) {
	if err := ValidateSchemaIdentifier("pigeonmath"); err != nil {
		t.Fatalf("expected pigeonmath valid, got %v", err)
	}
	if err := ValidateSchemaIdentifier(strings.Repeat("a", 64)); err == nil {
		t.Fatal("expected 64-char schema identifier to fail PostgreSQL NAMEDATALEN check")
	}
	if err := ValidateSchemaIdentifier("bad; DROP SCHEMA public"); err == nil {
		t.Fatal("expected SQL injection schema identifier to fail")
	}

	// ProvisionRoles allows defaults on loopback, rejects empty/default on remote hosts.
	if _, _, err := validateProvisionRolePasswords("127.0.0.1", "", ""); err != nil {
		t.Fatalf("expected loopback defaults to be allowed, got %v", err)
	}
	if _, _, err := validateProvisionRolePasswords("192.0.2.20", "", "strong_ro_pw"); err == nil {
		t.Fatal("expected empty ingest password on remote host to fail")
	}
	if _, _, err := validateProvisionRolePasswords("192.0.2.20", "ctd_ingest_password", "strong_ro_pw"); err == nil {
		t.Fatal("expected default ingest password on remote host to fail")
	}
	if _, _, err := validateProvisionRolePasswords("192.0.2.20", "strong_ing_pw", "pigeoncare_readonly"); err == nil {
		t.Fatal("expected default readonly password on remote host to fail")
	}
	if _, _, err := validateProvisionRolePasswords("192.0.2.20", "strong_ing_pw", "strong_ro_pw"); err != nil {
		t.Fatalf("expected rotated passwords on remote host to succeed, got %v", err)
	}

	// Bisect guard: only SQLSTATE 22xxx / 23xxx are bisectable.
	if !isBisectablePGError(&pgconn.PgError{Code: "22P02"}) {
		t.Fatal("expected SQLSTATE 22P02 to be bisectable")
	}
	if !isBisectablePGError(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("expected SQLSTATE 23505 to be bisectable")
	}
	for _, code := range []string{"08006", "57014", "40001", "40P01"} {
		if isBisectablePGError(&pgconn.PgError{Code: code}) {
			t.Fatalf("expected SQLSTATE %s NOT to be bisectable", code)
		}
	}
	if isBisectablePGError(errors.New("connection reset by peer")) {
		t.Fatal("expected generic network error NOT to be bisectable")
	}
}

func TestDeriveReadOnlyURLFromAdmin(t *testing.T) {
	roURL := DeriveReadOnlyURLFromAdmin("postgres://ctd_ingestor:ingestor_pw@127.0.0.1:5432/edge_ledger?sslmode=disable", "ro_secret")
	if roURL != "postgres://pigeoncare_ro:ro_secret@127.0.0.1:5432/edge_ledger?sslmode=disable" {
		t.Fatalf("unexpected derived URL: %q", roURL)
	}
}

func TestReadOnlyAgentStoreExposesNoWriteCapability(t *testing.T) {
	var safe any = (*ReadOnlyAgentStore)(nil)
	if _, ok := safe.(AgentSafeDB); !ok {
		t.Fatal("ReadOnlyAgentStore must implement AgentSafeDB")
	}
	if _, ok := safe.(BaseDB); ok {
		t.Fatal("ReadOnlyAgentStore MUST NOT implement BaseDB write interface")
	}
}

func TestMarshalObjectOrEmpty(t *testing.T) {
	var nilMap map[string]string
	raw, err := marshalObjectOrEmpty(nilMap)
	if err != nil || string(raw) != "{}" {
		t.Fatalf("expected empty JSON object '{}' for nil map, got %q (err=%v)", string(raw), err)
	}

	populated, err := marshalObjectOrEmpty(map[string]any{"ipc": 1.85})
	if err != nil || string(populated) != `{"ipc":1.85}` {
		t.Fatalf("unexpected marshaled map: %q (err=%v)", string(populated), err)
	}
}

func TestResolveDBURLAndAbnormalStatus(t *testing.T) {
	t.Setenv("PIGEONS_DB_URL", "")
	t.Setenv("PIGEONCOOP_DB_URL", "postgres://coop@db:5432/ctd")
	if got := ResolveDBURL(); got != "postgres://coop@db:5432/ctd" {
		t.Fatalf("expected PIGEONCOOP_DB_URL fallback, got %q", got)
	}

	t.Setenv("PIGEONS_DB_URL", "postgres://cli@db:5432/ctd")
	if got := ResolveDBURL(); got != "postgres://cli@db:5432/ctd" {
		t.Fatalf("expected PIGEONS_DB_URL precedence, got %q", got)
	}

	if !isAbnormalStatus("sensor-error") || !isAbnormalStatus("OFFLINE") {
		t.Fatal("expected error and OFFLINE statuses to be abnormal")
	}
	if isAbnormalStatus("active") || isAbnormalStatus("runner-start") {
		t.Fatal("expected active and runner-start statuses to be healthy")
	}
}

func TestBuildSCRAMSHA256VerifierRFC7677TestVector(t *testing.T) {
	// RFC 7677 Section 3 test vector: password="pencil", salt=base64("W22ZaJ0SNY7soEsUEjb6gQ=="), iterations=4096.
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	if err != nil {
		t.Fatalf("decode RFC 7677 salt: %v", err)
	}
	got, err := buildSCRAMSHA256VerifierWithSalt("pencil", salt, 4096)
	if err != nil {
		t.Fatalf("buildSCRAMSHA256VerifierWithSalt failed: %v", err)
	}
	want := "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU="
	if got != want {
		t.Fatalf("SCRAM-SHA-256 verifier mismatch:\n got: %s\nwant: %s", got, want)
	}

	if _, err := buildSCRAMSHA256VerifierWithSalt("", salt, 4096); err == nil {
		t.Fatal("expected empty password to fail")
	}
	if _, err := buildSCRAMSHA256VerifierWithSalt("bad\x00pw", salt, 4096); err == nil {
		t.Fatal("expected NUL byte in password to fail")
	}
	if _, err := buildSCRAMSHA256VerifierWithSalt("pencil", nil, 4096); err == nil {
		t.Fatal("expected empty salt to fail")
	}
	if _, err := buildSCRAMSHA256VerifierWithSalt("pencil", salt, 0); err == nil {
		t.Fatal("expected non-positive iterations to fail")
	}

	v1, err := buildSCRAMSHA256Verifier("ingest_secret_value")
	if err != nil {
		t.Fatalf("buildSCRAMSHA256Verifier failed: %v", err)
	}
	v2, err := buildSCRAMSHA256Verifier("ingest_secret_value")
	if err != nil {
		t.Fatalf("buildSCRAMSHA256Verifier second call failed: %v", err)
	}
	if v1 == v2 {
		t.Fatal("expected unique random salt per SCRAM-SHA-256 verifier")
	}
	if !strings.HasPrefix(v1, "SCRAM-SHA-256$4096:") {
		t.Fatalf("expected PostgreSQL SCRAM-SHA-256 prefix, got %q", v1)
	}
	roVerifier, err := buildSCRAMSHA256Verifier("readonly_secret_value")
	if err != nil {
		t.Fatalf("buildSCRAMSHA256Verifier readonly failed: %v", err)
	}

	ddl := buildProvisionRolesSQL("pigeonmath", v1, roVerifier)
	if strings.Contains(ddl, "ingest_secret_value") || strings.Contains(ddl, "readonly_secret_value") {
		t.Fatal("ProvisionRoles DDL must never contain plaintext role passwords")
	}
	if !strings.Contains(ddl, v1) || !strings.Contains(ddl, roVerifier) {
		t.Fatal("ProvisionRoles DDL must embed SCRAM-SHA-256 verifiers")
	}
	if !strings.Contains(ddl, "DO $provision$") || !strings.Contains(ddl, "$provision$;") {
		t.Fatal("ProvisionRoles DDL must use named dollar tag $provision$")
	}
	if !strings.Contains(ddl, "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM PUBLIC, pigeoncare_ro;") ||
		!strings.Contains(ddl, "REVOKE CREATE ON SCHEMA public FROM PUBLIC, pigeoncare_ro;") {
		t.Fatal("ProvisionRoles DDL must revoke write/CREATE privileges from both PUBLIC and pigeoncare_ro")
	}
}

func TestEnforceNonSuperuserQueryAndEvaluation(t *testing.T) {
	requiredFragments := []string{
		"rolsuper",
		"has_schema_privilege(SESSION_USER, 'public', 'CREATE')",
		"to_regclass('public.telemetric_ledger') IS NULL",
		"has_table_privilege(SESSION_USER, tbl, 'INSERT, UPDATE, DELETE, TRUNCATE')",
		"'public.telemetric_ledger'",
		"'public.ctd_nodes'",
		"'public.ctd_artifacts'",
		"'public.ctd_traces'",
		"'public.ctd_mesh'",
	}
	for _, frag := range requiredFragments {
		if !strings.Contains(enforceNonSuperuserQuery, frag) {
			t.Fatalf("enforceNonSuperuserQuery missing %q", frag)
		}
	}

	clean := roBoundaryStatus{SessionUser: "pigeoncare_ro"}
	if err := evaluateROBoundary(clean); err != nil {
		t.Fatalf("expected clean read-only status to pass, got %v", err)
	}

	cases := []struct {
		name string
		st   roBoundaryStatus
	}{
		{name: "missing_ledger_fails_closed", st: roBoundaryStatus{SessionUser: "pigeoncare_ro", LedgerMissing: true}},
		{name: "superuser_rejected", st: roBoundaryStatus{SessionUser: "postgres", IsSuper: true}},
		{name: "schema_public_create_rejected", st: roBoundaryStatus{SessionUser: "pigeoncare_ro", CanCreatePublic: true}},
		{name: "core_table_mutation_rejected", st: roBoundaryStatus{SessionUser: "pigeoncare_ro", CanMutateCore: true}},
	}
	for _, tc := range cases {
		if err := evaluateROBoundary(tc.st); err == nil {
			t.Fatalf("%s: expected error, got nil", tc.name)
		}
	}
}

func TestEnsureSchemaAndCopyBatchSQLHygiene(t *testing.T) {
	requiredReadyFragments := []string{
		"to_regclass('public.idx_ledger_metrics_path_gin') IS NULL",
		"to_regclass('public.idx_ledger_ts_desc') IS NULL",
		"to_regclass('public.idx_ledger_ingested_ts') IS NULL",
		"to_regclass('public.idx_ledger_ts_brin') IS NOT NULL",
		"to_regclass('public.idx_ledger_ingested_brin') IS NOT NULL",
		"to_regclass('public.idx_ledger_pause_node_ts') IS NOT NULL",
	}
	for _, frag := range requiredReadyFragments {
		if !strings.Contains(ensureSchemaReadyQuery, frag) {
			t.Fatalf("ensureSchemaReadyQuery missing %q", frag)
		}
	}

	requiredDDLFragments := []string{
		"DROP INDEX IF EXISTS idx_ledger_metrics_path_gin;",
		"DROP INDEX IF EXISTS idx_ledger_ts_desc;",
		"DROP INDEX IF EXISTS idx_ledger_ingested_ts;",
		"CREATE INDEX IF NOT EXISTS idx_ledger_node_ts ON telemetric_ledger (node_id, event_timestamp DESC);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_node_comp_ts ON telemetric_ledger (node_id, component, event_timestamp DESC);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_ts_brin ON telemetric_ledger USING BRIN (event_timestamp);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_ingested_brin ON telemetric_ledger USING BRIN (ingested_at);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_runner_ts ON telemetric_ledger (event_timestamp DESC) WHERE pillar = 'runner';",
		"CREATE INDEX IF NOT EXISTS idx_ledger_pause_node_ts",
		"WHERE pillar = 'runner' AND component IN ('runner:PauseRnr', 'runner:pause', 'runner:pausernr');",
	}
	for _, frag := range requiredDDLFragments {
		if !strings.Contains(ensureSchemaDDL, frag) {
			t.Fatalf("ensureSchemaDDL missing %q", frag)
		}
	}
	if strings.Contains(ensureSchemaDDL, "CREATE INDEX IF NOT EXISTS idx_ledger_metrics_path_gin") ||
		strings.Contains(ensureSchemaDDL, "jsonb_path_ops") ||
		strings.Contains(ensureSchemaDDL, "CREATE INDEX IF NOT EXISTS idx_ledger_ts_desc") ||
		strings.Contains(ensureSchemaDDL, "CREATE INDEX IF NOT EXISTS idx_ledger_ingested_ts") {
		t.Fatal("ensureSchemaDDL must not recreate dropped legacy indexes")
	}

	initSQLPath := filepath.Join("..", "..", "..", "deployments", "compose", "init.sql")
	initSQLBytes, err := os.ReadFile(initSQLPath)
	if err != nil {
		t.Fatalf("read %s: %v", initSQLPath, err)
	}
	initSQL := string(initSQLBytes)
	if strings.Contains(initSQL, "idx_ledger_metrics_path_gin") ||
		strings.Contains(initSQL, "idx_ledger_ts_desc") ||
		strings.Contains(initSQL, "idx_ledger_ingested_ts") {
		t.Fatalf("%s must not create dropped legacy indexes", initSQLPath)
	}
	requiredInitFragments := []string{
		"CREATE INDEX IF NOT EXISTS idx_ledger_node_ts ON telemetric_ledger (node_id, event_timestamp DESC);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_node_comp_ts ON telemetric_ledger (node_id, component, event_timestamp DESC);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_ts_brin ON telemetric_ledger USING BRIN (event_timestamp);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_ingested_brin ON telemetric_ledger USING BRIN (ingested_at);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_runner_ts ON telemetric_ledger (event_timestamp DESC) WHERE pillar = 'runner';",
		"idx_ledger_pause_node_ts",
		"WHERE pillar = 'runner' AND component IN ('runner:PauseRnr', 'runner:pause', 'runner:pausernr');",
		"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM PUBLIC, pigeoncare_ro;",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC, pigeoncare_ro;",
	}
	for _, frag := range requiredInitFragments {
		if !strings.Contains(initSQL, frag) {
			t.Fatalf("%s missing %q", initSQLPath, frag)
		}
	}

	for name, sqlStmt := range map[string]string{
		"upsertBatchNodeLastSeenSQL": upsertBatchNodeLastSeenSQL,
		"upsertNodeLastSeenSQL":      upsertNodeLastSeenSQL,
	} {
		if !strings.Contains(sqlStmt, "WHERE ctd_nodes.last_seen IS NULL") ||
			!strings.Contains(sqlStmt, "ctd_nodes.last_seen < EXCLUDED.last_seen - INTERVAL '5 seconds'") ||
			!strings.Contains(sqlStmt, "GREATEST(ctd_nodes.last_seen, EXCLUDED.last_seen)") {
			t.Fatalf("%s missing 5s throttle predicate: %s", name, sqlStmt)
		}
	}
}

func TestGetClusterStatusQueryUsesPartialPauseIndex(t *testing.T) {
	if !strings.Contains(getClusterStatusQuery, "AND p.pillar = 'runner'") {
		t.Fatalf("getClusterStatusQuery pause_rnr LATERAL must filter p.pillar = 'runner' to hit idx_ledger_pause_node_ts")
	}
	if !strings.Contains(getClusterStatusQuery, "p.component IN ('runner:PauseRnr', 'runner:pause', 'runner:pausernr')") {
		t.Fatalf("getClusterStatusQuery pause_rnr LATERAL missing expected pause component predicate")
	}
}

func TestBatchInsertDivideAndConquerMarshalErrors(t *testing.T) {
	t.Run("single row fails marshaling before touching pool", func(t *testing.T) {
		store := &PostgresStore{}
		bad := core.NewCTDPayload("node-single-nan", "cpu", "active")
		bad.Metrics["nan_val"] = math.NaN()

		copied, err := store.BatchInsertEvents(context.Background(), []*core.CTDPayload{bad})
		if copied != 0 {
			t.Fatalf("expected 0 copied rows, got %d", copied)
		}
		if err == nil {
			t.Fatal("expected non-nil marshal error for batch-size-1 NaN payload")
		}
		if !strings.Contains(err.Error(), "node-single-nan") {
			t.Fatalf("expected error to include node-single-nan, got: %v", err)
		}

		if err := store.InsertEvent(context.Background(), bad); err == nil || !strings.Contains(err.Error(), "node-single-nan") {
			t.Fatalf("expected direct InsertEvent to return marshal error before Begin(), got: %v", err)
		}
	})

	t.Run("all rows fail marshaling returns joined error", func(t *testing.T) {
		store := &PostgresStore{}
		p1 := core.NewCTDPayload("node-a", "cpu", "active")
		p1.Metrics["nan_val"] = math.NaN()
		p2 := core.NewCTDPayload("node-b", "mem", "active")
		p2.Metrics["inf_val"] = math.Inf(1)

		copied, err := store.BatchInsertEvents(context.Background(), []*core.CTDPayload{p1, p2})
		if copied != 0 {
			t.Fatalf("expected 0 copied rows, got %d", copied)
		}
		if err == nil {
			t.Fatal("expected non-nil joined error when all rows fail marshaling")
		}
		if !strings.Contains(err.Error(), "node-a") || !strings.Contains(err.Error(), "node-b") {
			t.Fatalf("expected error to include both failed nodes, got: %v", err)
		}
	})

	t.Run("partial marshal failure still copies valid rows and returns joined error", func(t *testing.T) {
		var seenRows int
		store := &PostgresStore{
			copyBatchFn: func(_ context.Context, copyRows [][]any, _ map[string]time.Time, _ []*core.Artifact, _ []*core.TraceEdge) (int64, error) {
				seenRows = len(copyRows)
				return int64(len(copyRows)), nil
			},
		}
		ok1 := core.NewCTDPayload("node-ok-1", "cpu", "active")
		ok1.Metrics["temp"] = 42.5
		bad1 := core.NewCTDPayload("node-bad-metrics", "gpu", "active")
		bad1.Metrics["nan"] = math.NaN()
		ok2 := core.NewCTDPayload("node-ok-2", "disk", "active")
		ok2.Params["mount"] = "/nvme"
		bad2 := core.NewCTDPayload("node-bad-inf", "net", "active")
		bad2.Metrics["inf"] = math.Inf(-1)

		copied, err := store.BatchInsertEvents(context.Background(), []*core.CTDPayload{ok1, bad1, ok2, bad2})
		if copied != 2 || seenRows != 2 {
			t.Fatalf("expected 2 copied rows (seen=%d), got %d", seenRows, copied)
		}
		if err == nil {
			t.Fatal("expected non-nil joined error when some rows fail marshaling")
		}
		if !strings.Contains(err.Error(), "node-bad-metrics") || !strings.Contains(err.Error(), "node-bad-inf") {
			t.Fatalf("expected joined error to mention both failed nodes, got: %v", err)
		}
	})

	t.Run("bisect with marshal error preserves both marshal and row errors", func(t *testing.T) {
		poisonErr := errors.New("poison row constraint failure")
		store := &PostgresStore{
			copyBatchFn: func(_ context.Context, copyRows [][]any, _ map[string]time.Time, _ []*core.Artifact, _ []*core.TraceEdge) (int64, error) {
				return 0, &pgconn.PgError{Code: "22P02"}
			},
			insertEventFn: func(_ context.Context, p *core.CTDPayload) error {
				if p.NodeID == "node-poison" {
					return poisonErr
				}
				return nil
			},
		}
		ok := core.NewCTDPayload("node-good", "cpu", "active")
		poison := core.NewCTDPayload("node-poison", "cpu", "active")
		badMarshal := core.NewCTDPayload("node-nan", "cpu", "active")
		badMarshal.Metrics["bad"] = math.NaN()

		copied, err := store.BatchInsertEvents(context.Background(), []*core.CTDPayload{ok, poison, badMarshal})
		if copied != 1 {
			t.Fatalf("expected 1 copied row after bisection, got %d", copied)
		}
		if !errors.Is(err, poisonErr) {
			t.Fatalf("expected joined error to wrap poisonErr, got: %v", err)
		}
		if !strings.Contains(err.Error(), "node-nan") {
			t.Fatalf("expected joined error to include marshal failure for node-nan, got: %v", err)
		}
	})
}

type recordedExec struct {
	sql  string
	args []any
}

type fakePGXTx struct {
	pgx.Tx
	execs   []recordedExec
	queryFn func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (f *fakePGXTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.execs = append(f.execs, recordedExec{sql: sql, args: args})
	return pgconn.CommandTag{}, nil
}

func (f *fakePGXTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return f.queryFn(ctx, sql, args...)
}

type fakePGXRows struct {
	pgx.Rows
	remaining int
	scanFn    func(dest ...any) error
	fields    []pgconn.FieldDescription
	valuesFn  func() ([]any, error)
	rawFn     func() [][]byte
	err       error
}

func (r *fakePGXRows) Close() {}

func (r *fakePGXRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.fields
}

func (r *fakePGXRows) Values() ([]any, error) {
	if r.valuesFn != nil {
		return r.valuesFn()
	}
	return nil, nil
}

func (r *fakePGXRows) RawValues() [][]byte {
	if r.rawFn != nil {
		return r.rawFn()
	}
	return nil
}

func (r *fakePGXRows) Next() bool {
	if r.remaining <= 0 {
		return false
	}
	r.remaining--
	return true
}

func (r *fakePGXRows) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return r.scanRowFallback(dest...)
}

func (r *fakePGXRows) scanRowFallback(dest ...any) error {
	if len(dest) == 1 {
		if rs, ok := dest[0].(pgx.RowScanner); ok {
			return rs.ScanRow(r)
		}
	}
	return nil
}

func (r *fakePGXRows) Err() error {
	return r.err
}

func TestInsertEventUpsertsNodeLastSeen(t *testing.T) {
	store := &PostgresStore{}
	tx := &fakePGXTx{}
	ts := time.Date(2026, 11, 24, 18, 0, 0, 0, time.UTC)
	p := core.NewCTDPayload("node-single-01", "host-os", "active")
	p.EventTimestamp = ts

	metricsJSON, paramsJSON, err := prepareEventJSON(p)
	if err != nil {
		t.Fatalf("prepareEventJSON failed: %v", err)
	}
	if err := store.insertPreparedEventWithTx(context.Background(), tx, p, metricsJSON, paramsJSON); err != nil {
		t.Fatalf("insertPreparedEventWithTx failed: %v", err)
	}
	if len(tx.execs) != 2 {
		t.Fatalf("expected 2 Exec statements (ledger insert + ctd_nodes last_seen upsert), got %d", len(tx.execs))
	}
	if !strings.Contains(tx.execs[0].sql, "INSERT INTO telemetric_ledger") {
		t.Fatalf("unexpected first exec SQL: %s", tx.execs[0].sql)
	}
	if tx.execs[1].sql != upsertNodeLastSeenSQL {
		t.Fatalf("expected second exec SQL to equal upsertNodeLastSeenSQL, got: %s", tx.execs[1].sql)
	}
	if len(tx.execs[1].args) != 2 || tx.execs[1].args[0] != "node-single-01" || !tx.execs[1].args[1].(time.Time).Equal(ts) {
		t.Fatalf("unexpected upsertNodeLastSeenSQL args: %#v", tx.execs[1].args)
	}
}

func TestGetLineageDAGPropagatesArtifactErrors(t *testing.T) {
	now := time.Now().UTC()

	newTraceRows := func() pgx.Rows {
		return &fakePGXRows{
			remaining: 1,
			scanFn: func(dest ...any) error {
				*dest[0].(*string) = "art-root"
				*dest[1].(*string) = "PRODUCED"
				*dest[2].(*string) = "art-child"
				*dest[3].(*string) = "node-01"
				*dest[4].(*string) = "run-01"
				*dest[5].(*[]byte) = []byte(`{}`)
				*dest[6].(*time.Time) = now
				return nil
			},
		}
	}

	t.Run("artifact query error returns error instead of truncated DAG", func(t *testing.T) {
		wantErr := errors.New("artifact query timeout")
		call := 0
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				call++
				if call == 1 {
					return newTraceRows(), nil
				}
				return nil, wantErr
			},
		}
		dag, err := (&PostgresStore{}).getLineageDAGWithTx(context.Background(), tx, "art-root", 5)
		if !errors.Is(err, wantErr) || dag != nil {
			t.Fatalf("expected (nil, %v), got (%v, %v)", wantErr, dag, err)
		}
	})

	t.Run("artifact scan error returns error instead of skipping row", func(t *testing.T) {
		wantErr := errors.New("artifact column type mismatch")
		call := 0
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				call++
				if call == 1 {
					return newTraceRows(), nil
				}
				return &fakePGXRows{
					remaining: 1,
					scanFn:    func(_ ...any) error { return wantErr },
				}, nil
			},
		}
		dag, err := (&PostgresStore{}).getLineageDAGWithTx(context.Background(), tx, "art-root", 5)
		if !errors.Is(err, wantErr) || dag != nil {
			t.Fatalf("expected (nil, %v), got (%v, %v)", wantErr, dag, err)
		}
	})

	t.Run("artifact rows Err returns error instead of truncated DAG", func(t *testing.T) {
		wantErr := errors.New("stream reset during artifact fetch")
		call := 0
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				call++
				if call == 1 {
					return newTraceRows(), nil
				}
				return &fakePGXRows{
					remaining: 0,
					err:       wantErr,
				}, nil
			},
		}
		dag, err := (&PostgresStore{}).getLineageDAGWithTx(context.Background(), tx, "art-root", 5)
		if !errors.Is(err, wantErr) || dag != nil {
			t.Fatalf("expected (nil, %v), got (%v, %v)", wantErr, dag, err)
		}
	})
}

func TestQueryReadOnlyMapsByteCeiling(t *testing.T) {
	if DefaultReadOnlyMaxBytes != 2<<20 {
		t.Fatalf("expected DefaultReadOnlyMaxBytes == 2 MiB, got %d", DefaultReadOnlyMaxBytes)
	}

	makeRows := func(count int, payload string, useRaw bool) pgx.Rows {
		var r *fakePGXRows
		r = &fakePGXRows{
			remaining: count,
			fields: []pgconn.FieldDescription{
				{Name: "payload"},
			},
			valuesFn: func() ([]any, error) {
				return []any{payload}, nil
			},
		}
		if useRaw {
			r.rawFn = func() [][]byte {
				return [][]byte{[]byte(payload)}
			}
		}
		return r
	}

	t.Run("within row and byte bounds succeeds", func(t *testing.T) {
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				return makeRows(3, strings.Repeat("a", 20), true), nil
			},
		}
		got, err := (&PostgresStore{}).queryReadOnlyMapsWithTx(context.Background(), tx, 10, 512, "SELECT payload FROM ctd_nodes")
		if err != nil {
			t.Fatalf("expected scan within byte limit to succeed, got %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(got))
		}
		if len(tx.execs) != 1 || !strings.Contains(tx.execs[0].sql, "statement_timeout") {
			t.Fatalf("expected statement_timeout SET LOCAL, got %+v", tx.execs)
		}
	})

	t.Run("cumulative rows exceeding maxBytes fail fast", func(t *testing.T) {
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				return makeRows(10, strings.Repeat("b", 40), false), nil
			},
		}
		_, err := (&PostgresStore{}).queryReadOnlyMapsWithTx(context.Background(), tx, 100, 120, "SELECT payload FROM ctd_nodes")
		if !errors.Is(err, ErrReadOnlyByteLimitExceeded) {
			t.Fatalf("expected ErrReadOnlyByteLimitExceeded, got %v", err)
		}
	})

	t.Run("single oversized raw row fails before RowToMap decode", func(t *testing.T) {
		valuesCalled := false
		r := &fakePGXRows{
			remaining: 1,
			fields:    []pgconn.FieldDescription{{Name: "payload"}},
			rawFn: func() [][]byte {
				return [][]byte{make([]byte, 512)}
			},
			valuesFn: func() ([]any, error) {
				valuesCalled = true
				return []any{"should-not-decode"}, nil
			},
		}
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
				return r, nil
			},
		}
		_, err := (&PostgresStore{}).queryReadOnlyMapsWithTx(context.Background(), tx, 100, 256, "SELECT payload FROM ctd_nodes")
		if !errors.Is(err, ErrReadOnlyByteLimitExceeded) {
			t.Fatalf("expected ErrReadOnlyByteLimitExceeded, got %v", err)
		}
		if valuesCalled {
			t.Fatal("expected RawValues byte check to abort before decoding oversized row")
		}
	})
}

func TestResolveNodesAllFreshnessAndLineageCTEBoundsAndPrune(t *testing.T) {
	t.Run("ResolveNodes All includes last_seen freshness bound", func(t *testing.T) {
		if !strings.Contains(resolveNodesAllQuery, "last_seen >= NOW() - INTERVAL '15 minutes'") {
			t.Fatalf("resolveNodesAllQuery missing last_seen freshness predicate:\n%s", resolveNodesAllQuery)
		}
		var capturedSQL string
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
				capturedSQL = sql
				return &fakePGXRows{
					remaining: 1,
					scanFn: func(dest ...any) error {
						*dest[0].(*string) = "node-fresh-01"
						return nil
					},
				}, nil
			},
		}
		nodes, err := (&PostgresStore{}).resolveNodesWithTx(context.Background(), tx, core.RunnerSelector{All: true})
		if err != nil {
			t.Fatalf("resolveNodesWithTx failed: %v", err)
		}
		if capturedSQL != resolveNodesAllQuery {
			t.Fatal("expected ResolveNodes(All: true) to execute resolveNodesAllQuery")
		}
		if len(nodes) != 1 || nodes[0] != "node-fresh-01" {
			t.Fatalf("unexpected resolved nodes: %v", nodes)
		}
	})

	t.Run("getLineageDAGWithTx bounds recursion depth, per-hop rows, and cycles", func(t *testing.T) {
		for _, tok := range []string{
			"CROSS JOIN LATERAL",
			"LIMIT $3",
			"CYCLE id SET is_cycle USING path",
			"WHERE NOT is_cycle",
			"LIMIT $4",
		} {
			if !strings.Contains(getLineageDAGQuery, tok) {
				t.Fatalf("getLineageDAGQuery missing %q", tok)
			}
		}

		var gotSQL string
		var gotArgs []any
		call := 0
		tx := &fakePGXTx{
			queryFn: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
				call++
				if call == 1 {
					gotSQL = sql
					gotArgs = args
				}
				return &fakePGXRows{remaining: 0}, nil
			},
		}
		dag, err := (&PostgresStore{}).getLineageDAGWithTx(context.Background(), tx, "art-root", 9999)
		if err != nil || dag == nil {
			t.Fatalf("getLineageDAGWithTx failed: dag=%v err=%v", dag, err)
		}
		if gotSQL != getLineageDAGQuery {
			t.Fatal("expected getLineageDAGWithTx to execute getLineageDAGQuery")
		}
		if len(gotArgs) != 4 || gotArgs[0] != "art-root" || gotArgs[1] != maxLineageDepth || gotArgs[2] != defaultLineageHopLimit || gotArgs[3] != defaultLineageTotalRows {
			t.Fatalf("unexpected lineage CTE args: %#v", gotArgs)
		}
	})

	t.Run("PruneLedgerBefore validates cutoff, clamps batchLimit, and returns affected count", func(t *testing.T) {
		store := &PostgresStore{}
		if _, err := store.PruneLedgerBefore(context.Background(), time.Time{}, 100); err == nil {
			t.Fatal("expected zero cutoff error")
		}

		var gotSQL string
		var gotArgs []any
		store.pruneExecFn = func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL = sql
			gotArgs = args
			return pgconn.NewCommandTag("DELETE 42"), nil
		}

		loc := time.FixedZone("CST", -6*3600)
		cutoff := time.Date(2026, 11, 1, 12, 0, 0, 0, loc)
		deleted, err := store.PruneLedgerBefore(context.Background(), cutoff, 0)
		if err != nil {
			t.Fatalf("PruneLedgerBefore failed: %v", err)
		}
		if deleted != 42 {
			t.Fatalf("expected 42 deleted rows, got %d", deleted)
		}
		if !strings.Contains(gotSQL, "FOR UPDATE SKIP LOCKED") || !strings.Contains(gotSQL, "DELETE FROM telemetric_ledger") {
			t.Fatalf("unexpected prune SQL: %s", gotSQL)
		}
		if len(gotArgs) != 2 || !gotArgs[0].(time.Time).Equal(cutoff.UTC()) || gotArgs[0].(time.Time).Location() != time.UTC || gotArgs[1] != DefaultLedgerPruneBatchLimit {
			t.Fatalf("unexpected prune args for default batchLimit: %#v", gotArgs)
		}

		if _, err := store.PruneLedgerBefore(context.Background(), cutoff, maxLedgerPruneBatchLimit*5); err != nil {
			t.Fatalf("PruneLedgerBefore clamp failed: %v", err)
		}
		if gotArgs[1] != maxLedgerPruneBatchLimit {
			t.Fatalf("expected batchLimit clamped to %d, got %v", maxLedgerPruneBatchLimit, gotArgs[1])
		}
	})
}
