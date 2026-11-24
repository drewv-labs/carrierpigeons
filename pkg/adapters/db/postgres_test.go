package db

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

	validQueries := []string{
		"SELECT node_id, created_at, updated_at FROM ctd_nodes;",
		"WITH recent AS (SELECT node_id, metrics FROM telemetric_ledger) SELECT * FROM recent",
		"SELECT 'drop table telemetric_ledger; update ctd_nodes' AS literal_note FROM ctd_nodes",
		"SELECT $$delete from telemetric_ledger$$ AS dollar_literal /* nested /* drop */ comment */ FROM ctd_nodes",
		"SELECT node_id -- trailing comment with delete keyword\nFROM ctd_nodes",
		"SELECT E'escaped\\'quote; drop table x' AS esc_lit FROM ctd_nodes",
		"SELECT U&'d\\0061t\\+000061' AS u_lit, U&\"col\\0020name\" FROM ctd_nodes",
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
	if !strings.Contains(ensureSchemaReadyQuery, "to_regclass('public.idx_ledger_metrics_path_gin') IS NULL") {
		t.Fatal("ensureSchemaReadyQuery must check that idx_ledger_metrics_path_gin has been dropped before taking fast-path")
	}
	if !strings.Contains(ensureSchemaDDL, "DROP INDEX IF EXISTS idx_ledger_metrics_path_gin;") {
		t.Fatal("ensureSchemaDDL must drop legacy idx_ledger_metrics_path_gin")
	}
	if strings.Contains(ensureSchemaDDL, "CREATE INDEX IF NOT EXISTS idx_ledger_metrics_path_gin") ||
		strings.Contains(ensureSchemaDDL, "jsonb_path_ops") {
		t.Fatal("ensureSchemaDDL must not recreate unused idx_ledger_metrics_path_gin")
	}

	initSQLPath := filepath.Join("..", "..", "..", "deployments", "compose", "init.sql")
	initSQL, err := os.ReadFile(initSQLPath)
	if err != nil {
		t.Fatalf("read %s: %v", initSQLPath, err)
	}
	if strings.Contains(string(initSQL), "idx_ledger_metrics_path_gin") {
		t.Fatalf("%s must not create unused idx_ledger_metrics_path_gin", initSQLPath)
	}
	if !strings.Contains(string(initSQL), "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM PUBLIC, pigeoncare_ro;") ||
		!strings.Contains(string(initSQL), "REVOKE CREATE ON SCHEMA public FROM PUBLIC, pigeoncare_ro;") {
		t.Fatalf("%s must revoke write/CREATE privileges from both PUBLIC and pigeoncare_ro", initSQLPath)
	}

	if !strings.Contains(upsertBatchNodeLastSeenSQL, "WHERE ctd_nodes.last_seen IS NULL") ||
		!strings.Contains(upsertBatchNodeLastSeenSQL, "ctd_nodes.last_seen < EXCLUDED.last_seen - INTERVAL '5 seconds'") {
		t.Fatalf("upsertBatchNodeLastSeenSQL missing 5s throttle predicate: %s", upsertBatchNodeLastSeenSQL)
	}
}
