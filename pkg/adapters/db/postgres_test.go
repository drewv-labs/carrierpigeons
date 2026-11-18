package db

import (
	"testing"
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
		"SELECT pg_sleep(30)",
		"SELECT lo_import('/etc/passwd')",
		"SELECT pg_read_file('/etc/hostname')",
	}
	for _, q := range blockedQueries {
		if _, err := ValidateSelectOnlySQL(q); err == nil {
			t.Fatalf("expected mutating or dangerous query %q to be rejected", q)
		}
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
