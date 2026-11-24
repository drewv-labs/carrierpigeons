package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoopConfigValidateSecurityFailClosed(t *testing.T) {
	cfg := DefaultCoopConfig()
	// Default binds 127.0.0.1:1883 -> allowed without auth for local dev.
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected loopback default CoopConfig to pass ValidateSecurity, got %v", err)
	}

	// Configured username/edge_username without password, or password without username, must fail closed.
	cfg.MQTT.Username = "coop-admin"
	cfg.MQTT.Password = ""
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected mqtt.username with empty password on loopback to fail ValidateSecurity")
	}
	cfg.MQTT.Username = ""
	cfg.MQTT.Password = "orphan-admin-pw"
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected mqtt.password without mqtt.username on loopback to fail ValidateSecurity")
	}
	cfg.MQTT.Password = ""
	cfg.MQTT.EdgeUsername = "edge-node"
	cfg.MQTT.EdgePassword = ""
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected mqtt.edge_username with empty password on loopback to fail ValidateSecurity")
	}
	cfg.MQTT.EdgeUsername = ""
	cfg.MQTT.EdgePassword = "orphan-edge-pw"
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected mqtt.edge_password without mqtt.edge_username on loopback to fail ValidateSecurity")
	}
	cfg.MQTT.EdgeUsername = "edge-node"
	cfg.MQTT.EdgePassword = "strong-edge-pw"
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected loopback CoopConfig with edge credentials to pass ValidateSecurity, got %v", err)
	}
	cfg.MQTT.EdgeUsername = ""
	cfg.MQTT.EdgePassword = ""

	// Invalid MNS mesh_name must fail closed.
	cfg.MeshName = "Bad_Mesh_Name"
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected uppercase/underscore MeshName to fail ValidateSecurity")
	}
	cfg.MeshName = "rv64-lab"
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected valid kebab-case MeshName to pass ValidateSecurity, got %v", err)
	}

	// Non-loopback without credentials must fail closed.
	cfg.MQTT.Listen = "0.0.0.0:1883"
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected non-loopback broker without credentials to fail ValidateSecurity")
	}

	// Explicit opt-in allows anonymous lab broker, but partial edge credentials still fail closed.
	cfg.MQTT.AllowInsecureAnonymous = true
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected AllowInsecureAnonymous opt-in to pass, got %v", err)
	}
	cfg.MQTT.EdgeUsername = "edge-node"
	cfg.MQTT.EdgePassword = ""
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected edge_username with empty edge_password to fail ValidateSecurity even with AllowInsecureAnonymous")
	}
	cfg.MQTT.EdgeUsername = ""

	// Credentials without opt-in pass.
	cfg.MQTT.AllowInsecureAnonymous = false
	cfg.MQTT.Username = "coop-admin"
	cfg.MQTT.Password = "strong-admin-pw"
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected authenticated non-loopback broker to pass, got %v", err)
	}

	// Remote database URL using default dev password must fail closed.
	cfg.Database.URL = "postgres://pigeoncoop_admin:ctd_dev_password@192.0.2.20:5432/edge_ledger"
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected remote database URL with default dev password to fail ValidateSecurity")
	}
	cfg.Database.AllowInsecureDefaults = true
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected AllowInsecureDefaults opt-in to pass, got %v", err)
	}
	cfg.Database.AllowInsecureDefaults = false
	cfg.Database.URL = "postgres://pigeoncoop_admin:rotated_prod_secret@192.0.2.20:5432/edge_ledger"
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected remote database URL with rotated password to pass, got %v", err)
	}
}

func TestConfigTopicValidationAndWorkstationReadOnlyDerivation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))

	badEdgePath := filepath.Join(dir, "bad-edge.toml")
	if err := os.WriteFile(badEdgePath, []byte(`node_id = "bad/node+#"`), 0o600); err != nil {
		t.Fatalf("write bad-edge.toml: %v", err)
	}
	if _, err := LoadEdgeConfig(badEdgePath); err == nil {
		t.Fatal("expected LoadEdgeConfig to reject invalid node_id")
	}

	badEdgeRootPath := filepath.Join(dir, "bad-edge-root.toml")
	if err := os.WriteFile(badEdgeRootPath, []byte("node_id = \"rv64-edge-01\"\n[brand]\ntopic_root = \"bad+root\"\n"), 0o600); err != nil {
		t.Fatalf("write bad-edge-root.toml: %v", err)
	}
	if _, err := LoadEdgeConfig(badEdgeRootPath); err == nil {
		t.Fatal("expected LoadEdgeConfig to reject invalid brand topic_root")
	}

	for _, badPrefix := range []string{"bad/+/ctd", "bad/#", "/leading/ctd", "trailing/ctd/", "bad//ctd", "bad space/ctd"} {
		badPrefixPath := filepath.Join(dir, "bad-edge-prefix.toml")
		content := "node_id = \"rv64-edge-01\"\ntopic_prefix = \"" + badPrefix + "\"\n"
		if err := os.WriteFile(badPrefixPath, []byte(content), 0o600); err != nil {
			t.Fatalf("write bad-edge-prefix.toml: %v", err)
		}
		if _, err := LoadEdgeConfig(badPrefixPath); err == nil {
			t.Fatalf("expected LoadEdgeConfig to reject invalid topic_prefix %q", badPrefix)
		}
	}

	badCoopPath := filepath.Join(dir, "bad-coop.toml")
	if err := os.WriteFile(badCoopPath, []byte("[brand]\ntopic_root = \"bad+root\"\n"), 0o600); err != nil {
		t.Fatalf("write bad-coop.toml: %v", err)
	}
	if _, err := LoadCoopConfig(badCoopPath); err == nil {
		t.Fatal("expected LoadCoopConfig to reject invalid topic_root")
	}

	badMeshCoopPath := filepath.Join(dir, "bad-mesh-coop.toml")
	if err := os.WriteFile(badMeshCoopPath, []byte("mesh_name = \"-bad-mesh\"\n"), 0o600); err != nil {
		t.Fatalf("write bad-mesh-coop.toml: %v", err)
	}
	if _, err := LoadCoopConfig(badMeshCoopPath); err == nil {
		t.Fatal("expected LoadCoopConfig to reject invalid mesh_name")
	}

	wsPath := filepath.Join(dir, "ws-remote.toml")
	wsContent := `
database_url = "postgres://pigeoncoop_admin:prod_pw@192.0.2.50:5432/edge_ledger"
readonly_database_url = "postgres://pigeoncare_ro:old@127.0.0.1:5432/edge_ledger"
`
	if err := os.WriteFile(wsPath, []byte(wsContent), 0o600); err != nil {
		t.Fatalf("write ws-remote.toml: %v", err)
	}
	t.Setenv("PIGEONS_MQTT_TLS_CERT", "/tmp/client.crt")
	t.Setenv("PIGEONS_MQTT_TLS_KEY", "/tmp/client.key")
	t.Setenv("PIGEONS_MQTT_TLS_INSECURE_SKIP_VERIFY", "true")
	t.Setenv("PIGEONEER_MQTT_TLS_INSECURE_SKIP_VERIFY", "true")

	wsCfg, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig failed: %v", err)
	}
	if wsCfg.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected non-loopback mismatched loopback readonly_database_url to be cleared without default password injection, got %q", wsCfg.ReadOnlyDatabaseURL)
	}
	t.Setenv("PIGEONCARE_RO_PASSWORD", "ro_explicit_secret")
	wsCfgWithROPW, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig with PIGEONCARE_RO_PASSWORD failed: %v", err)
	}
	if wsCfgWithROPW.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:ro_explicit_secret@192.0.2.50:5432/edge_ledger" {
		t.Fatalf("expected derived remote readonly_database_url when PIGEONCARE_RO_PASSWORD is set, got %q", wsCfgWithROPW.ReadOnlyDatabaseURL)
	}
	t.Setenv("PIGEONCARE_RO_PASSWORD", "")
	auth := wsCfg.EffectiveMQTTAuth()
	if auth.TLSCertFile != "/tmp/client.crt" || auth.TLSKeyFile != "/tmp/client.key" || !auth.TLSInsecureSkipVerify {
		t.Fatalf("expected workstation MQTT TLS env overrides, got %+v", auth)
	}

	okEdgePath := filepath.Join(dir, "ok-edge.toml")
	if err := os.WriteFile(okEdgePath, []byte("node_id = \"rv64-edge-01\"\ntopic_prefix = \"custom/edge/ctd\"\n"), 0o600); err != nil {
		t.Fatalf("write ok-edge.toml: %v", err)
	}
	edgeCfg, err := LoadEdgeConfig(okEdgePath)
	if err != nil {
		t.Fatalf("LoadEdgeConfig failed: %v", err)
	}
	if edgeCfg.TopicPrefix != "custom/edge/ctd" {
		t.Fatalf("expected custom topic_prefix 'custom/edge/ctd', got %q", edgeCfg.TopicPrefix)
	}
	if !edgeCfg.EffectiveMQTTAuth().TLSInsecureSkipVerify {
		t.Fatal("expected PIGEONEER_MQTT_TLS_INSECURE_SKIP_VERIFY override to apply")
	}

	t.Setenv("PIGEONEER_TOPIC_PREFIX", "custom/+/ctd")
	if _, err := LoadEdgeConfig(okEdgePath); err == nil {
		t.Fatal("expected LoadEdgeConfig to reject invalid PIGEONEER_TOPIC_PREFIX env override")
	}
	t.Setenv("PIGEONEER_TOPIC_PREFIX", "")
}

func TestLoadEdgeAndWorkstationBrandAndEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))

	edgePath := filepath.Join(dir, "edge.toml")
	content := `
node_id = "edge-rv64-01"
broker_url = "tcp://192.0.2.5:1883"

[brand]
topic_root = "fleetx"
env_prefix = "FLEETX"
`
	if err := os.WriteFile(edgePath, []byte(content), 0o600); err != nil {
		t.Fatalf("write edge.toml: %v", err)
	}

	t.Setenv("FLEETX_MQTT_USERNAME", "edge-agent")
	t.Setenv("FLEETX_MQTT_PASSWORD", "edge-secret")

	edgeCfg, err := LoadEdgeConfig(edgePath)
	if err != nil {
		t.Fatalf("LoadEdgeConfig failed: %v", err)
	}
	if edgeCfg.Runners.AllowEphemeral {
		t.Fatal("expected AllowEphemeral to default to false")
	}
	if edgeCfg.TopicPrefix != "fleetx/ctd" {
		t.Fatalf("expected TopicPrefix to follow brand topic_root 'fleetx/ctd', got %q", edgeCfg.TopicPrefix)
	}
	mqttAuth := edgeCfg.EffectiveMQTTAuth()
	if mqttAuth.Username != "edge-agent" || mqttAuth.Password != "edge-secret" {
		t.Fatalf("expected EffectiveMQTTAuth from FLEETX_MQTT_*, got user=%q pass=%q", mqttAuth.Username, mqttAuth.Password)
	}

	wsPath := filepath.Join(dir, "pigeons.toml")
	wsToml := `
[brand]
env_prefix = "FLEETX"
`
	if err := os.WriteFile(wsPath, []byte(wsToml), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}
	t.Setenv("FLEETX_MCP_AUTH_TOKEN", "tok-12345")
	wsCfg, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig failed: %v", err)
	}
	if wsCfg.MCPAuthToken != "tok-12345" {
		t.Fatalf("expected MCPAuthToken from FLEETX_MCP_AUTH_TOKEN, got %q", wsCfg.MCPAuthToken)
	}
}

func TestWorkstationMeshProfilesCurrentMeshStateAndTOMLUpsert(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state", "current_mesh")
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", stateFile)

	wsPath := filepath.Join(dir, "pigeons.toml")
	initialTOML := `# Operator workstation config comment preserved
operator = "drew"
broker_url = "tcp://127.0.0.1:1883"
database_url = "postgres://pigeoncoop_admin:ctd_dev_password@127.0.0.1:5432/edge_ledger"
readonly_database_url = "postgres://pigeoncare_ro:pigeoncare_readonly@127.0.0.1:5432/edge_ledger"

[llm]
provider = "ollama"
model = "qwen2.5-coder"
endpoint = "http://127.0.0.1:11434"

[permissions]
mode = "auto_edit"

[meshes.compute-mesh]
broker_url = "tcp://192.0.2.10:1883"
database_url = "postgres://pigeoncoop_admin:compute_pw@192.0.2.10:5432/edge_ledger"

[meshes.sensor-mesh]
broker_url = "tcp://192.0.2.20:1883"
database_url = "postgres://pigeoncoop_admin:sensor_pw@192.0.2.20:5432/edge_ledger"
readonly_database_url = "postgres://pigeoncare_ro:sensor_ro_pw@192.0.2.20:5432/edge_ledger"
`
	if err := os.WriteFile(wsPath, []byte(initialTOML), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	// 1. Switch active working mesh to compute-mesh via WriteCurrentMesh.
	if _, err := WriteCurrentMesh("compute-mesh"); err != nil {
		t.Fatalf("WriteCurrentMesh(compute-mesh): %v", err)
	}
	cfgA, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig(compute-mesh): %v", err)
	}
	if cfgA.ActiveMesh != "compute-mesh" || cfgA.ActiveMeshSource != "state" {
		t.Fatalf("expected ActiveMesh=compute-mesh (source=state), got %q (%q)", cfgA.ActiveMesh, cfgA.ActiveMeshSource)
	}
	if cfgA.BrokerURL != "tcp://192.0.2.10:1883" {
		t.Fatalf("unexpected BrokerURL for compute-mesh: %q", cfgA.BrokerURL)
	}
	// Top-level loopback ReadOnlyDatabaseURL must NOT leak into remote mesh profile,
	// and default "pigeoncare_readonly" must NOT be silently injected for non-loopback hosts.
	if cfgA.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected empty ReadOnlyDatabaseURL for remote compute-mesh without explicit RO config, got %q", cfgA.ReadOnlyDatabaseURL)
	}
	// Inherited top-level operator, LLM, and permissions must be preserved.
	if cfgA.Operator != "drew" || cfgA.Permissions.Mode != "auto_edit" || cfgA.LLM.Model != "qwen2.5-coder" {
		t.Fatalf("expected inherited workstation defaults, got operator=%q mode=%q model=%q",
			cfgA.Operator, cfgA.Permissions.Mode, cfgA.LLM.Model)
	}

	// 2. Switch to sensor-mesh and verify previous_mesh records compute-mesh.
	if _, err := WriteCurrentMesh("sensor-mesh"); err != nil {
		t.Fatalf("WriteCurrentMesh(sensor-mesh): %v", err)
	}
	prev, err := ReadPreviousMesh()
	if err != nil || prev != "compute-mesh" {
		t.Fatalf("expected ReadPreviousMesh()=compute-mesh, got %q (err=%v)", prev, err)
	}
	cfgB, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig(sensor-mesh): %v", err)
	}
	if cfgB.ActiveMesh != "sensor-mesh" || cfgB.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:sensor_ro_pw@192.0.2.20:5432/edge_ledger" {
		t.Fatalf("unexpected sensor-mesh config: active=%q ro=%q", cfgB.ActiveMesh, cfgB.ReadOnlyDatabaseURL)
	}

	// 3. Precedence: explicitMesh flag > PIGEONS_MESH env > current_mesh state.
	t.Setenv("PIGEONS_MESH", "compute-mesh")
	cfgEnv, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig with PIGEONS_MESH: %v", err)
	}
	if cfgEnv.ActiveMesh != "compute-mesh" || cfgEnv.ActiveMeshSource != "env" {
		t.Fatalf("expected env override compute-mesh, got %q (%q)", cfgEnv.ActiveMesh, cfgEnv.ActiveMeshSource)
	}
	cfgFlag, err := LoadWorkstationConfigForMesh(wsPath, "sensor-mesh")
	if err != nil {
		t.Fatalf("LoadWorkstationConfigForMesh(sensor-mesh): %v", err)
	}
	if cfgFlag.ActiveMesh != "sensor-mesh" || cfgFlag.ActiveMeshSource != "flag" {
		t.Fatalf("expected flag override sensor-mesh, got %q (%q)", cfgFlag.ActiveMesh, cfgFlag.ActiveMeshSource)
	}
	t.Setenv("PIGEONS_MESH", "")

	// 4. UpsertWorkstationMeshProfile preserves comments and adds/updates `[meshes.edge-lab]`.
	newBroker := "tcp://192.0.2.30:1883"
	newDB := "postgres://pigeoncoop_admin:edge_pw@192.0.2.30:5432/edge_ledger"
	newTopicRoot := "edgemesh"
	if _, err := UpsertWorkstationMeshProfile(wsPath, "edge-lab", MeshProfilePatch{
		BrokerURL:   &newBroker,
		DatabaseURL: &newDB,
		TopicRoot:   &newTopicRoot,
	}); err != nil {
		t.Fatalf("UpsertWorkstationMeshProfile(edge-lab): %v", err)
	}
	rawAfter, err := os.ReadFile(wsPath)
	if err != nil {
		t.Fatalf("read updated pigeons.toml: %v", err)
	}
	if !strings.Contains(string(rawAfter), "# Operator workstation config comment preserved") {
		t.Fatalf("expected UpsertWorkstationMeshProfile to preserve top-level comments, got:\n%s", string(rawAfter))
	}
	cfgEdge, err := LoadWorkstationConfigForMesh(wsPath, "edge-lab")
	if err != nil {
		t.Fatalf("LoadWorkstationConfigForMesh(edge-lab): %v", err)
	}
	if cfgEdge.BrokerURL != newBroker || cfgEdge.Brand.TopicRoot != "edgemesh" {
		t.Fatalf("unexpected upserted profile: broker=%q topic_root=%q", cfgEdge.BrokerURL, cfgEdge.Brand.TopicRoot)
	}

	// 5. ClearCurrentMesh removes active state cleanly.
	if err := ClearCurrentMesh(); err != nil {
		t.Fatalf("ClearCurrentMesh: %v", err)
	}
	cur, err := ReadCurrentMesh()
	if err != nil || cur != "" {
		t.Fatalf("expected empty current mesh after ClearCurrentMesh, got %q (err=%v)", cur, err)
	}
}

func TestWorkstationDBURLOverridePreventsSplitBrainAndDefaultROPassword(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	t.Setenv("PIGEONCARE_RO_PASSWORD", "")
	t.Setenv("PIGEONS_RO_DB_URL", "")
	t.Setenv("PIGEONCARE_DB_URL", "")
	t.Setenv("PIGEONMCP_DB_URL", "")

	wsPath := filepath.Join(dir, "pigeons.toml")
	wsTOML := `
database_url = "postgres://pigeoncoop_admin:mesh1_pw@192.0.2.10:5432/edge_ledger"
readonly_database_url = "postgres://pigeoncare_ro:mesh1_ro_pw@192.0.2.10:5432/edge_ledger"
`
	if err := os.WriteFile(wsPath, []byte(wsTOML), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	// 1. Cross-mesh DB_URL override to another non-loopback host clears stale ReadOnlyDatabaseURL
	//    without injecting default "pigeoncare_readonly".
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeoncoop_admin:mesh2_pw@192.0.2.20:5432/edge_ledger")
	cfg, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig failed: %v", err)
	}
	if cfg.DatabaseURL != "postgres://pigeoncoop_admin:mesh2_pw@192.0.2.20:5432/edge_ledger" {
		t.Fatalf("unexpected DatabaseURL: %q", cfg.DatabaseURL)
	}
	if cfg.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected stale cross-mesh ReadOnlyDatabaseURL to be cleared, got %q", cfg.ReadOnlyDatabaseURL)
	}

	// 2. Same cross-mesh override WITH PIGEONCARE_RO_PASSWORD re-derives against the new host.
	t.Setenv("PIGEONCARE_RO_PASSWORD", "mesh2_ro_pw")
	cfgWithROPW, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig with PIGEONCARE_RO_PASSWORD failed: %v", err)
	}
	if cfgWithROPW.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:mesh2_ro_pw@192.0.2.20:5432/edge_ledger" {
		t.Fatalf("expected re-derived ReadOnlyDatabaseURL on new host, got %q", cfgWithROPW.ReadOnlyDatabaseURL)
	}
	t.Setenv("PIGEONCARE_RO_PASSWORD", "")

	// 3. DB_URL override to loopback host re-derives local dev ReadOnlyDatabaseURL cleanly.
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeoncoop_admin:local_pw@127.0.0.1:5432/edge_ledger")
	cfgLoopback, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig loopback override failed: %v", err)
	}
	if cfgLoopback.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:pigeoncare_readonly@127.0.0.1:5432/edge_ledger" {
		t.Fatalf("expected loopback re-derived ReadOnlyDatabaseURL, got %q", cfgLoopback.ReadOnlyDatabaseURL)
	}

	// 4. DB_URL override on the same host preserves explicit readonly_database_url.
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeoncoop_admin:rotated_pw@192.0.2.10:5432/edge_ledger")
	cfgSameHost, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig same-host override failed: %v", err)
	}
	if cfgSameHost.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:mesh1_ro_pw@192.0.2.10:5432/edge_ledger" {
		t.Fatalf("expected same-host ReadOnlyDatabaseURL preserved, got %q", cfgSameHost.ReadOnlyDatabaseURL)
	}

	// 5. DB_URL override on the same host but different port (e.g. SSH tunnel) or DB path re-derives/clears RO URL.
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeoncoop_admin:tunnel_pw@192.0.2.10:15432/edge_ledger")
	cfgDiffPort, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig different-port override failed: %v", err)
	}
	if cfgDiffPort.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected different-port override to clear stale ReadOnlyDatabaseURL, got %q", cfgDiffPort.ReadOnlyDatabaseURL)
	}
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeoncoop_admin:mesh_b_pw@192.0.2.10:5432/mesh_b")
	cfgDiffPath, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig different-path override failed: %v", err)
	}
	if cfgDiffPath.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected different-path override to clear stale ReadOnlyDatabaseURL, got %q", cfgDiffPath.ReadOnlyDatabaseURL)
	}

	// 6. Explicit RO_DB_URL override always wins.
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeoncoop_admin:mesh2_pw@192.0.2.20:5432/edge_ledger")
	t.Setenv("PIGEONS_RO_DB_URL", "postgres://pigeoncare_ro:explicit_ro@192.0.2.20:5432/edge_ledger")
	cfgExplicitRO, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig explicit RO override failed: %v", err)
	}
	if cfgExplicitRO.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:explicit_ro@192.0.2.20:5432/edge_ledger" {
		t.Fatalf("expected explicit PIGEONS_RO_DB_URL to apply, got %q", cfgExplicitRO.ReadOnlyDatabaseURL)
	}
}

func TestRedactURLAndBrandPrefixedEnvHelpers(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"   ", ""},
		{"tcp://192.0.2.10:1883", "tcp://192.0.2.10:1883"},
		{"postgres://pigeoncoop_admin:super_secret@192.0.2.10:5432/edge_ledger?sslmode=require", "postgres://pigeoncoop_admin:xxxxx@192.0.2.10:5432/edge_ledger?sslmode=require"},
		{"postgres://admin:p%40ss@[2001:db8::1]:5432/edge_ledger", "postgres://admin:xxxxx@[2001:db8::1]:5432/edge_ledger"},
		{"postgres://192.0.2.10:5432/edge_ledger?user=admin&password=secret", "postgres://192.0.2.10:5432/edge_ledger?password=xxxxx&user=admin"},
		{"postgres://192.0.2.10:5432/edge_ledger?token=tok123&api_key=k999&sslmode=require", "postgres://192.0.2.10:5432/edge_ledger?api_key=xxxxx&sslmode=require&token=xxxxx"},
		{"tcp://mqtt_user:mqtt_secret@192.0.2.10:1883", "tcp://mqtt_user:xxxxx@192.0.2.10:1883"},
		{"postgres://pigeoncoop_admin@192.0.2.10:5432/edge_ledger", "postgres://pigeoncoop_admin@192.0.2.10:5432/edge_ledger"},
		{"postgres://admin:bad%ZZpw@192.0.2.10:5432/edge_ledger?password=secret", "postgres://admin:xxxxx@192.0.2.10:5432/edge_ledger"},
		{"admin:secret@192.0.2.10:5432", "admin:xxxxx@192.0.2.10:5432"},
		{"host=192.0.2.10 password=secret", "[redacted-invalid-url]"},
		{"://broken", "[redacted-invalid-url]"},
	}
	for _, tc := range cases {
		got := RedactURL(tc.in)
		if got != tc.want {
			t.Fatalf("RedactURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Contains(got, "secret") || strings.Contains(got, "tok123") || strings.Contains(got, "k999") {
			t.Fatalf("RedactURL(%q) leaked credential in %q", tc.in, got)
		}
	}

	t.Setenv("DATABASE_URL", "postgres://fallback@192.0.2.1:5432/db")
	t.Setenv("PIGEONCOOP_DB_URL", "postgres://coop@192.0.2.2:5432/db")
	t.Setenv("PIGEONS_DB_URL", "postgres://pigeons@192.0.2.3:5432/db")
	t.Setenv("FLEETX_DB_URL", "postgres://fleetx@192.0.2.4:5432/db")

	if got := ResolveWorkstationDBURLEnv("fleetx_"); got != "postgres://fleetx@192.0.2.4:5432/db" {
		t.Fatalf("expected FLEETX_DB_URL precedence, got %q", got)
	}
	if got := ResolveWorkstationDBURLEnv(""); got != "postgres://pigeons@192.0.2.3:5432/db" {
		t.Fatalf("expected PIGEONS_DB_URL fallback when no brand prefix, got %q", got)
	}

	t.Setenv("PIGEONMCP_DB_URL", "postgres://mcp@192.0.2.1:5432/db")
	t.Setenv("PIGEONCARE_DB_URL", "postgres://care@192.0.2.2:5432/db")
	t.Setenv("PIGEONS_RO_DB_URL", "postgres://pigeons_ro@192.0.2.3:5432/db")
	t.Setenv("FLEETX_RO_DB_URL", "postgres://fleetx_ro@192.0.2.4:5432/db")

	if got := ResolveWorkstationRODBURLEnv("FLEETX"); got != "postgres://fleetx_ro@192.0.2.4:5432/db" {
		t.Fatalf("expected FLEETX_RO_DB_URL precedence, got %q", got)
	}
	if got := ResolveWorkstationRODBURLEnv(""); got != "postgres://pigeons_ro@192.0.2.3:5432/db" {
		t.Fatalf("expected PIGEONS_RO_DB_URL fallback when no brand prefix, got %q", got)
	}
}
