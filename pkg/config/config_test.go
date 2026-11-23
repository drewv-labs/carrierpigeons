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

	// Explicit opt-in allows anonymous lab broker.
	cfg.MQTT.AllowInsecureAnonymous = true
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected AllowInsecureAnonymous opt-in to pass, got %v", err)
	}

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
	if wsCfg.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:pigeoncare_readonly@192.0.2.50:5432/edge_ledger" {
		t.Fatalf("expected auto-derived remote readonly_database_url, got %q", wsCfg.ReadOnlyDatabaseURL)
	}
	auth := wsCfg.EffectiveMQTTAuth()
	if auth.TLSCertFile != "/tmp/client.crt" || auth.TLSKeyFile != "/tmp/client.key" || !auth.TLSInsecureSkipVerify {
		t.Fatalf("expected workstation MQTT TLS env overrides, got %+v", auth)
	}

	okEdgePath := filepath.Join(dir, "ok-edge.toml")
	if err := os.WriteFile(okEdgePath, []byte(`node_id = "rv64-edge-01"`), 0o600); err != nil {
		t.Fatalf("write ok-edge.toml: %v", err)
	}
	edgeCfg, err := LoadEdgeConfig(okEdgePath)
	if err != nil {
		t.Fatalf("LoadEdgeConfig failed: %v", err)
	}
	if !edgeCfg.EffectiveMQTTAuth().TLSInsecureSkipVerify {
		t.Fatal("expected PIGEONEER_MQTT_TLS_INSECURE_SKIP_VERIFY override to apply")
	}
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
	// Top-level loopback ReadOnlyDatabaseURL must NOT leak into remote mesh profile!
	if cfgA.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:pigeoncare_readonly@192.0.2.10:5432/edge_ledger" {
		t.Fatalf("expected derived remote ReadOnlyDatabaseURL for compute-mesh, got %q", cfgA.ReadOnlyDatabaseURL)
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
