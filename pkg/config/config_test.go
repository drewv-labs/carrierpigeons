package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/jackc/pgx/v5/pgxpool"
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

func TestUpsertWorkstationMeshProfilePreservesArrayOfTables(t *testing.T) {
	if hdr, isArr, ok := parseTOMLTableHeaderPath("[[custom_items]] # trailing"); !ok || !isArr || hdr != "custom_items" {
		t.Fatalf("expected [[custom_items]] to parse as array table, got hdr=%q isArr=%v ok=%v", hdr, isArr, ok)
	}
	if hdr, isArr, ok := parseTOMLTableHeaderPath("[meshes.alpha]"); !ok || isArr || hdr != "meshes.alpha" {
		t.Fatalf("expected [meshes.alpha] to parse as standard table, got hdr=%q isArr=%v ok=%v", hdr, isArr, ok)
	}
	for _, bad := range []string{"[[1, 2]]", "[[unclosed]", "[single]]", "[]", "[[]]"} {
		if hdr, isArr, ok := parseTOMLTableHeaderPath(bad); ok {
			t.Fatalf("expected %q not to parse as table header, got hdr=%q isArr=%v", bad, hdr, isArr)
		}
	}

	dir := t.TempDir()
	wsPath := filepath.Join(dir, "pigeons.toml")
	initialTOML := `# Preserve header comment
operator = "drew"

[[pre_items]]
name = "before-alpha"
weight = 1

[meshes.alpha]
broker_url = "tcp://192.0.2.10:1883"
database_url = "postgres://pigeoncoop_admin:old_pw@192.0.2.10:5432/edge_ledger"

[meshes.alpha.llm]
provider = "ollama"
model = "old-model"

[[custom_items]]
name = "keep-me-1"
tags = ["a", "b"]

[[custom_items]]
name = "keep-me-2"

[[meshes.other.hooks]]
event = "on_connect"
cmd = "echo other"

[[meshes.alpha.hooks]]
event = "on_connect"
cmd = "echo alpha"

[meshes.beta]
broker_url = "tcp://192.0.2.20:1883"
`
	if err := os.WriteFile(wsPath, []byte(initialTOML), 0o600); err != nil {
		t.Fatalf("write initial toml: %v", err)
	}

	newBroker := "tcp://192.0.2.11:1883"
	newDB := "postgres://pigeoncoop_admin:new_pw@192.0.2.11:5432/edge_ledger"
	if _, err := UpsertWorkstationMeshProfile(wsPath, "alpha", MeshProfilePatch{
		BrokerURL:   &newBroker,
		DatabaseURL: &newDB,
	}); err != nil {
		t.Fatalf("UpsertWorkstationMeshProfile(alpha): %v", err)
	}

	rawAfter, err := os.ReadFile(wsPath)
	if err != nil {
		t.Fatalf("read updated toml: %v", err)
	}
	got := string(rawAfter)
	for _, want := range []string{
		"# Preserve header comment",
		"[[pre_items]]",
		`name = "before-alpha"`,
		"[[custom_items]]",
		`name = "keep-me-1"`,
		`name = "keep-me-2"`,
		"[[meshes.other.hooks]]",
		`cmd = "echo other"`,
		"[[meshes.alpha.hooks]]",
		`cmd = "echo alpha"`,
		"[meshes.beta]",
		`broker_url = "tcp://192.0.2.20:1883"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected updated TOML to preserve %q verbatim, got:\n%s", want, got)
		}
	}

	cfgAlpha, err := LoadWorkstationConfigForMesh(wsPath, "alpha")
	if err != nil {
		t.Fatalf("LoadWorkstationConfigForMesh(alpha): %v", err)
	}
	if cfgAlpha.BrokerURL != newBroker || cfgAlpha.DatabaseURL != newDB {
		t.Fatalf("unexpected alpha config after upsert: broker=%q db=%q", cfgAlpha.BrokerURL, cfgAlpha.DatabaseURL)
	}
	if cfgAlpha.LLM.Model != "old-model" {
		t.Fatalf("expected existing [meshes.alpha.llm] fields preserved, got %q", cfgAlpha.LLM.Model)
	}
}

func TestBrandIsolatedConfigAndStateConcurrency(t *testing.T) {
	initialActive := brand.Active()
	rootDir := t.TempDir()
	fakeHome := filepath.Join(rootDir, "home")
	if err := os.MkdirAll(fakeHome, 0o700); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}
	t.Setenv("HOME", fakeHome)
	// Ensure default pigeons env vars do not bleed into custom brand isolation.
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(rootDir, "pigeons-state", "current_mesh"))
	t.Setenv("PIGEONS_CONFIG", filepath.Join(rootDir, "pigeons.toml"))
	t.Setenv("PIGEONS_MESH", "pigeons-only-mesh")
	if err := os.WriteFile(filepath.Join(rootDir, "pigeons.toml"), []byte("operator = \"pigeons-op\"\n[meshes.pigeons-only-mesh]\nbroker_url = \"tcp://127.0.0.1:1883\"\n"), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}

	brandA := brand.Spec{
		ProductName: "AlphaTelemetry",
		CLIName:     "alphacli",
		TopicRoot:   "alpha",
		EnvPrefix:   "ALPHA",
	}
	brandB := brand.Spec{
		ProductName: "BetaTelemetry",
		CLIName:     "betacli",
		TopicRoot:   "beta",
		EnvPrefix:   "BETA",
	}

	// Zero-value brand.Spec defaults to brand.Default() conventions.
	defPath, err := CurrentMeshStatePathForBrand(brand.Spec{})
	if err != nil {
		t.Fatalf("CurrentMeshStatePathForBrand(zero): %v", err)
	}
	if defPath != filepath.Join(rootDir, "pigeons-state", "current_mesh") {
		t.Fatalf("expected zero-value brand to use PIGEONS_CURRENT_MESH_FILE, got %q", defPath)
	}

	// Custom brands ignore PIGEONS_CURRENT_MESH_FILE and resolve ~/.config/<cli_slug>/current_mesh.
	pathAHome, err := CurrentMeshStatePathForBrand(brandA)
	if err != nil {
		t.Fatalf("CurrentMeshStatePathForBrand(brandA): %v", err)
	}
	wantAHome := filepath.Join(fakeHome, ".config", "alphacli", "current_mesh")
	if pathAHome != wantAHome {
		t.Fatalf("expected brandA default state path %q, got %q", wantAHome, pathAHome)
	}

	// Custom <BRAND>_CURRENT_MESH_FILE and <BRAND>_CONFIG override isolated state & config paths.
	stateFileA := filepath.Join(rootDir, "state-a", "current_mesh")
	stateFileB := filepath.Join(rootDir, "state-b", "current_mesh")
	cfgFileA := filepath.Join(rootDir, "cfg-a", "alphacli.toml")
	cfgFileB := filepath.Join(rootDir, "cfg-b", "betacli.toml")

	t.Setenv("ALPHA_CURRENT_MESH_FILE", stateFileA)
	t.Setenv("BETA_CURRENT_MESH_FILE", stateFileB)
	t.Setenv("ALPHA_CONFIG", cfgFileA)
	t.Setenv("BETA_CONFIG", cfgFileB)
	t.Setenv("ALPHA_OPERATOR", "alpha-op")
	t.Setenv("BETA_OPERATOR", "beta-op")

	if err := os.MkdirAll(filepath.Dir(cfgFileA), 0o700); err != nil {
		t.Fatalf("mkdir cfg-a: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfgFileB), 0o700); err != nil {
		t.Fatalf("mkdir cfg-b: %v", err)
	}
	if err := os.WriteFile(cfgFileA, []byte("[meshes.alpha-one]\nbroker_url = \"tcp://192.0.2.10:1883\"\n[meshes.alpha-two]\nbroker_url = \"tcp://192.0.2.11:1883\"\n"), 0o600); err != nil {
		t.Fatalf("write cfgFileA: %v", err)
	}
	if err := os.WriteFile(cfgFileB, []byte("[meshes.beta-one]\nbroker_url = \"tcp://192.0.2.20:1883\"\n[meshes.beta-two]\nbroker_url = \"tcp://192.0.2.21:1883\"\n"), 0o600); err != nil {
		t.Fatalf("write cfgFileB: %v", err)
	}

	var wg sync.WaitGroup
	runBrandCycle := func(spec brand.Spec, m1, m2, wantCfgPath, wantOp, wantTopicRoot, upsertBroker string) {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if _, err := WriteCurrentMeshForBrand(spec, m1); err != nil {
				t.Errorf("%s WriteCurrentMeshForBrand(%s): %v", spec.CLIName, m1, err)
				return
			}
			if _, err := WriteCurrentMeshForBrand(spec, m2); err != nil {
				t.Errorf("%s WriteCurrentMeshForBrand(%s): %v", spec.CLIName, m2, err)
				return
			}
			cur, err := ReadCurrentMeshForBrand(spec)
			if err != nil || cur != m2 {
				t.Errorf("%s ReadCurrentMeshForBrand = %q (err=%v), want %q", spec.CLIName, cur, err, m2)
				return
			}
			prev, err := ReadPreviousMeshForBrand(spec)
			if err != nil || prev != m1 {
				t.Errorf("%s ReadPreviousMeshForBrand = %q (err=%v), want %q", spec.CLIName, prev, err, m1)
				return
			}

			baseCfg, err := LoadBaseWorkstationConfigForBrand("", spec)
			if err != nil {
				t.Errorf("%s LoadBaseWorkstationConfigForBrand: %v", spec.CLIName, err)
				return
			}
			if baseCfg.ConfigPath != wantCfgPath {
				t.Errorf("%s ConfigPath = %q, want %q", spec.CLIName, baseCfg.ConfigPath, wantCfgPath)
				return
			}
			if baseCfg.Brand.TopicRoot != wantTopicRoot {
				t.Errorf("%s Brand.TopicRoot = %q, want %q", spec.CLIName, baseCfg.Brand.TopicRoot, wantTopicRoot)
				return
			}

			wsCfg, err := LoadWorkstationConfigForMeshAndBrand("", "", spec)
			if err != nil {
				t.Errorf("%s LoadWorkstationConfigForMeshAndBrand: %v", spec.CLIName, err)
				return
			}
			if wsCfg.ActiveMesh != m2 || wsCfg.ActiveMeshSource != "state" {
				t.Errorf("%s ActiveMesh=%q source=%q, want %q (state)", spec.CLIName, wsCfg.ActiveMesh, wsCfg.ActiveMeshSource, m2)
				return
			}
			if wsCfg.Operator != wantOp {
				t.Errorf("%s Operator=%q, want %q", spec.CLIName, wsCfg.Operator, wantOp)
				return
			}

			savedPath, err := UpsertWorkstationMeshProfileForBrand("", spec, m2, MeshProfilePatch{
				BrokerURL: &upsertBroker,
			})
			if err != nil {
				t.Errorf("%s UpsertWorkstationMeshProfileForBrand: %v", spec.CLIName, err)
				return
			}
			if savedPath != wantCfgPath {
				t.Errorf("%s upsert savedPath=%q, want %q", spec.CLIName, savedPath, wantCfgPath)
				return
			}

			if err := ClearCurrentMeshForBrand(spec); err != nil {
				t.Errorf("%s ClearCurrentMeshForBrand: %v", spec.CLIName, err)
				return
			}
			afterClear, err := ReadCurrentMeshForBrand(spec)
			if err != nil || afterClear != "" {
				t.Errorf("%s ReadCurrentMeshForBrand after clear = %q (err=%v), want empty", spec.CLIName, afterClear, err)
				return
			}
		}
	}

	wg.Add(2)
	go runBrandCycle(brandA, "alpha-one", "alpha-two", cfgFileA, "alpha-op", "alpha", "tcp://192.0.2.111:1883")
	go runBrandCycle(brandB, "beta-one", "beta-two", cfgFileB, "beta-op", "beta", "tcp://192.0.2.222:1883")
	wg.Wait()

	// Verify brand-prefixed MESH env override is also isolated per brand.
	t.Setenv("ALPHA_MESH", "alpha-one")
	t.Setenv("BETA_MESH", "beta-one")
	wg.Add(2)
	go func() {
		defer wg.Done()
		cfg, err := LoadWorkstationConfigForMeshAndBrand("", "", brandA)
		if err != nil || cfg.ActiveMesh != "alpha-one" || cfg.ActiveMeshSource != "env" {
			t.Errorf("brandA env mesh resolution failed: cfg=%+v err=%v", cfg, err)
		}
	}()
	go func() {
		defer wg.Done()
		cfg, err := LoadWorkstationConfigForMeshAndBrand("", "", brandB)
		if err != nil || cfg.ActiveMesh != "beta-one" || cfg.ActiveMeshSource != "env" {
			t.Errorf("brandB env mesh resolution failed: cfg=%+v err=%v", cfg, err)
		}
	}()
	wg.Wait()

	if brand.Active() != initialActive {
		t.Fatalf("global brand.Active() mutated: got %+v, want %+v", brand.Active(), initialActive)
	}
}

func TestCWDConfigNonAutoloadAndEtcBootstrapMerge(t *testing.T) {
	t.Run("cwd_non_autoload", func(t *testing.T) {
		fakeHome := t.TempDir()
		fakeEtc := t.TempDir()
		cwdDir := t.TempDir()

		prevEtc := systemConfigRoot
		systemConfigRoot = fakeEtc
		t.Cleanup(func() { systemConfigRoot = prevEtc })

		t.Setenv("HOME", fakeHome)
		t.Setenv("PIGEONS_CONFIG", "")
		t.Setenv("PIGEONEER_CONFIG", "")
		t.Setenv("PIGEONCOOP_CONFIG", "")
		t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(fakeHome, "current_mesh"))

		if err := os.WriteFile(filepath.Join(cwdDir, "pigeons.toml"), []byte("operator = \"evil-cwd\"\n[meshes.cwd-only]\nbroker_url = \"tcp://192.0.2.99:1883\"\n"), 0o600); err != nil {
			t.Fatalf("write cwd pigeons.toml: %v", err)
		}
		if err := os.WriteFile(filepath.Join(cwdDir, "pigeoneer.toml"), []byte("node_id = \"evil-cwd-edge\"\n"), 0o600); err != nil {
			t.Fatalf("write cwd pigeoneer.toml: %v", err)
		}
		if err := os.WriteFile(filepath.Join(cwdDir, "pigeoncoop.toml"), []byte("mesh_name = \"evil-cwd-mesh\"\n"), 0o600); err != nil {
			t.Fatalf("write cwd pigeoncoop.toml: %v", err)
		}

		origWD, err := os.Getwd()
		if err != nil {
			t.Fatalf("Getwd: %v", err)
		}
		if err := os.Chdir(cwdDir); err != nil {
			t.Fatalf("Chdir(%s): %v", cwdDir, err)
		}
		t.Cleanup(func() { _ = os.Chdir(origWD) })

		baseWS, err := LoadBaseWorkstationConfig("")
		if err != nil {
			t.Fatalf("LoadBaseWorkstationConfig: %v", err)
		}
		if baseWS.ConfigPath != "" || baseWS.Operator == "evil-cwd" || len(baseWS.Meshes) != 0 {
			t.Fatalf("expected CWD pigeons.toml NOT to autoload, got path=%q operator=%q meshes=%v", baseWS.ConfigPath, baseWS.Operator, baseWS.Meshes)
		}

		wsCfg, err := LoadWorkstationConfig("")
		if err != nil {
			t.Fatalf("LoadWorkstationConfig: %v", err)
		}
		if wsCfg.ConfigPath != "" || wsCfg.Operator == "evil-cwd" {
			t.Fatalf("expected LoadWorkstationConfig NOT to autoload CWD pigeons.toml, got %+v", wsCfg)
		}

		edgeCfg, err := LoadEdgeConfig("")
		if err != nil {
			t.Fatalf("LoadEdgeConfig: %v", err)
		}
		if edgeCfg.NodeID == "evil-cwd-edge" {
			t.Fatalf("expected LoadEdgeConfig NOT to autoload CWD pigeoneer.toml")
		}

		coopCfg, err := LoadCoopConfig("")
		if err != nil {
			t.Fatalf("LoadCoopConfig: %v", err)
		}
		if coopCfg.MeshName == "evil-cwd-mesh" {
			t.Fatalf("expected LoadCoopConfig NOT to autoload CWD pigeoncoop.toml")
		}

		// Upsert without explicit path or *_CONFIG must write to ~/.config/<cli>/<cli>.toml, NOT CWD ./pigeons.toml.
		userBroker := "tcp://192.0.2.55:1883"
		savedPath, err := UpsertWorkstationMeshProfile("", "user-mesh", MeshProfilePatch{BrokerURL: &userBroker})
		if err != nil {
			t.Fatalf("UpsertWorkstationMeshProfile: %v", err)
		}
		wantUserPath := filepath.Join(fakeHome, ".config", "pigeons", "pigeons.toml")
		if savedPath != wantUserPath {
			t.Fatalf("expected upsert path %q, got %q", wantUserPath, savedPath)
		}
		cwdRaw, err := os.ReadFile(filepath.Join(cwdDir, "pigeons.toml"))
		if err != nil || strings.Contains(string(cwdRaw), "user-mesh") {
			t.Fatalf("expected CWD pigeons.toml to remain untouched, got err=%v raw=%q", err, string(cwdRaw))
		}

		// Explicit path and <BRAND>_CONFIG still load local files cleanly.
		explicitWS, err := LoadWorkstationConfig(filepath.Join(cwdDir, "pigeons.toml"))
		if err != nil || explicitWS.Operator != "evil-cwd" {
			t.Fatalf("expected explicit path load to succeed, got op=%q err=%v", explicitWS.Operator, err)
		}
		t.Setenv("PIGEONS_CONFIG", filepath.Join(cwdDir, "pigeons.toml"))
		envWS, err := LoadWorkstationConfig("")
		if err != nil || envWS.Operator != "evil-cwd" {
			t.Fatalf("expected PIGEONS_CONFIG load to succeed, got op=%q err=%v", envWS.Operator, err)
		}
	})

	t.Run("etc_bootstrap_merge", func(t *testing.T) {
		fakeHome := t.TempDir()
		fakeEtc := t.TempDir()

		prevEtc := systemConfigRoot
		systemConfigRoot = fakeEtc
		t.Cleanup(func() { systemConfigRoot = prevEtc })

		t.Setenv("HOME", fakeHome)
		t.Setenv("PIGEONS_CONFIG", "")
		t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(fakeHome, "current_mesh"))

		etcPath := filepath.Join(fakeEtc, "pigeons", "pigeons.toml")
		if err := os.MkdirAll(filepath.Dir(etcPath), 0o755); err != nil {
			t.Fatalf("mkdir fake etc: %v", err)
		}
		etcTOML := `# System-wide fleet defaults
operator = "fleet-admin"

[llm]
provider = "ollama"
model = "qwen2.5-coder"

[meshes.prod-mesh]
broker_url = "tls://192.0.2.10:8883"
database_url = "postgres://pigeoncoop_admin:prod_pw@192.0.2.10:5432/edge_ledger"

[meshes.shared-mesh]
broker_url = "tcp://192.0.2.20:1883"
database_url = "postgres://pigeoncoop_admin:shared_pw@192.0.2.20:5432/edge_ledger"
`
		if err := os.WriteFile(etcPath, []byte(etcTOML), 0o644); err != nil {
			t.Fatalf("write etc pigeons.toml: %v", err)
		}

		// Confirm /etc is initially active.
		baseBefore, err := LoadBaseWorkstationConfig("")
		if err != nil || baseBefore.ConfigPath != etcPath {
			t.Fatalf("expected active config %q, got %q (err=%v)", etcPath, baseBefore.ConfigPath, err)
		}

		// Bootstrap ~/.config/pigeons/pigeons.toml by adding a new mesh profile.
		labBroker := "tcp://192.0.2.30:1883"
		userPath, err := UpsertWorkstationMeshProfile("", "lab-mesh", MeshProfilePatch{BrokerURL: &labBroker})
		if err != nil {
			t.Fatalf("UpsertWorkstationMeshProfile bootstrap failed: %v", err)
		}
		wantUserPath := filepath.Join(fakeHome, ".config", "pigeons", "pigeons.toml")
		if userPath != wantUserPath {
			t.Fatalf("expected user path %q, got %q", wantUserPath, userPath)
		}

		rawUser, err := os.ReadFile(userPath)
		if err != nil {
			t.Fatalf("read bootstrapped user config: %v", err)
		}
		if !strings.Contains(string(rawUser), "# System-wide fleet defaults") {
			t.Fatalf("expected /etc comments preserved in user config, got:\n%s", string(rawUser))
		}

		// All /etc base settings and profiles must still be active!
		prodCfg, err := LoadWorkstationConfigForMesh("", "prod-mesh")
		if err != nil {
			t.Fatalf("LoadWorkstationConfigForMesh(prod-mesh) after bootstrap: %v", err)
		}
		if prodCfg.ConfigPath != wantUserPath || prodCfg.Operator != "fleet-admin" ||
			prodCfg.BrokerURL != "tls://192.0.2.10:8883" ||
			prodCfg.DatabaseURL != "postgres://pigeoncoop_admin:prod_pw@192.0.2.10:5432/edge_ledger" {
			t.Fatalf("expected /etc base profile prod-mesh preserved after bootstrap, got %+v", prodCfg)
		}

		// Also verify updating an existing /etc profile on bootstrap merges its unpatched fields.
		fakeHome2 := t.TempDir()
		t.Setenv("HOME", fakeHome2)
		updatedBroker := "tcp://192.0.2.21:1883"
		if _, err := UpsertWorkstationMeshProfile("", "shared-mesh", MeshProfilePatch{BrokerURL: &updatedBroker}); err != nil {
			t.Fatalf("UpsertWorkstationMeshProfile(shared-mesh): %v", err)
		}
		sharedCfg, err := LoadWorkstationConfigForMesh("", "shared-mesh")
		if err != nil {
			t.Fatalf("LoadWorkstationConfigForMesh(shared-mesh): %v", err)
		}
		if sharedCfg.BrokerURL != updatedBroker || sharedCfg.DatabaseURL != "postgres://pigeoncoop_admin:shared_pw@192.0.2.20:5432/edge_ledger" {
			t.Fatalf("expected /etc shared-mesh database_url merged with patched broker_url, got broker=%q db=%q", sharedCfg.BrokerURL, sharedCfg.DatabaseURL)
		}
		if _, hasProd := sharedCfg.Meshes["prod-mesh"]; !hasProd {
			t.Fatalf("expected /etc prod-mesh preserved alongside patched shared-mesh")
		}
	})
}

func TestPostgresDSNLoopbackAndCoopSecurityValidation(t *testing.T) {
	t.Setenv("PGHOST", "")
	t.Setenv("PGPORT", "")
	t.Setenv("PGUSER", "")
	t.Setenv("PGPASSWORD", "")
	t.Setenv("PGDATABASE", "")
	t.Setenv("PGSSLMODE", "")

	loopbackCases := []string{
		"postgres://pigeoncoop_admin:ctd_dev_password@127.0.0.1:5432/edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@localhost:5432/edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@[::1]:5432/edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@?host=127.0.0.1&dbname=edge_ledger",
		"host=127.0.0.1 port=5432 user=pigeoncoop_admin password=ctd_dev_password dbname=edge_ledger",
		"host=localhost dbname=edge_ledger",
		"host=::1 port=5432 dbname=edge_ledger",
		"host=/var/run/postgresql dbname=edge_ledger",
	}
	for _, dsn := range loopbackCases {
		if !isURLHostLoopback(dsn) {
			t.Fatalf("expected isURLHostLoopback(%q) == true", dsn)
		}
	}

	remoteOrInvalidCases := []string{
		"",
		"://broken-dsn",
		"postgres://pigeoncoop_admin:ctd_dev_password@192.0.2.20:5432/edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@?host=192.0.2.20&dbname=edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@127.0.0.1:5432,192.0.2.20:5432/edge_ledger",
		"host=192.0.2.20 port=5432 user=pigeoncoop_admin password=ctd_dev_password dbname=edge_ledger",
		"host=127.0.0.1,192.0.2.20 port=5432,5432 user=pigeoncoop_admin password=ctd_dev_password dbname=edge_ledger",
	}
	for _, dsn := range remoteOrInvalidCases {
		if isURLHostLoopback(dsn) {
			t.Fatalf("expected isURLHostLoopback(%q) == false", dsn)
		}
	}

	// CoopConfig.ValidateSecurity must enforce default-password guard on libpq key=value,
	// query ?host=, and multi-host DSNs just like URI DSNs.
	for _, badRemote := range []string{
		"host=192.0.2.20 port=5432 user=pigeoncoop_admin password=ctd_dev_password dbname=edge_ledger",
		"host=192.0.2.20 port=5432 user=ctd_ingestor password=ctd_ingest_password dbname=edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@?host=192.0.2.20&dbname=edge_ledger",
		"host=127.0.0.1,192.0.2.20 port=5432,5432 user=pigeoncoop_admin password=ctd_dev_password dbname=edge_ledger",
		"postgres://pigeoncoop_admin:ctd_dev_password@127.0.0.1:5432,192.0.2.20:5432/edge_ledger",
	} {
		cfg := DefaultCoopConfig()
		cfg.Database.URL = badRemote
		if err := cfg.ValidateSecurity(); err == nil {
			t.Fatalf("expected ValidateSecurity to reject remote DSN %q with default password", badRemote)
		}
	}

	// Malformed DSN with password must fail closed without echoing the secret.
	badCfg := DefaultCoopConfig()
	badCfg.Database.URL = "postgres://admin:super_secret_pw@192.0.2.20:5432/edge_ledger?sslmode=bogus"
	if err := badCfg.ValidateSecurity(); err == nil {
		t.Fatal("expected ValidateSecurity to fail closed on invalid sslmode DSN")
	} else if strings.Contains(err.Error(), "super_secret_pw") {
		t.Fatalf("ValidateSecurity leaked password in error: %v", err)
	}

	// Valid remote libpq key=value with rotated password & loopback key=value with default password pass.
	for _, goodDSN := range []string{
		"host=192.0.2.20 port=5432 user=pigeoncoop_admin password=rotated_prod_secret dbname=edge_ledger sslmode=require",
		"host=127.0.0.1 port=5432 user=pigeoncoop_admin password=ctd_dev_password dbname=edge_ledger",
	} {
		cfg := DefaultCoopConfig()
		cfg.Database.URL = goodDSN
		if err := cfg.ValidateSecurity(); err != nil {
			t.Fatalf("expected ValidateSecurity to allow %q, got %v", goodDSN, err)
		}
	}

	// Workstation RO derivation with libpq key=value DSN must clear loopback RO on remote host
	// and never leak admin credentials into ReadOnlyDatabaseURL.
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))
	t.Setenv("PIGEONCARE_RO_PASSWORD", "")
	t.Setenv("PIGEONS_RO_DB_URL", "")
	t.Setenv("PIGEONCARE_DB_URL", "")
	t.Setenv("PIGEONMCP_DB_URL", "")

	wsPath := filepath.Join(dir, "pigeons.toml")
	if err := os.WriteFile(wsPath, []byte(`readonly_database_url = "postgres://pigeoncare_ro:old@127.0.0.1:5432/edge_ledger"`+"\n"), 0o600); err != nil {
		t.Fatalf("write pigeons.toml: %v", err)
	}
	t.Setenv("PIGEONS_DB_URL", "host=192.0.2.50 port=5432 user=pigeoncoop_admin password=admin_secret dbname=edge_ledger")
	wsRemote, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig remote libpq: %v", err)
	}
	if wsRemote.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected empty ReadOnlyDatabaseURL for remote libpq DSN without PIGEONCARE_RO_PASSWORD, got %q", wsRemote.ReadOnlyDatabaseURL)
	}

	t.Setenv("PIGEONS_DB_URL", "host=127.0.0.1 port=5432 user=pigeoncoop_admin password=admin_secret dbname=edge_ledger")
	wsLoopbackSameTarget, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig loopback same-target libpq: %v", err)
	}
	if wsLoopbackSameTarget.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:old@127.0.0.1:5432/edge_ledger" {
		t.Fatalf("expected explicit same-target ReadOnlyDatabaseURL preserved for loopback libpq, got %q", wsLoopbackSameTarget.ReadOnlyDatabaseURL)
	}

	t.Setenv("PIGEONS_DB_URL", "host=127.0.0.1 port=5432 user=pigeoncoop_admin password=admin_secret dbname=edge_ledger_local")
	wsLoopback, err := LoadWorkstationConfig(wsPath)
	if err != nil {
		t.Fatalf("LoadWorkstationConfig loopback libpq: %v", err)
	}
	if wsLoopback.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:pigeoncare_readonly@127.0.0.1:5432/edge_ledger_local" {
		t.Fatalf("expected derived pigeoncare_ro DSN for loopback libpq, got %q", wsLoopback.ReadOnlyDatabaseURL)
	}
	if strings.Contains(wsLoopback.ReadOnlyDatabaseURL, "admin_secret") {
		t.Fatalf("derived ReadOnlyDatabaseURL leaked admin password: %q", wsLoopback.ReadOnlyDatabaseURL)
	}

	// Libpq key=value SSL settings (sslmode=verify-full, sslrootcert, etc.) must be preserved
	// across RO derivation instead of silently downgrading TLS to prefer.
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pg-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	caFile := filepath.Join(dir, "ca root.pem")
	certFile := filepath.Join(dir, "client.crt")
	keyFile := filepath.Join(dir, "client.key")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write caFile: %v", err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write certFile: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write keyFile: %v", err)
	}

	t.Setenv("PIGEONCARE_RO_PASSWORD", "ro_tls_secret")
	libpqTLSDSN := "host=192.0.2.50 port=5432 user=pigeoncoop_admin password=admin_secret dbname=edge_ledger sslmode=verify-full sslrootcert='" + caFile + "' sslcert=" + certFile + " sslkey=" + keyFile + " sslsni=1"
	gotLibpqTLS := deriveReadOnlyURL(libpqTLSDSN)
	parsedLibpqRO, err := pgxpool.ParseConfig(gotLibpqTLS)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig(%q) failed: %v", gotLibpqTLS, err)
	}
	if parsedLibpqRO.ConnConfig.User != "pigeoncare_ro" ||
		parsedLibpqRO.ConnConfig.Password != "ro_tls_secret" ||
		parsedLibpqRO.ConnConfig.TLSConfig == nil ||
		parsedLibpqRO.ConnConfig.TLSConfig.ServerName != "192.0.2.50" {
		t.Fatalf("expected derived libpq RO DSN to retain verify-full TLS & pigeoncare_ro creds, got cfg=%+v (dsn=%q)", parsedLibpqRO.ConnConfig, gotLibpqTLS)
	}
	for _, wantParam := range []string{
		"sslmode=verify-full",
		"sslrootcert=" + strings.ReplaceAll(url.QueryEscape(caFile), "+", "%20"),
		"sslcert=" + strings.ReplaceAll(url.QueryEscape(certFile), "+", "%20"),
		"sslkey=" + strings.ReplaceAll(url.QueryEscape(keyFile), "+", "%20"),
		"sslsni=1",
	} {
		if !strings.Contains(gotLibpqTLS, wantParam) {
			t.Fatalf("deriveReadOnlyURL(%q) = %q, missing %q", libpqTLSDSN, gotLibpqTLS, wantParam)
		}
	}
	if strings.Contains(gotLibpqTLS, "admin_secret") {
		t.Fatalf("deriveReadOnlyURL leaked admin secret: %q", gotLibpqTLS)
	}

	// Non-existent sslrootcert in libpq DSN must still preserve sslmode=verify-full + sslrootcert when deriving RO URL.
	missingCA := filepath.Join(dir, "missing-ca.pem")
	libpqMissingCA := "host=192.0.2.50 port=5432 user=pigeoncoop_admin password=admin_secret dbname=edge_ledger sslmode=verify-full sslrootcert=" + missingCA
	gotMissingCA := deriveReadOnlyURL(libpqMissingCA)
	if !strings.Contains(gotMissingCA, "sslmode=verify-full") || !strings.Contains(gotMissingCA, "sslrootcert="+url.QueryEscape(missingCA)) {
		t.Fatalf("expected deriveReadOnlyURL fallback to preserve sslmode=verify-full & sslrootcert, got %q", gotMissingCA)
	}

	// URI DSNs carrying ?user= / ?password= / ?passfile= query parameters must have those
	// credential keys stripped during RO derivation while retaining TLS query parameters.
	uriQueryCredDSN := "postgres://192.0.2.50:5432/edge_ledger?user=pigeoncoop_admin&password=admin_query_secret&passfile=/tmp/admin.pgpass&sslmode=verify-full&sslrootcert=" + strings.ReplaceAll(url.QueryEscape(caFile), "+", "%20")
	gotURIQuery := deriveReadOnlyURL(uriQueryCredDSN)
	if strings.Contains(gotURIQuery, "admin_query_secret") ||
		strings.Contains(gotURIQuery, "pigeoncoop_admin") ||
		strings.Contains(gotURIQuery, "passfile=") ||
		strings.Contains(gotURIQuery, "password=") ||
		strings.Contains(gotURIQuery, "user=") {
		t.Fatalf("deriveReadOnlyURL leaked or retained credential query params: %q", gotURIQuery)
	}
	parsedURIRO, err := pgxpool.ParseConfig(gotURIQuery)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig(%q) failed: %v", gotURIQuery, err)
	}
	if parsedURIRO.ConnConfig.User != "pigeoncare_ro" ||
		parsedURIRO.ConnConfig.Password != "ro_tls_secret" ||
		parsedURIRO.ConnConfig.TLSConfig == nil {
		t.Fatalf("expected URI RO DSN to use pigeoncare_ro and verify-full TLS, got %+v (dsn=%q)", parsedURIRO.ConnConfig, gotURIQuery)
	}
	t.Setenv("PIGEONCARE_RO_PASSWORD", "")
}

func TestApplyMeshProfileIsolation(t *testing.T) {
	t.Setenv("PIGEONCARE_RO_PASSWORD", "")

	base := DefaultWorkstationConfig()
	base.Operator = "alice"
	base.BrokerURL = "tls://192.0.2.1:8883"
	base.MQTTUsername = "top-user"
	base.MQTTPassword = "top-password"
	base.TLSInsecureSkipVerify = true
	base.MQTT = EdgeMQTTConfig{
		Username:              "top-user",
		Password:              "top-password",
		TLSInsecureSkipVerify: true,
	}
	base.DatabaseURL = "postgres://top_admin:top_pw@192.0.2.1:5432/edge_ledger"
	base.ReadOnlyDatabaseURL = "postgres://top_ro:top_ro_pw@192.0.2.1:5432/edge_ledger"

	// 1. Overriding BrokerURL alone clears inherited MQTT creds, resets TLSInsecureSkipVerify=false,
	//    and clears inherited DatabaseURL / ReadOnlyDatabaseURL while preserving operator/LLM/permissions.
	brokerOnly := ApplyMeshProfile(base, "prod-broker", MeshProfileConfig{
		BrokerURL: "tls://192.0.2.50:8883",
	})
	if brokerOnly.BrokerURL != "tls://192.0.2.50:8883" {
		t.Fatalf("unexpected BrokerURL: %q", brokerOnly.BrokerURL)
	}
	if brokerOnly.MQTTUsername != "" || brokerOnly.MQTTPassword != "" ||
		brokerOnly.MQTT.Username != "" || brokerOnly.MQTT.Password != "" {
		t.Fatalf("expected inherited MQTT credentials cleared, got top=(%q,%q) section=(%q,%q)",
			brokerOnly.MQTTUsername, brokerOnly.MQTTPassword, brokerOnly.MQTT.Username, brokerOnly.MQTT.Password)
	}
	if brokerOnly.TLSInsecureSkipVerify || brokerOnly.MQTT.TLSInsecureSkipVerify {
		t.Fatalf("expected TLSInsecureSkipVerify reset to false, got top=%v section=%v",
			brokerOnly.TLSInsecureSkipVerify, brokerOnly.MQTT.TLSInsecureSkipVerify)
	}
	if brokerOnly.EffectiveMQTTAuth().TLSInsecureSkipVerify || brokerOnly.EffectiveMQTTAuth().Username != "" {
		t.Fatalf("expected EffectiveMQTTAuth isolated, got %+v", brokerOnly.EffectiveMQTTAuth())
	}
	if brokerOnly.DatabaseURL != "" || brokerOnly.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected inherited DB URLs cleared, got db=%q ro=%q", brokerOnly.DatabaseURL, brokerOnly.ReadOnlyDatabaseURL)
	}
	if brokerOnly.Operator != "alice" || brokerOnly.LLM.Model == "" {
		t.Fatalf("expected operator and LLM defaults preserved, got op=%q llm=%+v", brokerOnly.Operator, brokerOnly.LLM)
	}

	// 2. Overriding ReadOnlyDatabaseURL clears inherited DatabaseURL, MQTT creds, and TLSInsecureSkipVerify.
	roOnly := ApplyMeshProfile(base, "ro-mesh", MeshProfileConfig{
		ReadOnlyDatabaseURL: "postgres://ro:ro_pw@192.0.2.60:5432/edge_ledger",
	})
	if roOnly.DatabaseURL != "" || roOnly.ReadOnlyDatabaseURL != "postgres://ro:ro_pw@192.0.2.60:5432/edge_ledger" {
		t.Fatalf("expected DatabaseURL cleared and ReadOnlyDatabaseURL applied, got db=%q ro=%q", roOnly.DatabaseURL, roOnly.ReadOnlyDatabaseURL)
	}
	if roOnly.MQTTUsername != "" || roOnly.MQTTPassword != "" || roOnly.TLSInsecureSkipVerify || roOnly.MQTT.TLSInsecureSkipVerify {
		t.Fatalf("expected MQTT creds/TLS reset on DB override, got user=%q skip=%v", roOnly.MQTTUsername, roOnly.TLSInsecureSkipVerify)
	}

	// 3. Overriding DatabaseURL clears inherited RO URL, MQTT creds, and TLSInsecureSkipVerify.
	dbOnly := ApplyMeshProfile(base, "db-mesh", MeshProfileConfig{
		DatabaseURL: "postgres://admin:admin_pw@192.0.2.70:5432/edge_ledger",
	})
	if dbOnly.DatabaseURL != "postgres://admin:admin_pw@192.0.2.70:5432/edge_ledger" || dbOnly.ReadOnlyDatabaseURL != "" {
		t.Fatalf("expected remote DatabaseURL applied and stale RO cleared, got db=%q ro=%q", dbOnly.DatabaseURL, dbOnly.ReadOnlyDatabaseURL)
	}
	if dbOnly.MQTTUsername != "" || dbOnly.MQTTPassword != "" || dbOnly.TLSInsecureSkipVerify {
		t.Fatalf("expected MQTT creds/TLS reset on DatabaseURL override")
	}

	// 4. Profile with explicit own credentials & TLSInsecureSkipVerify retains its own values.
	skipTrue := true
	explicitProf := ApplyMeshProfile(base, "lab-tls", MeshProfileConfig{
		BrokerURL:             "tls://192.0.2.80:8883",
		MQTTUsername:          "lab-user",
		MQTTPassword:          "lab-pw",
		TLSInsecureSkipVerify: &skipTrue,
	})
	if explicitProf.MQTTUsername != "lab-user" || explicitProf.MQTTPassword != "lab-pw" ||
		!explicitProf.TLSInsecureSkipVerify || !explicitProf.MQTT.TLSInsecureSkipVerify {
		t.Fatalf("expected explicit profile creds/TLS preserved, got %+v", explicitProf.EffectiveMQTTAuth())
	}

	// 5. Profile that overrides neither BrokerURL nor DB URLs inherits top-level connection fields.
	opOnly := ApplyMeshProfile(base, "op-only", MeshProfileConfig{Operator: "bob"})
	if opOnly.Operator != "bob" || opOnly.MQTTUsername != "top-user" || opOnly.DatabaseURL != base.DatabaseURL || !opOnly.TLSInsecureSkipVerify {
		t.Fatalf("expected non-endpoint profile to preserve inherited connection fields, got %+v", opOnly)
	}
}

func TestCoopMQTTConfigMTLSFieldsAndEnvBindings(t *testing.T) {
	dir := t.TempDir()
	coopPath := filepath.Join(dir, "pigeoncoop.toml")
	tomlContent := `
mesh_name = "mtls-mesh"
node_id = "coop-mtls-01"

[mqtt]
mode = "embedded"
listen = "127.0.0.1:8883"
tls_cert_file = "/etc/pigeons/server.crt"
tls_key_file = "/etc/pigeons/server.key"
tls_client_ca_file = "/etc/pigeons/client-ca.pem"
tls_client_cert_file = "/etc/pigeons/self-client.crt"
tls_client_key_file = "/etc/pigeons/self-client.key"
require_client_cert = true
`
	if err := os.WriteFile(coopPath, []byte(tomlContent), 0o600); err != nil {
		t.Fatalf("write pigeoncoop.toml: %v", err)
	}

	cfg, err := LoadCoopConfig(coopPath)
	if err != nil {
		t.Fatalf("LoadCoopConfig failed: %v", err)
	}
	if cfg.MQTT.TLSClientCAFile != "/etc/pigeons/client-ca.pem" ||
		cfg.MQTT.TLSClientCertFile != "/etc/pigeons/self-client.crt" ||
		cfg.MQTT.TLSClientKeyFile != "/etc/pigeons/self-client.key" ||
		!cfg.MQTT.RequireClientCert {
		t.Fatalf("unexpected decoded CoopMQTTConfig mTLS fields: %+v", cfg.MQTT)
	}
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected complete Coop mTLS config to pass ValidateSecurity, got %v", err)
	}

	// Partial/misconfigured mTLS must fail closed.
	missingCA := cfg
	missingCA.MQTT.TLSClientCAFile = ""
	if err := missingCA.ValidateSecurity(); err == nil {
		t.Fatal("expected require_client_cert=true without tls_client_ca_file to fail ValidateSecurity")
	}
	missingServerCert := cfg
	missingServerCert.MQTT.TLSCertFile = ""
	missingServerCert.MQTT.TLSKeyFile = ""
	if err := missingServerCert.ValidateSecurity(); err == nil {
		t.Fatal("expected require_client_cert=true without server tls_cert_file/tls_key_file to fail ValidateSecurity")
	}
	halfClientPair := cfg
	halfClientPair.MQTT.TLSClientKeyFile = ""
	if err := halfClientPair.ValidateSecurity(); err == nil {
		t.Fatal("expected tls_client_cert_file without tls_client_key_file to fail ValidateSecurity")
	}

	// Verify env bindings (both PIGEONCOOP_* and custom brand prefix).
	t.Setenv("PIGEONCOOP_MQTT_TLS_CLIENT_CA", "/tmp/env-client-ca.pem")
	t.Setenv("PIGEONCOOP_MQTT_TLS_CLIENT_CERT", "/tmp/env-client.crt")
	t.Setenv("PIGEONCOOP_MQTT_TLS_CLIENT_KEY", "/tmp/env-client.key")
	t.Setenv("PIGEONCOOP_MQTT_REQUIRE_CLIENT_CERT", "false")

	envCfg, err := LoadCoopConfig(coopPath)
	if err != nil {
		t.Fatalf("LoadCoopConfig with env failed: %v", err)
	}
	if envCfg.MQTT.TLSClientCAFile != "/tmp/env-client-ca.pem" ||
		envCfg.MQTT.TLSClientCertFile != "/tmp/env-client.crt" ||
		envCfg.MQTT.TLSClientKeyFile != "/tmp/env-client.key" ||
		envCfg.MQTT.RequireClientCert {
		t.Fatalf("unexpected env-overridden CoopMQTTConfig mTLS fields: %+v", envCfg.MQTT)
	}

	brandPath := filepath.Join(dir, "branded-coop.toml")
	if err := os.WriteFile(brandPath, []byte("[brand]\nenv_prefix = \"ORBIT\"\n"), 0o600); err != nil {
		t.Fatalf("write branded-coop.toml: %v", err)
	}
	t.Setenv("ORBIT_MQTT_TLS_CLIENT_CA", "/tmp/orbit-ca.pem")
	t.Setenv("ORBIT_MQTT_TLS_CLIENT_CERT", "/tmp/orbit-client.crt")
	t.Setenv("ORBIT_MQTT_TLS_CLIENT_KEY", "/tmp/orbit-client.key")
	t.Setenv("ORBIT_MQTT_REQUIRE_CLIENT_CERT", "true")

	brandCfg, err := LoadCoopConfig(brandPath)
	if err != nil {
		t.Fatalf("LoadCoopConfig branded env failed: %v", err)
	}
	if brandCfg.MQTT.TLSClientCAFile != "/tmp/orbit-ca.pem" ||
		brandCfg.MQTT.TLSClientCertFile != "/tmp/orbit-client.crt" ||
		brandCfg.MQTT.TLSClientKeyFile != "/tmp/orbit-client.key" ||
		!brandCfg.MQTT.RequireClientCert {
		t.Fatalf("unexpected branded env CoopMQTTConfig mTLS fields: %+v", brandCfg.MQTT)
	}
}

func TestBrandMathSchemaValidationInLoaders(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PIGEONS_CURRENT_MESH_FILE", filepath.Join(dir, "current_mesh"))

	invalidSchemas := []string{
		"bad-schema",
		"123leading_digit",
		"pigeonmath; DROP SCHEMA public",
		"pigeon math",
		" pigeonmath",
		strings.Repeat("a", 64),
	}
	for _, bad := range invalidSchemas {
		coopPath := filepath.Join(dir, "bad-coop-schema.toml")
		if err := os.WriteFile(coopPath, []byte("[brand]\nmath_schema = \""+bad+"\"\n"), 0o600); err != nil {
			t.Fatalf("write bad-coop-schema.toml: %v", err)
		}
		if _, err := LoadCoopConfig(coopPath); err == nil {
			t.Fatalf("expected LoadCoopConfig to reject invalid math_schema %q", bad)
		}

		edgePath := filepath.Join(dir, "bad-edge-schema.toml")
		if err := os.WriteFile(edgePath, []byte("node_id = \"rv64-edge-01\"\n[brand]\nmath_schema = \""+bad+"\"\n"), 0o600); err != nil {
			t.Fatalf("write bad-edge-schema.toml: %v", err)
		}
		if _, err := LoadEdgeConfig(edgePath); err == nil {
			t.Fatalf("expected LoadEdgeConfig to reject invalid math_schema %q", bad)
		}

		wsPath := filepath.Join(dir, "bad-ws-schema.toml")
		if err := os.WriteFile(wsPath, []byte("[brand]\nmath_schema = \""+bad+"\"\n"), 0o600); err != nil {
			t.Fatalf("write bad-ws-schema.toml: %v", err)
		}
		if _, err := LoadWorkstationConfig(wsPath); err == nil {
			t.Fatalf("expected LoadWorkstationConfig to reject invalid math_schema %q", bad)
		}

		meshWSPath := filepath.Join(dir, "bad-mesh-ws-schema.toml")
		if err := os.WriteFile(meshWSPath, []byte("[meshes.lab.brand]\nmath_schema = \""+bad+"\"\n"), 0o600); err != nil {
			t.Fatalf("write bad-mesh-ws-schema.toml: %v", err)
		}
		if _, err := LoadBaseWorkstationConfig(meshWSPath); err == nil {
			t.Fatalf("expected LoadBaseWorkstationConfig to reject invalid mesh math_schema %q", bad)
		}

		coopStruct := DefaultCoopConfig()
		coopStruct.Brand.MathSchema = bad
		if err := coopStruct.ValidateSecurity(); err == nil {
			t.Fatalf("expected CoopConfig.ValidateSecurity to reject invalid math_schema %q", bad)
		}

		wsStruct := DefaultWorkstationConfig()
		wsStruct.Brand.MathSchema = bad
		if _, err := FinalizeWorkstationConfig(wsStruct); err == nil {
			t.Fatalf("expected FinalizeWorkstationConfig to reject invalid math_schema %q", bad)
		}
	}

	// Valid schemas (including 63-char max and leading underscore) must succeed.
	for _, good := range []string{"pigeonmath", "_analytics_v2", strings.Repeat("a", 63)} {
		wsPath := filepath.Join(dir, "good-ws-schema.toml")
		if err := os.WriteFile(wsPath, []byte("[brand]\nmath_schema = \""+good+"\"\n"), 0o600); err != nil {
			t.Fatalf("write good-ws-schema.toml: %v", err)
		}
		cfg, err := LoadWorkstationConfig(wsPath)
		if err != nil {
			t.Fatalf("expected LoadWorkstationConfig to accept valid math_schema %q, got %v", good, err)
		}
		if cfg.Brand.MathSchema != good {
			t.Fatalf("expected MathSchema %q, got %q", good, cfg.Brand.MathSchema)
		}
	}
}

func TestWriteAtomicFile0600FsyncDurability(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")
	target := filepath.Join(dir, "current_mesh")

	if err := writeAtomicFile0600(target, []byte("mesh-one\n")); err != nil {
		t.Fatalf("initial writeAtomicFile0600 failed: %v", err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("expected file mode 0600, got %o", info.Mode().Perm())
	}

	// Verify tmp.Sync() runs BEFORE os.Rename() and syncDir() runs AFTER os.Rename().
	origSyncFile := syncFile
	origSyncDir := syncDir
	t.Cleanup(func() {
		syncFile = origSyncFile
		syncDir = origSyncDir
	})

	var order []string
	syncFile = func(f *os.File) error {
		order = append(order, "syncFile")
		cur, readErr := os.ReadFile(target)
		if readErr != nil || string(cur) != "mesh-one\n" {
			t.Fatalf("expected old content before rename during syncFile, got %q (err=%v)", string(cur), readErr)
		}
		return origSyncFile(f)
	}
	syncDir = func(d string) error {
		order = append(order, "syncDir")
		if d != dir {
			t.Fatalf("expected syncDir(%q), got %q", dir, d)
		}
		cur, readErr := os.ReadFile(target)
		if readErr != nil || string(cur) != "mesh-two\n" {
			t.Fatalf("expected committed content after rename during syncDir, got %q (err=%v)", string(cur), readErr)
		}
		return origSyncDir(d)
	}

	if err := writeAtomicFile0600(target, []byte("mesh-two\n")); err != nil {
		t.Fatalf("second writeAtomicFile0600 failed: %v", err)
	}
	if len(order) != 2 || order[0] != "syncFile" || order[1] != "syncDir" {
		t.Fatalf("unexpected sync call sequence: %v", order)
	}

	// Verify syncFile failure aborts commit, preserves prior target, and leaves no temp file.
	syncFile = func(_ *os.File) error {
		return errors.New("simulated disk sync error")
	}
	if err := writeAtomicFile0600(target, []byte("mesh-three\n")); err == nil {
		t.Fatal("expected writeAtomicFile0600 to fail when syncFile fails")
	}
	cur, err := os.ReadFile(target)
	if err != nil || string(cur) != "mesh-two\n" {
		t.Fatalf("expected prior target preserved on sync failure, got %q (err=%v)", string(cur), err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	for _, ent := range entries {
		if strings.Contains(ent.Name(), ".tmp-") {
			t.Fatalf("unexpected leftover temp file %q after failed writeAtomicFile0600", ent.Name())
		}
	}

	// Verify ignorable vs non-ignorable directory sync error classification.
	if !isIgnorableDirSyncErr(errors.ErrUnsupported) || !isIgnorableDirSyncErr(syscall.EINVAL) {
		t.Fatal("expected ErrUnsupported and EINVAL to be ignorable dir sync errors")
	}
	if isIgnorableDirSyncErr(syscall.EIO) {
		t.Fatal("expected EIO NOT to be ignored during dir sync")
	}
}
