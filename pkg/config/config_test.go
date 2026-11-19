package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCoopConfigValidateSecurityFailClosed(t *testing.T) {
	cfg := DefaultCoopConfig()
	// Default binds 127.0.0.1:1883 -> allowed without auth for local dev.
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected loopback default CoopConfig to pass ValidateSecurity, got %v", err)
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
	cfg.Database.URL = "postgres://pigeoncoop_admin:ctd_dev_password@10.0.10.20:5432/edge_ledger"
	if err := cfg.ValidateSecurity(); err == nil {
		t.Fatal("expected remote database URL with default dev password to fail ValidateSecurity")
	}
	cfg.Database.AllowInsecureDefaults = true
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected AllowInsecureDefaults opt-in to pass, got %v", err)
	}
	cfg.Database.AllowInsecureDefaults = false
	cfg.Database.URL = "postgres://pigeoncoop_admin:rotated_prod_secret@10.0.10.20:5432/edge_ledger"
	if err := cfg.ValidateSecurity(); err != nil {
		t.Fatalf("expected remote database URL with rotated password to pass, got %v", err)
	}
}

func TestConfigTopicValidationAndWorkstationReadOnlyDerivation(t *testing.T) {
	dir := t.TempDir()

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

	wsPath := filepath.Join(dir, "ws-remote.toml")
	wsContent := `
database_url = "postgres://pigeoncoop_admin:prod_pw@10.0.10.50:5432/edge_ledger"
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
	if wsCfg.ReadOnlyDatabaseURL != "postgres://pigeoncare_ro:pigeoncare_readonly@10.0.10.50:5432/edge_ledger" {
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
	edgePath := filepath.Join(dir, "edge.toml")
	content := `
node_id = "edge-rv64-01"
broker_url = "tcp://10.0.0.5:1883"

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
