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
