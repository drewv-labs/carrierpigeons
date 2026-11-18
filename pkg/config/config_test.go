package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadCoopConfigHasNoExplicitPigeoneerSection(t *testing.T) {
	examplePath := filepath.Join("..", "..", "configs", "pigeoncoop.example.toml")
	raw, err := os.ReadFile(examplePath)
	if err != nil {
		t.Fatalf("failed reading %s: %v", examplePath, err)
	}
	if strings.Contains(strings.ToLower(string(raw)), "pigeoneer") {
		t.Fatalf("pigeoncoop.example.toml must not reference 'pigeoneer' explicitly")
	}

	cfg, err := LoadCoopConfig(examplePath)
	if err != nil {
		t.Fatalf("LoadCoopConfig failed: %v", err)
	}
	if !cfg.SelfObserve {
		t.Fatal("expected SelfObserve=true by default in pigeoncoop.example.toml")
	}
	if cfg.SpoolCapacity != 10000 {
		t.Fatalf("expected SpoolCapacity=10000, got %d", cfg.SpoolCapacity)
	}
	if cfg.MQTT.Mode != "embedded" {
		t.Fatalf("expected embedded MQTT mode, got %q", cfg.MQTT.Mode)
	}
}

func TestLoadEdgeAndWorkstationExampleConfigs(t *testing.T) {
	edgeCfg, err := LoadEdgeConfig(filepath.Join("..", "..", "configs", "pigeoneer.example.toml"))
	if err != nil {
		t.Fatalf("LoadEdgeConfig failed: %v", err)
	}
	if edgeCfg.NodeID != "rv64-compute-01" {
		t.Fatalf("expected rv64-compute-01, got %q", edgeCfg.NodeID)
	}
	if len(edgeCfg.Tags) == 0 || edgeCfg.Tags[0] == "" {
		t.Fatal("expected populated normalized tags in pigeoneer.example.toml")
	}

	wsCfg, err := LoadWorkstationConfig(filepath.Join("..", "..", "configs", "pigeons.example.toml"))
	if err != nil {
		t.Fatalf("LoadWorkstationConfig failed: %v", err)
	}
	if wsCfg.Permissions.Mode != "default" {
		t.Fatalf("expected default permission mode, got %q", wsCfg.Permissions.Mode)
	}
	if wsCfg.Math.ParsedRefreshInterval().Seconds() != 60 {
		t.Fatalf("expected 60s refresh interval, got %v", wsCfg.Math.ParsedRefreshInterval())
	}
}
