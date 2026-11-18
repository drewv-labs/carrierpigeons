package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// DefaultPostgresURL is the local development fallback DSN matching deployments/compose.
const DefaultPostgresURL = "postgres://drewv:ctd_password@localhost:5432/edge_ledger"

// ObserversConfig toggles built-in hardware observers.
type ObserversConfig struct {
	OS  bool `toml:"os"`
	Net bool `toml:"net"`
	GPU bool `toml:"gpu"`
	NPU bool `toml:"npu"`
}

// RunnersConfig governs remote runner execution and ephemeral script sandboxing on a node.
type RunnersConfig struct {
	Enabled             bool     `toml:"enabled"`
	AllowEphemeral      bool     `toml:"allow_ephemeral"`
	AllowedInterpreters []string `toml:"allowed_interpreters"`
	MaxTimeoutSec       int      `toml:"max_timeout_sec"`
}

// CoopMQTTConfig configures PigeonCoop's embedded pure-Go broker or external broker link.
type CoopMQTTConfig struct {
	Mode     string `toml:"mode"`   // "embedded" (default) or "external"
	Listen   string `toml:"listen"` // TCP listen address when mode="embedded" (e.g., "0.0.0.0:1883")
	URL      string `toml:"url"`    // Broker URL when mode="external" (or client dial override)
	Username string `toml:"username"`
	Password string `toml:"password"`
}

// CoopDBConfig configures PostgreSQL ledger pooling on PigeonCoop.
type CoopDBConfig struct {
	URL      string `toml:"url"`
	MaxConns int32  `toml:"max_conns"`
	MinConns int32  `toml:"min_conns"`
}

// IngestConfig tunes the high-throughput MQTT -> PostgreSQL COPY batch engine.
type IngestConfig struct {
	QueueSize     int    `toml:"queue_size"`
	BatchSize     int    `toml:"batch_size"`
	FlushInterval string `toml:"flush_interval"`
}

func (i IngestConfig) ParsedFlushInterval() time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(i.FlushInterval)); err == nil && d > 0 {
		return d
	}
	return 100 * time.Millisecond
}

// NtfyConfig configures webhook notifications on node presence transitions.
type NtfyConfig struct {
	Enabled bool   `toml:"enabled"`
	URL     string `toml:"url"`
}

// CoopConfig represents the unified PigeonCoop configuration (`pigeoncoop.toml`).
// Self-observation and runner execution settings live natively at the top level without
// exposing any separate edge-daemon section naming.
type CoopConfig struct {
	NodeID        string          `toml:"node_id"`
	Tags          []string        `toml:"tags"`
	SelfObserve   bool            `toml:"self_observe"`
	PollInterval  string          `toml:"poll_interval"`
	SpoolCapacity int             `toml:"spool_capacity"`
	MQTT          CoopMQTTConfig  `toml:"mqtt"`
	Database      CoopDBConfig    `toml:"database"`
	Ingest        IngestConfig    `toml:"ingest"`
	Ntfy          NtfyConfig      `toml:"ntfy"`
	Observers     ObserversConfig `toml:"observers"`
	Runners       RunnersConfig   `toml:"runners"`
	Brand         brand.Spec      `toml:"brand"`
}

func (c CoopConfig) ParsedPollInterval() time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(c.PollInterval)); err == nil && d > 0 {
		return d
	}
	return 5 * time.Second
}

// LocalBrokerURL returns the dialable tcp:// URL for local clients connecting to Coop's MQTT broker.
func (c CoopConfig) LocalBrokerURL() string {
	if strings.EqualFold(strings.TrimSpace(c.MQTT.Mode), "external") {
		if strings.TrimSpace(c.MQTT.URL) != "" {
			return strings.TrimSpace(c.MQTT.URL)
		}
		return "tcp://localhost:1883"
	}
	listen := strings.TrimSpace(c.MQTT.Listen)
	if listen == "" {
		return "tcp://127.0.0.1:1883"
	}
	hostPort := listen
	if strings.HasPrefix(hostPort, "0.0.0.0:") {
		hostPort = "127.0.0.1:" + strings.TrimPrefix(hostPort, "0.0.0.0:")
	} else if strings.HasPrefix(hostPort, ":") {
		hostPort = "127.0.0.1" + hostPort
	} else if strings.HasPrefix(hostPort, "[::]:") {
		hostPort = "127.0.0.1:" + strings.TrimPrefix(hostPort, "[::]:")
	}
	return "tcp://" + hostPort
}

// EdgeConfig represents the Pigeoneer edge daemon configuration (`pigeoneer.toml`).
type EdgeConfig struct {
	NodeID        string          `toml:"node_id"`
	Tags          []string        `toml:"tags"`
	PollInterval  string          `toml:"poll_interval"`
	SpoolCapacity int             `toml:"spool_capacity"`
	BrokerURL     string          `toml:"broker_url"`
	TopicPrefix   string          `toml:"topic_prefix"`
	Observers     ObserversConfig `toml:"observers"`
	Runners       RunnersConfig   `toml:"runners"`
	Brand         brand.Spec      `toml:"brand"`
}

func (e EdgeConfig) ParsedPollInterval() time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(e.PollInterval)); err == nil && d > 0 {
		return d
	}
	return 5 * time.Second
}

// WorkstationLLMConfig configures the inference provider backing PigeonCare and PigeonMath.
type WorkstationLLMConfig struct {
	Provider string `toml:"provider"`
	Model    string `toml:"model"`
	Endpoint string `toml:"endpoint"`
	APIKey   string `toml:"api_key"`
}

// PermissionConfig governs PigeonCare's execution & confirmation mode (`default`, `auto_edit`, `yolo`).
type PermissionConfig struct {
	Mode                string   `toml:"mode"` // "default", "auto_edit", or "yolo"
	AllowedRunners      []string `toml:"allowed_runners"`
	AllowedScriptHashes []string `toml:"allowed_script_hashes"`
}

// MathConfig configures PigeonMath TUI refresh intervals and analytical view behavior.
type MathConfig struct {
	RefreshInterval string `toml:"refresh_interval"`
}

func (m MathConfig) ParsedRefreshInterval() time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(m.RefreshInterval)); err == nil && d > 0 {
		return d
	}
	return 60 * time.Second
}

// WorkstationConfig configures the operator workstation CLI/TUI (`pigeons.toml`).
type WorkstationConfig struct {
	BrokerURL           string               `toml:"broker_url"`
	DatabaseURL         string               `toml:"database_url"`
	ReadOnlyDatabaseURL string               `toml:"readonly_database_url"`
	Operator            string               `toml:"operator"`
	LLM                 WorkstationLLMConfig `toml:"llm"`
	Permissions         PermissionConfig     `toml:"permissions"`
	Math                MathConfig           `toml:"math"`
	Brand               brand.Spec           `toml:"brand"`
}

func defaultHostname(fallback string) string {
	if h, err := os.Hostname(); err == nil && strings.TrimSpace(h) != "" {
		return strings.TrimSpace(h)
	}
	return fallback
}

func defaultOperator() string {
	if u, err := user.Current(); err == nil && strings.TrimSpace(u.Username) != "" {
		return strings.TrimSpace(u.Username)
	}
	if v := strings.TrimSpace(os.Getenv("USER")); v != "" {
		return v
	}
	return "operator"
}

// DefaultCoopConfig returns out-of-the-box PigeonCoop defaults with embedded MQTT and self-observation active.
func DefaultCoopConfig() CoopConfig {
	return CoopConfig{
		NodeID:        defaultHostname("rv64-coop-01"),
		Tags:          core.NormalizeTags([]string{"coop", runtime.GOARCH}),
		SelfObserve:   true,
		PollInterval:  "5s",
		SpoolCapacity: 10000,
		MQTT: CoopMQTTConfig{
			Mode:   "embedded",
			Listen: "0.0.0.0:1883",
			URL:    "tcp://localhost:1883",
		},
		Database: CoopDBConfig{
			URL:      DefaultPostgresURL,
			MaxConns: 24,
			MinConns: 2,
		},
		Ingest: IngestConfig{
			QueueSize:     65536,
			BatchSize:     2000,
			FlushInterval: "100ms",
		},
		Ntfy: NtfyConfig{
			Enabled: true,
			URL:     "http://localhost:2586/carrier-pigeons",
		},
		Observers: ObserversConfig{
			OS:  true,
			Net: true,
			GPU: true,
			NPU: true,
		},
		Runners: RunnersConfig{
			Enabled:             true,
			AllowEphemeral:      true,
			AllowedInterpreters: []string{"sh", "bash", "python3"},
			MaxTimeoutSec:       300,
		},
		Brand: brand.Default(),
	}
}

// DefaultEdgeConfig returns out-of-the-box Pigeoneer defaults with hardware auto-detection and ring spooling.
func DefaultEdgeConfig() EdgeConfig {
	return EdgeConfig{
		NodeID:        defaultHostname("rv64-edge-01"),
		Tags:          core.NormalizeTags([]string{runtime.GOARCH}),
		PollInterval:  "5s",
		SpoolCapacity: 10000,
		BrokerURL:     "tcp://localhost:1883",
		TopicPrefix:   brand.Default().CTDPrefix(),
		Observers: ObserversConfig{
			OS:  true,
			Net: true,
			GPU: true,
			NPU: true,
		},
		Runners: RunnersConfig{
			Enabled:             true,
			AllowEphemeral:      true,
			AllowedInterpreters: []string{"sh", "bash", "python3"},
			MaxTimeoutSec:       300,
		},
		Brand: brand.Default(),
	}
}

// DefaultWorkstationConfig returns out-of-the-box Pigeons CLI/TUI defaults.
func DefaultWorkstationConfig() WorkstationConfig {
	return WorkstationConfig{
		BrokerURL:           "tcp://localhost:1883",
		DatabaseURL:         DefaultPostgresURL,
		ReadOnlyDatabaseURL: "",
		Operator:            defaultOperator(),
		LLM: WorkstationLLMConfig{
			Provider: "ollama",
			Model:    "qwen2.5-coder",
			Endpoint: "http://localhost:11434",
		},
		Permissions: PermissionConfig{
			Mode: "default",
		},
		Math: MathConfig{
			RefreshInterval: "60s",
		},
		Brand: brand.Default(),
	}
}

func resolveConfigFile(explicitPath, envKey, filename string) string {
	if p := strings.TrimSpace(explicitPath); p != "" {
		return p
	}
	if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
		return p
	}
	candidates := []string{filename}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates = append(candidates, filepath.Join(home, ".config", "pigeons", filename))
	}
	candidates = append(candidates, filepath.Join("/etc/pigeons", filename))
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c
		}
	}
	return ""
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func splitCSVTags(raw string) []string {
	parts := strings.Split(raw, ",")
	return core.NormalizeTags(parts)
}

// LoadCoopConfig loads `pigeoncoop.toml` (if present) and layers environment variable overrides.
func LoadCoopConfig(explicitPath string) (CoopConfig, error) {
	cfg := DefaultCoopConfig()
	path := resolveConfigFile(explicitPath, "PIGEONCOOP_CONFIG", "pigeoncoop.toml")
	if path != "" {
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return cfg, fmt.Errorf("decode %s: %w", path, err)
		}
	} else if explicitPath != "" {
		return cfg, fmt.Errorf("config file not found: %s", explicitPath)
	}

	if v := firstEnv("PIGEONCOOP_NODE_ID", "PIGEONS_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := firstEnv("PIGEONCOOP_TAGS", "PIGEONS_TAGS"); v != "" {
		cfg.Tags = splitCSVTags(v)
	}
	if v := firstEnv("PIGEONCOOP_SELF_OBSERVE"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.SelfObserve = b
		}
	}
	if v := firstEnv("PIGEONCOOP_SPOOL_CAPACITY", "PIGEONS_SPOOL_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.SpoolCapacity = n
		}
	}
	if v := firstEnv("PIGEONCOOP_MQTT_MODE"); v != "" {
		cfg.MQTT.Mode = strings.ToLower(v)
	}
	if v := firstEnv("PIGEONCOOP_MQTT_LISTEN"); v != "" {
		cfg.MQTT.Listen = v
	}
	if v := firstEnv("PIGEONCOOP_BROKER_URL", "PIGEONS_BROKER_URL", "MQTT_BROKER_URL"); v != "" {
		cfg.MQTT.URL = v
		if firstEnv("PIGEONCOOP_MQTT_MODE") == "" && path == "" {
			// Explicit external broker env overrides embedded mode when no TOML specifies mode
			cfg.MQTT.Mode = "external"
		}
	}
	if v := firstEnv("PIGEONCOOP_DB_URL", "PIGEONS_DB_URL", "DATABASE_URL"); v != "" {
		cfg.Database.URL = v
	}
	if v := firstEnv("PIGEONCOOP_NTFY_URL", "NTFY_URL"); v != "" {
		if v == "off" || v == "disabled" || v == "none" {
			cfg.Ntfy.Enabled = false
		} else {
			cfg.Ntfy.URL = v
		}
	}

	cfg.Tags = core.NormalizeTags(cfg.Tags)
	cfg.Brand = cfg.Brand.MergeWithDefaults()
	return cfg, nil
}

// LoadEdgeConfig loads `pigeoneer.toml` (if present) and layers environment variable overrides.
func LoadEdgeConfig(explicitPath string) (EdgeConfig, error) {
	cfg := DefaultEdgeConfig()
	path := resolveConfigFile(explicitPath, "PIGEONEER_CONFIG", "pigeoneer.toml")
	if path != "" {
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return cfg, fmt.Errorf("decode %s: %w", path, err)
		}
	} else if explicitPath != "" {
		return cfg, fmt.Errorf("config file not found: %s", explicitPath)
	}

	if v := firstEnv("PIGEONEER_NODE_ID", "PIGEONS_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := firstEnv("PIGEONEER_TAGS", "PIGEONS_TAGS"); v != "" {
		cfg.Tags = splitCSVTags(v)
	}
	if v := firstEnv("PIGEONEER_BROKER_URL", "PIGEONS_BROKER_URL", "MQTT_BROKER_URL"); v != "" {
		cfg.BrokerURL = v
	}
	if v := firstEnv("PIGEONEER_TOPIC_PREFIX", "PIGEONS_TOPIC_PREFIX"); v != "" {
		cfg.TopicPrefix = v
	}
	if v := firstEnv("PIGEONEER_SPOOL_CAPACITY", "PIGEONS_SPOOL_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.SpoolCapacity = n
		}
	}

	cfg.Tags = core.NormalizeTags(cfg.Tags)
	cfg.Brand = cfg.Brand.MergeWithDefaults()
	if strings.TrimSpace(cfg.TopicPrefix) == "" {
		cfg.TopicPrefix = cfg.Brand.CTDPrefix()
	}
	return cfg, nil
}

// LoadWorkstationConfig loads `pigeons.toml` (if present) and layers environment variable overrides.
func LoadWorkstationConfig(explicitPath string) (WorkstationConfig, error) {
	cfg := DefaultWorkstationConfig()
	path := resolveConfigFile(explicitPath, "PIGEONS_CONFIG", "pigeons.toml")
	if path != "" {
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return cfg, fmt.Errorf("decode %s: %w", path, err)
		}
	} else if explicitPath != "" {
		return cfg, fmt.Errorf("config file not found: %s", explicitPath)
	}

	if v := firstEnv("PIGEONS_BROKER_URL", "PIGEONCOOP_BROKER_URL", "MQTT_BROKER_URL"); v != "" {
		cfg.BrokerURL = v
	}
	if v := firstEnv("PIGEONS_DB_URL", "PIGEONCOOP_DB_URL", "DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := firstEnv("PIGEONS_RO_DB_URL", "PIGEONCARE_DB_URL"); v != "" {
		cfg.ReadOnlyDatabaseURL = v
	}
	if v := firstEnv("PIGEONS_OPERATOR"); v != "" {
		cfg.Operator = v
	}
	if v := firstEnv("PIGEONS_LLM_PROVIDER"); v != "" {
		cfg.LLM.Provider = v
	}
	if v := firstEnv("PIGEONS_LLM_MODEL"); v != "" {
		cfg.LLM.Model = v
	}
	if v := firstEnv("PIGEONS_LLM_ENDPOINT"); v != "" {
		cfg.LLM.Endpoint = v
	}
	if v := firstEnv("PIGEONS_LLM_API_KEY"); v != "" {
		cfg.LLM.APIKey = v
	}
	if v := firstEnv("PIGEONS_MODE", "PIGEONCARE_MODE"); v != "" {
		cfg.Permissions.Mode = strings.ToLower(v)
	}

	cfg.Brand = cfg.Brand.MergeWithDefaults()
	if strings.TrimSpace(cfg.ReadOnlyDatabaseURL) == "" {
		cfg.ReadOnlyDatabaseURL = cfg.DatabaseURL
	}
	return cfg, nil
}
