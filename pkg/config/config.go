package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// DefaultPostgresURL is the local development fallback DSN matching deployments/compose.
const DefaultPostgresURL = "postgres://pigeoncoop_admin:ctd_dev_password@127.0.0.1:5432/edge_ledger"

// DefaultHealthyWindow is the canonical cluster health staleness threshold shared across CLI, TUI, and MCP.
const DefaultHealthyWindow = 3 * time.Minute

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
	AllowedScriptHashes []string `toml:"allowed_script_hashes"`
	MaxTimeoutSec       int      `toml:"max_timeout_sec"`
}

// CoopMQTTConfig configures PigeonCoop's embedded pure-Go broker or external broker link.
type CoopMQTTConfig struct {
	Mode                   string `toml:"mode"`   // "embedded" (default) or "external"
	Listen                 string `toml:"listen"` // TCP listen address when mode="embedded" (default "127.0.0.1:1883")
	URL                    string `toml:"url"`    // Broker URL when mode="external" (or client dial override)
	Username               string `toml:"username"`
	Password               string `toml:"password"`
	EdgeUsername           string `toml:"edge_username"`
	EdgePassword           string `toml:"edge_password"`
	TLSCertFile            string `toml:"tls_cert_file"`
	TLSKeyFile             string `toml:"tls_key_file"`
	TLSCAFile              string `toml:"tls_ca_file"`
	TLSInsecureSkipVerify  bool   `toml:"tls_insecure_skip_verify"`
	AllowInsecureAnonymous bool   `toml:"allow_insecure_anonymous"`
}

// EdgeMQTTConfig provides optional structured [mqtt] credentials and TLS settings on edge/workstation configs.
type EdgeMQTTConfig struct {
	Username              string `toml:"username"`
	Password              string `toml:"password"`
	TLSCAFile             string `toml:"tls_ca_file"`
	TLSCertFile           string `toml:"tls_cert_file"`
	TLSKeyFile            string `toml:"tls_key_file"`
	TLSInsecureSkipVerify bool   `toml:"tls_insecure_skip_verify"`
}

// BuildMQTTTLSConfig constructs a *tls.Config when CA/client certs or skip-verify are configured.
func BuildMQTTTLSConfig(caFile, certFile, keyFile string, insecureSkipVerify bool) (*tls.Config, error) {
	caFile = strings.TrimSpace(caFile)
	certFile = strings.TrimSpace(certFile)
	keyFile = strings.TrimSpace(keyFile)
	if caFile == "" && certFile == "" && keyFile == "" && !insecureSkipVerify {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify, //nolint:gosec // explicit operator opt-in for lab/self-signed certs
	}

	if caFile != "" {
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read MQTT TLS CA file %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("no valid PEM certificates in %s", caFile)
		}
		tlsCfg.RootCAs = pool
	}

	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("both tls_cert_file and tls_key_file must be provided together")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load MQTT TLS client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// IsLoopbackListen reports whether an address like "127.0.0.1:1883" or "localhost:1883" binds strictly to loopback.
func IsLoopbackListen(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// CoopDBConfig configures PostgreSQL ledger pooling on PigeonCoop.
type CoopDBConfig struct {
	URL                   string `toml:"url"`
	MaxConns              int32  `toml:"max_conns"`
	MinConns              int32  `toml:"min_conns"`
	AllowInsecureDefaults bool   `toml:"allow_insecure_defaults"`
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

func isKnownDefaultDBPassword(pw string) bool {
	switch pw {
	case "ctd_dev_password", "ctd_ingest_password", "pigeoncare_readonly":
		return true
	default:
		return false
	}
}

func isURLHostLoopback(rawURL string) bool {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		return true
	}
	return IsLoopbackListen(host)
}

// ValidateSecurity enforces fail-closed security invariants before starting network listeners.
func (c CoopConfig) ValidateSecurity() error {
	isExternal := strings.EqualFold(strings.TrimSpace(c.MQTT.Mode), "external")
	if !isExternal && !IsLoopbackListen(c.MQTT.Listen) {
		hasAuth := strings.TrimSpace(c.MQTT.Username) != "" && c.MQTT.Password != ""
		if !hasAuth && !c.MQTT.AllowInsecureAnonymous {
			return fmt.Errorf(
				"refusing insecure startup: embedded MQTT listens on non-loopback %q without mqtt.username/password configured (set mqtt credentials, bind 127.0.0.1:1883, or set mqtt.allow_insecure_anonymous=true for isolated lab networks)",
				c.MQTT.Listen,
			)
		}
	}

	if dbURL := strings.TrimSpace(c.Database.URL); dbURL != "" && !c.Database.AllowInsecureDefaults {
		if u, err := url.Parse(dbURL); err == nil {
			host := strings.TrimSpace(u.Hostname())
			if host != "" && !IsLoopbackListen(host) && u.User != nil {
				if pw, ok := u.User.Password(); ok && isKnownDefaultDBPassword(pw) {
					return fmt.Errorf(
						"refusing insecure startup: database.url targets non-loopback host %q with a default development password (rotate credentials or set database.allow_insecure_defaults=true explicitly)",
						host,
					)
				}
			}
		}
	}
	return nil
}

// LocalBrokerURL returns the dialable tcp:// or tls:// URL for local clients connecting to Coop's MQTT broker.
func (c CoopConfig) LocalBrokerURL() string {
	if strings.EqualFold(strings.TrimSpace(c.MQTT.Mode), "external") {
		if strings.TrimSpace(c.MQTT.URL) != "" {
			return strings.TrimSpace(c.MQTT.URL)
		}
		return "tcp://127.0.0.1:1883"
	}
	scheme := "tcp://"
	if strings.TrimSpace(c.MQTT.TLSCertFile) != "" && strings.TrimSpace(c.MQTT.TLSKeyFile) != "" {
		scheme = "tls://"
	}
	listen := strings.TrimSpace(c.MQTT.Listen)
	if listen == "" {
		return scheme + "127.0.0.1:1883"
	}
	hostPort := listen
	if strings.HasPrefix(hostPort, "0.0.0.0:") {
		hostPort = "127.0.0.1:" + strings.TrimPrefix(hostPort, "0.0.0.0:")
	} else if strings.HasPrefix(hostPort, ":") {
		hostPort = "127.0.0.1" + hostPort
	} else if strings.HasPrefix(hostPort, "[::]:") {
		hostPort = "127.0.0.1:" + strings.TrimPrefix(hostPort, "[::]:")
	}
	return scheme + hostPort
}

// EdgeConfig represents the Pigeoneer edge daemon configuration (`pigeoneer.toml`).
type EdgeConfig struct {
	NodeID                string          `toml:"node_id"`
	Tags                  []string        `toml:"tags"`
	PollInterval          string          `toml:"poll_interval"`
	SpoolCapacity         int             `toml:"spool_capacity"`
	BrokerURL             string          `toml:"broker_url"`
	TopicPrefix           string          `toml:"topic_prefix"`
	MQTTUsername          string          `toml:"mqtt_username"`
	MQTTPassword          string          `toml:"mqtt_password"`
	TLSCAFile             string          `toml:"tls_ca_file"`
	TLSCertFile           string          `toml:"tls_cert_file"`
	TLSKeyFile            string          `toml:"tls_key_file"`
	TLSInsecureSkipVerify bool            `toml:"tls_insecure_skip_verify"`
	MQTT                  EdgeMQTTConfig  `toml:"mqtt"`
	Observers             ObserversConfig `toml:"observers"`
	Runners               RunnersConfig   `toml:"runners"`
	Brand                 brand.Spec      `toml:"brand"`
}

func (e EdgeConfig) ParsedPollInterval() time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(e.PollInterval)); err == nil && d > 0 {
		return d
	}
	return 5 * time.Second
}

// EffectiveMQTTAuth resolves merged top-level and [mqtt] section credentials/TLS settings.
func (e EdgeConfig) EffectiveMQTTAuth() EdgeMQTTConfig {
	out := e.MQTT
	if strings.TrimSpace(out.Username) == "" {
		out.Username = strings.TrimSpace(e.MQTTUsername)
	}
	if out.Password == "" {
		out.Password = e.MQTTPassword
	}
	if strings.TrimSpace(out.TLSCAFile) == "" {
		out.TLSCAFile = strings.TrimSpace(e.TLSCAFile)
	}
	if strings.TrimSpace(out.TLSCertFile) == "" {
		out.TLSCertFile = strings.TrimSpace(e.TLSCertFile)
	}
	if strings.TrimSpace(out.TLSKeyFile) == "" {
		out.TLSKeyFile = strings.TrimSpace(e.TLSKeyFile)
	}
	if !out.TLSInsecureSkipVerify {
		out.TLSInsecureSkipVerify = e.TLSInsecureSkipVerify
	}
	return out
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
	BrokerURL              string               `toml:"broker_url"`
	MQTTUsername           string               `toml:"mqtt_username"`
	MQTTPassword           string               `toml:"mqtt_password"`
	TLSCAFile              string               `toml:"tls_ca_file"`
	TLSCertFile            string               `toml:"tls_cert_file"`
	TLSKeyFile             string               `toml:"tls_key_file"`
	TLSInsecureSkipVerify  bool                 `toml:"tls_insecure_skip_verify"`
	MQTT                   EdgeMQTTConfig       `toml:"mqtt"`
	DatabaseURL            string               `toml:"database_url"`
	ReadOnlyDatabaseURL    string               `toml:"readonly_database_url"`
	AllowSuperuserReadOnly bool                 `toml:"allow_superuser_readonly"`
	MCPAuthToken           string               `toml:"mcp_auth_token"`
	Operator               string               `toml:"operator"`
	LLM                    WorkstationLLMConfig `toml:"llm"`
	Permissions            PermissionConfig     `toml:"permissions"`
	Math                   MathConfig           `toml:"math"`
	Brand                  brand.Spec           `toml:"brand"`
}

// EffectiveMQTTAuth resolves merged top-level and [mqtt] section credentials/TLS settings.
func (w WorkstationConfig) EffectiveMQTTAuth() EdgeMQTTConfig {
	out := w.MQTT
	if strings.TrimSpace(out.Username) == "" {
		out.Username = strings.TrimSpace(w.MQTTUsername)
	}
	if out.Password == "" {
		out.Password = w.MQTTPassword
	}
	if strings.TrimSpace(out.TLSCAFile) == "" {
		out.TLSCAFile = strings.TrimSpace(w.TLSCAFile)
	}
	if strings.TrimSpace(out.TLSCertFile) == "" {
		out.TLSCertFile = strings.TrimSpace(w.TLSCertFile)
	}
	if strings.TrimSpace(out.TLSKeyFile) == "" {
		out.TLSKeyFile = strings.TrimSpace(w.TLSKeyFile)
	}
	if !out.TLSInsecureSkipVerify {
		out.TLSInsecureSkipVerify = w.TLSInsecureSkipVerify
	}
	return out
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

// DefaultCoopConfig returns safe out-of-the-box PigeonCoop defaults with loopback MQTT and ephemeral scripts disabled.
func DefaultCoopConfig() CoopConfig {
	return CoopConfig{
		NodeID:        defaultHostname("rv64-coop-01"),
		Tags:          core.NormalizeTags([]string{"coop", runtime.GOARCH}),
		SelfObserve:   true,
		PollInterval:  "5s",
		SpoolCapacity: 10000,
		MQTT: CoopMQTTConfig{
			Mode:   "embedded",
			Listen: "127.0.0.1:1883",
			URL:    "tcp://127.0.0.1:1883",
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
			URL:     "http://127.0.0.1:2586/carrier-pigeons",
		},
		Observers: ObserversConfig{
			OS:  true,
			Net: true,
			GPU: true,
			NPU: true,
		},
		Runners: RunnersConfig{
			Enabled:             true,
			AllowEphemeral:      false,
			AllowedInterpreters: []string{"sh", "bash", "python3"},
			MaxTimeoutSec:       300,
		},
		Brand: brand.Default(),
	}
}

// DefaultEdgeConfig returns safe out-of-the-box Pigeoneer defaults with hardware auto-detection and ring spooling.
func DefaultEdgeConfig() EdgeConfig {
	return EdgeConfig{
		NodeID:        defaultHostname("rv64-edge-01"),
		Tags:          core.NormalizeTags([]string{runtime.GOARCH}),
		PollInterval:  "5s",
		SpoolCapacity: 10000,
		BrokerURL:     "tcp://127.0.0.1:1883",
		TopicPrefix:   "", // Resolved dynamically from Brand.CTDPrefix() unless explicitly overridden
		Observers: ObserversConfig{
			OS:  true,
			Net: true,
			GPU: true,
			NPU: true,
		},
		Runners: RunnersConfig{
			Enabled:             true,
			AllowEphemeral:      false,
			AllowedInterpreters: []string{"sh", "bash", "python3"},
			MaxTimeoutSec:       300,
		},
		Brand: brand.Default(),
	}
}

// DefaultWorkstationConfig returns out-of-the-box Pigeons CLI/TUI defaults.
func DefaultWorkstationConfig() WorkstationConfig {
	return WorkstationConfig{
		BrokerURL:           "tcp://127.0.0.1:1883",
		DatabaseURL:         DefaultPostgresURL,
		ReadOnlyDatabaseURL: "",
		Operator:            defaultOperator(),
		LLM: WorkstationLLMConfig{
			Provider: "ollama",
			Model:    "qwen2.5-coder",
			Endpoint: "http://127.0.0.1:11434",
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
	bSpec := brand.Active().MergeWithDefaults()
	dirSlug := strings.ToLower(strings.TrimSpace(bSpec.CLIName))
	if dirSlug == "" {
		dirSlug = "pigeons"
	}

	candidates := []string{filename}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates = append(candidates, filepath.Join(home, ".config", dirSlug, filename))
		if dirSlug != "pigeons" {
			candidates = append(candidates, filepath.Join(home, ".config", "pigeons", filename))
		}
	}
	candidates = append(candidates, filepath.Join("/etc", dirSlug, filename))
	if dirSlug != "pigeons" {
		candidates = append(candidates, filepath.Join("/etc/pigeons", filename))
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c
		}
	}
	return ""
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if k == "" {
			continue
		}
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func prefixedEnv(brandPrefix, suffix string, fallbacks ...string) string {
	var keys []string
	if p := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(brandPrefix)), "_"); p != "" {
		keys = append(keys, p+"_"+suffix)
	}
	keys = append(keys, fallbacks...)
	return firstEnv(keys...)
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

	cfg.Brand = cfg.Brand.MergeWithDefaults()
	bp := cfg.Brand.EnvPrefix

	if v := prefixedEnv(bp, "NODE_ID", "PIGEONCOOP_NODE_ID", "PIGEONS_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := prefixedEnv(bp, "TAGS", "PIGEONCOOP_TAGS", "PIGEONS_TAGS"); v != "" {
		cfg.Tags = splitCSVTags(v)
	}
	if v := prefixedEnv(bp, "SELF_OBSERVE", "PIGEONCOOP_SELF_OBSERVE"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.SelfObserve = b
		}
	}
	if v := prefixedEnv(bp, "SPOOL_CAPACITY", "PIGEONCOOP_SPOOL_CAPACITY", "PIGEONS_SPOOL_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.SpoolCapacity = n
		}
	}
	if v := prefixedEnv(bp, "MQTT_MODE", "PIGEONCOOP_MQTT_MODE"); v != "" {
		cfg.MQTT.Mode = strings.ToLower(v)
	}
	if v := prefixedEnv(bp, "MQTT_LISTEN", "PIGEONCOOP_MQTT_LISTEN"); v != "" {
		cfg.MQTT.Listen = v
	}
	if v := prefixedEnv(bp, "BROKER_URL", "PIGEONCOOP_BROKER_URL", "PIGEONS_BROKER_URL", "MQTT_BROKER_URL"); v != "" {
		cfg.MQTT.URL = v
		if prefixedEnv(bp, "MQTT_MODE", "PIGEONCOOP_MQTT_MODE") == "" && path == "" {
			cfg.MQTT.Mode = "external"
		}
	}
	if v := prefixedEnv(bp, "MQTT_USERNAME", "PIGEONCOOP_MQTT_USERNAME", "PIGEONS_MQTT_USERNAME", "MQTT_USERNAME"); v != "" {
		cfg.MQTT.Username = v
	}
	if v := prefixedEnv(bp, "MQTT_PASSWORD", "PIGEONCOOP_MQTT_PASSWORD", "PIGEONS_MQTT_PASSWORD", "MQTT_PASSWORD"); v != "" {
		cfg.MQTT.Password = v
	}
	if v := prefixedEnv(bp, "MQTT_EDGE_USERNAME", "PIGEONCOOP_MQTT_EDGE_USERNAME"); v != "" {
		cfg.MQTT.EdgeUsername = v
	}
	if v := prefixedEnv(bp, "MQTT_EDGE_PASSWORD", "PIGEONCOOP_MQTT_EDGE_PASSWORD"); v != "" {
		cfg.MQTT.EdgePassword = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CERT", "PIGEONCOOP_MQTT_TLS_CERT"); v != "" {
		cfg.MQTT.TLSCertFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_KEY", "PIGEONCOOP_MQTT_TLS_KEY"); v != "" {
		cfg.MQTT.TLSKeyFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CA", "PIGEONCOOP_MQTT_TLS_CA", "PIGEONS_MQTT_TLS_CA"); v != "" {
		cfg.MQTT.TLSCAFile = v
	}
	if v := prefixedEnv(bp, "ALLOW_INSECURE_ANONYMOUS", "PIGEONCOOP_ALLOW_INSECURE_ANONYMOUS"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.MQTT.AllowInsecureAnonymous = b
		}
	}
	if v := prefixedEnv(bp, "ALLOW_EPHEMERAL", "PIGEONCOOP_ALLOW_EPHEMERAL"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.Runners.AllowEphemeral = b
		}
	}
	if v := prefixedEnv(bp, "DB_URL", "PIGEONCOOP_DB_URL", "PIGEONS_DB_URL", "DATABASE_URL"); v != "" {
		cfg.Database.URL = v
	}
	if v := prefixedEnv(bp, "ALLOW_INSECURE_DEFAULTS", "PIGEONCOOP_ALLOW_INSECURE_DEFAULTS"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.Database.AllowInsecureDefaults = b
		}
	}
	if v := prefixedEnv(bp, "NTFY_URL", "PIGEONCOOP_NTFY_URL", "NTFY_URL"); v != "" {
		if v == "off" || v == "disabled" || v == "none" {
			cfg.Ntfy.Enabled = false
		} else {
			cfg.Ntfy.URL = v
		}
	}

	if err := core.ValidateTopicToken(cfg.NodeID); err != nil {
		return cfg, fmt.Errorf("invalid coop node_id: %w", err)
	}
	if err := core.ValidateTopicToken(cfg.Brand.TopicRoot); err != nil {
		return cfg, fmt.Errorf("invalid brand topic_root: %w", err)
	}

	cfg.Tags = core.NormalizeTags(cfg.Tags)
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

	cfg.Brand = cfg.Brand.MergeWithDefaults()
	bp := cfg.Brand.EnvPrefix

	if v := prefixedEnv(bp, "NODE_ID", "PIGEONEER_NODE_ID", "PIGEONS_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := prefixedEnv(bp, "TAGS", "PIGEONEER_TAGS", "PIGEONS_TAGS"); v != "" {
		cfg.Tags = splitCSVTags(v)
	}
	if v := prefixedEnv(bp, "BROKER_URL", "PIGEONEER_BROKER_URL", "PIGEONS_BROKER_URL", "MQTT_BROKER_URL"); v != "" {
		cfg.BrokerURL = v
	}
	if v := prefixedEnv(bp, "TOPIC_PREFIX", "PIGEONEER_TOPIC_PREFIX", "PIGEONS_TOPIC_PREFIX"); v != "" {
		cfg.TopicPrefix = v
	}
	if v := prefixedEnv(bp, "SPOOL_CAPACITY", "PIGEONEER_SPOOL_CAPACITY", "PIGEONS_SPOOL_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.SpoolCapacity = n
		}
	}
	if v := prefixedEnv(bp, "MQTT_USERNAME", "PIGEONEER_MQTT_USERNAME", "PIGEONS_MQTT_USERNAME", "MQTT_USERNAME"); v != "" {
		cfg.MQTTUsername = v
		cfg.MQTT.Username = v
	}
	if v := prefixedEnv(bp, "MQTT_PASSWORD", "PIGEONEER_MQTT_PASSWORD", "PIGEONS_MQTT_PASSWORD", "MQTT_PASSWORD"); v != "" {
		cfg.MQTTPassword = v
		cfg.MQTT.Password = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CA", "PIGEONEER_MQTT_TLS_CA", "PIGEONS_MQTT_TLS_CA"); v != "" {
		cfg.TLSCAFile = v
		cfg.MQTT.TLSCAFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CERT", "PIGEONEER_MQTT_TLS_CERT", "PIGEONS_MQTT_TLS_CERT"); v != "" {
		cfg.TLSCertFile = v
		cfg.MQTT.TLSCertFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_KEY", "PIGEONEER_MQTT_TLS_KEY", "PIGEONS_MQTT_TLS_KEY"); v != "" {
		cfg.TLSKeyFile = v
		cfg.MQTT.TLSKeyFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_INSECURE_SKIP_VERIFY", "PIGEONEER_MQTT_TLS_INSECURE_SKIP_VERIFY", "PIGEONS_MQTT_TLS_INSECURE_SKIP_VERIFY"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.TLSInsecureSkipVerify = b
			cfg.MQTT.TLSInsecureSkipVerify = b
		}
	}
	if v := prefixedEnv(bp, "ALLOW_EPHEMERAL", "PIGEONEER_ALLOW_EPHEMERAL"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.Runners.AllowEphemeral = b
		}
	}

	if err := core.ValidateTopicToken(cfg.NodeID); err != nil {
		return cfg, fmt.Errorf("invalid edge node_id: %w", err)
	}

	cfg.Tags = core.NormalizeTags(cfg.Tags)
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

	cfg.Brand = cfg.Brand.MergeWithDefaults()
	bp := cfg.Brand.EnvPrefix

	if v := prefixedEnv(bp, "BROKER_URL", "PIGEONS_BROKER_URL", "PIGEONCOOP_BROKER_URL", "MQTT_BROKER_URL"); v != "" {
		cfg.BrokerURL = v
	}
	if v := prefixedEnv(bp, "MQTT_USERNAME", "PIGEONS_MQTT_USERNAME", "PIGEONCOOP_MQTT_USERNAME", "MQTT_USERNAME"); v != "" {
		cfg.MQTTUsername = v
		cfg.MQTT.Username = v
	}
	if v := prefixedEnv(bp, "MQTT_PASSWORD", "PIGEONS_MQTT_PASSWORD", "PIGEONCOOP_MQTT_PASSWORD", "MQTT_PASSWORD"); v != "" {
		cfg.MQTTPassword = v
		cfg.MQTT.Password = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CA", "PIGEONS_MQTT_TLS_CA"); v != "" {
		cfg.TLSCAFile = v
		cfg.MQTT.TLSCAFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CERT", "PIGEONS_MQTT_TLS_CERT"); v != "" {
		cfg.TLSCertFile = v
		cfg.MQTT.TLSCertFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_KEY", "PIGEONS_MQTT_TLS_KEY"); v != "" {
		cfg.TLSKeyFile = v
		cfg.MQTT.TLSKeyFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_INSECURE_SKIP_VERIFY", "PIGEONS_MQTT_TLS_INSECURE_SKIP_VERIFY"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.TLSInsecureSkipVerify = b
			cfg.MQTT.TLSInsecureSkipVerify = b
		}
	}
	if v := prefixedEnv(bp, "DB_URL", "PIGEONS_DB_URL", "PIGEONCOOP_DB_URL", "DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	roFromEnv := false
	if v := prefixedEnv(bp, "RO_DB_URL", "PIGEONS_RO_DB_URL", "PIGEONCARE_DB_URL", "PIGEONMCP_DB_URL"); v != "" {
		cfg.ReadOnlyDatabaseURL = v
		roFromEnv = true
	}
	if !roFromEnv && strings.TrimSpace(cfg.DatabaseURL) != "" {
		ro := strings.TrimSpace(cfg.ReadOnlyDatabaseURL)
		if ro == "" || (isURLHostLoopback(ro) && !isURLHostLoopback(cfg.DatabaseURL)) {
			cfg.ReadOnlyDatabaseURL = db.DeriveReadOnlyURLFromAdmin(cfg.DatabaseURL, "")
		}
	}
	if v := prefixedEnv(bp, "ALLOW_SUPERUSER_RO", "PIGEONS_ALLOW_SUPERUSER_RO"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.AllowSuperuserReadOnly = b
		}
	}
	if v := prefixedEnv(bp, "MCP_AUTH_TOKEN", "PIGEONS_MCP_AUTH_TOKEN"); v != "" {
		cfg.MCPAuthToken = v
	}
	if v := prefixedEnv(bp, "OPERATOR", "PIGEONS_OPERATOR"); v != "" {
		cfg.Operator = v
	}
	if v := prefixedEnv(bp, "LLM_PROVIDER", "PIGEONS_LLM_PROVIDER"); v != "" {
		cfg.LLM.Provider = v
	}
	if v := prefixedEnv(bp, "LLM_MODEL", "PIGEONS_LLM_MODEL"); v != "" {
		cfg.LLM.Model = v
	}
	if v := prefixedEnv(bp, "LLM_ENDPOINT", "PIGEONS_LLM_ENDPOINT"); v != "" {
		cfg.LLM.Endpoint = v
	}
	if v := prefixedEnv(bp, "LLM_API_KEY", "PIGEONS_LLM_API_KEY"); v != "" {
		cfg.LLM.APIKey = v
	}
	if v := prefixedEnv(bp, "MODE", "PIGEONS_MODE", "PIGEONCARE_MODE"); v != "" {
		cfg.Permissions.Mode = strings.ToLower(v)
	}

	return cfg, nil
}
