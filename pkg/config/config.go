package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/jackc/pgx/v5/pgxpool"
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
	TLSClientCAFile        string `toml:"tls_client_ca_file"`
	TLSClientCertFile      string `toml:"tls_client_cert_file"`
	TLSClientKeyFile       string `toml:"tls_client_key_file"`
	RequireClientCert      bool   `toml:"require_client_cert"`
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
	MeshName      string          `toml:"mesh_name"`
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

func isPostgresHostLocal(host string) bool {
	h := strings.Trim(strings.TrimSpace(host), "[]")
	if h == "" || strings.HasPrefix(h, "/") {
		return true
	}
	return IsLoopbackListen(h)
}

func nonLoopbackPGHost(pgCfg *pgxpool.Config) (string, bool) {
	if pgCfg == nil || pgCfg.ConnConfig == nil {
		return "", false
	}
	if host := strings.TrimSpace(pgCfg.ConnConfig.Host); !isPostgresHostLocal(host) {
		return host, true
	}
	for _, fb := range pgCfg.ConnConfig.Fallbacks {
		if fb == nil {
			continue
		}
		if host := strings.TrimSpace(fb.Host); !isPostgresHostLocal(host) {
			return host, true
		}
	}
	return "", false
}

func isURLHostLoopback(rawURL string) bool {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return false
	}
	pgCfg, err := pgxpool.ParseConfig(trimmed)
	if err != nil {
		return false
	}
	_, nonLoopback := nonLoopbackPGHost(pgCfg)
	return !nonLoopback
}

// ValidateSecurity enforces fail-closed security invariants before starting network listeners.
func (c CoopConfig) ValidateSecurity() error {
	meshName := strings.TrimSpace(c.MeshName)
	if meshName == "" {
		meshName = core.DefaultMeshName
	}
	if err := core.ValidateMeshName(meshName); err != nil {
		return fmt.Errorf("invalid coop mesh_name: %w", err)
	}
	if err := db.ValidateSchemaIdentifier(c.Brand.MergeWithDefaults().MathSchema); err != nil {
		return fmt.Errorf("invalid brand math_schema: %w", err)
	}

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
	if !isExternal && strings.TrimSpace(c.MQTT.Username) != "" && c.MQTT.Password == "" {
		return fmt.Errorf(
			"refusing insecure startup: mqtt.username %q configured without mqtt.password",
			strings.TrimSpace(c.MQTT.Username),
		)
	}
	if !isExternal && c.MQTT.Password != "" && strings.TrimSpace(c.MQTT.Username) == "" {
		return fmt.Errorf("refusing insecure startup: mqtt.password configured without mqtt.username")
	}
	if strings.TrimSpace(c.MQTT.EdgeUsername) != "" && c.MQTT.EdgePassword == "" {
		return fmt.Errorf(
			"refusing insecure startup: mqtt.edge_username %q configured without mqtt.edge_password",
			strings.TrimSpace(c.MQTT.EdgeUsername),
		)
	}
	if c.MQTT.EdgePassword != "" && strings.TrimSpace(c.MQTT.EdgeUsername) == "" {
		return fmt.Errorf("refusing insecure startup: mqtt.edge_password configured without mqtt.edge_username")
	}
	if (strings.TrimSpace(c.MQTT.TLSCertFile) != "") != (strings.TrimSpace(c.MQTT.TLSKeyFile) != "") {
		return fmt.Errorf("refusing insecure startup: both mqtt.tls_cert_file and mqtt.tls_key_file must be provided together")
	}
	if (strings.TrimSpace(c.MQTT.TLSClientCertFile) != "") != (strings.TrimSpace(c.MQTT.TLSClientKeyFile) != "") {
		return fmt.Errorf("refusing insecure startup: both mqtt.tls_client_cert_file and mqtt.tls_client_key_file must be provided together")
	}
	if !isExternal && c.MQTT.RequireClientCert {
		if strings.TrimSpace(c.MQTT.TLSCertFile) == "" || strings.TrimSpace(c.MQTT.TLSKeyFile) == "" {
			return fmt.Errorf("refusing insecure startup: mqtt.require_client_cert=true requires mqtt.tls_cert_file and mqtt.tls_key_file")
		}
		if strings.TrimSpace(c.MQTT.TLSClientCAFile) == "" {
			return fmt.Errorf("refusing insecure startup: mqtt.require_client_cert=true requires mqtt.tls_client_ca_file")
		}
	}

	if dbURL := strings.TrimSpace(c.Database.URL); dbURL != "" && !c.Database.AllowInsecureDefaults {
		pgCfg, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			return fmt.Errorf("refusing insecure startup: invalid database.url (%s)", RedactURL(dbURL))
		}
		if host, nonLoopback := nonLoopbackPGHost(pgCfg); nonLoopback && isKnownDefaultDBPassword(pgCfg.ConnConfig.Password) {
			return fmt.Errorf(
				"refusing insecure startup: database.url targets non-loopback host %q with a default development password (rotate credentials or set database.allow_insecure_defaults=true explicitly)",
				host,
			)
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

// MeshProfileConfig defines per-mesh connection & runtime overrides under `[meshes.<mesh_name>]` in `pigeons.toml`.
// Unset fields inherit top-level WorkstationConfig defaults.
type MeshProfileConfig struct {
	BrokerURL              string               `toml:"broker_url,omitempty"`
	MQTTUsername           string               `toml:"mqtt_username,omitempty"`
	MQTTPassword           string               `toml:"mqtt_password,omitempty"`
	TLSCAFile              string               `toml:"tls_ca_file,omitempty"`
	TLSCertFile            string               `toml:"tls_cert_file,omitempty"`
	TLSKeyFile             string               `toml:"tls_key_file,omitempty"`
	TLSInsecureSkipVerify  *bool                `toml:"tls_insecure_skip_verify,omitempty"`
	MQTT                   EdgeMQTTConfig       `toml:"mqtt,omitempty"`
	DatabaseURL            string               `toml:"database_url,omitempty"`
	ReadOnlyDatabaseURL    string               `toml:"readonly_database_url,omitempty"`
	AllowSuperuserReadOnly *bool                `toml:"allow_superuser_readonly,omitempty"`
	MCPAuthToken           string               `toml:"mcp_auth_token,omitempty"`
	Operator               string               `toml:"operator,omitempty"`
	LLM                    WorkstationLLMConfig `toml:"llm,omitempty"`
	Permissions            PermissionConfig     `toml:"permissions,omitempty"`
	Math                   MathConfig           `toml:"math,omitempty"`
	Brand                  brand.Spec           `toml:"brand,omitempty"`
}

// MeshProfilePatch carries explicit CLI flag updates for `pigeons connect <mesh> --broker ... --db ...`.
type MeshProfilePatch struct {
	BrokerURL             *string
	DatabaseURL           *string
	ReadOnlyDatabaseURL   *string
	MQTTUsername          *string
	MQTTPassword          *string
	TLSCAFile             *string
	TLSCertFile           *string
	TLSKeyFile            *string
	TLSInsecureSkipVerify *bool
	TopicRoot             *string
}

// IsEmpty reports whether no profile registration/update flags were supplied.
func (p MeshProfilePatch) IsEmpty() bool {
	return p.BrokerURL == nil &&
		p.DatabaseURL == nil &&
		p.ReadOnlyDatabaseURL == nil &&
		p.MQTTUsername == nil &&
		p.MQTTPassword == nil &&
		p.TLSCAFile == nil &&
		p.TLSCertFile == nil &&
		p.TLSKeyFile == nil &&
		p.TLSInsecureSkipVerify == nil &&
		p.TopicRoot == nil
}

// Apply overlays non-nil fields onto an existing MeshProfileConfig.
func (p MeshProfilePatch) Apply(dst MeshProfileConfig) MeshProfileConfig {
	if p.BrokerURL != nil {
		dst.BrokerURL = strings.TrimSpace(*p.BrokerURL)
	}
	if p.DatabaseURL != nil {
		dst.DatabaseURL = strings.TrimSpace(*p.DatabaseURL)
	}
	if p.ReadOnlyDatabaseURL != nil {
		dst.ReadOnlyDatabaseURL = strings.TrimSpace(*p.ReadOnlyDatabaseURL)
	}
	if p.MQTTUsername != nil {
		u := strings.TrimSpace(*p.MQTTUsername)
		dst.MQTTUsername = u
		dst.MQTT.Username = u
	}
	if p.MQTTPassword != nil {
		dst.MQTTPassword = *p.MQTTPassword
		dst.MQTT.Password = *p.MQTTPassword
	}
	if p.TLSCAFile != nil {
		v := strings.TrimSpace(*p.TLSCAFile)
		dst.TLSCAFile = v
		dst.MQTT.TLSCAFile = v
	}
	if p.TLSCertFile != nil {
		v := strings.TrimSpace(*p.TLSCertFile)
		dst.TLSCertFile = v
		dst.MQTT.TLSCertFile = v
	}
	if p.TLSKeyFile != nil {
		v := strings.TrimSpace(*p.TLSKeyFile)
		dst.TLSKeyFile = v
		dst.MQTT.TLSKeyFile = v
	}
	if p.TLSInsecureSkipVerify != nil {
		b := *p.TLSInsecureSkipVerify
		dst.TLSInsecureSkipVerify = &b
		dst.MQTT.TLSInsecureSkipVerify = b
	}
	if p.TopicRoot != nil {
		dst.Brand.TopicRoot = strings.Trim(strings.TrimSpace(*p.TopicRoot), "/")
	}
	return dst
}

// WorkstationConfig configures the operator workstation CLI/TUI (`pigeons.toml`).
type WorkstationConfig struct {
	ActiveMesh             string                       `toml:"-"`
	ActiveMeshSource       string                       `toml:"-"`
	ConfigPath             string                       `toml:"-"`
	MeshName               string                       `toml:"mesh_name"`
	BrokerURL              string                       `toml:"broker_url"`
	MQTTUsername           string                       `toml:"mqtt_username"`
	MQTTPassword           string                       `toml:"mqtt_password"`
	TLSCAFile              string                       `toml:"tls_ca_file"`
	TLSCertFile            string                       `toml:"tls_cert_file"`
	TLSKeyFile             string                       `toml:"tls_key_file"`
	TLSInsecureSkipVerify  bool                         `toml:"tls_insecure_skip_verify"`
	MQTT                   EdgeMQTTConfig               `toml:"mqtt"`
	DatabaseURL            string                       `toml:"database_url"`
	ReadOnlyDatabaseURL    string                       `toml:"readonly_database_url"`
	AllowSuperuserReadOnly bool                         `toml:"allow_superuser_readonly"`
	MCPAuthToken           string                       `toml:"mcp_auth_token"`
	Operator               string                       `toml:"operator"`
	LLM                    WorkstationLLMConfig         `toml:"llm"`
	Permissions            PermissionConfig             `toml:"permissions"`
	Math                   MathConfig                   `toml:"math"`
	Brand                  brand.Spec                   `toml:"brand"`
	Meshes                 map[string]MeshProfileConfig `toml:"meshes"`
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

// ConfiguredMeshNames returns all sorted MNS names declared under `[meshes.*]` (plus top-level `mesh_name` if set).
func (w WorkstationConfig) ConfiguredMeshNames() []string {
	seen := make(map[string]struct{}, len(w.Meshes)+1)
	if top := strings.TrimSpace(w.MeshName); top != "" {
		seen[top] = struct{}{}
	}
	for k := range w.Meshes {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func mergeBrandOverlay(base, over brand.Spec) brand.Spec {
	out := base
	if strings.TrimSpace(over.ProductName) != "" {
		out.ProductName = over.ProductName
	}
	if strings.TrimSpace(over.CoopName) != "" {
		out.CoopName = over.CoopName
	}
	if strings.TrimSpace(over.EdgeName) != "" {
		out.EdgeName = over.EdgeName
	}
	if strings.TrimSpace(over.CLIName) != "" {
		out.CLIName = over.CLIName
	}
	if strings.TrimSpace(over.AgentName) != "" {
		out.AgentName = over.AgentName
	}
	if strings.TrimSpace(over.MCPName) != "" {
		out.MCPName = over.MCPName
	}
	if strings.TrimSpace(over.MathName) != "" {
		out.MathName = over.MathName
	}
	if strings.Trim(strings.TrimSpace(over.TopicRoot), "/") != "" {
		out.TopicRoot = strings.Trim(strings.TrimSpace(over.TopicRoot), "/")
	}
	if strings.TrimSpace(over.EnvPrefix) != "" {
		out.EnvPrefix = over.EnvPrefix
	}
	if strings.TrimSpace(over.MathSchema) != "" {
		out.MathSchema = over.MathSchema
	}
	if strings.TrimSpace(over.AccentColor) != "" {
		out.AccentColor = over.AccentColor
	}
	if strings.TrimSpace(over.SecondaryColor) != "" {
		out.SecondaryColor = over.SecondaryColor
	}
	if strings.TrimSpace(over.MutedColor) != "" {
		out.MutedColor = over.MutedColor
	}
	return out.MergeWithDefaults()
}

// ApplyMeshProfile layers a named `[meshes.<meshName>]` profile onto a base WorkstationConfig,
// inheriting top-level LLM, permission, and operator defaults for any unset fields.
func ApplyMeshProfile(base WorkstationConfig, meshName string, p MeshProfileConfig) WorkstationConfig {
	out := base
	out.ActiveMesh = meshName
	out.MeshName = meshName

	overridesEndpoint := strings.TrimSpace(p.BrokerURL) != "" ||
		strings.TrimSpace(p.DatabaseURL) != "" ||
		strings.TrimSpace(p.ReadOnlyDatabaseURL) != ""
	if overridesEndpoint {
		out.MQTTUsername = ""
		out.MQTTPassword = ""
		out.MQTT.Username = ""
		out.MQTT.Password = ""
		out.TLSInsecureSkipVerify = false
		out.MQTT.TLSInsecureSkipVerify = false
		out.DatabaseURL = ""
		out.ReadOnlyDatabaseURL = ""
	}

	if v := strings.TrimSpace(p.BrokerURL); v != "" {
		out.BrokerURL = v
	}

	effUser := strings.TrimSpace(p.MQTT.Username)
	if effUser == "" {
		effUser = strings.TrimSpace(p.MQTTUsername)
	}
	if effUser != "" {
		out.MQTTUsername = effUser
		out.MQTT.Username = effUser
	}

	effPass := p.MQTT.Password
	if effPass == "" {
		effPass = p.MQTTPassword
	}
	if effPass != "" {
		out.MQTTPassword = effPass
		out.MQTT.Password = effPass
	}

	effCA := strings.TrimSpace(p.MQTT.TLSCAFile)
	if effCA == "" {
		effCA = strings.TrimSpace(p.TLSCAFile)
	}
	if effCA != "" {
		out.TLSCAFile = effCA
		out.MQTT.TLSCAFile = effCA
	}

	effCert := strings.TrimSpace(p.MQTT.TLSCertFile)
	if effCert == "" {
		effCert = strings.TrimSpace(p.TLSCertFile)
	}
	if effCert != "" {
		out.TLSCertFile = effCert
		out.MQTT.TLSCertFile = effCert
	}

	effKey := strings.TrimSpace(p.MQTT.TLSKeyFile)
	if effKey == "" {
		effKey = strings.TrimSpace(p.TLSKeyFile)
	}
	if effKey != "" {
		out.TLSKeyFile = effKey
		out.MQTT.TLSKeyFile = effKey
	}

	if p.TLSInsecureSkipVerify != nil {
		out.TLSInsecureSkipVerify = *p.TLSInsecureSkipVerify
		out.MQTT.TLSInsecureSkipVerify = *p.TLSInsecureSkipVerify
	} else if p.MQTT.TLSInsecureSkipVerify {
		out.TLSInsecureSkipVerify = true
		out.MQTT.TLSInsecureSkipVerify = true
	}

	dbChanged := false
	if v := strings.TrimSpace(p.DatabaseURL); v != "" {
		out.DatabaseURL = v
		dbChanged = true
	}
	if v := strings.TrimSpace(p.ReadOnlyDatabaseURL); v != "" {
		out.ReadOnlyDatabaseURL = v
	} else if dbChanged {
		out.ReadOnlyDatabaseURL = deriveReadOnlyURL(out.DatabaseURL)
	}

	if p.AllowSuperuserReadOnly != nil {
		out.AllowSuperuserReadOnly = *p.AllowSuperuserReadOnly
	}
	if v := strings.TrimSpace(p.MCPAuthToken); v != "" {
		out.MCPAuthToken = v
	}
	if v := strings.TrimSpace(p.Operator); v != "" {
		out.Operator = v
	}

	if v := strings.TrimSpace(p.LLM.Provider); v != "" {
		out.LLM.Provider = v
	}
	if v := strings.TrimSpace(p.LLM.Model); v != "" {
		out.LLM.Model = v
	}
	if v := strings.TrimSpace(p.LLM.Endpoint); v != "" {
		out.LLM.Endpoint = v
	}
	if v := strings.TrimSpace(p.LLM.APIKey); v != "" {
		out.LLM.APIKey = v
	}

	if v := strings.TrimSpace(p.Permissions.Mode); v != "" {
		out.Permissions.Mode = v
	}
	if len(p.Permissions.AllowedRunners) > 0 {
		out.Permissions.AllowedRunners = slices.Clone(p.Permissions.AllowedRunners)
	}
	if len(p.Permissions.AllowedScriptHashes) > 0 {
		out.Permissions.AllowedScriptHashes = slices.Clone(p.Permissions.AllowedScriptHashes)
	}

	if v := strings.TrimSpace(p.Math.RefreshInterval); v != "" {
		out.Math.RefreshInterval = v
	}

	out.Brand = mergeBrandOverlay(out.Brand, p.Brand)
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
		MeshName:      core.DefaultMeshName,
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

func brandCLIDirSlug(b brand.Spec) string {
	bSpec := b.MergeWithDefaults()
	dirSlug := strings.ToLower(strings.TrimSpace(bSpec.CLIName))
	if dirSlug == "" {
		return "pigeons"
	}
	return dirSlug
}

func brandWorkstationConfigFilename(b brand.Spec) string {
	return brandCLIDirSlug(b) + ".toml"
}

func activeCLIDirSlug() string {
	return brandCLIDirSlug(brand.Active())
}

// CurrentMeshStatePathForBrand resolves `~/.config/<cli_slug>/current_mesh`
// (or `<BRAND_ENV_PREFIX>_CURRENT_MESH_FILE` override) for the given brand.
func CurrentMeshStatePathForBrand(b brand.Spec) (string, error) {
	bSpec := b.MergeWithDefaults()
	envKey := bSpec.EnvPrefix + "_CURRENT_MESH_FILE"
	if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("resolve user home directory for current_mesh state: %w", err)
	}
	return filepath.Join(home, ".config", brandCLIDirSlug(bSpec), "current_mesh"), nil
}

// CurrentMeshStatePath resolves `~/.config/<cli_slug>/current_mesh` (or `PIGEONS_CURRENT_MESH_FILE` override).
func CurrentMeshStatePath() (string, error) {
	return CurrentMeshStatePathForBrand(brand.Active())
}

func previousMeshStatePathForBrand(b brand.Spec) (string, error) {
	curPath, err := CurrentMeshStatePathForBrand(b)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(curPath), "previous_mesh"), nil
}

// ReadCurrentMeshForBrand reads the persisted "current working mesh" name for the given brand.
// Returns ("", nil) when no active mesh state file exists.
func ReadCurrentMeshForBrand(b brand.Spec) (string, error) {
	path, err := CurrentMeshStatePathForBrand(b)
	if err != nil {
		return "", nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	name := strings.TrimSpace(string(raw))
	if name == "" {
		return "", nil
	}
	if err := core.ValidateMeshName(name); err != nil {
		return "", fmt.Errorf("invalid mesh name in %s: %w", path, err)
	}
	return name, nil
}

// ReadCurrentMesh reads the persisted "current working mesh" name from `~/.config/pigeons/current_mesh`.
// Returns ("", nil) when no active mesh state file exists.
func ReadCurrentMesh() (string, error) {
	return ReadCurrentMeshForBrand(brand.Active())
}

// ReadPreviousMeshForBrand reads the prior working mesh name saved during the last `<cli> connect` switch for the given brand.
func ReadPreviousMeshForBrand(b brand.Spec) (string, error) {
	path, err := previousMeshStatePathForBrand(b)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	name := strings.TrimSpace(string(raw))
	if name == "" {
		return "", nil
	}
	if err := core.ValidateMeshName(name); err != nil {
		return "", fmt.Errorf("invalid previous mesh name in %s: %w", path, err)
	}
	return name, nil
}

// ReadPreviousMesh reads the prior working mesh name saved during the last `pigeons connect` switch (for `pigeons connect -`).
func ReadPreviousMesh() (string, error) {
	return ReadPreviousMeshForBrand(brand.Active())
}

var (
	systemConfigRoot = "/etc"
	syncFile         = func(f *os.File) error { return f.Sync() }
	syncDir          = defaultSyncDir
)

func isIgnorableDirSyncErr(err error) bool {
	return errors.Is(err, errors.ErrUnsupported) || errors.Is(err, syscall.EINVAL)
}

func defaultSyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open config directory %s for sync: %w", dir, err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil && !isIgnorableDirSyncErr(syncErr) {
		return fmt.Errorf("sync config directory %s: %w", dir, syncErr)
	}
	return closeErr
}

func writeAtomicFile0600(targetPath string, content []byte) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := syncFile(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		return fmt.Errorf("commit %s: %w", targetPath, err)
	}
	cleanup = false
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

// WriteCurrentMeshForBrand atomically persists the active "current working mesh" for the given brand
// and records any distinct prior mesh in `previous_mesh` so `<cli> connect -` toggles cleanly.
func WriteCurrentMeshForBrand(b brand.Spec, meshName string) (string, error) {
	bSpec := b.MergeWithDefaults()
	meshName = strings.TrimSpace(meshName)
	if err := core.ValidateMeshName(meshName); err != nil {
		return "", err
	}
	statePath, err := CurrentMeshStatePathForBrand(bSpec)
	if err != nil {
		return "", err
	}
	if prev, prevErr := ReadCurrentMeshForBrand(bSpec); prevErr == nil && prev != "" && prev != meshName {
		if prevPath, pErr := previousMeshStatePathForBrand(bSpec); pErr == nil {
			_ = writeAtomicFile0600(prevPath, []byte(prev+"\n"))
		}
	}
	if err := writeAtomicFile0600(statePath, []byte(meshName+"\n")); err != nil {
		return "", err
	}
	return statePath, nil
}

// WriteCurrentMesh atomically persists the active "current working mesh" in `~/.config/pigeons/current_mesh`
// and records any distinct prior mesh in `~/.config/pigeons/previous_mesh` so `pigeons connect -` toggles cleanly.
func WriteCurrentMesh(meshName string) (string, error) {
	return WriteCurrentMeshForBrand(brand.Active(), meshName)
}

// ClearCurrentMeshForBrand removes `current_mesh` for the given brand, reverting workstation commands to top-level defaults.
func ClearCurrentMeshForBrand(b brand.Spec) error {
	bSpec := b.MergeWithDefaults()
	statePath, err := CurrentMeshStatePathForBrand(bSpec)
	if err != nil {
		return err
	}
	if prev, prevErr := ReadCurrentMeshForBrand(bSpec); prevErr == nil && prev != "" {
		if prevPath, pErr := previousMeshStatePathForBrand(bSpec); pErr == nil {
			_ = writeAtomicFile0600(prevPath, []byte(prev+"\n"))
		}
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", statePath, err)
	}
	return nil
}

// ClearCurrentMesh removes `~/.config/pigeons/current_mesh`, reverting workstation commands to top-level defaults.
func ClearCurrentMesh() error {
	return ClearCurrentMeshForBrand(brand.Active())
}

func resolveConfigFile(explicitPath, envKey, filename string) string {
	if p := strings.TrimSpace(explicitPath); p != "" {
		return p
	}
	if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
		return p
	}
	dirSlug := activeCLIDirSlug()

	var candidates []string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates = append(candidates, filepath.Join(home, ".config", dirSlug, filename))
		if dirSlug != "pigeons" {
			candidates = append(candidates, filepath.Join(home, ".config", "pigeons", filename))
		}
	}
	candidates = append(candidates, filepath.Join(systemConfigRoot, dirSlug, filename))
	if dirSlug != "pigeons" {
		candidates = append(candidates, filepath.Join(systemConfigRoot, "pigeons", filename))
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c
		}
	}
	return ""
}

func resolveWorkstationConfigFileForBrand(explicitPath string, b brand.Spec) string {
	if p := strings.TrimSpace(explicitPath); p != "" {
		return p
	}
	bSpec := b.MergeWithDefaults()
	envKey := bSpec.EnvPrefix + "_CONFIG"
	if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
		return p
	}
	dirSlug := brandCLIDirSlug(bSpec)
	filename := dirSlug + ".toml"

	var candidates []string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates = append(candidates, filepath.Join(home, ".config", dirSlug, filename))
	}
	candidates = append(candidates, filepath.Join(systemConfigRoot, dirSlug, filename))
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c
		}
	}
	return ""
}

func resolveWritableWorkstationConfigPathForBrand(explicitPath string, b brand.Spec) (string, error) {
	if p := strings.TrimSpace(explicitPath); p != "" {
		return p, nil
	}
	bSpec := b.MergeWithDefaults()
	envKey := bSpec.EnvPrefix + "_CONFIG"
	if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
		return p, nil
	}
	dirSlug := brandCLIDirSlug(bSpec)
	filename := dirSlug + ".toml"
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		if err != nil {
			return "", fmt.Errorf("resolve user home directory for %s config: %w", filename, err)
		}
		return "", fmt.Errorf("resolve user home directory for %s config: empty home directory", filename)
	}
	return filepath.Join(home, ".config", dirSlug, filename), nil
}

func splitTOMLKeyPath(inner string) ([]string, bool) {
	var parts []string
	var cur strings.Builder
	inSingle, inDouble, escaped := false, false, false
	hasQuote := false
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if escaped {
			cur.WriteByte(ch)
			escaped = false
			continue
		}
		if inDouble && ch == '\\' {
			cur.WriteByte(ch)
			escaped = true
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			hasQuote = true
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			hasQuote = true
			continue
		}
		if !inSingle && !inDouble {
			if ch == '.' {
				part := strings.TrimSpace(cur.String())
				if part == "" && !hasQuote {
					return nil, false
				}
				parts = append(parts, part)
				cur.Reset()
				hasQuote = false
				continue
			}
			// Reject unquoted characters invalid in TOML key paths (e.g. multiline array literals like [[1, 2]]).
			if ch == ',' || ch == '[' || ch == ']' || ch == '=' {
				return nil, false
			}
		}
		cur.WriteByte(ch)
	}
	if inSingle || inDouble || escaped {
		return nil, false
	}
	part := strings.TrimSpace(cur.String())
	if part == "" && !hasQuote {
		return nil, false
	}
	parts = append(parts, part)
	return parts, true
}

func parseTOMLTableHeaderPath(line string) (path string, isArray bool, ok bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[") {
		return "", false, false
	}
	isArray = strings.HasPrefix(trimmed, "[[")
	startIdx := 1
	if isArray {
		startIdx = 2
	}
	// Strip trailing comment outside quotes.
	inSingle, inDouble, escaped := false, false, false
	closeIdx := -1
	for i := startIdx; i < len(trimmed); i++ {
		ch := trimmed[i]
		if escaped {
			escaped = false
			continue
		}
		if inDouble && ch == '\\' {
			escaped = true
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if !inSingle && !inDouble && ch == ']' {
			if isArray {
				if i+1 < len(trimmed) && trimmed[i+1] == ']' {
					closeIdx = i
					break
				}
				return "", false, false
			}
			closeIdx = i
			break
		}
	}
	if closeIdx < 0 {
		return "", false, false
	}
	closeEnd := closeIdx + 1
	if isArray {
		closeEnd = closeIdx + 2
	}
	rest := strings.TrimSpace(trimmed[closeEnd:])
	if rest != "" && !strings.HasPrefix(rest, "#") {
		return "", false, false
	}
	inner := strings.TrimSpace(trimmed[startIdx:closeIdx])
	if inner == "" {
		return "", false, false
	}
	parts, valid := splitTOMLKeyPath(inner)
	if !valid {
		return "", false, false
	}
	return strings.Join(parts, "."), isArray, true
}

func isMeshSectionHeader(headerPath, meshName string) bool {
	prefix := "meshes." + meshName
	return headerPath == prefix || strings.HasPrefix(headerPath, prefix+".")
}

func renderMeshProfileTOML(meshName string, p MeshProfileConfig) (string, error) {
	var sb strings.Builder
	enc := toml.NewEncoder(&sb)
	wrapper := struct {
		Meshes map[string]MeshProfileConfig `toml:"meshes"`
	}{
		Meshes: map[string]MeshProfileConfig{meshName: p},
	}
	if err := enc.Encode(wrapper); err != nil {
		return "", err
	}
	// Omit redundant bare `[meshes]` parent header emitted by Encoder when `[meshes.<name>]` follows.
	lines := strings.Split(strings.TrimSpace(sb.String()), "\n")
	var filtered []string
	for _, l := range lines {
		if strings.TrimSpace(l) == "[meshes]" {
			continue
		}
		filtered = append(filtered, strings.TrimPrefix(l, "  "))
	}
	return strings.Join(filtered, "\n") + "\n", nil
}

// UpsertWorkstationMeshProfileForBrand writes or updates `[meshes.<meshName>]` in the brand's workstation config,
// preserving existing comments, unrelated sections, and `[[array_of_tables]]` blocks across the rest of the file.
func UpsertWorkstationMeshProfileForBrand(explicitPath string, b brand.Spec, meshName string, patch MeshProfilePatch) (string, error) {
	bSpec := b.MergeWithDefaults()
	meshName = strings.TrimSpace(meshName)
	if err := core.ValidateMeshName(meshName); err != nil {
		return "", err
	}
	if patch.TopicRoot != nil && strings.TrimSpace(*patch.TopicRoot) != "" {
		if err := core.ValidateTopicToken(strings.Trim(strings.TrimSpace(*patch.TopicRoot), "/")); err != nil {
			return "", fmt.Errorf("invalid brand topic_root: %w", err)
		}
	}

	targetPath, err := resolveWritableWorkstationConfigPathForBrand(explicitPath, bSpec)
	if err != nil {
		return "", err
	}

	var existingRaw []byte
	var existingCfg WorkstationConfig
	if raw, readErr := os.ReadFile(targetPath); readErr == nil {
		existingRaw = raw
		if _, decErr := toml.Decode(string(raw), &existingCfg); decErr != nil {
			return "", fmt.Errorf("decode existing %s: %w", targetPath, decErr)
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return "", fmt.Errorf("read %s: %w", targetPath, readErr)
	} else if strings.TrimSpace(explicitPath) == "" && strings.TrimSpace(os.Getenv(bSpec.EnvPrefix+"_CONFIG")) == "" {
		// When bootstrapping ~/.config/<cli>/<cli>.toml, preserve any active /etc/<cli>/<cli>.toml
		// base settings and profiles instead of silently shadowing them.
		if sysPath := resolveWorkstationConfigFileForBrand("", bSpec); sysPath != "" && sysPath != targetPath {
			sysRaw, sysErr := os.ReadFile(sysPath)
			if sysErr != nil {
				return "", fmt.Errorf("read active base config %s: %w", sysPath, sysErr)
			}
			existingRaw = sysRaw
			if _, decErr := toml.Decode(string(sysRaw), &existingCfg); decErr != nil {
				return "", fmt.Errorf("decode active base config %s: %w", sysPath, decErr)
			}
		}
	}

	mergedProfile := patch.Apply(existingCfg.Meshes[meshName])
	renderedBlock, err := renderMeshProfileTOML(meshName, mergedProfile)
	if err != nil {
		return "", fmt.Errorf("encode [meshes.%s]: %w", meshName, err)
	}

	var updatedText string
	if len(existingRaw) == 0 {
		headerTitle := "Pigeons"
		if brandCLIDirSlug(bSpec) != "pigeons" {
			headerTitle = bSpec.ProductName
		}
		updatedText = fmt.Sprintf("# %s Workstation Control Plane Configuration\n\n%s", headerTitle, renderedBlock)
	} else {
		lines := strings.Split(string(existingRaw), "\n")
		var outLines []string
		inTargetMesh := false
		replaced := false

		for _, line := range lines {
			if hdr, isArray, ok := parseTOMLTableHeaderPath(line); ok {
				if !isArray && isMeshSectionHeader(hdr, meshName) {
					if !replaced {
						outLines = append(outLines, strings.TrimRight(renderedBlock, "\n"))
						replaced = true
					}
					inTargetMesh = true
					continue
				}
				inTargetMesh = false
			}
			if !inTargetMesh {
				outLines = append(outLines, line)
			}
		}

		if !replaced {
			trimmedExisting := strings.TrimRight(string(existingRaw), " \t\r\n")
			if trimmedExisting == "" {
				updatedText = renderedBlock
			} else {
				updatedText = trimmedExisting + "\n\n" + renderedBlock
			}
		} else {
			updatedText = strings.TrimRight(strings.Join(outLines, "\n"), " \t\r\n") + "\n"
		}
	}

	var verify WorkstationConfig
	if _, err := toml.Decode(updatedText, &verify); err != nil {
		return "", fmt.Errorf("verify updated TOML for %s: %w", targetPath, err)
	}

	if err := writeAtomicFile0600(targetPath, []byte(updatedText)); err != nil {
		return "", err
	}
	return targetPath, nil
}

// UpsertWorkstationMeshProfile writes or updates `[meshes.<meshName>]` in `pigeons.toml`,
// preserving existing comments and unrelated sections across the rest of the file.
func UpsertWorkstationMeshProfile(explicitPath, meshName string, patch MeshProfilePatch) (string, error) {
	return UpsertWorkstationMeshProfileForBrand(explicitPath, brand.Active(), meshName, patch)
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

// PrefixedEnv returns the first non-empty trimmed environment variable from
// `<BRAND_ENV_PREFIX>_<suffix>` followed by the fallback keys in order.
func PrefixedEnv(brandPrefix, suffix string, fallbacks ...string) string {
	var keys []string
	if p := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(brandPrefix)), "_"); p != "" {
		keys = append(keys, p+"_"+suffix)
	}
	keys = append(keys, fallbacks...)
	return firstEnv(keys...)
}

// ResolveWorkstationDBURLEnv returns the first non-empty database URL environment override
// for brandPrefix (`<BRAND>_DB_URL`, `PIGEONS_DB_URL`, `PIGEONCOOP_DB_URL`, `DATABASE_URL`).
func ResolveWorkstationDBURLEnv(brandPrefix string) string {
	return PrefixedEnv(brandPrefix, "DB_URL", "PIGEONS_DB_URL", "PIGEONCOOP_DB_URL", "DATABASE_URL")
}

// ResolveWorkstationRODBURLEnv returns the first non-empty read-only database URL environment override
// for brandPrefix (`<BRAND>_RO_DB_URL`, `PIGEONS_RO_DB_URL`, `PIGEONCARE_DB_URL`, `PIGEONMCP_DB_URL`).
func ResolveWorkstationRODBURLEnv(brandPrefix string) string {
	return PrefixedEnv(brandPrefix, "RO_DB_URL", "PIGEONS_RO_DB_URL", "PIGEONCARE_DB_URL", "PIGEONMCP_DB_URL")
}

func isSensitiveQueryKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	switch k {
	case "pass", "pwd", "auth", "apikey":
		return true
	}
	return strings.Contains(k, "password") ||
		strings.Contains(k, "passwd") ||
		strings.Contains(k, "secret") ||
		strings.Contains(k, "token") ||
		strings.Contains(k, "api_key")
}

func redactURLQuery(u *url.URL) {
	if u.RawQuery == "" {
		return
	}
	q := u.Query()
	changed := false
	for k := range q {
		if isSensitiveQueryKey(k) {
			q.Set(k, "xxxxx")
			changed = true
		}
	}
	if changed {
		u.RawQuery = q.Encode()
	}
}

// RedactURL masks any userinfo password and sensitive query parameters in raw before CLI or log printing.
// Non-URL / schemeless inputs are masked or redacted fail-closed so credentials are never echoed verbatim.
func RedactURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err == nil && u.Scheme != "" && u.Opaque == "" {
			redactURLQuery(u)
			return u.Redacted()
		}
		// Fallback for malformed hierarchical URLs (e.g. unescaped '%' in password) without leaking query secrets.
		if schemeIdx := strings.Index(trimmed, "://"); schemeIdx >= 0 {
			rest := trimmed[schemeIdx+3:]
			pathIdx := strings.IndexAny(rest, "/?#")
			authority := rest
			pathOnly := ""
			if pathIdx >= 0 {
				authority = rest[:pathIdx]
				if rest[pathIdx] == '/' {
					endPath := strings.IndexAny(rest[pathIdx:], "?#")
					if endPath >= 0 {
						pathOnly = rest[pathIdx : pathIdx+endPath]
					} else {
						pathOnly = rest[pathIdx:]
					}
				}
			}
			if atIdx := strings.LastIndexByte(authority, '@'); atIdx >= 0 {
				userInfo := authority[:atIdx]
				host := authority[atIdx+1:]
				if colonIdx := strings.IndexByte(userInfo, ':'); colonIdx >= 0 {
					userInfo = userInfo[:colonIdx] + ":xxxxx"
				}
				return trimmed[:schemeIdx+3] + userInfo + "@" + host + pathOnly
			}
		}
		return "[redacted-invalid-url]"
	}
	// Schemeless user:pass@host[:port] without whitespace or key=value pairs.
	if !strings.ContainsAny(trimmed, " \t\r\n=?#") {
		if atIdx := strings.LastIndexByte(trimmed, '@'); atIdx > 0 {
			userInfo := trimmed[:atIdx]
			hostPart := trimmed[atIdx+1:]
			if colonIdx := strings.IndexByte(userInfo, ':'); colonIdx >= 0 {
				return userInfo[:colonIdx] + ":xxxxx@" + hostPart
			}
		}
	}
	return "[redacted-invalid-url]"
}

func prefixedEnv(brandPrefix, suffix string, fallbacks ...string) string {
	return PrefixedEnv(brandPrefix, suffix, fallbacks...)
}

func pgTargetEndpoints(pgCfg *pgxpool.Config) []string {
	if pgCfg == nil || pgCfg.ConnConfig == nil {
		return nil
	}
	seen := make(map[string]struct{}, 1+len(pgCfg.ConnConfig.Fallbacks))
	var out []string
	add := func(host string, port uint16) {
		h := strings.ToLower(strings.Trim(strings.TrimSpace(host), "[]"))
		if h == "" {
			return
		}
		if port == 0 {
			port = 5432
		}
		key := fmt.Sprintf("%s:%d", h, port)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			out = append(out, key)
		}
	}
	add(pgCfg.ConnConfig.Host, pgCfg.ConnConfig.Port)
	for _, fb := range pgCfg.ConnConfig.Fallbacks {
		if fb != nil {
			add(fb.Host, fb.Port)
		}
	}
	return out
}

func urlTargetsEqual(a, b string) bool {
	ta := strings.TrimSpace(a)
	tb := strings.TrimSpace(b)
	if ta == "" || tb == "" {
		return false
	}
	pgA, errA := pgxpool.ParseConfig(ta)
	pgB, errB := pgxpool.ParseConfig(tb)
	if errA != nil || errB != nil || pgA.ConnConfig == nil || pgB.ConnConfig == nil {
		return false
	}
	if pgA.ConnConfig.Database != pgB.ConnConfig.Database {
		return false
	}
	return slices.Equal(pgTargetEndpoints(pgA), pgTargetEndpoints(pgB))
}

func parseLibpqKeywordValue(dsn string) map[string]string {
	out := make(map[string]string)
	n := len(dsn)
	i := 0
	isSpace := func(b byte) bool {
		return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v'
	}
	for i < n {
		for i < n && isSpace(dsn[i]) {
			i++
		}
		if i >= n {
			break
		}
		keyStart := i
		for i < n && !isSpace(dsn[i]) && dsn[i] != '=' {
			i++
		}
		key := strings.ToLower(dsn[keyStart:i])
		for i < n && isSpace(dsn[i]) {
			i++
		}
		if i >= n || dsn[i] != '=' {
			break
		}
		i++
		for i < n && isSpace(dsn[i]) {
			i++
		}
		var val strings.Builder
		if i < n && dsn[i] == '\'' {
			i++
			for i < n {
				if dsn[i] == '\\' && i+1 < n {
					i++
					val.WriteByte(dsn[i])
					i++
					continue
				}
				if dsn[i] == '\'' {
					i++
					break
				}
				val.WriteByte(dsn[i])
				i++
			}
		} else {
			for i < n && !isSpace(dsn[i]) {
				if dsn[i] == '\\' && i+1 < n {
					i++
					val.WriteByte(dsn[i])
					i++
					continue
				}
				val.WriteByte(dsn[i])
				i++
			}
		}
		if key != "" {
			out[key] = val.String()
		}
	}
	return out
}

func encodeURIQuery(q url.Values) string {
	// pgconn URI parser percent-unescapes query values without treating '+' as a space.
	return strings.ReplaceAll(q.Encode(), "+", "%20")
}

func stripCredentialQueryKeys(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	for k := range q {
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "user", "password", "passfile":
			q.Del(k)
		}
	}
	return encodeURIQuery(q)
}

// deriveReadOnlyURL derives a least-privilege `pigeoncare_ro` DSN from baseDSN.
// For non-loopback hosts without an explicit PIGEONCARE_RO_PASSWORD, it returns ""
// rather than silently injecting the default development password.
func deriveReadOnlyURL(baseDSN string) string {
	trimmed := strings.TrimSpace(baseDSN)
	if trimmed == "" {
		return ""
	}
	roPW := strings.TrimSpace(os.Getenv("PIGEONCARE_RO_PASSWORD"))
	if !isURLHostLoopback(trimmed) && roPW == "" {
		return ""
	}
	if u, err := url.Parse(trimmed); err == nil && u.Scheme != "" {
		derived := db.DeriveReadOnlyURLFromAdmin(trimmed, "")
		du, dErr := url.Parse(derived)
		if dErr != nil {
			return derived
		}
		du.RawQuery = stripCredentialQueryKeys(du.RawQuery)
		return du.String()
	}
	kv := parseLibpqKeywordValue(trimmed)
	host := ""
	var port uint16 = 5432
	dbname := ""
	if pgCfg, err := pgxpool.ParseConfig(trimmed); err == nil && pgCfg.ConnConfig != nil {
		host = strings.TrimSpace(pgCfg.ConnConfig.Host)
		if pgCfg.ConnConfig.Port != 0 {
			port = pgCfg.ConnConfig.Port
		}
		dbname = pgCfg.ConnConfig.Database
	} else {
		host = strings.TrimSpace(kv["host"])
		if p, pErr := strconv.ParseUint(strings.TrimSpace(kv["port"]), 10, 16); pErr == nil && p > 0 {
			port = uint16(p)
		}
		dbname = strings.TrimSpace(kv["dbname"])
	}
	if host == "" || strings.HasPrefix(host, "/") {
		return ""
	}
	if roPW == "" {
		roPW = "pigeoncare_readonly"
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword("pigeoncare_ro", roPW),
		Host:   net.JoinHostPort(host, strconv.Itoa(int(port))),
	}
	if dbname != "" {
		u.Path = "/" + dbname
	}
	q := url.Values{}
	for _, key := range []string{"sslmode", "sslrootcert", "sslcert", "sslkey", "sslsni", "sslnegotiation", "channel_binding"} {
		if val := strings.TrimSpace(kv[key]); val != "" {
			q.Set(key, val)
		}
	}
	if len(q) > 0 {
		u.RawQuery = encodeURIQuery(q)
	}
	return u.String()
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

	if v := prefixedEnv(bp, "MESH_NAME", "PIGEONCOOP_MESH_NAME", "PIGEONS_MESH_NAME"); v != "" {
		cfg.MeshName = v
	}
	cfg.MeshName = strings.TrimSpace(cfg.MeshName)
	if cfg.MeshName == "" {
		cfg.MeshName = core.DefaultMeshName
	}

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
	if v := prefixedEnv(bp, "MQTT_TLS_CLIENT_CA", "PIGEONCOOP_MQTT_TLS_CLIENT_CA", "PIGEONS_MQTT_TLS_CLIENT_CA"); v != "" {
		cfg.MQTT.TLSClientCAFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CLIENT_CERT", "PIGEONCOOP_MQTT_TLS_CLIENT_CERT"); v != "" {
		cfg.MQTT.TLSClientCertFile = v
	}
	if v := prefixedEnv(bp, "MQTT_TLS_CLIENT_KEY", "PIGEONCOOP_MQTT_TLS_CLIENT_KEY"); v != "" {
		cfg.MQTT.TLSClientKeyFile = v
	}
	if v := prefixedEnv(bp, "MQTT_REQUIRE_CLIENT_CERT", "PIGEONCOOP_MQTT_REQUIRE_CLIENT_CERT", "PIGEONCOOP_REQUIRE_CLIENT_CERT"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.MQTT.RequireClientCert = b
		}
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

	if err := core.ValidateMeshName(cfg.MeshName); err != nil {
		return cfg, fmt.Errorf("invalid coop mesh_name: %w", err)
	}
	if err := core.ValidateTopicToken(cfg.NodeID); err != nil {
		return cfg, fmt.Errorf("invalid coop node_id: %w", err)
	}
	if err := core.ValidateTopicToken(cfg.Brand.TopicRoot); err != nil {
		return cfg, fmt.Errorf("invalid brand topic_root: %w", err)
	}
	if err := db.ValidateSchemaIdentifier(cfg.Brand.MathSchema); err != nil {
		return cfg, fmt.Errorf("invalid brand math_schema: %w", err)
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
	if err := core.ValidateTopicToken(cfg.Brand.TopicRoot); err != nil {
		return cfg, fmt.Errorf("invalid brand topic_root: %w", err)
	}
	if err := db.ValidateSchemaIdentifier(cfg.Brand.MathSchema); err != nil {
		return cfg, fmt.Errorf("invalid brand math_schema: %w", err)
	}

	cfg.Tags = core.NormalizeTags(cfg.Tags)
	cfg.TopicPrefix = strings.TrimSpace(cfg.TopicPrefix)
	if cfg.TopicPrefix == "" {
		cfg.TopicPrefix = cfg.Brand.CTDPrefix()
	}
	for _, seg := range strings.Split(cfg.TopicPrefix, "/") {
		if err := core.ValidateTopicToken(seg); err != nil {
			return cfg, fmt.Errorf("invalid edge topic_prefix %q: %w", cfg.TopicPrefix, err)
		}
	}
	return cfg, nil
}

func applyWorkstationEnvAndReadOnlyDerivation(cfg WorkstationConfig) (WorkstationConfig, error) {
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
	dbFromEnv := false
	if v := ResolveWorkstationDBURLEnv(bp); v != "" {
		cfg.DatabaseURL = v
		dbFromEnv = true
	}
	roFromEnv := false
	if v := ResolveWorkstationRODBURLEnv(bp); v != "" {
		cfg.ReadOnlyDatabaseURL = v
		roFromEnv = true
	}
	if !roFromEnv && strings.TrimSpace(cfg.DatabaseURL) != "" {
		ro := strings.TrimSpace(cfg.ReadOnlyDatabaseURL)
		if ro == "" || (isURLHostLoopback(ro) && !isURLHostLoopback(cfg.DatabaseURL)) || (dbFromEnv && !urlTargetsEqual(ro, cfg.DatabaseURL)) {
			cfg.ReadOnlyDatabaseURL = deriveReadOnlyURL(cfg.DatabaseURL)
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

	if err := core.ValidateTopicToken(cfg.Brand.TopicRoot); err != nil {
		return cfg, fmt.Errorf("invalid brand topic_root: %w", err)
	}
	if err := db.ValidateSchemaIdentifier(cfg.Brand.MathSchema); err != nil {
		return cfg, fmt.Errorf("invalid brand math_schema: %w", err)
	}
	return cfg, nil
}

// LoadBaseWorkstationConfigForBrand decodes the brand's workstation config (`<cli>.toml`) and validates all `[meshes.*]` identifiers
// without resolving or enforcing `~/.config/<cli>/current_mesh` state.
func LoadBaseWorkstationConfigForBrand(explicitPath string, b brand.Spec) (WorkstationConfig, error) {
	bSpec := b.MergeWithDefaults()
	cfg := DefaultWorkstationConfig()
	cfg.Brand = bSpec
	path := resolveWorkstationConfigFileForBrand(explicitPath, bSpec)
	if path != "" {
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return cfg, fmt.Errorf("decode %s: %w", path, err)
		}
		cfg.ConfigPath = path
	} else if explicitPath != "" {
		return cfg, fmt.Errorf("config file not found: %s", explicitPath)
	}

	cfg.Brand = mergeBrandOverlay(bSpec, cfg.Brand)
	if err := db.ValidateSchemaIdentifier(cfg.Brand.MathSchema); err != nil {
		return cfg, fmt.Errorf("invalid brand math_schema: %w", err)
	}
	cfg.MeshName = strings.TrimSpace(cfg.MeshName)
	if cfg.MeshName != "" {
		if err := core.ValidateMeshName(cfg.MeshName); err != nil {
			return cfg, fmt.Errorf("invalid workstation mesh_name: %w", err)
		}
	}
	for meshKey, mp := range cfg.Meshes {
		if err := core.ValidateMeshName(meshKey); err != nil {
			return cfg, fmt.Errorf("invalid mesh key %q in [meshes]: %w", meshKey, err)
		}
		if root := strings.Trim(strings.TrimSpace(mp.Brand.TopicRoot), "/"); root != "" {
			if err := core.ValidateTopicToken(root); err != nil {
				return cfg, fmt.Errorf("invalid brand topic_root in [meshes.%s]: %w", meshKey, err)
			}
		}
		if strings.TrimSpace(mp.Brand.MathSchema) != "" {
			if err := db.ValidateSchemaIdentifier(mp.Brand.MathSchema); err != nil {
				return cfg, fmt.Errorf("invalid brand math_schema in [meshes.%s]: %w", meshKey, err)
			}
		}
	}
	return cfg, nil
}

// LoadBaseWorkstationConfig decodes `pigeons.toml` and validates all `[meshes.*]` identifiers
// without resolving or enforcing `~/.config/pigeons/current_mesh` state.
func LoadBaseWorkstationConfig(explicitPath string) (WorkstationConfig, error) {
	return LoadBaseWorkstationConfigForBrand(explicitPath, brand.Active())
}

// FinalizeWorkstationConfig applies environment variable overrides and Read-Only DSN derivation to a base or profile-merged config.
func FinalizeWorkstationConfig(cfg WorkstationConfig) (WorkstationConfig, error) {
	return applyWorkstationEnvAndReadOnlyDerivation(cfg)
}

// LoadWorkstationConfig loads `pigeons.toml` (if present), resolves the active mesh (`PIGEONS_MESH` or `current_mesh`),
// overlays `[meshes.<active>]`, and applies environment variable overrides.
func LoadWorkstationConfig(explicitPath string) (WorkstationConfig, error) {
	return LoadWorkstationConfigForMeshAndBrand(explicitPath, "", brand.Active())
}

// LoadWorkstationConfigForMesh loads `pigeons.toml`, selects `explicitMesh` (or `PIGEONS_MESH` / `current_mesh` state),
// overlays `[meshes.<mesh>]`, and applies environment variable overrides.
func LoadWorkstationConfigForMesh(explicitPath, explicitMesh string) (WorkstationConfig, error) {
	return LoadWorkstationConfigForMeshAndBrand(explicitPath, explicitMesh, brand.Active())
}

// LoadWorkstationConfigForMeshAndBrand loads the brand's workstation config, selects `meshOverride`
// (or `<BRAND>_MESH` / brand `current_mesh` state), overlays `[meshes.<mesh>]`, and applies environment overrides.
func LoadWorkstationConfigForMeshAndBrand(explicitPath, meshOverride string, b brand.Spec) (WorkstationConfig, error) {
	bSpec := b.MergeWithDefaults()
	cfg, err := LoadBaseWorkstationConfigForBrand(explicitPath, bSpec)
	if err != nil {
		return cfg, err
	}

	bp := cfg.Brand.EnvPrefix
	targetMesh := strings.TrimSpace(meshOverride)
	source := ""
	if targetMesh != "" {
		source = "flag"
	} else {
		var envMesh string
		if bSpec.EnvPrefix != brand.Default().EnvPrefix {
			if bp != bSpec.EnvPrefix {
				envMesh = prefixedEnv(bp, "MESH", bSpec.EnvPrefix+"_MESH")
			} else {
				envMesh = prefixedEnv(bp, "MESH")
			}
		} else {
			envMesh = prefixedEnv(bp, "MESH", "PIGEONS_MESH")
		}
		if envMesh != "" {
			targetMesh = envMesh
			source = "env"
		} else {
			stateMesh, stateErr := ReadCurrentMeshForBrand(bSpec)
			if stateErr != nil {
				return cfg, stateErr
			}
			if stateMesh != "" {
				targetMesh = stateMesh
				source = "state"
			}
		}
	}

	if targetMesh != "" {
		if err := core.ValidateMeshName(targetMesh); err != nil {
			return cfg, err
		}
		if profile, ok := cfg.Meshes[targetMesh]; ok {
			cfg = ApplyMeshProfile(cfg, targetMesh, profile)
			cfg.ActiveMeshSource = source
		} else if cfg.MeshName == targetMesh {
			cfg.ActiveMesh = targetMesh
			cfg.ActiveMeshSource = source
		} else {
			available := cfg.ConfiguredMeshNames()
			return cfg, fmt.Errorf(
				"mesh %q (from %s) is not configured in %s (available meshes: %v; run `%s connect %s --broker <tcp://host:1883> --db <postgres://...>` to register it)",
				targetMesh, source, brandWorkstationConfigFilename(bSpec), available, cfg.Brand.CLIName, targetMesh,
			)
		}
	} else if cfg.MeshName != "" {
		cfg.ActiveMesh = cfg.MeshName
		cfg.ActiveMeshSource = "default"
	}

	return applyWorkstationEnvAndReadOnlyDerivation(cfg)
}
