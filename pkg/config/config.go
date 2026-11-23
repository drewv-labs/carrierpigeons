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
	meshName := strings.TrimSpace(c.MeshName)
	if meshName == "" {
		meshName = core.DefaultMeshName
	}
	if err := core.ValidateMeshName(meshName); err != nil {
		return fmt.Errorf("invalid coop mesh_name: %w", err)
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
		out.ReadOnlyDatabaseURL = db.DeriveReadOnlyURLFromAdmin(out.DatabaseURL, "")
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

func activeCLIDirSlug() string {
	bSpec := brand.Active().MergeWithDefaults()
	dirSlug := strings.ToLower(strings.TrimSpace(bSpec.CLIName))
	if dirSlug == "" {
		return "pigeons"
	}
	return dirSlug
}

// CurrentMeshStatePath resolves `~/.config/<cli_slug>/current_mesh` (or `PIGEONS_CURRENT_MESH_FILE` override).
func CurrentMeshStatePath() (string, error) {
	if p := strings.TrimSpace(os.Getenv("PIGEONS_CURRENT_MESH_FILE")); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("resolve user home directory for current_mesh state: %w", err)
	}
	return filepath.Join(home, ".config", activeCLIDirSlug(), "current_mesh"), nil
}

func previousMeshStatePath() (string, error) {
	curPath, err := CurrentMeshStatePath()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(curPath), "previous_mesh"), nil
}

// ReadCurrentMesh reads the persisted "current working mesh" name from `~/.config/pigeons/current_mesh`.
// Returns ("", nil) when no active mesh state file exists.
func ReadCurrentMesh() (string, error) {
	path, err := CurrentMeshStatePath()
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

// ReadPreviousMesh reads the prior working mesh name saved during the last `pigeons connect` switch (for `pigeons connect -`).
func ReadPreviousMesh() (string, error) {
	path, err := previousMeshStatePath()
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
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		return fmt.Errorf("commit %s: %w", targetPath, err)
	}
	cleanup = false
	return nil
}

// WriteCurrentMesh atomically persists the active "current working mesh" in `~/.config/pigeons/current_mesh`
// and records any distinct prior mesh in `~/.config/pigeons/previous_mesh` so `pigeons connect -` toggles cleanly.
func WriteCurrentMesh(meshName string) (string, error) {
	meshName = strings.TrimSpace(meshName)
	if err := core.ValidateMeshName(meshName); err != nil {
		return "", err
	}
	statePath, err := CurrentMeshStatePath()
	if err != nil {
		return "", err
	}
	if prev, prevErr := ReadCurrentMesh(); prevErr == nil && prev != "" && prev != meshName {
		if prevPath, pErr := previousMeshStatePath(); pErr == nil {
			_ = writeAtomicFile0600(prevPath, []byte(prev+"\n"))
		}
	}
	if err := writeAtomicFile0600(statePath, []byte(meshName+"\n")); err != nil {
		return "", err
	}
	return statePath, nil
}

// ClearCurrentMesh removes `~/.config/pigeons/current_mesh`, reverting workstation commands to top-level defaults.
func ClearCurrentMesh() error {
	statePath, err := CurrentMeshStatePath()
	if err != nil {
		return err
	}
	if prev, prevErr := ReadCurrentMesh(); prevErr == nil && prev != "" {
		if prevPath, pErr := previousMeshStatePath(); pErr == nil {
			_ = writeAtomicFile0600(prevPath, []byte(prev+"\n"))
		}
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", statePath, err)
	}
	return nil
}

func resolveConfigFile(explicitPath, envKey, filename string) string {
	if p := strings.TrimSpace(explicitPath); p != "" {
		return p
	}
	if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
		return p
	}
	dirSlug := activeCLIDirSlug()

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

func resolveWritableWorkstationConfigPath(explicitPath string) (string, error) {
	if p := strings.TrimSpace(explicitPath); p != "" {
		return p, nil
	}
	if p := strings.TrimSpace(os.Getenv("PIGEONS_CONFIG")); p != "" {
		return p, nil
	}
	dirSlug := activeCLIDirSlug()
	candidates := []string{"pigeons.toml"}
	var defaultUserPath string
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		defaultUserPath = filepath.Join(home, ".config", dirSlug, "pigeons.toml")
		candidates = append(candidates, defaultUserPath)
		if dirSlug != "pigeons" {
			candidates = append(candidates, filepath.Join(home, ".config", "pigeons", "pigeons.toml"))
		}
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && !info.IsDir() {
			return c, nil
		}
	}
	if defaultUserPath != "" {
		return defaultUserPath, nil
	}
	return "pigeons.toml", nil
}

func parseTOMLTableHeaderPath(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "[[") {
		return "", false
	}
	// Strip trailing comment outside quotes.
	inSingle, inDouble, escaped := false, false, false
	closeIdx := -1
	for i := 1; i < len(trimmed); i++ {
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
			closeIdx = i
			break
		}
	}
	if closeIdx < 0 {
		return "", false
	}
	rest := strings.TrimSpace(trimmed[closeIdx+1:])
	if rest != "" && !strings.HasPrefix(rest, "#") {
		return "", false
	}
	inner := strings.TrimSpace(trimmed[1:closeIdx])
	if inner == "" {
		return "", false
	}
	rawParts := strings.Split(inner, ".")
	normParts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		p := strings.TrimSpace(part)
		p = strings.Trim(p, `"'`)
		normParts = append(normParts, p)
	}
	return strings.Join(normParts, "."), true
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

// UpsertWorkstationMeshProfile writes or updates `[meshes.<meshName>]` in `pigeons.toml`,
// preserving existing comments and unrelated sections across the rest of the file.
func UpsertWorkstationMeshProfile(explicitPath, meshName string, patch MeshProfilePatch) (string, error) {
	meshName = strings.TrimSpace(meshName)
	if err := core.ValidateMeshName(meshName); err != nil {
		return "", err
	}
	if patch.TopicRoot != nil && strings.TrimSpace(*patch.TopicRoot) != "" {
		if err := core.ValidateTopicToken(strings.Trim(strings.TrimSpace(*patch.TopicRoot), "/")); err != nil {
			return "", fmt.Errorf("invalid brand topic_root: %w", err)
		}
	}

	targetPath, err := resolveWritableWorkstationConfigPath(explicitPath)
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
	}

	mergedProfile := patch.Apply(existingCfg.Meshes[meshName])
	renderedBlock, err := renderMeshProfileTOML(meshName, mergedProfile)
	if err != nil {
		return "", fmt.Errorf("encode [meshes.%s]: %w", meshName, err)
	}

	var updatedText string
	if len(existingRaw) == 0 {
		updatedText = "# Pigeons Workstation Control Plane Configuration\n\n" + renderedBlock
	} else {
		lines := strings.Split(string(existingRaw), "\n")
		var outLines []string
		inTargetMesh := false
		replaced := false

		for _, line := range lines {
			if hdr, ok := parseTOMLTableHeaderPath(line); ok {
				if isMeshSectionHeader(hdr, meshName) {
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

	if err := core.ValidateTopicToken(cfg.Brand.TopicRoot); err != nil {
		return cfg, fmt.Errorf("invalid brand topic_root: %w", err)
	}
	return cfg, nil
}

// LoadBaseWorkstationConfig decodes `pigeons.toml` and validates all `[meshes.*]` identifiers
// without resolving or enforcing `~/.config/pigeons/current_mesh` state.
func LoadBaseWorkstationConfig(explicitPath string) (WorkstationConfig, error) {
	cfg := DefaultWorkstationConfig()
	path := resolveConfigFile(explicitPath, "PIGEONS_CONFIG", "pigeons.toml")
	if path != "" {
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return cfg, fmt.Errorf("decode %s: %w", path, err)
		}
		cfg.ConfigPath = path
	} else if explicitPath != "" {
		return cfg, fmt.Errorf("config file not found: %s", explicitPath)
	}

	cfg.Brand = cfg.Brand.MergeWithDefaults()
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
	}
	return cfg, nil
}

// FinalizeWorkstationConfig applies environment variable overrides and Read-Only DSN derivation to a base or profile-merged config.
func FinalizeWorkstationConfig(cfg WorkstationConfig) (WorkstationConfig, error) {
	return applyWorkstationEnvAndReadOnlyDerivation(cfg)
}

// LoadWorkstationConfig loads `pigeons.toml` (if present), resolves the active mesh (`PIGEONS_MESH` or `current_mesh`),
// overlays `[meshes.<active>]`, and applies environment variable overrides.
func LoadWorkstationConfig(explicitPath string) (WorkstationConfig, error) {
	return LoadWorkstationConfigForMesh(explicitPath, "")
}

// LoadWorkstationConfigForMesh loads `pigeons.toml`, selects `explicitMesh` (or `PIGEONS_MESH` / `current_mesh` state),
// overlays `[meshes.<mesh>]`, and applies environment variable overrides.
func LoadWorkstationConfigForMesh(explicitPath, explicitMesh string) (WorkstationConfig, error) {
	cfg, err := LoadBaseWorkstationConfig(explicitPath)
	if err != nil {
		return cfg, err
	}

	bp := cfg.Brand.EnvPrefix
	targetMesh := strings.TrimSpace(explicitMesh)
	source := ""
	if targetMesh != "" {
		source = "flag"
	} else if envMesh := prefixedEnv(bp, "MESH", "PIGEONS_MESH"); envMesh != "" {
		targetMesh = envMesh
		source = "env"
	} else {
		stateMesh, stateErr := ReadCurrentMesh()
		if stateErr != nil {
			return cfg, stateErr
		}
		if stateMesh != "" {
			targetMesh = stateMesh
			source = "state"
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
				"mesh %q (from %s) is not configured in pigeons.toml (available meshes: %v; run `%s connect %s --broker <tcp://host:1883> --db <postgres://...>` to register it)",
				targetMesh, source, available, cfg.Brand.CLIName, targetMesh,
			)
		}
	} else if cfg.MeshName != "" {
		cfg.ActiveMesh = cfg.MeshName
		cfg.ActiveMeshSource = "default"
	}

	return applyWorkstationEnvAndReadOnlyDerivation(cfg)
}
