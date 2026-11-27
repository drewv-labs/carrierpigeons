package coop

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/edge"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

// Options allows downstream Go projects to customize or extend PigeonCoop when embedding it as a library.
type Options struct {
	Store            db.BaseDB
	Brand            brand.Spec
	ProvisionRoles   bool
	IngestPassword   string
	ReadOnlyPassword string
	ExtraObservers   []observer.Observer
	ExtraRunners     []runner.Runner
}

// StatusSnapshot reports live PigeonCoop operational state and ingestion/drop counters.
type StatusSnapshot struct {
	MeshName                  string `json:"mesh_name"`
	NodeID                    string `json:"node_id"`
	BrokerURL                 string `json:"broker_url,omitempty"`
	Started                   bool   `json:"started"`
	IngestedCount             uint64 `json:"ingested_count"`
	BatcherDroppedCount       uint64 `json:"batcher_dropped_count"`
	TelemetryDroppedCount     uint64 `json:"telemetry_dropped_count"`
	ControlDroppedCount       uint64 `json:"control_dropped_count"`
	RunnerRequestDroppedCount uint64 `json:"runner_request_dropped_count"`
}

type coopDropCounters struct {
	ingested         uint64
	batcherDropped   uint64
	telemetryDropped uint64
	controlDropped   uint64
	runnerReqDropped uint64
}

// Server orchestrates the full PigeonCoop hub stack:
// Embedded pure-Go MQTT Broker (optional external) + PostgreSQL Ledger + Batched COPY Injector +
// RunnerRequestCTD Router + Ntfy Watchdog + implicit self-observation & Runner runtime.
type Server struct {
	cfg  config.CoopConfig
	opts Options

	mu             sync.Mutex
	starting       bool
	started        bool
	stopped        bool
	startDone      chan struct{}
	bootCancel     context.CancelFunc
	runCtx         context.Context
	runCancel      context.CancelFunc
	store          db.BaseDB
	ownsStore      bool
	embedded       *broker.EmbeddedBroker
	loopbackBridge *loopbackTLSBridge
	subscriber     *broker.Subscriber
	selfNode       *edge.Agent
	brokerURL      string
	lastCounters   coopDropCounters

	stopOnce sync.Once
}

type coopTelemetryObserver struct {
	srv      *Server
	interval time.Duration
}

func (o *coopTelemetryObserver) Name() string { return "coop-ingest" }

func (o *coopTelemetryObserver) emit(nodeID string, pub core.TelemetryPublisher) {
	if o.srv == nil || pub == nil {
		return
	}
	st := o.srv.Status()
	health := "healthy"
	if st.BatcherDroppedCount > 0 || st.TelemetryDroppedCount > 0 || st.ControlDroppedCount > 0 || st.RunnerRequestDroppedCount > 0 {
		health = "degraded"
	}
	ev := core.NewCTDPayload(nodeID, o.Name(), health).
		AddMetric("ingested_count", st.IngestedCount).
		AddMetric("batcher_dropped_count", st.BatcherDroppedCount).
		AddMetric("telemetry_dropped_count", st.TelemetryDroppedCount).
		AddMetric("control_dropped_count", st.ControlDroppedCount).
		AddMetric("runner_request_dropped_count", st.RunnerRequestDroppedCount)
	_ = pub.Publish(ev)
}

func (o *coopTelemetryObserver) Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	if ctx == nil || pub == nil || ctx.Err() != nil {
		return
	}
	interval := o.interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	o.emit(nodeID, pub)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			o.emit(nodeID, pub)
		}
	}
}

// New constructs a configured PigeonCoop Server ready to Start(ctx).
func New(cfg config.CoopConfig, opts ...Options) *Server {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Brand != (brand.Spec{}) {
		cfg.Brand = opt.Brand
	}
	cfg.Brand = cfg.Brand.MergeWithDefaults()
	cfg.MeshName = strings.TrimSpace(cfg.MeshName)
	if cfg.MeshName == "" {
		cfg.MeshName = core.DefaultMeshName
	}
	cfg.Tags = core.NormalizeTags(cfg.Tags)
	return &Server{
		cfg:  cfg,
		opts: opt,
	}
}

func (s *Server) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// Start boots the PostgreSQL schema, MNS registration, MQTT broker, batched CTD ingestor, and implicit self-observation/runners.
// Caller ctx bounds startup; long-lived children run under Server's internal runCtx until Stop().
func (s *Server) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.cfg.ValidateSecurity(); err != nil {
		return err
	}
	mode := strings.ToLower(strings.TrimSpace(s.cfg.MQTT.Mode))
	if mode != "" && mode != "embedded" && mode != "external" {
		return fmt.Errorf("invalid mqtt.mode %q: must be 'embedded' or 'external'", s.cfg.MQTT.Mode)
	}
	isExternal := mode == "external"

	clientCertFile := strings.TrimSpace(s.cfg.MQTT.TLSClientCertFile)
	clientKeyFile := strings.TrimSpace(s.cfg.MQTT.TLSClientKeyFile)
	if isExternal && clientCertFile == "" && clientKeyFile == "" {
		clientCertFile = strings.TrimSpace(s.cfg.MQTT.TLSCertFile)
		clientKeyFile = strings.TrimSpace(s.cfg.MQTT.TLSKeyFile)
	}
	if !isExternal && s.cfg.MQTT.RequireClientCert {
		if clientCertFile == "" || clientKeyFile == "" {
			return fmt.Errorf("configure PigeonCoop embedded mTLS: mqtt.require_client_cert=true requires mqtt.tls_client_cert_file and mqtt.tls_client_key_file")
		}
		if strings.TrimSpace(s.cfg.MQTT.Username) == "" || s.cfg.MQTT.Password == "" {
			return fmt.Errorf("configure PigeonCoop embedded mTLS: mqtt.require_client_cert=true requires mqtt.username and mqtt.password for internal subscriber")
		}
	}

	clientTLS, err := config.BuildMQTTTLSConfig(
		s.cfg.MQTT.TLSCAFile,
		clientCertFile,
		clientKeyFile,
		isExternal && s.cfg.MQTT.TLSInsecureSkipVerify,
	)
	if err != nil {
		return fmt.Errorf("configure PigeonCoop MQTT TLS client: %w", err)
	}

	// In embedded TLS mode, check whether the embedded leaf cert is a self-signed root anchor
	// when no explicit CA file is provided; CA-signed leaf certs (Issuer != Subject) cannot act
	// as Roots in x509.CertPool and instead use leaf pinning + verified loopback bridge.
	selfCAFile := strings.TrimSpace(s.cfg.MQTT.TLSCAFile)
	var embeddedLeaf *x509.Certificate
	var embeddedSelfSignedAnchor bool
	if !isExternal && strings.TrimSpace(s.cfg.MQTT.TLSCertFile) != "" && strings.TrimSpace(s.cfg.MQTT.TLSKeyFile) != "" {
		pemBytes, leaf, parseErr := parseEmbeddedLeafCert(s.cfg.MQTT.TLSCertFile)
		if parseErr != nil {
			return fmt.Errorf("read embedded MQTT TLS cert %s for loopback trust: %w", s.cfg.MQTT.TLSCertFile, parseErr)
		}
		embeddedLeaf = leaf
		if selfCAFile == "" {
			if clientTLS == nil {
				clientTLS = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			isSelfSigned := bytes.Equal(leaf.RawIssuer, leaf.RawSubject) && leaf.CheckSignatureFrom(leaf) == nil
			if isSelfSigned && (!leaf.BasicConstraintsValid || leaf.IsCA) {
				pool := clientTLS.RootCAs
				if pool == nil {
					if sysPool, poolErr := x509.SystemCertPool(); poolErr == nil && sysPool != nil {
						pool = sysPool
					} else {
						pool = x509.NewCertPool()
					}
				}
				if !pool.AppendCertsFromPEM(pemBytes) {
					return fmt.Errorf("no valid PEM certificates in %s", s.cfg.MQTT.TLSCertFile)
				}
				clientTLS.RootCAs = pool
				selfCAFile = s.cfg.MQTT.TLSCertFile
				embeddedSelfSignedAnchor = true
			}
		}
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("coop server already stopped")
	}
	if s.started || s.starting {
		s.mu.Unlock()
		return errors.New("coop server already started")
	}
	bootCtx, bootCancel := context.WithCancel(ctx)
	runCtx, runCancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})
	s.starting = true
	s.startDone = startDone
	s.bootCancel = bootCancel
	s.runCtx = runCtx
	s.runCancel = runCancel
	s.lastCounters = coopDropCounters{}
	s.mu.Unlock()

	bootOK := false
	defer func() {
		bootCancel()
		if !bootOK {
			s.cleanupPartial()
			s.mu.Lock()
			s.starting = false
			s.bootCancel = nil
			close(startDone)
			s.mu.Unlock()
		}
	}()

	checkBoot := func() error {
		if err := bootCtx.Err(); err != nil {
			return err
		}
		if s.isStopped() {
			return errors.New("coop server stopped during startup")
		}
		return nil
	}

	// 1. Initialize or attach PostgreSQL Ledger
	if err := checkBoot(); err != nil {
		return err
	}
	var activeStore db.BaseDB
	if s.opts.Store != nil {
		activeStore = s.opts.Store
		s.mu.Lock()
		s.store = activeStore
		s.ownsStore = false
		s.mu.Unlock()
	} else {
		pgStore, err := db.NewPostgresStoreWithOptions(bootCtx, s.cfg.Database.URL, db.StoreOptions{
			AppActor:   strings.ToLower(s.cfg.Brand.CoopName) + "-ingestor",
			MaxConns:   s.cfg.Database.MaxConns,
			MinConns:   s.cfg.Database.MinConns,
			MathSchema: s.cfg.Brand.MathSchema,
		})
		if err != nil {
			return fmt.Errorf("connect PostgreSQL ledger: %w", err)
		}
		activeStore = pgStore
		s.mu.Lock()
		s.store = activeStore
		s.ownsStore = true
		s.mu.Unlock()
	}

	if err := activeStore.EnsureSchema(bootCtx); err != nil {
		return fmt.Errorf("ensure CTD & %s schema: %w", s.cfg.Brand.MathName, err)
	}
	if err := checkBoot(); err != nil {
		return err
	}
	if err := activeStore.UpsertMeshIdentity(bootCtx, core.MeshIdentity{
		MeshName:   s.cfg.MeshName,
		CoopNodeID: s.cfg.NodeID,
		TopicRoot:  s.cfg.Brand.TopicRoot,
		UpdatedAt:  time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("register MNS mesh identity %q: %w", s.cfg.MeshName, err)
	}

	if s.opts.ProvisionRoles {
		if err := activeStore.ProvisionRoles(bootCtx, s.opts.IngestPassword, s.opts.ReadOnlyPassword); err != nil {
			return fmt.Errorf("provision PostgreSQL roles: %w", err)
		}
		log.Printf("[%s] Provisioned least-privilege roles (ctd_ingestor, pigeoncare_ro).", s.cfg.Brand.CoopName)
	}

	// 2. Start embedded pure-Go MQTT broker (unless external broker mode is selected)
	if err := checkBoot(); err != nil {
		return err
	}
	edgePrefix := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(s.cfg.Brand.EdgeName)), "-")
	if edgePrefix == "" {
		edgePrefix = "pigeoneer"
	}

	var brokerURL string
	if !isExternal {
		emb, err := broker.StartEmbedded(broker.EmbeddedOptions{
			ListenAddr:        s.cfg.MQTT.Listen,
			Username:          s.cfg.MQTT.Username,
			Password:          s.cfg.MQTT.Password,
			EdgeUsername:      s.cfg.MQTT.EdgeUsername,
			EdgePassword:      s.cfg.MQTT.EdgePassword,
			EdgeClientPrefix:  edgePrefix,
			TLSCertFile:       s.cfg.MQTT.TLSCertFile,
			TLSKeyFile:        s.cfg.MQTT.TLSKeyFile,
			TLSClientCAFile:   s.cfg.MQTT.TLSClientCAFile,
			RequireClientCert: s.cfg.MQTT.RequireClientCert,
			TopicRoot:         s.cfg.Brand.TopicRoot,
		})
		if err != nil {
			return fmt.Errorf("start embedded MQTT broker: %w", err)
		}
		brokerURL = emb.LocalClientURL()
		s.mu.Lock()
		s.embedded = emb
		s.brokerURL = brokerURL
		s.mu.Unlock()
		log.Printf("[%s] Embedded MQTT broker listening on %s", s.cfg.Brand.CoopName, emb.BoundAddr())
	} else {
		brokerURL = s.cfg.LocalBrokerURL()
		s.mu.Lock()
		s.brokerURL = brokerURL
		s.mu.Unlock()
		log.Printf("[%s] Using external MQTT broker %s", s.cfg.Brand.CoopName, config.RedactURL(brokerURL))
	}

	// Align SNI / certificate pinning when embedded leaf cert cannot be verified directly
	// against the dialed loopback host (missing SANs, non-loopback SANs, or CA-signed leaf without TLSCAFile).
	needLoopbackBridge := false
	targetHost := ""
	if !isExternal && embeddedLeaf != nil {
		if parsedURL, parseErr := url.Parse(brokerURL); parseErr == nil {
			targetHost = parsedURL.Hostname()
		}
		if targetHost == "" {
			targetHost = "127.0.0.1"
		}
		hasExplicitCA := strings.TrimSpace(s.cfg.MQTT.TLSCAFile) != ""
		if !hasExplicitCA && !embeddedSelfSignedAnchor {
			needLoopbackBridge = true
			if clientTLS == nil {
				clientTLS = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			pinEmbeddedLeafTLS(clientTLS, embeddedLeaf, false)
		} else if embeddedLeaf.VerifyHostname(targetHost) != nil {
			needLoopbackBridge = true
			if clientTLS == nil {
				clientTLS = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			if serverName := extractLeafServerName(embeddedLeaf); serverName != "" {
				if clientTLS.ServerName == "" {
					clientTLS.ServerName = serverName
				}
			} else {
				// Leaf cert has neither DNSNames nor IPAddresses; pin the exact leaf certificate over loopback.
				pinEmbeddedLeafTLS(clientTLS, embeddedLeaf, hasExplicitCA)
			}
		}
	}

	// 3. Start high-throughput COPY Ingestion Subscriber & RunnerRequestCTD Router
	if err := checkBoot(); err != nil {
		return err
	}
	sub, err := broker.NewSubscriberWithOptions(broker.SubscriberOptions{
		BrokerURL:  brokerURL,
		MeshName:   s.cfg.MeshName,
		CoopNodeID: s.cfg.NodeID,
		Username:   s.cfg.MQTT.Username,
		Password:   s.cfg.MQTT.Password,
		TLSConfig:  clientTLS,
		Ingest: ingest.Options{
			QueueSize:     s.cfg.Ingest.QueueSize,
			BatchSize:     s.cfg.Ingest.BatchSize,
			FlushInterval: s.cfg.Ingest.ParsedFlushInterval(),
		},
		NtfyEnabled: s.cfg.Ntfy.Enabled,
		NtfyURL:     s.cfg.Ntfy.URL,
		Brand:       s.cfg.Brand,
	}, activeStore)
	if err != nil {
		return fmt.Errorf("start %s MQTT subscriber: %w", s.cfg.Brand.CoopName, err)
	}
	s.mu.Lock()
	s.subscriber = sub
	s.mu.Unlock()

	// 4. Implicitly start embedded Observer + Runner runtime on the Coop host using scoped edge credentials & verified TLS.
	if err := checkBoot(); err != nil {
		return err
	}
	if s.cfg.SelfObserve || s.cfg.Runners.Enabled {
		selfBrokerURL := brokerURL
		selfClientCertFile := clientCertFile
		selfClientKeyFile := clientKeyFile
		selfTLSInsecureSkipVerify := isExternal && s.cfg.MQTT.TLSInsecureSkipVerify
		if !isExternal && needLoopbackBridge {
			bridge, bridgeErr := startLoopbackTLSBridge(
				brokerURL,
				clientTLS,
				targetHost,
			)
			if bridgeErr != nil {
				return fmt.Errorf("start %s loopback TLS bridge for self-node: %w", s.cfg.Brand.CoopName, bridgeErr)
			}
			s.mu.Lock()
			s.loopbackBridge = bridge
			s.mu.Unlock()
			selfBrokerURL = bridge.BrokerURL()
			selfCAFile = bridge.CAFile()
			selfClientCertFile = bridge.ClientCertFile()
			selfClientKeyFile = bridge.ClientKeyFile()
		}

		selfUser := strings.TrimSpace(s.cfg.MQTT.EdgeUsername)
		selfPass := s.cfg.MQTT.EdgePassword
		if selfUser == "" {
			if !isExternal && s.cfg.MQTT.RequireClientCert {
				// Embedded cert-only edge mTLS mode authenticates strictly via node-matched client certificate.
				selfUser = ""
				selfPass = ""
			} else {
				selfUser = strings.TrimSpace(s.cfg.MQTT.Username)
				selfPass = s.cfg.MQTT.Password
			}
		}

		extraObservers := s.opts.ExtraObservers
		if s.cfg.SelfObserve {
			extraObservers = make([]observer.Observer, 0, len(s.opts.ExtraObservers)+1)
			extraObservers = append(extraObservers, &coopTelemetryObserver{
				srv:      s,
				interval: s.cfg.ParsedPollInterval(),
			})
			extraObservers = append(extraObservers, s.opts.ExtraObservers...)
		}

		selfCfg := config.EdgeConfig{
			NodeID:                s.cfg.NodeID,
			Tags:                  s.cfg.Tags,
			PollInterval:          s.cfg.PollInterval,
			SpoolCapacity:         s.cfg.SpoolCapacity,
			BrokerURL:             selfBrokerURL,
			TopicPrefix:           s.cfg.Brand.CTDPrefix(),
			MQTTUsername:          selfUser,
			MQTTPassword:          selfPass,
			TLSCAFile:             selfCAFile,
			TLSCertFile:           selfClientCertFile,
			TLSKeyFile:            selfClientKeyFile,
			TLSInsecureSkipVerify: selfTLSInsecureSkipVerify,
			Observers:             s.cfg.Observers,
			Runners:               s.cfg.Runners,
			Brand:                 s.cfg.Brand,
		}
		selfNode := edge.New(selfCfg, edge.Options{
			Brand:            s.cfg.Brand,
			ClientPrefix:     edgePrefix,
			Username:         selfUser,
			Password:         selfPass,
			DisableObservers: !s.cfg.SelfObserve,
			Meta: map[string]string{
				"role":      "coop",
				"mesh_name": s.cfg.MeshName,
			},
			ExtraObservers: extraObservers,
			ExtraRunners:   s.opts.ExtraRunners,
		})
		s.mu.Lock()
		s.selfNode = selfNode
		s.mu.Unlock()
		if err := selfNode.Start(runCtx); err != nil {
			log.Printf("[%s] Warning: self-observation startup degraded: %v", s.cfg.Brand.CoopName, err)
		}
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errors.New("coop server stopped during startup")
	}
	s.started = true
	s.starting = false
	s.bootCancel = nil
	close(startDone)
	s.mu.Unlock()
	bootOK = true
	return nil
}

// Store returns the active BaseDB backing PigeonCoop.
func (s *Server) Store() db.BaseDB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store
}

// BrokerURL returns the active dialable MQTT URL for local clients.
func (s *Server) BrokerURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.brokerURL
}

// MeshName returns the registered MNS identifier for this PigeonCoop instance.
func (s *Server) MeshName() string {
	return s.cfg.MeshName
}

func snapshotSubscriberCounters(sub *broker.Subscriber) coopDropCounters {
	if sub == nil {
		return coopDropCounters{}
	}
	return coopDropCounters{
		ingested:         sub.IngestedCount(),
		batcherDropped:   sub.BatcherDroppedCount(),
		telemetryDropped: sub.TelemetryDroppedCount(),
		controlDropped:   sub.ControlDroppedCount(),
		runnerReqDropped: sub.RunnerRequestDroppedCount(),
	}
}

func (s *Server) currentCounters() coopDropCounters {
	s.mu.Lock()
	sub := s.subscriber
	cached := s.lastCounters
	s.mu.Unlock()
	if sub != nil {
		return snapshotSubscriberCounters(sub)
	}
	return cached
}

// Status returns a live snapshot of Coop state and ingestion/drop counters.
func (s *Server) Status() StatusSnapshot {
	s.mu.Lock()
	sub := s.subscriber
	cached := s.lastCounters
	started := s.started && !s.stopped
	brokerURL := s.brokerURL
	s.mu.Unlock()

	counters := cached
	if sub != nil {
		counters = snapshotSubscriberCounters(sub)
	}
	return StatusSnapshot{
		MeshName:                  s.cfg.MeshName,
		NodeID:                    s.cfg.NodeID,
		BrokerURL:                 config.RedactURL(brokerURL),
		Started:                   started,
		IngestedCount:             counters.ingested,
		BatcherDroppedCount:       counters.batcherDropped,
		TelemetryDroppedCount:     counters.telemetryDropped,
		ControlDroppedCount:       counters.controlDropped,
		RunnerRequestDroppedCount: counters.runnerReqDropped,
	}
}

// IngestedCount returns total CTD events flushed to PostgreSQL since startup.
func (s *Server) IngestedCount() uint64 {
	return s.currentCounters().ingested
}

// BatcherDroppedCount returns total CTD events dropped by the COPY batcher due to queue saturation or flush failure.
func (s *Server) BatcherDroppedCount() uint64 {
	return s.currentCounters().batcherDropped
}

// TelemetryDroppedCount returns total telemetry messages dropped due to decode queue, byte-budget saturation, or post-stop arrival.
func (s *Server) TelemetryDroppedCount() uint64 {
	return s.currentCounters().telemetryDropped
}

// ControlDroppedCount returns total control/registry messages dropped due to shard saturation or post-stop arrival.
func (s *Server) ControlDroppedCount() uint64 {
	return s.currentCounters().controlDropped
}

// RunnerRequestDroppedCount returns total RunnerRequestCTD messages dropped due to queue saturation or post-stop arrival.
func (s *Server) RunnerRequestDroppedCount() uint64 {
	return s.currentCounters().runnerReqDropped
}

func parseEmbeddedLeafCert(certFile string) ([]byte, *x509.Certificate, error) {
	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		return nil, nil, err
	}
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, nil, fmt.Errorf("no valid CERTIFICATE PEM block in %s", certFile)
		}
		if block.Type == "CERTIFICATE" {
			leaf, parseErr := x509.ParseCertificate(block.Bytes)
			if parseErr != nil {
				return nil, nil, fmt.Errorf("parse X.509 certificate in %s: %w", certFile, parseErr)
			}
			return pemBytes, leaf, nil
		}
	}
}

func extractLeafServerName(leaf *x509.Certificate) string {
	if leaf == nil {
		return ""
	}
	for _, dns := range leaf.DNSNames {
		if trimmed := strings.TrimSpace(dns); trimmed != "" {
			return trimmed
		}
	}
	for _, ip := range leaf.IPAddresses {
		if ip != nil {
			return ip.String()
		}
	}
	return ""
}

func pinEmbeddedLeafTLS(clientTLS *tls.Config, leaf *x509.Certificate, verifyAgainstRoots bool) {
	expectedRaw := leaf.Raw
	explicitRoots := clientTLS.RootCAs
	clientTLS.InsecureSkipVerify = true //nolint:gosec // verified explicitly via VerifyPeerCertificate byte match + expiry/chain check below
	clientTLS.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 || !bytes.Equal(rawCerts[0], expectedRaw) {
			return errors.New("embedded loopback TLS certificate mismatch")
		}
		parsed, parseErr := x509.ParseCertificate(rawCerts[0])
		if parseErr != nil {
			return fmt.Errorf("parse embedded loopback certificate: %w", parseErr)
		}
		now := time.Now()
		if now.Before(parsed.NotBefore) || now.After(parsed.NotAfter) {
			return errors.New("embedded loopback TLS certificate expired or not yet valid")
		}
		if verifyAgainstRoots && explicitRoots != nil {
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if ic, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(ic)
				}
			}
			if _, err := parsed.Verify(x509.VerifyOptions{
				Roots:         explicitRoots,
				Intermediates: intermediates,
				CurrentTime:   now,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return fmt.Errorf("verify embedded loopback certificate against CA: %w", err)
			}
		}
		return nil
	}
}

func randomCertSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

// loopbackTLSBridge exposes an ephemeral, per-instance pinned-mTLS loopback endpoint for edge.Agent
// when the embedded broker's certificate cannot be directly verified against the dialed loopback host,
// forwarding to the embedded broker using SNI/leaf-pinned upstream TLS verification.
type loopbackTLSBridge struct {
	ln           net.Listener
	brokerURL    string
	upstreamAddr string
	upstreamTLS  *tls.Config
	caFile       string
	clientCert   string
	clientKey    string
	tmpDir       string
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	closeOnce    sync.Once

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
}

func startLoopbackTLSBridge(upstreamURL string, upstreamTLS *tls.Config, targetHost string) (*loopbackTLSBridge, error) {
	if upstreamTLS == nil {
		return nil, errors.New("upstream TLS configuration is required")
	}
	u, err := url.Parse(upstreamURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("parse embedded broker URL: %w", err)
	}

	bindIP := net.ParseIP("127.0.0.1")
	if parsed := net.ParseIP(strings.TrimSpace(targetHost)); parsed != nil && parsed.IsLoopback() {
		bindIP = parsed
	}

	now := time.Now().Add(-1 * time.Minute)
	exp := now.Add(10 * 365 * 24 * time.Hour)

	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate loopback bridge CA key: %w", err)
	}
	caSerial, err := randomCertSerial()
	if err != nil {
		return nil, fmt.Errorf("generate loopback bridge CA serial: %w", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "pigeoncoop-loopback-ca"},
		NotBefore:             now,
		NotAfter:              exp,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caPriv.PublicKey, caPriv)
	if err != nil {
		return nil, fmt.Errorf("create loopback bridge CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse loopback bridge CA certificate: %w", err)
	}

	srvPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate loopback bridge server key: %w", err)
	}
	srvSerial, err := randomCertSerial()
	if err != nil {
		return nil, fmt.Errorf("generate loopback bridge server serial: %w", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber:          srvSerial,
		Subject:               pkix.Name{CommonName: "pigeoncoop-loopback-bridge"},
		NotBefore:             now,
		NotAfter:              exp,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{bindIP},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvPriv.PublicKey, caPriv)
	if err != nil {
		return nil, fmt.Errorf("create loopback bridge server certificate: %w", err)
	}

	cliPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate loopback bridge client key: %w", err)
	}
	cliSerial, err := randomCertSerial()
	if err != nil {
		return nil, fmt.Errorf("generate loopback bridge client serial: %w", err)
	}
	cliTmpl := &x509.Certificate{
		SerialNumber:          cliSerial,
		Subject:               pkix.Name{CommonName: "pigeoncoop-loopback-client"},
		NotBefore:             now,
		NotAfter:              exp,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliPriv.PublicKey, caPriv)
	if err != nil {
		return nil, fmt.Errorf("create loopback bridge client certificate: %w", err)
	}
	cliKeyDER, err := x509.MarshalECPrivateKey(cliPriv)
	if err != nil {
		return nil, fmt.Errorf("marshal loopback bridge client key: %w", err)
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)
	expectedClientDER := cliDER
	srvTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{srvDER, caDER}, PrivateKey: srvPriv}},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 || !bytes.Equal(rawCerts[0], expectedClientDER) {
				return errors.New("loopback bridge client certificate mismatch")
			}
			return nil
		},
	}

	tmpDir, err := os.MkdirTemp("", "coop-loopback-tls-*")
	if err != nil {
		return nil, fmt.Errorf("create loopback bridge temp dir: %w", err)
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.RemoveAll(tmpDir)
		}
	}()
	caFile := filepath.Join(tmpDir, "loopback-ca.pem")
	clientCertFile := filepath.Join(tmpDir, "loopback-client.crt")
	clientKeyFile := filepath.Join(tmpDir, "loopback-client.key")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return nil, fmt.Errorf("write loopback bridge CA file: %w", err)
	}
	if err := os.WriteFile(clientCertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cliDER}), 0o600); err != nil {
		return nil, fmt.Errorf("write loopback bridge client cert: %w", err)
	}
	if err := os.WriteFile(clientKeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: cliKeyDER}), 0o600); err != nil {
		return nil, fmt.Errorf("write loopback bridge client key: %w", err)
	}

	ln, err := tls.Listen("tcp", net.JoinHostPort(bindIP.String(), "0"), srvTLS)
	if err != nil {
		return nil, fmt.Errorf("listen loopback bridge: %w", err)
	}

	cleanupTmp = false
	ctx, cancel := context.WithCancel(context.Background())
	b := &loopbackTLSBridge{
		ln:           ln,
		brokerURL:    "tls://" + ln.Addr().String(),
		upstreamAddr: u.Host,
		upstreamTLS:  upstreamTLS.Clone(),
		caFile:       caFile,
		clientCert:   clientCertFile,
		clientKey:    clientKeyFile,
		tmpDir:       tmpDir,
		ctx:          ctx,
		cancel:       cancel,
		conns:        make(map[net.Conn]struct{}),
	}
	b.wg.Add(1)
	go b.serve()
	return b, nil
}

func (b *loopbackTLSBridge) BrokerURL() string      { return b.brokerURL }
func (b *loopbackTLSBridge) CAFile() string         { return b.caFile }
func (b *loopbackTLSBridge) ClientCertFile() string { return b.clientCert }
func (b *loopbackTLSBridge) ClientKeyFile() string  { return b.clientKey }

func (b *loopbackTLSBridge) trackConn(c net.Conn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.conns[c] = struct{}{}
	return true
}

func (b *loopbackTLSBridge) untrackAndClose(c net.Conn) {
	b.mu.Lock()
	delete(b.conns, c)
	b.mu.Unlock()
	_ = c.Close()
}

func (b *loopbackTLSBridge) serve() {
	defer b.wg.Done()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		if !b.trackConn(conn) {
			_ = conn.Close()
			return
		}
		b.wg.Add(1)
		go b.handleConn(conn)
	}
}

func (b *loopbackTLSBridge) handleConn(clientConn net.Conn) {
	defer b.wg.Done()
	defer b.untrackAndClose(clientConn)

	if tlsConn, ok := clientConn.(*tls.Conn); ok {
		hsCtx, hsCancel := context.WithTimeout(b.ctx, 5*time.Second)
		err := tlsConn.HandshakeContext(hsCtx)
		hsCancel()
		if err != nil {
			return
		}
	}

	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 5 * time.Second},
		Config:    b.upstreamTLS.Clone(),
	}
	upstreamConn, err := dialer.DialContext(b.ctx, "tcp", b.upstreamAddr)
	if err != nil {
		return
	}
	if !b.trackConn(upstreamConn) {
		_ = upstreamConn.Close()
		return
	}
	defer b.untrackAndClose(upstreamConn)

	var copyWG sync.WaitGroup
	copyWG.Add(2)
	go func() {
		defer copyWG.Done()
		_, _ = io.Copy(upstreamConn, clientConn)
		if cw, ok := upstreamConn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}()
	go func() {
		defer copyWG.Done()
		_, _ = io.Copy(clientConn, upstreamConn)
		if cw, ok := clientConn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}()
	copyWG.Wait()
}

func (b *loopbackTLSBridge) Close() {
	b.closeOnce.Do(func() {
		b.cancel()
		b.mu.Lock()
		b.closed = true
		conns := make([]net.Conn, 0, len(b.conns))
		for c := range b.conns {
			conns = append(conns, c)
		}
		b.mu.Unlock()

		if b.ln != nil {
			_ = b.ln.Close()
		}
		for _, c := range conns {
			_ = c.Close()
		}
		b.wg.Wait()
		if b.tmpDir != "" {
			_ = os.RemoveAll(b.tmpDir)
		}
	})
}

func (s *Server) teardownResources() {
	s.mu.Lock()
	runCancel := s.runCancel
	selfNode := s.selfNode
	subscriber := s.subscriber
	bridge := s.loopbackBridge
	embedded := s.embedded
	store := s.store
	ownsStore := s.ownsStore

	s.runCtx = nil
	s.runCancel = nil
	s.selfNode = nil
	s.loopbackBridge = nil
	s.embedded = nil
	s.store = nil
	s.ownsStore = false
	s.brokerURL = ""
	s.mu.Unlock()

	if runCancel != nil {
		runCancel()
	}
	if selfNode != nil {
		selfNode.Stop()
	}
	if subscriber != nil {
		subscriber.Disconnect()
		final := snapshotSubscriberCounters(subscriber)
		s.mu.Lock()
		s.lastCounters = final
		s.subscriber = nil
		s.mu.Unlock()
	}
	if bridge != nil {
		bridge.Close()
	}
	if embedded != nil {
		embedded.Close()
	}
	if ownsStore && store != nil {
		store.Close()
	}
}

func (s *Server) cleanupPartial() {
	s.teardownResources()
}

// Stop gracefully shuts down self-observers, flushes pending COPY batches, stops the embedded broker, and closes DB pools.
func (s *Server) Stop() {
	s.mu.Lock()
	s.stopped = true
	bootCancel := s.bootCancel
	runCancel := s.runCancel
	starting := s.starting
	startDone := s.startDone
	s.mu.Unlock()

	if bootCancel != nil {
		bootCancel()
	}
	if runCancel != nil {
		runCancel()
	}
	if starting && startDone != nil {
		<-startDone
	}

	s.stopOnce.Do(func() {
		s.teardownResources()
	})
}
