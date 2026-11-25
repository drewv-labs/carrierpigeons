package coop

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
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

// Server orchestrates the full PigeonCoop hub stack:
// Embedded pure-Go MQTT Broker (optional external) + PostgreSQL Ledger + Batched COPY Injector +
// RunnerRequestCTD Router + Ntfy Watchdog + implicit self-observation & Runner runtime.
type Server struct {
	cfg        config.CoopConfig
	opts       Options
	store      db.BaseDB
	ownsStore  bool
	embedded   *broker.EmbeddedBroker
	subscriber *broker.Subscriber
	selfNode   *edge.Agent
	brokerURL  string

	stopOnce sync.Once
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

// Start boots the PostgreSQL schema, MNS registration, MQTT broker, batched CTD ingestor, and implicit self-observation/runners.
func (s *Server) Start(ctx context.Context) error {
	if err := s.cfg.ValidateSecurity(); err != nil {
		return err
	}

	isExternal := strings.EqualFold(strings.TrimSpace(s.cfg.MQTT.Mode), "external")
	clientCertFile := ""
	clientKeyFile := ""
	if isExternal {
		clientCertFile = s.cfg.MQTT.TLSCertFile
		clientKeyFile = s.cfg.MQTT.TLSKeyFile
	}

	clientTLS, err := config.BuildMQTTTLSConfig(
		s.cfg.MQTT.TLSCAFile,
		clientCertFile,
		clientKeyFile,
		s.cfg.MQTT.TLSInsecureSkipVerify,
	)
	if err != nil {
		return fmt.Errorf("configure PigeonCoop MQTT TLS client: %w", err)
	}

	// In embedded TLS mode, trust the embedded broker's leaf cert when no explicit CA is provided,
	// and align ServerName for loopback connections when the cert SANs only list a LAN hostname/IP.
	selfCAFile := s.cfg.MQTT.TLSCAFile
	var embeddedLeaf *x509.Certificate
	loopbackLacksLocalhost := false
	if !isExternal && strings.TrimSpace(s.cfg.MQTT.TLSCertFile) != "" && strings.TrimSpace(s.cfg.MQTT.TLSKeyFile) != "" {
		pemBytes, leaf, parseErr := parseEmbeddedLeafCert(s.cfg.MQTT.TLSCertFile)
		if parseErr != nil {
			return fmt.Errorf("read embedded MQTT TLS cert %s for loopback trust: %w", s.cfg.MQTT.TLSCertFile, parseErr)
		}
		embeddedLeaf = leaf
		if strings.TrimSpace(s.cfg.MQTT.TLSCAFile) == "" {
			if clientTLS == nil {
				clientTLS = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			pool := clientTLS.RootCAs
			if pool == nil {
				if sysPool, poolErr := x509.SystemCertPool(); poolErr == nil && sysPool != nil {
					pool = sysPool
				} else {
					pool = x509.NewCertPool()
				}
			}
			if !pool.AppendCertsFromPEM(pemBytes) {
				return fmt.Errorf("no valid PEM certificates in embedded MQTT TLS cert %s", s.cfg.MQTT.TLSCertFile)
			}
			clientTLS.RootCAs = pool
			selfCAFile = s.cfg.MQTT.TLSCertFile
		}
		if leaf.VerifyHostname("127.0.0.1") != nil {
			loopbackLacksLocalhost = true
			if clientTLS == nil {
				clientTLS = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			if serverName := extractLeafServerName(leaf); serverName != "" {
				if clientTLS.ServerName == "" {
					clientTLS.ServerName = serverName
				}
			} else {
				// Leaf cert has neither DNSNames nor IPAddresses; pin the exact leaf certificate over loopback.
				clientTLS.InsecureSkipVerify = true //nolint:gosec // verified explicitly via VerifyPeerCertificate byte match below
				expectedRaw := leaf.Raw
				clientTLS.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
					if len(rawCerts) == 0 || !bytes.Equal(rawCerts[0], expectedRaw) {
						return errors.New("embedded loopback TLS certificate mismatch")
					}
					return nil
				}
			}
		}
	}

	// 1. Initialize or attach PostgreSQL Ledger
	if s.opts.Store != nil {
		s.store = s.opts.Store
	} else {
		pgStore, err := db.NewPostgresStoreWithOptions(ctx, s.cfg.Database.URL, db.StoreOptions{
			AppActor:   strings.ToLower(s.cfg.Brand.CoopName) + "-ingestor",
			MaxConns:   s.cfg.Database.MaxConns,
			MinConns:   s.cfg.Database.MinConns,
			MathSchema: s.cfg.Brand.MathSchema,
		})
		if err != nil {
			return fmt.Errorf("connect PostgreSQL ledger: %w", err)
		}
		s.store = pgStore
		s.ownsStore = true
	}

	if err := s.store.EnsureSchema(ctx); err != nil {
		s.Stop()
		return fmt.Errorf("ensure CTD & %s schema: %w", s.cfg.Brand.MathName, err)
	}

	if err := s.store.UpsertMeshIdentity(ctx, core.MeshIdentity{
		MeshName:   s.cfg.MeshName,
		CoopNodeID: s.cfg.NodeID,
		TopicRoot:  s.cfg.Brand.TopicRoot,
		UpdatedAt:  time.Now().UTC(),
	}); err != nil {
		s.Stop()
		return fmt.Errorf("register MNS mesh identity %q: %w", s.cfg.MeshName, err)
	}

	if s.opts.ProvisionRoles {
		if err := s.store.ProvisionRoles(ctx, s.opts.IngestPassword, s.opts.ReadOnlyPassword); err != nil {
			log.Printf("[%s] Warning: ProvisionRoles skipped/failed (insufficient superuser privileges?): %v",
				s.cfg.Brand.CoopName, err)
		} else {
			log.Printf("[%s] Provisioned least-privilege roles (ctd_ingestor, pigeoncare_ro).", s.cfg.Brand.CoopName)
		}
	}

	// 2. Start embedded pure-Go MQTT broker (unless external broker mode is selected)
	if !isExternal {
		emb, err := broker.StartEmbedded(broker.EmbeddedOptions{
			ListenAddr:       s.cfg.MQTT.Listen,
			Username:         s.cfg.MQTT.Username,
			Password:         s.cfg.MQTT.Password,
			EdgeUsername:     s.cfg.MQTT.EdgeUsername,
			EdgePassword:     s.cfg.MQTT.EdgePassword,
			EdgeClientPrefix: strings.ToLower(s.cfg.Brand.EdgeName),
			TLSCertFile:      s.cfg.MQTT.TLSCertFile,
			TLSKeyFile:       s.cfg.MQTT.TLSKeyFile,
			TopicRoot:        s.cfg.Brand.TopicRoot,
		})
		if err != nil {
			s.Stop()
			return fmt.Errorf("start embedded MQTT broker: %w", err)
		}
		s.embedded = emb
		s.brokerURL = emb.LocalClientURL()
		log.Printf("[%s] Embedded MQTT broker listening on %s", s.cfg.Brand.CoopName, emb.BoundAddr())
	} else {
		s.brokerURL = s.cfg.LocalBrokerURL()
		log.Printf("[%s] Using external MQTT broker %s", s.cfg.Brand.CoopName, s.brokerURL)
	}

	// 3. Start high-throughput COPY Ingestion Subscriber & RunnerRequestCTD Router
	sub, err := broker.NewSubscriberWithOptions(broker.SubscriberOptions{
		BrokerURL:  s.brokerURL,
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
	}, s.store)
	if err != nil {
		s.Stop()
		return fmt.Errorf("start %s MQTT subscriber: %w", s.cfg.Brand.CoopName, err)
	}
	s.subscriber = sub

	// 4. Implicitly start embedded Observer + Runner runtime on the Coop host using native CoopConfig settings
	if s.cfg.SelfObserve || s.cfg.Runners.Enabled {
		selfBrokerURL := s.brokerURL
		selfTLSInsecureSkipVerify := s.cfg.MQTT.TLSInsecureSkipVerify
		if !isExternal && loopbackLacksLocalhost {
			matchedBoundIP := false
			if s.embedded != nil && embeddedLeaf != nil {
				if host, port, splitErr := net.SplitHostPort(s.embedded.BoundAddr()); splitErr == nil {
					if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() && embeddedLeaf.VerifyHostname(host) == nil {
						selfBrokerURL = "tls://" + net.JoinHostPort(host, port)
						matchedBoundIP = true
					}
				}
			}
			if !matchedBoundIP {
				// edge.Agent constructs TLS config from EdgeConfig (no ServerName field); skip hostname check
				// strictly on the in-process 127.0.0.1 loopback connection to our own embedded broker.
				selfTLSInsecureSkipVerify = true
			}
		}

		selfCfg := config.EdgeConfig{
			NodeID:                s.cfg.NodeID,
			Tags:                  s.cfg.Tags,
			PollInterval:          s.cfg.PollInterval,
			SpoolCapacity:         s.cfg.SpoolCapacity,
			BrokerURL:             selfBrokerURL,
			TopicPrefix:           s.cfg.Brand.CTDPrefix(),
			MQTTUsername:          s.cfg.MQTT.Username,
			MQTTPassword:          s.cfg.MQTT.Password,
			TLSCAFile:             selfCAFile,
			TLSCertFile:           clientCertFile,
			TLSKeyFile:            clientKeyFile,
			TLSInsecureSkipVerify: selfTLSInsecureSkipVerify,
			Observers:             s.cfg.Observers,
			Runners:               s.cfg.Runners,
			Brand:                 s.cfg.Brand,
		}
		s.selfNode = edge.New(selfCfg, edge.Options{
			Brand:            s.cfg.Brand,
			ClientPrefix:     strings.ToLower(s.cfg.Brand.CoopName) + "-node",
			Username:         s.cfg.MQTT.Username,
			Password:         s.cfg.MQTT.Password,
			DisableObservers: !s.cfg.SelfObserve,
			Meta: map[string]string{
				"role":      "coop",
				"mesh_name": s.cfg.MeshName,
			},
			ExtraObservers: s.opts.ExtraObservers,
			ExtraRunners:   s.opts.ExtraRunners,
		})
		if err := s.selfNode.Start(ctx); err != nil {
			log.Printf("[%s] Warning: self-observation startup degraded: %v", s.cfg.Brand.CoopName, err)
		}
	}

	return nil
}

// Store returns the active BaseDB backing PigeonCoop.
func (s *Server) Store() db.BaseDB {
	return s.store
}

// BrokerURL returns the active dialable MQTT URL for local clients.
func (s *Server) BrokerURL() string {
	return s.brokerURL
}

// MeshName returns the registered MNS identifier for this PigeonCoop instance.
func (s *Server) MeshName() string {
	return s.cfg.MeshName
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

// Stop gracefully shuts down self-observers, flushes pending COPY batches, stops the embedded broker, and closes DB pools.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		if s.selfNode != nil {
			s.selfNode.Stop()
		}
		if s.subscriber != nil {
			s.subscriber.Disconnect()
		}
		if s.embedded != nil {
			s.embedded.Close()
		}
		if s.ownsStore && s.store != nil {
			s.store.Close()
		}
	})
}
