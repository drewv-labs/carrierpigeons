package coop

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

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
	cfg.Tags = core.NormalizeTags(cfg.Tags)
	return &Server{
		cfg:  cfg,
		opts: opt,
	}
}

// Start boots the PostgreSQL schema, MQTT broker, batched CTD ingestor, and implicit self-observation/runners.
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

	// In embedded TLS mode without an explicit CA file, trust the embedded broker's leaf cert
	// so internal loopback subscribers connect securely without requiring TLSInsecureSkipVerify.
	selfCAFile := s.cfg.MQTT.TLSCAFile
	if !isExternal && strings.TrimSpace(s.cfg.MQTT.TLSCertFile) != "" && strings.TrimSpace(s.cfg.MQTT.TLSKeyFile) != "" && strings.TrimSpace(s.cfg.MQTT.TLSCAFile) == "" {
		pemBytes, readErr := os.ReadFile(s.cfg.MQTT.TLSCertFile)
		if readErr != nil {
			return fmt.Errorf("read embedded MQTT TLS cert %s for loopback trust: %w", s.cfg.MQTT.TLSCertFile, readErr)
		}
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
		selfCfg := config.EdgeConfig{
			NodeID:                s.cfg.NodeID,
			Tags:                  s.cfg.Tags,
			PollInterval:          s.cfg.PollInterval,
			SpoolCapacity:         s.cfg.SpoolCapacity,
			BrokerURL:             s.brokerURL,
			TopicPrefix:           s.cfg.Brand.CTDPrefix(),
			MQTTUsername:          s.cfg.MQTT.Username,
			MQTTPassword:          s.cfg.MQTT.Password,
			TLSCAFile:             selfCAFile,
			TLSCertFile:           clientCertFile,
			TLSKeyFile:            clientKeyFile,
			TLSInsecureSkipVerify: s.cfg.MQTT.TLSInsecureSkipVerify,
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
			Meta:             map[string]string{"role": "coop"},
			ExtraObservers:   s.opts.ExtraObservers,
			ExtraRunners:     s.opts.ExtraRunners,
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
