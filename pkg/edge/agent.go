package edge

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/drewv-labs/carrierpigeons/internal/node/observers"
	"github.com/drewv-labs/carrierpigeons/internal/node/relay"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"

	// Register built-in runners on package import.
	_ "github.com/drewv-labs/carrierpigeons/internal/node/runners"
)

// Options customizes an embedded or standalone Edge Agent instance for SDK callers.
type Options struct {
	Brand            brand.Spec
	ClientPrefix     string
	Username         string
	Password         string
	DisableObservers bool
	Meta             map[string]string
	ExtraObservers   []observer.Observer
	ExtraRunners     []runner.Runner
	ObserverRegistry *observer.Registry
	RunnerRegistry   *runner.Registry
}

// Agent encapsulates the MQTT-driven Observer + Runner runtime with variable-size ring spooling.
// It powers standalone Pigeoneer daemons, implicit self-reporting inside PigeonCoop, and downstream SDK appliances.
type Agent struct {
	cfg       config.EdgeConfig
	opts      Options
	obsReg    *observer.Registry
	runnerReg *runner.Registry
	relay     *relay.MQTTClient

	stopOnce sync.Once
	cancel   context.CancelFunc
}

// New creates a configured Edge Agent ready to Start(ctx) using isolated per-instance Observer & Runner registries.
func New(cfg config.EdgeConfig, opts ...Options) *Agent {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Brand != (brand.Spec{}) {
		cfg.Brand = opt.Brand
	}
	cfg.Brand = cfg.Brand.MergeWithDefaults()
	cfg.Tags = core.NormalizeTags(cfg.Tags)
	if strings.TrimSpace(cfg.TopicPrefix) == "" {
		cfg.TopicPrefix = cfg.Brand.CTDPrefix()
	}

	obsReg := opt.ObserverRegistry
	if obsReg == nil {
		obsReg = observer.CloneDefaultRegistry()
	}
	for _, o := range opt.ExtraObservers {
		obsReg.Register(o)
	}

	runnerReg := opt.RunnerRegistry
	if runnerReg == nil {
		runnerReg = runner.CloneDefaultRegistry()
	}
	for _, r := range opt.ExtraRunners {
		runnerReg.Register(r)
	}

	return &Agent{
		cfg:       cfg,
		opts:      opt,
		obsReg:    obsReg,
		runnerReg: runnerReg,
	}
}

// Start connects (or spools offline) to the MQTT broker, binds Runner control topics, and launches Observers.
func (a *Agent) Start(parentCtx context.Context) error {
	mqttAuth := a.cfg.EffectiveMQTTAuth()
	username := strings.TrimSpace(a.opts.Username)
	if username == "" {
		username = mqttAuth.Username
	}
	password := a.opts.Password
	if password == "" {
		password = mqttAuth.Password
	}

	tlsCfg, err := config.BuildMQTTTLSConfig(
		mqttAuth.TLSCAFile,
		mqttAuth.TLSCertFile,
		mqttAuth.TLSKeyFile,
		mqttAuth.TLSInsecureSkipVerify,
	)
	if err != nil {
		return fmt.Errorf("configure edge MQTT TLS: %w", err)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	a.cancel = cancel

	execPolicy := runner.ExecutionPolicy{
		AllowEphemeral:      a.cfg.Runners.AllowEphemeral,
		AllowedInterpreters: a.cfg.Runners.AllowedInterpreters,
		AllowedScriptHashes: a.cfg.Runners.AllowedScriptHashes,
		MaxTimeoutSec:       a.cfg.Runners.MaxTimeoutSec,
	}

	a.relay = relay.NewMQTTClientWithOptions(relay.Options{
		BrokerURL:      a.cfg.BrokerURL,
		NodeID:         a.cfg.NodeID,
		ClientPrefix:   a.opts.ClientPrefix,
		TopicPrefix:    a.cfg.TopicPrefix,
		Brand:          a.cfg.Brand,
		SpoolCapacity:  a.cfg.SpoolCapacity,
		BootTags:       a.cfg.Tags,
		Username:       username,
		Password:       password,
		TLSConfig:      tlsCfg,
		Meta:           a.opts.Meta,
		RunnerPolicy:   execPolicy,
		RunnerRegistry: a.runnerReg,
	})

	if a.cfg.Runners.Enabled {
		if err := a.relay.ListenForRunners(ctx, a.cfg.NodeID, a.relay); err != nil {
			log.Printf("[%s] Runner control bind deferred until broker reconnect: %v", a.cfg.Brand.EdgeName, err)
		}
	}

	if !a.opts.DisableObservers {
		active := observers.StartConfiguredWithRegistry(ctx, a.cfg.NodeID, a.relay, a.cfg.Observers, a.cfg.ParsedPollInterval(), a.obsReg)
		log.Printf("[%s] Active observers on %q (tags=%v, spool=%d): %v",
			a.cfg.Brand.EdgeName, a.cfg.NodeID, a.cfg.Tags, a.cfg.SpoolCapacity, active)
	}

	return nil
}

// Publish emits a custom CTD payload through the agent's ring-buffered MQTT relay.
func (a *Agent) Publish(p *core.CTDPayload) error {
	if a.relay == nil {
		return nil
	}
	return a.relay.Publish(p)
}

// Stop cancels observers/runners and gracefully disconnects the MQTT relay with OFFLINE status.
func (a *Agent) Stop() {
	a.stopOnce.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
		if a.relay != nil {
			a.relay.Disconnect()
		}
	})
}
