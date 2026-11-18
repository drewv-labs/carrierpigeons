package edge

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/drewv-labs/carrier-pigeons/internal/node/observers"
	"github.com/drewv-labs/carrier-pigeons/internal/node/relay"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/config"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/observer"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"

	// Register built-in hardware observers and runners on package import.
	_ "github.com/drewv-labs/carrier-pigeons/internal/node/observers"
	_ "github.com/drewv-labs/carrier-pigeons/internal/node/runners"
)

// Options customizes an embedded or standalone Edge Agent instance for SDK callers.
type Options struct {
	ClientPrefix     string
	Username         string
	Password         string
	DisableObservers bool
	Meta             map[string]string
	ExtraObservers   []observer.Observer
	ExtraRunners     []runner.Runner
}

// Agent encapsulates the MQTT-driven Observer + Runner runtime with variable-size ring spooling.
// It powers standalone Pigeoneer daemons, implicit self-reporting inside PigeonCoop, and downstream SDK appliances.
type Agent struct {
	cfg   config.EdgeConfig
	opts  Options
	relay *relay.MQTTClient

	stopOnce sync.Once
	cancel   context.CancelFunc
}

// New creates a configured Edge Agent ready to Start(ctx).
func New(cfg config.EdgeConfig, opts ...Options) *Agent {
	cfg.Brand = cfg.Brand.MergeWithDefaults()
	cfg.Tags = core.NormalizeTags(cfg.Tags)
	if strings.TrimSpace(cfg.TopicPrefix) == "" {
		cfg.TopicPrefix = cfg.Brand.CTDPrefix()
	}

	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	for _, o := range opt.ExtraObservers {
		observer.Register(o)
	}
	for _, r := range opt.ExtraRunners {
		runner.Register(r)
	}

	return &Agent{
		cfg:  cfg,
		opts: opt,
	}
}

// Start connects (or spools offline) to the MQTT broker, binds Runner control topics, and launches Observers.
func (a *Agent) Start(parentCtx context.Context) error {
	brand.SetActive(a.cfg.Brand)
	ctx, cancel := context.WithCancel(parentCtx)
	a.cancel = cancel

	execPolicy := runner.ExecutionPolicy{
		AllowEphemeral:      a.cfg.Runners.AllowEphemeral,
		AllowedInterpreters: a.cfg.Runners.AllowedInterpreters,
		MaxTimeoutSec:       a.cfg.Runners.MaxTimeoutSec,
	}

	a.relay = relay.NewMQTTClientWithOptions(relay.Options{
		BrokerURL:     a.cfg.BrokerURL,
		NodeID:        a.cfg.NodeID,
		ClientPrefix:  a.opts.ClientPrefix,
		TopicPrefix:   a.cfg.TopicPrefix,
		SpoolCapacity: a.cfg.SpoolCapacity,
		BootTags:      a.cfg.Tags,
		Username:      a.opts.Username,
		Password:      a.opts.Password,
		Meta:          a.opts.Meta,
		RunnerPolicy:  execPolicy,
	})

	if a.cfg.Runners.Enabled {
		if err := a.relay.ListenForRunners(ctx, a.cfg.NodeID, a.relay); err != nil {
			log.Printf("[%s] Runner control bind deferred until broker reconnect: %v", a.cfg.Brand.EdgeName, err)
		}
	}

	if !a.opts.DisableObservers {
		active := observers.StartConfigured(ctx, a.cfg.NodeID, a.relay, a.cfg.Observers, a.cfg.ParsedPollInterval())
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
