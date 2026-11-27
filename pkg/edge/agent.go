package edge

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

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

const stopWaitTimeout = 2 * time.Second

// Options customizes an embedded or standalone Edge Agent instance for SDK callers.
type Options struct {
	Brand            brand.Spec
	ClientPrefix     string
	Username         string
	Password         string
	DisableObservers bool
	MaxSpoolBytes    int64
	Meta             map[string]string
	ExtraObservers   []observer.Observer
	ExtraRunners     []runner.Runner
	ObserverRegistry *observer.Registry
	RunnerRegistry   *runner.Registry
	PauseController  *observer.PauseController
}

// Agent encapsulates the MQTT-driven Observer + Runner runtime with variable-size ring spooling.
// It powers standalone Pigeoneer daemons, implicit self-reporting inside PigeonCoop, and downstream SDK appliances.
type Agent struct {
	cfg             config.EdgeConfig
	opts            Options
	obsReg          *observer.Registry
	runnerReg       *runner.Registry
	pauseCtrl       *observer.PauseController
	startConfigured func(context.Context, string, core.TelemetryPublisher, config.ObserversConfig, time.Duration, *observer.Registry, *sync.WaitGroup) []string

	mu        sync.Mutex
	starting  bool
	started   bool
	stopped   bool
	startDone chan struct{}
	stopDone  chan struct{}
	cancel    context.CancelFunc
	relay     *relay.MQTTClient
	obsWG     sync.WaitGroup
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

	pauseCtrl := opt.PauseController
	if pauseCtrl == nil {
		pauseCtrl = observer.NewPauseController()
	}

	return &Agent{
		cfg:             cfg,
		opts:            opt,
		obsReg:          obsReg,
		runnerReg:       runnerReg,
		pauseCtrl:       pauseCtrl,
		startConfigured: observers.StartConfiguredWithRegistryAndWaitGroup,
	}
}

// PauseController returns the node's isolated observer PauseController for SDK callers.
func (a *Agent) PauseController() *observer.PauseController {
	return a.pauseCtrl
}

// Start connects (or spools offline) to the MQTT broker, binds Runner control topics, and launches Observers.
func (a *Agent) Start(parentCtx context.Context) error {
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	mqttAuth := a.cfg.EffectiveMQTTAuth()
	username := strings.TrimSpace(a.opts.Username)
	if username == "" {
		username = mqttAuth.Username
	}
	password := a.opts.Password
	if password == "" {
		password = mqttAuth.Password
	}

	a.mu.Lock()
	if a.started || a.starting {
		a.mu.Unlock()
		return fmt.Errorf("edge agent already started")
	}
	if a.stopped {
		a.mu.Unlock()
		return fmt.Errorf("edge agent already stopped")
	}

	tlsCfg, err := config.BuildMQTTTLSConfig(
		mqttAuth.TLSCAFile,
		mqttAuth.TLSCertFile,
		mqttAuth.TLSKeyFile,
		mqttAuth.TLSInsecureSkipVerify,
	)
	if err != nil {
		a.mu.Unlock()
		return fmt.Errorf("configure edge MQTT TLS: %w", err)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	ctx = observer.WithPauseController(ctx, a.pauseCtrl)
	startDone := make(chan struct{})
	a.starting = true
	a.cancel = cancel
	a.startDone = startDone
	a.mu.Unlock()

	execPolicy := runner.ExecutionPolicy{
		AllowEphemeral:      a.cfg.Runners.AllowEphemeral,
		AllowedInterpreters: a.cfg.Runners.AllowedInterpreters,
		AllowedScriptHashes: a.cfg.Runners.AllowedScriptHashes,
		MaxTimeoutSec:       a.cfg.Runners.MaxTimeoutSec,
	}

	r := relay.NewMQTTClientWithOptions(relay.Options{
		BrokerURL:       a.cfg.BrokerURL,
		NodeID:          a.cfg.NodeID,
		ClientPrefix:    a.opts.ClientPrefix,
		TopicPrefix:     a.cfg.TopicPrefix,
		Brand:           a.cfg.Brand,
		SpoolCapacity:   a.cfg.SpoolCapacity,
		MaxSpoolBytes:   a.opts.MaxSpoolBytes,
		BootTags:        a.cfg.Tags,
		Username:        username,
		Password:        password,
		TLSConfig:       tlsCfg,
		Meta:            a.opts.Meta,
		RunnerPolicy:    execPolicy,
		RunnerRegistry:  a.runnerReg,
		PauseController: a.pauseCtrl,
	})

	a.mu.Lock()
	if a.stopped {
		a.starting = false
		close(startDone)
		a.mu.Unlock()
		cancel()
		r.Disconnect()
		return fmt.Errorf("edge agent stopped during startup")
	}

	a.relay = r
	if a.cfg.Runners.Enabled {
		if err := r.ListenForRunners(ctx, a.cfg.NodeID, r); err != nil {
			log.Printf("[%s] Runner control bind deferred until broker reconnect: %v", a.cfg.Brand.EdgeName, err)
		}
	}

	if !a.opts.DisableObservers {
		active := a.startObserversLocked(ctx, r)
		log.Printf("[%s] Active observers on %q (tags=%v, spool=%d): %v",
			a.cfg.Brand.EdgeName, a.cfg.NodeID, a.cfg.Tags, a.cfg.SpoolCapacity, active)
	}

	a.started = true
	a.starting = false
	close(startDone)
	a.mu.Unlock()

	return nil
}

func (a *Agent) startObserversLocked(ctx context.Context, r *relay.MQTTClient) []string {
	gatedPub := observer.NewGatedPublisher(r, a.pauseCtrl)
	obsCtx := observers.WithPollInterval(ctx, a.cfg.ParsedPollInterval())

	var active []string
	remCfg := a.cfg.Observers

	if remCfg.OS {
		if hostObs, ok := a.obsReg.Get("host-os"); ok {
			remCfg.OS = false
			active = append(active, "host-os")
			a.obsWG.Add(1)
			go func() {
				defer a.obsWG.Done()
				hostObs.Observe(obsCtx, a.cfg.NodeID, gatedPub)
			}()
		}
	}

	if remCfg.Net && remCfg.GPU && remCfg.NPU {
		if adaptersObs, ok := a.obsReg.Get("adapters"); ok {
			remCfg.Net = false
			remCfg.GPU = false
			remCfg.NPU = false
			active = append(active, "gpu", "npu", "net")
			a.obsWG.Add(1)
			go func() {
				defer a.obsWG.Done()
				adaptersObs.Observe(obsCtx, a.cfg.NodeID, gatedPub)
			}()
		}
	}

	if remCfg != (config.ObserversConfig{}) {
		launcher := a.startConfigured
		if launcher == nil {
			launcher = observers.StartConfiguredWithRegistryAndWaitGroup
		}
		subActive := launcher(ctx, a.cfg.NodeID, r, remCfg, a.cfg.ParsedPollInterval(), observer.NewRegistry(), &a.obsWG)
		active = append(active, subActive...)
	}

	for _, name := range a.obsReg.Names() {
		if name == "host-os" || name == "adapters" {
			continue
		}
		if custom, ok := a.obsReg.Get(name); ok {
			active = append(active, name)
			a.obsWG.Add(1)
			go func(obs observer.Observer) {
				defer a.obsWG.Done()
				obs.Observe(obsCtx, a.cfg.NodeID, gatedPub)
			}(custom)
		}
	}

	return active
}

// Publish emits a custom CTD payload through the agent's ring-buffered MQTT relay.
func (a *Agent) Publish(p *core.CTDPayload) error {
	a.mu.Lock()
	r := a.relay
	a.mu.Unlock()
	if r == nil {
		return nil
	}
	return r.Publish(p)
}

// Stop cancels observers/runners, joins observer and runner goroutines within bounded timeouts,
// and gracefully disconnects the MQTT relay.
func (a *Agent) Stop() {
	a.mu.Lock()
	if a.stopped {
		sd := a.stopDone
		a.mu.Unlock()
		if sd != nil {
			<-sd
		}
		return
	}
	a.stopped = true
	sd := make(chan struct{})
	a.stopDone = sd
	cancel := a.cancel
	starting := a.starting
	startDone := a.startDone
	a.mu.Unlock()

	defer close(sd)

	if cancel != nil {
		cancel()
	}
	if starting && startDone != nil {
		<-startDone
	}

	a.mu.Lock()
	r := a.relay
	a.mu.Unlock()

	obsDone := make(chan struct{})
	go func() {
		a.obsWG.Wait()
		close(obsDone)
	}()
	timer := time.NewTimer(stopWaitTimeout)
	select {
	case <-obsDone:
		timer.Stop()
	case <-timer.C:
		log.Printf("[%s] Timed out waiting for active observers to exit before disconnect", a.cfg.Brand.EdgeName)
	}

	if r != nil {
		if !r.WaitRunners(stopWaitTimeout) {
			log.Printf("[%s] Timed out waiting for active runners to flush bookends before disconnect", a.cfg.Brand.EdgeName)
		}
		r.Disconnect()
	}
}
