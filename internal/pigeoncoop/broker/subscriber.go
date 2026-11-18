package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	controlWorkerCount = 4
	controlQueueCap    = 256
	ntfyMaxConcurrency = 8
)

// Ledger defines the persistence & resolution contract required by PigeonCoop's broker router.
type Ledger interface {
	InsertEvent(ctx context.Context, event *core.CTDPayload) error
	BatchInsertEvents(ctx context.Context, payloads []*core.CTDPayload) (int64, error)
	UpsertNodeHello(ctx context.Context, hello core.NodeHello) ([]string, error)
	UpdateNodePresence(ctx context.Context, nodeID, status string) error
	ResolveNodes(ctx context.Context, sel core.RunnerSelector) ([]string, error)
	UpsertArtifact(ctx context.Context, a core.Artifact) error
	InsertTrace(ctx context.Context, t core.TraceEdge) error
}

// TransitionLedger is an optional extension allowing atomic presence transition detection so retained
// reconnect messages do not spam duplicate Ntfy alerts.
type TransitionLedger interface {
	UpdateNodePresenceTransition(ctx context.Context, nodeID, status string) (prevStatus string, changed bool, err error)
}

// SubscriberOptions configures Coop's MQTT ingestion, Ntfy alerting, TLS, and COPY batching parameters.
type SubscriberOptions struct {
	BrokerURL   string
	CoopNodeID  string
	Username    string
	Password    string
	TLSConfig   *tls.Config
	Ingest      ingest.Options
	NtfyEnabled bool
	NtfyURL     string
}

// Subscriber manages Coop's MQTT subscriptions, batched PostgreSQL COPY injection, and RunnerRequestCTD routing.
type Subscriber struct {
	client      mqtt.Client
	ledger      Ledger
	batcher     *ingest.Batcher
	coopNodeID  string
	ntfyEnabled bool
	ntfyURL     string
	ntfySem     chan struct{}
	ctrlQueue   chan func()
	ctrlStop    chan struct{}
	ctrlWG      sync.WaitGroup
	closeOnce   sync.Once
}

// NewSubscriber creates a new MQTT listener with default batching and Ntfy settings.
func NewSubscriber(brokerURL string, ledger Ledger) (*Subscriber, error) {
	return NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   brokerURL,
		CoopNodeID:  "coop",
		NtfyEnabled: true,
		NtfyURL:     "http://127.0.0.1:2586/carrier-pigeons",
	}, ledger)
}

// NewSubscriberWithOptions initializes Coop's batched ingestion engine and non-blocking control-plane worker pool.
func NewSubscriberWithOptions(cfg SubscriberOptions, ledger Ledger) (*Subscriber, error) {
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		cfg.BrokerURL = "tcp://127.0.0.1:1883"
	}
	if strings.TrimSpace(cfg.CoopNodeID) == "" {
		cfg.CoopNodeID = "coop"
	}

	sub := &Subscriber{
		ledger:      ledger,
		batcher:     ingest.NewBatcher(ledger, cfg.Ingest),
		coopNodeID:  cfg.CoopNodeID,
		ntfyEnabled: cfg.NtfyEnabled,
		ntfyURL:     cfg.NtfyURL,
		ntfySem:     make(chan struct{}, ntfyMaxConcurrency),
		ctrlQueue:   make(chan func(), controlQueueCap),
		ctrlStop:    make(chan struct{}),
	}

	for range controlWorkerCount {
		sub.ctrlWG.Add(1)
		go sub.controlWorker()
	}

	bSpec := brand.Active()
	telemetryTopic := bSpec.TelemetryWildcard()
	registryTopic := bSpec.RegistryWildcard()
	helloTopic := bSpec.HelloWildcard()
	runnerReqTopic := bSpec.RunnerRequestTopic()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)
	opts.SetClientID(fmt.Sprintf("%s-ingestor-%s-%d", strings.ToLower(bSpec.CoopName), cfg.CoopNodeID, os.Getpid()))
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}
	if cfg.TLSConfig != nil {
		opts.SetTLSConfig(cfg.TLSConfig)
	}
	opts.SetOrderMatters(false)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(3 * time.Second)
	opts.SetMaxReconnectInterval(15 * time.Second)

	opts.OnConnect = func(client mqtt.Client) {
		if tok := client.Subscribe(telemetryTopic, 1, sub.handleTelemetry); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", telemetryTopic, tok.Error())
		}
		if tok := client.Subscribe(helloTopic, 1, sub.enqueueControl(sub.processHelloUpdate)); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", helloTopic, tok.Error())
		}
		if tok := client.Subscribe(registryTopic, 1, sub.enqueueControl(sub.processRegistryUpdate)); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", registryTopic, tok.Error())
		}
		if tok := client.Subscribe(runnerReqTopic, 1, sub.enqueueControlWithClient(sub.processRunnerRequest)); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", runnerReqTopic, tok.Error())
		}

		log.Printf("[%s] Subscribed to telemetry (%s), registry (%s), & runner requests (%s)",
			bSpec.CoopName, telemetryTopic, registryTopic, runnerReqTopic)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(5*time.Second) && token.Error() != nil {
		close(sub.ctrlStop)
		sub.ctrlWG.Wait()
		sub.batcher.Close()
		return nil, token.Error()
	}

	sub.client = client
	return sub, nil
}

func (s *Subscriber) controlWorker() {
	defer s.ctrlWG.Done()
	for {
		select {
		case fn := <-s.ctrlQueue:
			if fn != nil {
				fn()
			}
		case <-s.ctrlStop:
			for {
				select {
				case fn := <-s.ctrlQueue:
					if fn != nil {
						fn()
					}
				default:
					return
				}
			}
		}
	}
}

func (s *Subscriber) enqueueControl(handler func(topic string, payload []byte)) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := append([]byte(nil), msg.Payload()...)
		select {
		case <-s.ctrlStop:
			return
		default:
		}
		select {
		case s.ctrlQueue <- func() { handler(topic, payload) }:
		default:
			log.Printf("[Coop Broker] Control worker queue saturated (%d); dropping control message on %s", controlQueueCap, topic)
		}
	}
}

func (s *Subscriber) enqueueControlWithClient(handler func(client mqtt.Client, topic string, payload []byte)) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := append([]byte(nil), msg.Payload()...)
		select {
		case <-s.ctrlStop:
			return
		default:
		}
		select {
		case s.ctrlQueue <- func() { handler(client, topic, payload) }:
		default:
			log.Printf("[Coop Broker] Control worker queue saturated (%d); dropping request on %s", controlQueueCap, topic)
		}
	}
}

// handleTelemetry decodes CTD wide events and pushes them into the non-blocking PostgreSQL COPY batcher.
func (s *Subscriber) handleTelemetry(_ mqtt.Client, msg mqtt.Message) {
	var event core.CTDPayload
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		log.Printf("[Coop Ingest] Malformed CTD payload on %s dropped: %v", msg.Topic(), err)
		return
	}
	s.batcher.Enqueue(&event)
}

// processRunnerRequest resolves a RunnerRequestCTD against node/tag selectors, records lineage, and fans out envelopes.
func (s *Subscriber) processRunnerRequest(client mqtt.Client, _ string, payload []byte) {
	var req core.RunnerRequestCTD
	if err := json.Unmarshal(payload, &req); err != nil {
		log.Printf("[Coop Router] Invalid RunnerRequestCTD JSON dropped: %v", err)
		return
	}
	if err := req.Validate(); err != nil {
		log.Printf("[Coop Router] RunnerRequestCTD validation failed: %v", err)
		return
	}

	action := req.Action
	if action == "" {
		action = "start"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	targets, err := s.ledger.ResolveNodes(ctx, req.Selectors)
	if err != nil {
		log.Printf("[Coop Router] Failed resolving selectors for request %s: %v", req.RequestID, err)
		return
	}

	reqArtifactID := "runner-request:" + req.RequestID
	_ = s.ledger.UpsertArtifact(ctx, core.Artifact{
		ID:     reqArtifactID,
		Kind:   "runner_request",
		NodeID: s.coopNodeID,
		Meta: map[string]string{
			"runner":       req.Runner,
			"action":       action,
			"requested_by": req.RequestedBy,
			"targets":      strings.Join(targets, ","),
		},
		CreatedAt: time.Now().UTC(),
	})

	reqEvent := core.NewCTDPayload(s.coopNodeID, "runner-router:"+req.Runner, "request-dispatched").
		WithPillar(core.PillarRunner).
		AddMetric("target_count", len(targets)).
		AddMetric("targets", targets).
		AddParam("request_id", req.RequestID).
		AddParam("action", action).
		AddParam("requested_by", req.RequestedBy)
	if len(targets) == 0 {
		reqEvent.UpdateStatus("request-empty")
	}
	s.batcher.Enqueue(reqEvent)

	env := runner.Envelope{
		RequestID: req.RequestID,
		Action:    action,
		Runner:    req.Runner,
		Params:    req.Params,
	}
	rawEnv, err := json.Marshal(env)
	if err != nil {
		return
	}

	pubTokens := make([]mqtt.Token, 0, len(targets))
	for _, targetNode := range targets {
		controlTopic := core.ControlTopic(targetNode)
		pubTokens = append(pubTokens, client.Publish(controlTopic, 1, false, rawEnv))
	}
	for i, targetNode := range targets {
		pubTokens[i].WaitTimeout(2 * time.Second)
		_ = s.ledger.InsertTrace(ctx, core.TraceEdge{
			SourceID: reqArtifactID,
			Verb:     "dispatched_to",
			TargetID: "node:" + targetNode,
			NodeID:   targetNode,
			RunID:    req.RequestID,
			Meta: map[string]string{
				"runner": req.Runner,
				"action": action,
			},
			Timestamp: time.Now().UTC(),
		})
	}

	log.Printf("[Coop Router] Dispatched %s (%s) request=%s by %q -> %d nodes (%v)",
		req.Runner, action, req.RequestID, req.RequestedBy, len(targets), targets)
}

// IngestedCount returns total CTD events flushed to PostgreSQL since startup.
func (s *Subscriber) IngestedCount() uint64 {
	if s.batcher == nil {
		return 0
	}
	return s.batcher.IngestedCount()
}

// Disconnect drains all pending control workers and CTD batches and cleanly closes the MQTT connection.
func (s *Subscriber) Disconnect() {
	s.closeOnce.Do(func() {
		if s.client != nil {
			s.client.Disconnect(250)
		}
		close(s.ctrlStop)
		s.ctrlWG.Wait()
		if s.batcher != nil {
			s.batcher.Close()
		}
	})
}
