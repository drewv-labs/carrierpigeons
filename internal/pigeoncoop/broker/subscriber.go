package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrier-pigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
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

// SubscriberOptions configures Coop's MQTT ingestion, Ntfy alerting, and COPY batching parameters.
type SubscriberOptions struct {
	BrokerURL   string
	CoopNodeID  string
	Username    string
	Password    string
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
	closeOnce   sync.Once
}

// NewSubscriber creates a new MQTT listener with default batching and Ntfy settings.
func NewSubscriber(brokerURL string, ledger Ledger) (*Subscriber, error) {
	return NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   brokerURL,
		CoopNodeID:  "coop",
		NtfyEnabled: true,
		NtfyURL:     "http://localhost:2586/carrier-pigeons",
	}, ledger)
}

// NewSubscriberWithOptions initializes Coop's batched ingestion engine and control-plane router.
func NewSubscriberWithOptions(cfg SubscriberOptions, ledger Ledger) (*Subscriber, error) {
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		cfg.BrokerURL = "tcp://localhost:1883"
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
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(3 * time.Second)
	opts.SetMaxReconnectInterval(15 * time.Second)

	opts.OnConnect = func(client mqtt.Client) {
		if tok := client.Subscribe(telemetryTopic, 1, sub.handleTelemetry); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", telemetryTopic, tok.Error())
		}
		if tok := client.Subscribe(helloTopic, 1, sub.handleHelloUpdate); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", helloTopic, tok.Error())
		}
		if tok := client.Subscribe(registryTopic, 1, sub.handleRegistryUpdate); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", registryTopic, tok.Error())
		}
		if tok := client.Subscribe(runnerReqTopic, 1, sub.handleRunnerRequest); tok.Wait() && tok.Error() != nil {
			log.Printf("[Coop Broker] Failed subscribing to %s: %v", runnerReqTopic, tok.Error())
		}

		log.Printf("[%s] Subscribed to telemetry (%s), registry (%s), & runner requests (%s)",
			bSpec.CoopName, telemetryTopic, registryTopic, runnerReqTopic)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(5*time.Second) && token.Error() != nil {
		sub.batcher.Close()
		return nil, token.Error()
	}

	sub.client = client
	return sub, nil
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

// handleRunnerRequest resolves a RunnerRequestCTD against node/tag selectors, records lineage, and fans out envelopes.
func (s *Subscriber) handleRunnerRequest(client mqtt.Client, msg mqtt.Message) {
	var req core.RunnerRequestCTD
	if err := json.Unmarshal(msg.Payload(), &req); err != nil {
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

	for _, targetNode := range targets {
		env := runner.Envelope{
			RequestID: req.RequestID,
			Action:    action,
			Runner:    req.Runner,
			Params:    req.Params,
		}
		rawEnv, err := json.Marshal(env)
		if err != nil {
			continue
		}

		controlTopic := core.ControlTopic(targetNode)
		tok := client.Publish(controlTopic, 1, false, rawEnv)
		tok.WaitTimeout(2 * time.Second)

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

// Disconnect drains all pending CTD batches and cleanly closes the MQTT connection.
func (s *Subscriber) Disconnect() {
	s.closeOnce.Do(func() {
		if s.client != nil {
			s.client.Disconnect(250)
		}
		if s.batcher != nil {
			s.batcher.Close()
		}
	})
}
