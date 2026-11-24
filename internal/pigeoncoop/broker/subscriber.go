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
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	telemetryWorkerCount = 4
	telemetryQueueCap    = 4096
	controlWorkerCount   = 4
	controlQueueCap      = 256
	runnerReqWorkerCount = 2
	runnerReqQueueCap    = 256
	ntfyMaxConcurrency   = 8
	subscribeWaitTimeout = 5 * time.Second
	runnerPublishTimeout = 3 * time.Second
)

type rawTelemetryMsg struct {
	topic   string
	payload []byte
}

type rawRunnerReqMsg struct {
	client  mqtt.Client
	topic   string
	payload []byte
}

// meshLWT is the retained payload published to <root>/registry/mesh on unexpected Coop
// disconnect (via MQTT LWT) or graceful shutdown so workstations never verify against a dead Coop.
type meshLWT struct {
	MeshName   string    `json:"mesh_name"`
	CoopNodeID string    `json:"coop_node_id"`
	TopicRoot  string    `json:"topic_root,omitempty"`
	Status     string    `json:"status"`
	UpdatedAt  time.Time `json:"updated_at"`
}

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

// SubscriberOptions configures Coop's MQTT ingestion, MNS announcement, Ntfy alerting, TLS, and COPY batching parameters.
type SubscriberOptions struct {
	BrokerURL   string
	MeshName    string
	CoopNodeID  string
	Username    string
	Password    string
	TLSConfig   *tls.Config
	Ingest      ingest.Options
	NtfyEnabled bool
	NtfyURL     string
	Brand       brand.Spec
}

// Subscriber manages Coop's MQTT subscriptions, batched PostgreSQL COPY injection, and RunnerRequestCTD routing.
type Subscriber struct {
	client           mqtt.Client
	ledger           Ledger
	batcher          *ingest.Batcher
	meshName         string
	coopNodeID       string
	brand            brand.Spec
	topicRoot        string
	ntfyEnabled      bool
	ntfyURL          string
	ntfySem          chan struct{}
	ntfyCtx          context.Context
	ntfyCancel       context.CancelFunc
	ntfyWG           sync.WaitGroup
	telemetryQueue   chan rawTelemetryMsg
	telemetryStop    chan struct{}
	telemetryWG      sync.WaitGroup
	telemetryDropped atomic.Uint64
	ctrlQueue        chan func()
	ctrlStop         chan struct{}
	ctrlWG           sync.WaitGroup
	controlDropped   atomic.Uint64
	runnerReqQueue   chan rawRunnerReqMsg
	runnerReqStop    chan struct{}
	runnerReqWG      sync.WaitGroup
	runnerReqDropped atomic.Uint64
	runnerPubTimeout time.Duration
	closeOnce        sync.Once
}

// NewSubscriber creates a new MQTT listener with default batching and Ntfy settings.
func NewSubscriber(brokerURL string, ledger Ledger) (*Subscriber, error) {
	return NewSubscriberWithOptions(SubscriberOptions{
		BrokerURL:   brokerURL,
		MeshName:    core.DefaultMeshName,
		CoopNodeID:  "coop",
		NtfyEnabled: true,
		NtfyURL:     "http://127.0.0.1:2586/carrier-pigeons",
	}, ledger)
}

func (s *Subscriber) marshalMeshOfflinePayload() ([]byte, error) {
	return json.Marshal(meshLWT{
		MeshName:   "",
		CoopNodeID: s.coopNodeID,
		TopicRoot:  s.topicRoot,
		Status:     "OFFLINE",
		UpdatedAt:  time.Now().UTC(),
	})
}

func waitTokenTimeout(tok mqtt.Token, timeout time.Duration) error {
	if !tok.WaitTimeout(timeout) {
		return fmt.Errorf("mqtt operation timed out after %s", timeout)
	}
	return tok.Error()
}

// NewSubscriberWithOptions initializes Coop's batched ingestion engine and non-blocking control-plane worker pool.
func NewSubscriberWithOptions(cfg SubscriberOptions, ledger Ledger) (*Subscriber, error) {
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		cfg.BrokerURL = "tcp://127.0.0.1:1883"
	}
	if strings.TrimSpace(cfg.CoopNodeID) == "" {
		cfg.CoopNodeID = "coop"
	}
	meshName := strings.TrimSpace(cfg.MeshName)
	if meshName == "" {
		meshName = core.DefaultMeshName
	}
	if err := core.ValidateMeshName(meshName); err != nil {
		return nil, fmt.Errorf("invalid subscriber mesh_name: %w", err)
	}

	bSpec := cfg.Brand
	if bSpec == (brand.Spec{}) {
		bSpec = brand.Active()
	}
	bSpec = bSpec.MergeWithDefaults()

	ntfyCtx, ntfyCancel := context.WithCancel(context.Background())

	sub := &Subscriber{
		ledger:           ledger,
		batcher:          ingest.NewBatcher(ledger, cfg.Ingest),
		meshName:         meshName,
		coopNodeID:       cfg.CoopNodeID,
		brand:            bSpec,
		topicRoot:        strings.Trim(bSpec.TopicRoot, "/"),
		ntfyEnabled:      cfg.NtfyEnabled,
		ntfyURL:          cfg.NtfyURL,
		ntfySem:          make(chan struct{}, ntfyMaxConcurrency),
		ntfyCtx:          ntfyCtx,
		ntfyCancel:       ntfyCancel,
		telemetryQueue:   make(chan rawTelemetryMsg, telemetryQueueCap),
		telemetryStop:    make(chan struct{}),
		ctrlQueue:        make(chan func(), controlQueueCap),
		ctrlStop:         make(chan struct{}),
		runnerReqQueue:   make(chan rawRunnerReqMsg, runnerReqQueueCap),
		runnerReqStop:    make(chan struct{}),
		runnerPubTimeout: runnerPublishTimeout,
	}

	for range telemetryWorkerCount {
		sub.telemetryWG.Add(1)
		go sub.telemetryWorker()
	}
	for range controlWorkerCount {
		sub.ctrlWG.Add(1)
		go sub.controlWorker()
	}
	for range runnerReqWorkerCount {
		sub.runnerReqWG.Add(1)
		go sub.runnerReqWorker()
	}

	telemetryTopic := bSpec.TelemetryWildcard()
	registryTopic := bSpec.RegistryWildcard()
	helloTopic := bSpec.HelloWildcard()
	runnerReqTopic := bSpec.RunnerRequestTopic()
	meshTopic := bSpec.MeshTopic()

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

	if lwtPayload, err := sub.marshalMeshOfflinePayload(); err == nil {
		opts.SetWill(meshTopic, string(lwtPayload), 1, true)
	}

	opts.OnConnect = func(client mqtt.Client) {
		for _, subSpec := range []struct {
			topic   string
			handler mqtt.MessageHandler
		}{
			{telemetryTopic, sub.handleTelemetry},
			{helloTopic, sub.enqueueControl(sub.processHelloUpdate)},
			{registryTopic, sub.enqueueControl(sub.processRegistryUpdate)},
			{runnerReqTopic, sub.handleRunnerRequest},
		} {
			if err := waitTokenTimeout(client.Subscribe(subSpec.topic, 1, subSpec.handler), subscribeWaitTimeout); err != nil {
				log.Printf("[Coop Broker] Failed subscribing to %s: %v", subSpec.topic, err)
			}
		}

		// Publish retained Mesh Name System (MNS) identity so workstation CLI/TUI handshakes
		// can verify the remote PigeonCoop's registered mesh name before switching profiles.
		rawIdentity, err := json.Marshal(core.MeshIdentity{
			MeshName:   sub.meshName,
			CoopNodeID: sub.coopNodeID,
			TopicRoot:  sub.topicRoot,
			UpdatedAt:  time.Now().UTC(),
		})
		if err == nil {
			if err := waitTokenTimeout(client.Publish(meshTopic, 1, true, rawIdentity), 2*time.Second); err != nil {
				log.Printf("[Coop Broker] Failed publishing retained MNS identity to %s: %v", meshTopic, err)
			}
		}

		log.Printf("[%s] Subscribed to telemetry (%s), registry (%s), & runner requests (%s) [mesh=%s]",
			bSpec.CoopName, telemetryTopic, registryTopic, runnerReqTopic, sub.meshName)
	}

	client := mqtt.NewClient(opts)
	if err := waitTokenTimeout(client.Connect(), 5*time.Second); err != nil {
		client.Disconnect(50)
		ntfyCancel()
		close(sub.telemetryStop)
		sub.telemetryWG.Wait()
		close(sub.ctrlStop)
		sub.ctrlWG.Wait()
		close(sub.runnerReqStop)
		sub.runnerReqWG.Wait()
		sub.ntfyWG.Wait()
		sub.batcher.Close()
		return nil, err
	}

	sub.client = client
	return sub, nil
}

func (s *Subscriber) telemetryWorker() {
	defer s.telemetryWG.Done()
	for {
		select {
		case m := <-s.telemetryQueue:
			s.processTelemetry(m.topic, m.payload)
		case <-s.telemetryStop:
			for {
				select {
				case m := <-s.telemetryQueue:
					s.processTelemetry(m.topic, m.payload)
				default:
					return
				}
			}
		}
	}
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

func (s *Subscriber) runnerReqWorker() {
	defer s.runnerReqWG.Done()
	for {
		select {
		case m := <-s.runnerReqQueue:
			s.processRunnerRequest(m.client, m.topic, m.payload)
		case <-s.runnerReqStop:
			for {
				select {
				case m := <-s.runnerReqQueue:
					s.processRunnerRequest(m.client, m.topic, m.payload)
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
			n := s.controlDropped.Add(1)
			if n == 1 || n%500 == 0 {
				log.Printf("[Coop Broker] Control worker queue saturated (%d cap); dropped %d control messages (latest on %s)", cap(s.ctrlQueue), n, topic)
			}
		}
	}
}

// handleRunnerRequest isolates RunnerRequestCTD dispatch onto a dedicated queue so hello/status floods
// on ctrlQueue cannot starve or drop operator runner start/abort commands.
func (s *Subscriber) handleRunnerRequest(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := append([]byte(nil), msg.Payload()...)
	select {
	case <-s.runnerReqStop:
		return
	default:
	}
	select {
	case s.runnerReqQueue <- rawRunnerReqMsg{client: client, topic: topic, payload: payload}:
	default:
		n := s.runnerReqDropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Router] Runner request queue saturated (%d cap); dropped %d requests (latest on %s)", cap(s.runnerReqQueue), n, topic)
		}
	}
}

// handleTelemetry decouples MQTT reception from JSON envelope decoding and batcher ingestion
// so Paho callback dispatchers never block under burst load.
func (s *Subscriber) handleTelemetry(_ mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := append([]byte(nil), msg.Payload()...)
	select {
	case <-s.telemetryStop:
		return
	default:
	}
	select {
	case s.telemetryQueue <- rawTelemetryMsg{topic: topic, payload: payload}:
	default:
		n := s.telemetryDropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Ingest] Telemetry decode queue saturated (%d cap); dropped %d messages (latest on %s)", cap(s.telemetryQueue), n, topic)
		}
	}
}

// processTelemetry validates topic hierarchy tokens and decodes CTD wire envelopes without parsing
// full metrics/params maps before pushing them into the PostgreSQL COPY batcher.
func (s *Subscriber) processTelemetry(topic string, payload []byte) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != s.topicRoot || parts[1] != "ctd" {
		log.Printf("[Coop Ingest] Malformed CTD topic %q dropped", topic)
		return
	}
	topicPillar, topicNodeID, topicComponent := parts[2], parts[3], parts[4]
	if err := core.ValidateTopicToken(topicPillar); err != nil {
		log.Printf("[Coop Ingest] Invalid topic pillar on %q dropped: %v", topic, err)
		return
	}
	if err := core.ValidateTopicToken(topicNodeID); err != nil {
		log.Printf("[Coop Ingest] Invalid topic node_id on %q dropped: %v", topic, err)
		return
	}
	if err := core.ValidateTopicToken(topicComponent); err != nil {
		log.Printf("[Coop Ingest] Invalid topic component on %q dropped: %v", topic, err)
		return
	}

	event, err := core.DecodeWireCTDPayload(payload)
	if err != nil {
		log.Printf("[Coop Ingest] Malformed CTD payload on %s dropped: %v", topic, err)
		return
	}
	if event.Pillar == "" {
		event.Pillar = core.PillarObserver
	}
	if event.NodeID != topicNodeID || string(event.Pillar) != topicPillar {
		log.Printf("[Coop Ingest] Spoofed/mismatched CTD payload on %s dropped (payload node_id=%q pillar=%q vs topic node_id=%q pillar=%q)",
			topic, event.NodeID, event.Pillar, topicNodeID, topicPillar)
		return
	}
	s.batcher.Enqueue(event)
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

	resolveCtx, resolveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	targets, err := s.ledger.ResolveNodes(resolveCtx, req.Selectors)
	if err != nil {
		resolveCancel()
		log.Printf("[Coop Router] Failed resolving selectors for request %s: %v", req.RequestID, err)
		return
	}

	reqArtifactID := "runner-request:" + req.RequestID
	if err := s.ledger.UpsertArtifact(resolveCtx, core.Artifact{
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
	}); err != nil {
		log.Printf("[Coop Router] Failed upserting request artifact %s: %v", reqArtifactID, err)
	}
	resolveCancel()

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
	if s.batcher != nil {
		s.batcher.Enqueue(reqEvent)
	}

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
		controlTopic := s.brand.ControlTopic(targetNode)
		pubTokens = append(pubTokens, client.Publish(controlTopic, 1, false, rawEnv))
	}

	pubBudget := s.runnerPubTimeout
	if pubBudget <= 0 {
		pubBudget = runnerPublishTimeout
	}
	pubDeadline := time.Now().Add(pubBudget)
	succeededTargets := make([]string, 0, len(targets))
	for i, targetNode := range targets {
		tok := pubTokens[i]
		completed := false
		rem := time.Until(pubDeadline)
		if rem <= 0 {
			select {
			case <-tok.Done():
				completed = true
			default:
			}
		} else {
			completed = tok.WaitTimeout(rem)
		}
		if !completed {
			log.Printf("[Coop Router] Publish timed out (%s fan-out deadline) for request=%s target=%s",
				pubBudget, req.RequestID, targetNode)
			continue
		}
		if err := tok.Error(); err != nil {
			log.Printf("[Coop Router] Failed publishing request=%s to target=%s: %v",
				req.RequestID, targetNode, err)
			continue
		}
		succeededTargets = append(succeededTargets, targetNode)
	}

	if len(succeededTargets) > 0 {
		traceCtx, traceCancel := context.WithTimeout(context.Background(), 5*time.Second)
		for _, targetNode := range succeededTargets {
			if err := s.ledger.InsertTrace(traceCtx, core.TraceEdge{
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
			}); err != nil {
				log.Printf("[Coop Router] Failed recording trace edge for request=%s target=%s: %v",
					req.RequestID, targetNode, err)
			}
		}
		traceCancel()
	}

	log.Printf("[Coop Router] Dispatched %s (%s) request=%s by %q -> %d/%d nodes (%v)",
		req.Runner, action, req.RequestID, req.RequestedBy, len(succeededTargets), len(targets), succeededTargets)
}

// IngestedCount returns total CTD events flushed to PostgreSQL since startup.
func (s *Subscriber) IngestedCount() uint64 {
	if s.batcher == nil {
		return 0
	}
	return s.batcher.IngestedCount()
}

// TelemetryDroppedCount returns total telemetry messages dropped due to decode queue saturation.
func (s *Subscriber) TelemetryDroppedCount() uint64 {
	return s.telemetryDropped.Load()
}

// ControlDroppedCount returns total control/registry messages dropped due to control queue saturation.
func (s *Subscriber) ControlDroppedCount() uint64 {
	return s.controlDropped.Load()
}

// RunnerRequestDroppedCount returns total RunnerRequestCTD messages dropped due to runner request queue saturation.
func (s *Subscriber) RunnerRequestDroppedCount() uint64 {
	return s.runnerReqDropped.Load()
}

// Disconnect drains all pending telemetry/control/runner workers, Ntfy alerts, and CTD batches
// and cleanly marks the retained MNS topic offline before closing the MQTT connection.
func (s *Subscriber) Disconnect() {
	s.closeOnce.Do(func() {
		if s.ntfyCancel != nil {
			s.ntfyCancel()
		}
		if s.client != nil {
			if s.client.IsConnected() && s.topicRoot != "" {
				if rawLWT, err := s.marshalMeshOfflinePayload(); err == nil {
					if err := waitTokenTimeout(s.client.Publish(s.brand.MeshTopic(), 1, true, rawLWT), 1*time.Second); err != nil {
						log.Printf("[Coop Broker] Failed publishing offline MNS state to %s: %v", s.brand.MeshTopic(), err)
					}
				}
			}
			s.client.Disconnect(250)
		}
		if s.telemetryStop != nil {
			close(s.telemetryStop)
		}
		s.telemetryWG.Wait()
		if s.ctrlStop != nil {
			close(s.ctrlStop)
		}
		s.ctrlWG.Wait()
		if s.runnerReqStop != nil {
			close(s.runnerReqStop)
		}
		s.runnerReqWG.Wait()
		s.ntfyWG.Wait()
		if s.batcher != nil {
			s.batcher.Close()
		}
	})
}
