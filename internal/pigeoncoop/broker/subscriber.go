package broker

import (
	"container/list"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	telemetryWorkerCount     = 4
	telemetryQueueCap        = 4096
	defaultTelemetryMaxBytes = 16 << 20 // 16 MiB decode queue byte budget
	controlWorkerCount       = 4
	controlQueueCap          = 256
	runnerReqWorkerCount     = 2
	runnerReqQueueCap        = 256
	traceInsertWorkers       = 8
	ntfyMaxConcurrency       = 8
	subscribeWaitTimeout     = 5 * time.Second
	subscribeRetryBase       = 250 * time.Millisecond
	subscribeRetryMax        = 5 * time.Second
	runnerPublishTimeout     = 3 * time.Second
	defaultMNSRefresh        = 1 * time.Hour
	defaultShutdownGrace     = 5 * time.Second
	shutdownPostCancelWait   = 500 * time.Millisecond
	defaultReqDedupMax       = 4096
	defaultReqDedupTTL       = 10 * time.Minute
	defaultStaleNodeInterval = 1 * time.Minute
	defaultStaleNodeThreshold = 3 * time.Minute
)

var subscriberClientSeq atomic.Uint64

type rawTelemetryMsg struct {
	topic         string
	payload       []byte
	reservedBytes int64
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

// ClusterStatusLedger is an optional extension enabling periodic reconciliation of stale ONLINE nodes.
type ClusterStatusLedger interface {
	GetClusterStatus(ctx context.Context, staleThreshold time.Duration) ([]db.NodeSummary, error)
}

// TraceBatchLedger is an optional extension enabling single-roundtrip trace batch persistence.
type TraceBatchLedger interface {
	InsertTraces(ctx context.Context, traces []core.TraceEdge) error
}

// SubscriberOptions configures Coop's MQTT ingestion, MNS announcement, Ntfy alerting, TLS, and COPY batching parameters.
type SubscriberOptions struct {
	BrokerURL          string
	MeshName           string
	CoopNodeID         string
	Username           string
	Password           string
	TLSConfig          *tls.Config
	Ingest             ingest.Options
	NtfyEnabled        bool
	NtfyURL            string
	Brand              brand.Spec
	MNSRefreshInterval time.Duration
	MaxTelemetryBytes  int64
	StaleNodeInterval  time.Duration
	StaleNodeThreshold time.Duration
}

type reqDedupEntry struct {
	key       string
	inFlight  bool
	expiresAt time.Time
	elem      *list.Element
}

type requestDedupCache struct {
	maxEntries int
	ttl        time.Duration
	entries    map[string]*reqDedupEntry
	order      *list.List // front = most recently used, back = oldest
}

func newRequestDedupCache(maxEntries int, ttl time.Duration) *requestDedupCache {
	if maxEntries <= 0 {
		maxEntries = defaultReqDedupMax
	}
	if ttl <= 0 {
		ttl = defaultReqDedupTTL
	}
	return &requestDedupCache{
		maxEntries: maxEntries,
		ttl:        ttl,
		entries:    make(map[string]*reqDedupEntry),
		order:      list.New(),
	}
}

func (c *requestDedupCache) pruneExpired(now time.Time) {
	for c.order.Len() > 0 {
		back := c.order.Back()
		ent := back.Value.(*reqDedupEntry)
		if ent.inFlight || now.Before(ent.expiresAt) {
			break
		}
		c.order.Remove(back)
		delete(c.entries, ent.key)
	}
}

func (c *requestDedupCache) reserve(key string, now time.Time) (*reqDedupEntry, bool) {
	c.pruneExpired(now)
	if ent, ok := c.entries[key]; ok {
		if ent.inFlight || now.Before(ent.expiresAt) {
			c.order.MoveToFront(ent.elem)
			return nil, false
		}
		c.order.Remove(ent.elem)
		delete(c.entries, key)
	}
	for c.order.Len() >= c.maxEntries {
		evicted := false
		for back := c.order.Back(); back != nil; back = back.Prev() {
			ent := back.Value.(*reqDedupEntry)
			if !ent.inFlight {
				c.order.Remove(back)
				delete(c.entries, ent.key)
				evicted = true
				break
			}
		}
		if !evicted {
			break
		}
	}
	ent := &reqDedupEntry{
		key:       key,
		inFlight:  true,
		expiresAt: now.Add(c.ttl),
	}
	ent.elem = c.order.PushFront(ent)
	c.entries[key] = ent
	return ent, true
}

func (c *requestDedupCache) complete(ent *reqDedupEntry, now time.Time) {
	if ent == nil {
		return
	}
	if cur, ok := c.entries[ent.key]; ok && cur == ent {
		ent.inFlight = false
		ent.expiresAt = now.Add(c.ttl)
		c.order.MoveToFront(ent.elem)
	}
}

func (c *requestDedupCache) release(ent *reqDedupEntry) {
	if ent == nil {
		return
	}
	if cur, ok := c.entries[ent.key]; ok && cur == ent {
		c.order.Remove(ent.elem)
		delete(c.entries, ent.key)
	}
}

// Subscriber manages Coop's MQTT subscriptions, batched PostgreSQL COPY injection, and RunnerRequestCTD routing.
type Subscriber struct {
	client             mqtt.Client
	ledger             Ledger
	batcher            *ingest.Batcher
	meshName           string
	coopNodeID         string
	brand              brand.Spec
	topicRoot          string
	ntfyEnabled        bool
	ntfyURL            string
	ntfySem            chan struct{}
	ntfyCtx            context.Context
	ntfyCancel         context.CancelFunc
	ntfyMu             sync.Mutex
	ntfyClosed         bool
	ntfyWG             sync.WaitGroup
	workCtx            context.Context
	workCancel         context.CancelFunc
	onConnectMu        sync.Mutex
	onConnectClosed    bool
	onConnectWG        sync.WaitGroup
	onConnectGen       atomic.Uint64
	mnsRefreshInterval time.Duration
	mnsStop            chan struct{}
	mnsWG              sync.WaitGroup
	reaperCtx          context.Context
	reaperCancel       context.CancelFunc
	reaperStop         chan struct{}
	reaperWG           sync.WaitGroup
	staleNodeInterval  time.Duration
	staleNodeThreshold time.Duration
	telemetryQueue     chan rawTelemetryMsg
	telemetryStop      chan struct{}
	telemetryWG        sync.WaitGroup
	telemetryDropped   atomic.Uint64
	telemetryBytes     atomic.Int64
	telemetryMaxBytes  int64
	ctrlQueues         []chan func()
	ctrlStop           chan struct{}
	ctrlWG             sync.WaitGroup
	controlDropped     atomic.Uint64
	runnerReqQueue     chan rawRunnerReqMsg
	runnerReqStop      chan struct{}
	runnerReqWG        sync.WaitGroup
	runnerReqDropped   atomic.Uint64
	runnerPubTimeout   time.Duration
	reqDedupMu         sync.Mutex
	reqDedup           *requestDedupCache
	reqDedupMax        int
	reqDedupTTL        time.Duration
	shutdownGrace      time.Duration
	closeOnce          sync.Once
}

type subTarget struct {
	topic   string
	handler mqtt.MessageHandler
}

func (s *Subscriber) handleOnConnect(client mqtt.Client, allSubs []subTarget, retryBase time.Duration, onReady func()) {
	s.onConnectMu.Lock()
	if s.onConnectClosed {
		s.onConnectMu.Unlock()
		return
	}
	s.onConnectWG.Add(1)
	s.onConnectMu.Unlock()
	defer s.onConnectWG.Done()
	if onReady != nil {
		defer onReady()
	}

	if retryBase <= 0 {
		retryBase = subscribeRetryBase
	}
	gen := s.onConnectGen.Add(1)
	pending := make([]subTarget, len(allSubs))
	copy(pending, allSubs)
	backoff := retryBase

	for len(pending) > 0 {
		if s.onConnectGen.Load() != gen || !client.IsConnectionOpen() {
			return
		}
		select {
		case <-s.mnsStop:
			return
		case <-s.workContext().Done():
			return
		default:
		}

		var failed []subTarget
		for _, subSpec := range pending {
			if s.onConnectGen.Load() != gen || !client.IsConnectionOpen() {
				return
			}
			select {
			case <-s.mnsStop:
				return
			case <-s.workContext().Done():
				return
			default:
			}
			if err := waitTokenTimeoutOrStop(client.Subscribe(subSpec.topic, 1, subSpec.handler), subscribeWaitTimeout, s.mnsStop); err != nil {
				log.Printf("[Coop Broker] Failed subscribing to %s: %v", subSpec.topic, err)
				failed = append(failed, subSpec)
			}
		}
		pending = failed
		if len(pending) == 0 {
			break
		}

		timer := time.NewTimer(backoff)
		select {
		case <-s.mnsStop:
			timer.Stop()
			return
		case <-s.workContext().Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > subscribeRetryMax {
			backoff = subscribeRetryMax
		}
	}

	if s.onConnectGen.Load() != gen || !client.IsConnectionOpen() {
		return
	}
	select {
	case <-s.mnsStop:
		return
	case <-s.workContext().Done():
		return
	default:
	}

	// Publish retained Mesh Name System (MNS) identity so workstation CLI/TUI handshakes
	// can verify the remote PigeonCoop's registered mesh name before switching profiles.
	s.publishMeshIdentity(client)
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

func waitTokenTimeoutOrStop(tok mqtt.Token, timeout time.Duration, stop <-chan struct{}) error {
	if stop == nil {
		return waitTokenTimeout(tok, timeout)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-tok.Done():
		return tok.Error()
	case <-stop:
		return context.Canceled
	case <-timer.C:
		return fmt.Errorf("mqtt operation timed out after %s", timeout)
	}
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
	workCtx, workCancel := context.WithCancel(context.Background())
	reaperCtx, reaperCancel := context.WithCancel(workCtx)
	mnsInterval := cfg.MNSRefreshInterval
	if mnsInterval <= 0 {
		mnsInterval = defaultMNSRefresh
	}
	maxTelemetryBytes := cfg.MaxTelemetryBytes
	if maxTelemetryBytes <= 0 {
		maxTelemetryBytes = defaultTelemetryMaxBytes
	}
	staleInterval := cfg.StaleNodeInterval
	if staleInterval <= 0 {
		staleInterval = defaultStaleNodeInterval
	}
	staleThreshold := cfg.StaleNodeThreshold
	if staleThreshold <= 0 {
		staleThreshold = defaultStaleNodeThreshold
	}

	ctrlQueues := make([]chan func(), controlWorkerCount)
	for i := range ctrlQueues {
		ctrlQueues[i] = make(chan func(), controlQueueCap)
	}

	sub := &Subscriber{
		ledger:             ledger,
		batcher:            ingest.NewBatcher(ledger, cfg.Ingest),
		meshName:           meshName,
		coopNodeID:         cfg.CoopNodeID,
		brand:              bSpec,
		topicRoot:          strings.Trim(bSpec.TopicRoot, "/"),
		ntfyEnabled:        cfg.NtfyEnabled,
		ntfyURL:            cfg.NtfyURL,
		ntfySem:            make(chan struct{}, ntfyMaxConcurrency),
		ntfyCtx:            ntfyCtx,
		ntfyCancel:         ntfyCancel,
		workCtx:            workCtx,
		workCancel:         workCancel,
		mnsRefreshInterval: mnsInterval,
		mnsStop:            make(chan struct{}),
		reaperCtx:          reaperCtx,
		reaperCancel:       reaperCancel,
		reaperStop:         make(chan struct{}),
		staleNodeInterval:  staleInterval,
		staleNodeThreshold: staleThreshold,
		telemetryQueue:     make(chan rawTelemetryMsg, telemetryQueueCap),
		telemetryStop:      make(chan struct{}),
		telemetryMaxBytes:  maxTelemetryBytes,
		ctrlQueues:         ctrlQueues,
		ctrlStop:           make(chan struct{}),
		runnerReqQueue:     make(chan rawRunnerReqMsg, runnerReqQueueCap),
		runnerReqStop:      make(chan struct{}),
		runnerPubTimeout:   runnerPublishTimeout,
		reqDedup:           newRequestDedupCache(defaultReqDedupMax, defaultReqDedupTTL),
		reqDedupMax:        defaultReqDedupMax,
		reqDedupTTL:        defaultReqDedupTTL,
		shutdownGrace:      defaultShutdownGrace,
	}

	for range telemetryWorkerCount {
		sub.telemetryWG.Add(1)
		go sub.telemetryWorker()
	}
	for _, shardQ := range sub.ctrlQueues {
		sub.ctrlWG.Add(1)
		go sub.controlWorker(shardQ)
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
	opts.SetClientID(fmt.Sprintf("%s-ingestor-%s-%d-%d", strings.ToLower(bSpec.CoopName), cfg.CoopNodeID, os.Getpid(), subscriberClientSeq.Add(1)))
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}
	if cfg.TLSConfig != nil {
		opts.SetTLSConfig(cfg.TLSConfig)
	}
	opts.SetOrderMatters(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(false)
	opts.SetConnectTimeout(5 * time.Second)
	opts.SetConnectRetryInterval(3 * time.Second)
	opts.SetMaxReconnectInterval(15 * time.Second)

	if lwtPayload, err := sub.marshalMeshOfflinePayload(); err == nil {
		opts.SetWill(meshTopic, string(lwtPayload), 1, true)
	}

	initReady := make(chan struct{})
	var initOnce sync.Once

	allSubs := []subTarget{
		{telemetryTopic, sub.handleTelemetry},
		{helloTopic, sub.enqueueControl(sub.processHelloUpdate)},
		{registryTopic, sub.enqueueControl(sub.processRegistryUpdate)},
		{runnerReqTopic, sub.handleRunnerRequest},
	}

	opts.OnConnect = func(client mqtt.Client) {
		sub.handleOnConnect(client, allSubs, subscribeRetryBase, func() {
			initOnce.Do(func() { close(initReady) })
		})
		log.Printf("[%s] Subscribed to telemetry (%s), registry (%s), & runner requests (%s) [mesh=%s]",
			bSpec.CoopName, telemetryTopic, registryTopic, runnerReqTopic, sub.meshName)
	}

	client := mqtt.NewClient(opts)
	if err := waitTokenTimeout(client.Connect(), 5*time.Second); err != nil {
		client.Disconnect(50)
		ntfyCancel()
		sub.ntfyMu.Lock()
		sub.ntfyClosed = true
		sub.ntfyMu.Unlock()
		sub.onConnectMu.Lock()
		sub.onConnectClosed = true
		sub.onConnectMu.Unlock()
		reaperCancel()
		workCancel()
		close(sub.mnsStop)
		close(sub.reaperStop)
		close(sub.telemetryStop)
		sub.telemetryWG.Wait()
		close(sub.ctrlStop)
		sub.ctrlWG.Wait()
		close(sub.runnerReqStop)
		sub.runnerReqWG.Wait()
		sub.onConnectWG.Wait()
		sub.ntfyWG.Wait()
		sub.batcher.Close()
		return nil, err
	}

	initTimer := time.NewTimer(5 * time.Second)
	select {
	case <-initReady:
		initTimer.Stop()
	case <-initTimer.C:
		log.Printf("[Coop Broker] Timed out waiting 5s for initial OnConnect subscriptions")
	}

	sub.client = client
	sub.mnsWG.Add(1)
	go sub.mnsRefreshLoop()
	sub.reaperWG.Add(1)
	go sub.reaperLoop()
	return sub, nil
}

func (s *Subscriber) workContext() context.Context {
	if s.workCtx != nil {
		return s.workCtx
	}
	return context.Background()
}

func (s *Subscriber) publishMeshIdentity(client mqtt.Client) {
	if client == nil {
		return
	}
	rawIdentity, err := json.Marshal(core.MeshIdentity{
		MeshName:   s.meshName,
		CoopNodeID: s.coopNodeID,
		TopicRoot:  s.topicRoot,
		UpdatedAt:  time.Now().UTC(),
	})
	if err != nil {
		return
	}
	meshTopic := s.brand.MeshTopic()
	if err := waitTokenTimeoutOrStop(client.Publish(meshTopic, 1, true, rawIdentity), 2*time.Second, s.mnsStop); err != nil && err != context.Canceled {
		log.Printf("[Coop Broker] Failed publishing retained MNS identity to %s: %v", meshTopic, err)
	}
}

func (s *Subscriber) mnsRefreshLoop() {
	defer s.mnsWG.Done()
	ticker := time.NewTicker(s.mnsRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.mnsStop:
			return
		case <-ticker.C:
			if s.client != nil && s.client.IsConnected() {
				s.publishMeshIdentity(s.client)
			}
		}
	}
}

func (s *Subscriber) telemetryWorker() {
	defer s.telemetryWG.Done()
	for {
		select {
		case m := <-s.telemetryQueue:
			if m.reservedBytes > 0 {
				s.telemetryBytes.Add(-m.reservedBytes)
			}
			s.processTelemetry(m.topic, m.payload)
		case <-s.telemetryStop:
			for {
				select {
				case m := <-s.telemetryQueue:
					if m.reservedBytes > 0 {
						s.telemetryBytes.Add(-m.reservedBytes)
					}
					s.processTelemetry(m.topic, m.payload)
				default:
					return
				}
			}
		}
	}
}

func (s *Subscriber) controlWorker(q chan func()) {
	defer s.ctrlWG.Done()
	for {
		select {
		case fn := <-q:
			if fn != nil {
				fn()
			}
		case <-s.ctrlStop:
			for {
				if s.workCtx != nil && s.workCtx.Err() != nil {
					return
				}
				select {
				case fn := <-q:
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
				if s.workCtx != nil && s.workCtx.Err() != nil {
					return
				}
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

func controlShardIndex(key string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shardCount))
}

func (s *Subscriber) controlShardKey(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) >= 3 && parts[0] == s.topicRoot && parts[1] == "registry" && parts[2] != "" {
		return parts[2]
	}
	return topic
}

func (s *Subscriber) enqueueControl(handler func(topic string, payload []byte)) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		select {
		case <-s.ctrlStop:
			return
		default:
		}
		if len(s.ctrlQueues) == 0 {
			return
		}
		shard := s.ctrlQueues[controlShardIndex(s.controlShardKey(topic), len(s.ctrlQueues))]
		payload := append([]byte(nil), msg.Payload()...)
		select {
		case shard <- func() { handler(topic, payload) }:
		default:
			n := s.controlDropped.Add(1)
			if n == 1 || n%500 == 0 {
				log.Printf("[Coop Broker] Control worker shard saturated (%d cap); dropped %d control messages (latest on %s)", cap(shard), n, topic)
			}
		}
	}
}

// handleRunnerRequest isolates RunnerRequestCTD dispatch onto a dedicated queue so hello/status floods
// on ctrlQueues cannot starve or drop operator runner start/abort commands.
func (s *Subscriber) handleRunnerRequest(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	select {
	case <-s.runnerReqStop:
		return
	default:
	}
	payload := append([]byte(nil), msg.Payload()...)
	select {
	case s.runnerReqQueue <- rawRunnerReqMsg{client: client, topic: topic, payload: payload}:
	default:
		n := s.runnerReqDropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Router] Runner request queue saturated (%d cap); dropped %d requests (latest on %s)", cap(s.runnerReqQueue), n, topic)
		}
	}
}

func (s *Subscriber) effectiveTelemetryMaxBytes() int64 {
	if s.telemetryMaxBytes > 0 {
		return s.telemetryMaxBytes
	}
	return defaultTelemetryMaxBytes
}

// handleTelemetry decouples MQTT reception from JSON envelope decoding and batcher ingestion
// so Paho callback dispatchers never block under burst load, while bounding buffered RAM.
func (s *Subscriber) handleTelemetry(_ mqtt.Client, msg mqtt.Message) {
	select {
	case <-s.telemetryStop:
		return
	default:
	}
	topic := msg.Topic()
	rawPayload := msg.Payload()
	msgBytes := int64(len(topic) + len(rawPayload))
	maxBytes := s.effectiveTelemetryMaxBytes()
	if msgBytes > 0 {
		for {
			cur := s.telemetryBytes.Load()
			if cur+msgBytes > maxBytes {
				n := s.telemetryDropped.Add(1)
				if n == 1 || n%500 == 0 {
					log.Printf("[Coop Ingest] Telemetry byte budget saturated (%d/%d bytes); dropped %d messages (latest on %s)",
						cur, maxBytes, n, topic)
				}
				return
			}
			if s.telemetryBytes.CompareAndSwap(cur, cur+msgBytes) {
				break
			}
		}
	}

	payload := append([]byte(nil), rawPayload...)
	select {
	case s.telemetryQueue <- rawTelemetryMsg{topic: topic, payload: payload, reservedBytes: msgBytes}:
	default:
		if msgBytes > 0 {
			s.telemetryBytes.Add(-msgBytes)
		}
		n := s.telemetryDropped.Add(1)
		if n == 1 || n%500 == 0 {
			log.Printf("[Coop Ingest] Telemetry decode queue saturated (%d cap); dropped %d messages (latest on %s)", cap(s.telemetryQueue), n, topic)
		}
	}
}

func isValidPillar(p core.Pillar) bool {
	switch p {
	case core.PillarObserver, core.PillarRunner, core.PillarTracer:
		return true
	default:
		return false
	}
}

func isReservedRunnerRequestArtifactID(id string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(id)), "runner-request:")
}

// processTelemetry validates topic hierarchy tokens, pillar enum, component parity, and artifact/trace
// ownership before decoding full metrics/params or pushing events into the PostgreSQL COPY batcher.
func (s *Subscriber) processTelemetry(topic string, payload []byte) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != s.topicRoot || parts[1] != "ctd" {
		log.Printf("[Coop Ingest] Malformed CTD topic %q dropped", topic)
		return
	}
	topicPillar, topicNodeID, topicComponent := parts[2], parts[3], parts[4]
	if !isValidPillar(core.Pillar(topicPillar)) {
		log.Printf("[Coop Ingest] Invalid topic pillar on %q dropped", topic)
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
	if !isValidPillar(event.Pillar) ||
		event.NodeID != topicNodeID ||
		string(event.Pillar) != topicPillar ||
		event.Component != topicComponent {
		log.Printf("[Coop Ingest] Spoofed/mismatched CTD payload on %s dropped (payload node_id=%q pillar=%q component=%q vs topic node_id=%q pillar=%q component=%q)",
			topic, event.NodeID, event.Pillar, event.Component, topicNodeID, topicPillar, topicComponent)
		return
	}
	if event.Artifact != nil {
		if event.Artifact.NodeID != topicNodeID || isReservedRunnerRequestArtifactID(event.Artifact.ID) {
			log.Printf("[Coop Ingest] Rejected invalid/spoofed CTD artifact on %s (artifact_id=%q artifact_node_id=%q vs topic_node_id=%q)",
				topic, event.Artifact.ID, event.Artifact.NodeID, topicNodeID)
			return
		}
	}
	if event.Trace != nil && event.Trace.NodeID != topicNodeID {
		log.Printf("[Coop Ingest] Rejected spoofed CTD trace edge on %s (trace_node_id=%q vs topic_node_id=%q)",
			topic, event.Trace.NodeID, topicNodeID)
		return
	}
	if s.batcher != nil {
		s.batcher.Enqueue(event)
	}
}

func (s *Subscriber) reserveRequestDedup(key string) (*reqDedupEntry, bool) {
	s.reqDedupMu.Lock()
	defer s.reqDedupMu.Unlock()
	if s.reqDedup == nil {
		s.reqDedup = newRequestDedupCache(s.reqDedupMax, s.reqDedupTTL)
	}
	return s.reqDedup.reserve(key, time.Now())
}

func (s *Subscriber) completeRequestDedup(ent *reqDedupEntry) {
	s.reqDedupMu.Lock()
	defer s.reqDedupMu.Unlock()
	if s.reqDedup != nil {
		s.reqDedup.complete(ent, time.Now())
	}
}

func (s *Subscriber) releaseRequestDedup(ent *reqDedupEntry) {
	s.reqDedupMu.Lock()
	defer s.reqDedupMu.Unlock()
	if s.reqDedup != nil {
		s.reqDedup.release(ent)
	}
}

func (s *Subscriber) recordDispatchedTraces(ctx context.Context, edges []core.TraceEdge) {
	if len(edges) == 0 {
		return
	}
	if bl, ok := s.ledger.(TraceBatchLedger); ok {
		if err := bl.InsertTraces(ctx, edges); err != nil {
			log.Printf("[Coop Router] Failed batch recording %d trace edges: %v", len(edges), err)
		}
		return
	}
	if len(edges) == 1 {
		if err := s.ledger.InsertTrace(ctx, edges[0]); err != nil {
			log.Printf("[Coop Router] Failed recording trace edge for request=%s target=%s: %v",
				edges[0].RunID, edges[0].NodeID, err)
		}
		return
	}

	workerCount := min(len(edges), traceInsertWorkers)
	var nextIdx atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				idx := int(nextIdx.Add(1) - 1)
				if idx >= len(edges) {
					return
				}
				edge := edges[idx]
				if err := s.ledger.InsertTrace(ctx, edge); err != nil {
					log.Printf("[Coop Router] Failed recording trace edge for request=%s target=%s: %v",
						edge.RunID, edge.NodeID, err)
				}
			}
		}()
	}
	wg.Wait()
}

// processRunnerRequest validates & deduplicates a RunnerRequestCTD, resolves selectors,
// fans out envelopes, and records artifacts, events, and traces against actual published targets.
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
	if err := core.ValidateTopicToken(req.RequestID); err != nil {
		log.Printf("[Coop Router] Invalid RunnerRequestCTD request_id dropped: %v", err)
		return
	}

	action := req.Action
	if action == "" {
		action = "start"
	}

	dedupEntry, ok := s.reserveRequestDedup(req.RequestID + "\x00" + action)
	if !ok {
		log.Printf("[Coop Router] Duplicate RunnerRequestCTD request_id=%s (%s) ignored", req.RequestID, action)
		return
	}

	env := runner.Envelope{
		RequestID: req.RequestID,
		Action:    action,
		Runner:    req.Runner,
		Params:    req.Params,
	}
	rawEnv, err := json.Marshal(env)
	if err != nil {
		s.releaseRequestDedup(dedupEntry)
		return
	}

	resolveCtx, resolveCancel := context.WithTimeout(s.workContext(), 5*time.Second)
	targets, err := s.ledger.ResolveNodes(resolveCtx, req.Selectors)
	resolveCancel()
	if err != nil {
		s.releaseRequestDedup(dedupEntry)
		log.Printf("[Coop Router] Failed resolving selectors for request %s: %v", req.RequestID, err)
		return
	}

	succeededTargets := make([]string, 0, len(targets))
	if len(targets) > 0 && client != nil {
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
	}

	s.completeRequestDedup(dedupEntry)

	reqArtifactID := "runner-request:" + req.RequestID
	artCtx, artCancel := context.WithTimeout(s.workContext(), 5*time.Second)
	if err := s.ledger.UpsertArtifact(artCtx, core.Artifact{
		ID:     reqArtifactID,
		Kind:   "runner_request",
		NodeID: s.coopNodeID,
		Meta: map[string]string{
			"runner":           req.Runner,
			"action":           action,
			"requested_by":     req.RequestedBy,
			"targets":          strings.Join(succeededTargets, ","),
			"resolved_targets": strings.Join(targets, ","),
		},
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		log.Printf("[Coop Router] Failed upserting request artifact %s: %v", reqArtifactID, err)
	}
	artCancel()

	reqEvent := core.NewCTDPayload(s.coopNodeID, "runner-router:"+req.Runner, "request-dispatched").
		WithPillar(core.PillarRunner).
		AddMetric("target_count", len(succeededTargets)).
		AddMetric("targets", succeededTargets).
		AddMetric("resolved_target_count", len(targets)).
		AddParam("request_id", req.RequestID).
		AddParam("action", action).
		AddParam("requested_by", req.RequestedBy)
	if len(targets) == 0 {
		reqEvent.UpdateStatus("request-empty")
	} else if len(succeededTargets) == 0 {
		reqEvent.UpdateStatus("request-failed")
	}
	if s.batcher != nil && s.workContext().Err() == nil {
		s.batcher.Enqueue(reqEvent)
	}

	if len(succeededTargets) > 0 {
		now := time.Now().UTC()
		edges := make([]core.TraceEdge, 0, len(succeededTargets))
		for _, targetNode := range succeededTargets {
			edges = append(edges, core.TraceEdge{
				SourceID: reqArtifactID,
				Verb:     "dispatched_to",
				TargetID: "node:" + targetNode,
				NodeID:   targetNode,
				RunID:    req.RequestID,
				Meta: map[string]string{
					"runner": req.Runner,
					"action": action,
				},
				Timestamp: now,
			})
		}
		traceCtx, traceCancel := context.WithTimeout(s.workContext(), 5*time.Second)
		s.recordDispatchedTraces(traceCtx, edges)
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

// BatcherDroppedCount returns total CTD events dropped by the COPY batcher due to queue saturation or flush failure.
func (s *Subscriber) BatcherDroppedCount() uint64 {
	if s.batcher == nil {
		return 0
	}
	return s.batcher.DroppedCount()
}

// TelemetryDroppedCount returns total telemetry messages dropped due to decode queue or byte-budget saturation.
func (s *Subscriber) TelemetryDroppedCount() uint64 {
	return s.telemetryDropped.Load()
}

// ControlDroppedCount returns total control/registry messages dropped due to control shard saturation.
func (s *Subscriber) ControlDroppedCount() uint64 {
	return s.controlDropped.Load()
}

// RunnerRequestDroppedCount returns total RunnerRequestCTD messages dropped due to runner request queue saturation.
func (s *Subscriber) RunnerRequestDroppedCount() uint64 {
	return s.runnerReqDropped.Load()
}

// Disconnect stops periodic loops, joins telemetry/control/runner workers (canceling workCtx on grace expiration)
// before waiting for Ntfy alerts, closing the COPY batcher, publishing OFFLINE MNS state, and disconnecting MQTT.
func (s *Subscriber) Disconnect() {
	s.closeOnce.Do(func() {
		if s.reaperCancel != nil {
			s.reaperCancel()
		}
		if s.reaperStop != nil {
			close(s.reaperStop)
		}
		s.reaperWG.Wait()

		if s.mnsStop != nil {
			close(s.mnsStop)
		}
		s.mnsWG.Wait()

		s.onConnectMu.Lock()
		s.onConnectClosed = true
		s.onConnectMu.Unlock()
		s.onConnectWG.Wait()

		if s.ntfyCancel != nil {
			s.ntfyCancel()
		}
		s.ntfyMu.Lock()
		s.ntfyClosed = true
		s.ntfyMu.Unlock()

		if s.telemetryStop != nil {
			close(s.telemetryStop)
		}
		if s.ctrlStop != nil {
			close(s.ctrlStop)
		}
		if s.runnerReqStop != nil {
			close(s.runnerReqStop)
		}
		s.telemetryWG.Wait()

		workersDone := make(chan struct{})
		go func() {
			s.ctrlWG.Wait()
			s.runnerReqWG.Wait()
			close(workersDone)
		}()

		grace := s.shutdownGrace
		if grace <= 0 {
			grace = defaultShutdownGrace
		}
		timer := time.NewTimer(grace)
		select {
		case <-workersDone:
			timer.Stop()
			if s.workCancel != nil {
				s.workCancel()
			}
		case <-timer.C:
			if s.workCancel != nil {
				s.workCancel()
			}
			settle := time.NewTimer(shutdownPostCancelWait)
			select {
			case <-workersDone:
				settle.Stop()
			case <-settle.C:
				log.Printf("[Coop Broker] Shutdown grace (%s) expired with DB workers still blocked; proceeding with teardown", grace)
			}
		}

		s.ntfyWG.Wait()
		if s.batcher != nil {
			s.batcher.Close()
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
	})
}
