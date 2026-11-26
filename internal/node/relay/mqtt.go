package relay

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	maxPipelinedPublishes        = 64
	maxMQTTPayloadBytes          = 1 << 20 // 1 MiB
	maxPublishAttempts           = 5
	defaultDeadTransportTimeouts = 2
	defaultBatchTimeout          = 5 * time.Second
	defaultBackoffBase           = 500 * time.Millisecond
	defaultBackoffMax            = 30 * time.Second
	defaultSubscribeWait         = 5 * time.Second
)

var errRelayClosed = errors.New("relay closed")

// noopStore disables Paho's internal MemoryStore so RingSpool remains the sole bounded retry buffer
// and Paho's reconnect resume path cannot retain unbounded memory or replay duplicate QoS-1 packets.
type noopStore struct{}

func (noopStore) Open()                                 {}
func (noopStore) Put(string, packets.ControlPacket)     {}
func (noopStore) Get(string) packets.ControlPacket      { return nil }
func (noopStore) All() []string                         { return nil }
func (noopStore) Del(string)                            {}
func (noopStore) Close()                                {}
func (noopStore) Reset()                                {}

// Options configures a node's MQTT relay, ring-buffer spool, LWT presence, TLS, and NodeHello metadata.
type Options struct {
	BrokerURL       string
	NodeID          string
	ClientPrefix    string
	TopicPrefix     string
	Brand           brand.Spec
	SpoolCapacity   int
	MaxSpoolBytes   int64
	BootTags        []string
	Username        string
	Password        string
	TLSConfig       *tls.Config
	Meta            map[string]string
	RunnerPolicy    runner.ExecutionPolicy
	RunnerRegistry  *runner.Registry
	PauseController *observer.PauseController
}

// MQTTClient manages the MQTT connection, LWT presence, NodeHello boot announcement, and variable ring spool.
type MQTTClient struct {
	client       mqtt.Client
	nodeID       string
	topicPrefix  string
	brand        brand.Spec
	bootTags     []string
	meta         map[string]string
	runnerPolicy runner.ExecutionPolicy
	runnerReg    *runner.Registry
	pauseCtrl    *observer.PauseController
	spool        *RingSpool
	stopCh       chan struct{}
	flushDone    chan struct{}
	stopOnce     sync.Once

	connMu      sync.Mutex
	connectedCh chan struct{}
	connGen     atomic.Uint64

	onConnectMu sync.Mutex
	onConnectWG sync.WaitGroup
	subWG       sync.WaitGroup

	subMu        sync.RWMutex
	onConnectSub func(mqtt.Client, uint64, <-chan struct{})

	runnerMu       sync.Mutex
	runnersStopped bool
	runnerWG       sync.WaitGroup

	deliveryDropped atomic.Uint64
	backoffAttempt  atomic.Int64

	batchTimeout          time.Duration
	backoffBase           time.Duration
	backoffMax            time.Duration
	maxAttempts           int
	deadTransportTimeouts int
	subscribeTimeout      time.Duration
}

// NewMQTTClient initializes the broker connection with default CTD topic routing.
func NewMQTTClient(brokerURL, nodeID string) *MQTTClient {
	return NewMQTTClientWithOptions(Options{
		BrokerURL:     brokerURL,
		NodeID:        nodeID,
		TopicPrefix:   brand.Active().CTDPrefix(),
		SpoolCapacity: DefaultSpoolCapacity,
		MaxSpoolBytes: DefaultMaxSpoolBytes,
		RunnerPolicy:  runner.DefaultExecutionPolicy(),
	})
}

// NewMQTTClientWithPrefix initializes the broker connection, LWT will, and background FIFO flusher.
func NewMQTTClientWithPrefix(brokerURL, nodeID, topicPrefix string) *MQTTClient {
	return NewMQTTClientWithOptions(Options{
		BrokerURL:     brokerURL,
		NodeID:        nodeID,
		TopicPrefix:   topicPrefix,
		SpoolCapacity: DefaultSpoolCapacity,
		MaxSpoolBytes: DefaultMaxSpoolBytes,
		RunnerPolicy:  runner.DefaultExecutionPolicy(),
	})
}

// NewMQTTClientWithOptions initializes the broker connection with configurable spool sizing, TLS, and NodeHello tags.
func NewMQTTClientWithOptions(cfg Options) *MQTTClient {
	return newMQTTClientWithOptions(cfg, nil)
}

func newMQTTClientWithOptions(cfg Options, openConn func(*url.URL, mqtt.ClientOptions) (net.Conn, error)) *MQTTClient {
	bSpec := cfg.Brand
	if bSpec == (brand.Spec{}) {
		bSpec = brand.Active()
	}
	bSpec = bSpec.MergeWithDefaults()

	if strings.TrimSpace(cfg.BrokerURL) == "" {
		cfg.BrokerURL = "tcp://127.0.0.1:1883"
	}
	if strings.TrimSpace(cfg.TopicPrefix) == "" {
		cfg.TopicPrefix = bSpec.CTDPrefix()
	}
	clientPrefix := strings.TrimSpace(cfg.ClientPrefix)
	if clientPrefix == "" {
		clientPrefix = strings.ToLower(bSpec.EdgeName)
	}
	if cfg.SpoolCapacity <= 0 {
		cfg.SpoolCapacity = DefaultSpoolCapacity
	}
	if cfg.MaxSpoolBytes <= 0 {
		cfg.MaxSpoolBytes = DefaultMaxSpoolBytes
	}
	pauseCtrl := cfg.PauseController
	if pauseCtrl == nil {
		pauseCtrl = observer.NewPauseController()
	}

	c := &MQTTClient{
		nodeID:       cfg.NodeID,
		topicPrefix:  cfg.TopicPrefix,
		brand:        bSpec,
		bootTags:     core.NormalizeTags(cfg.BootTags),
		meta:         maps.Clone(cfg.Meta),
		runnerPolicy: cfg.RunnerPolicy,
		runnerReg:    cfg.RunnerRegistry,
		pauseCtrl:    pauseCtrl,
		spool:        NewRingSpoolWithByteLimit(cfg.SpoolCapacity, cfg.MaxSpoolBytes),
		stopCh:       make(chan struct{}),
		flushDone:    make(chan struct{}),
		connectedCh:  make(chan struct{}),
	}

	registryTopic := bSpec.RegistryTopic(cfg.NodeID)
	helloTopic := bSpec.HelloTopic(cfg.NodeID)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)
	opts.SetClientID(clientPrefix + "-" + cfg.NodeID)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}
	if cfg.TLSConfig != nil {
		opts.SetTLSConfig(cfg.TLSConfig)
	}
	if openConn != nil {
		opts.SetCustomOpenConnectionFn(openConn)
	}
	opts.SetOrderMatters(false)
	opts.SetCleanSession(true)
	opts.SetStore(noopStore{})
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetWriteTimeout(5 * time.Second)
	opts.SetKeepAlive(10 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	retryJitter := time.Duration(rand.Int64N(int64(2*time.Second) + 1))
	maxReconnJitter := time.Duration(rand.Int64N(int64(5*time.Second) + 1))
	opts.SetConnectRetryInterval(3*time.Second + retryJitter)
	opts.SetMaxReconnectInterval(15*time.Second + maxReconnJitter)
	opts.SetWill(registryTopic, "OFFLINE", 1, true)

	opts.OnConnect = func(client mqtt.Client) {
		c.onConnectMu.Lock()
		select {
		case <-c.stopCh:
			c.onConnectMu.Unlock()
			return
		default:
		}
		c.onConnectWG.Add(1)
		c.onConnectMu.Unlock()
		defer c.onConnectWG.Done()

		select {
		case <-c.stopCh:
			return
		default:
		}
		// 1. Announce node ONLINE status (retained)
		client.Publish(registryTopic, 1, true, []byte("ONLINE"))

		select {
		case <-c.stopCh:
			return
		default:
		}
		// 2. Announce hardware capabilities & boot tags via NodeHello (retained)
		hello := core.NodeHello{
			NodeID:    cfg.NodeID,
			Arch:      runtime.GOARCH,
			OS:        runtime.GOOS,
			NumCPU:    runtime.NumCPU(),
			BootTags:  c.bootTags,
			Meta:      maps.Clone(c.meta),
			Timestamp: time.Now().UTC(),
		}
		if rawHello, err := json.Marshal(hello); err == nil {
			client.Publish(helloTopic, 1, true, rawHello)
		}

		select {
		case <-c.stopCh:
			return
		default:
		}
		// 3. Advance connection generation and wake flushLoop / backoff waiters
		gen, nextConnCh := c.notifyConnected()

		// 4. Re-bind control subscriptions across broker restarts
		c.subMu.RLock()
		subFn := c.onConnectSub
		c.subMu.RUnlock()
		if subFn != nil {
			subFn(client, gen, nextConnCh)
		}
	}

	client := mqtt.NewClient(opts)
	c.client = client

	if token := client.Connect(); !token.WaitTimeout(3 * time.Second) {
		log.Printf("[Relay] Broker connect timed out after 3s on boot, spooling up to %d events (%d MiB cap)",
			c.spool.Cap(), c.spool.MaxBytes()>>20)
	} else if err := token.Error(); err != nil {
		log.Printf("[Relay] Broker unreachable on boot, spooling up to %d events (%d MiB cap): %v",
			c.spool.Cap(), c.spool.MaxBytes()>>20, err)
	}

	go c.flushLoop()

	return c
}

// SpoolLen returns the number of queued CTD events awaiting broker delivery.
func (c *MQTTClient) SpoolLen() int {
	return c.spool.Len()
}

// SpoolBytes returns the serialized byte size of queued CTD events currently buffered in the spool.
func (c *MQTTClient) SpoolBytes() int64 {
	return c.spool.Bytes()
}

// SpoolDropped returns the total number of evicted events when the ring buffer saturated.
func (c *MQTTClient) SpoolDropped() uint64 {
	return c.spool.Dropped()
}

func (c *MQTTClient) isConnected() bool {
	return c.client != nil && c.client.IsConnected() && c.client.IsConnectionOpen()
}

func (c *MQTTClient) notifyConnected() (uint64, <-chan struct{}) {
	c.connMu.Lock()
	if c.connectedCh != nil {
		close(c.connectedCh)
	}
	nextCh := make(chan struct{})
	c.connectedCh = nextCh
	gen := c.connGen.Add(1)
	c.connMu.Unlock()
	return gen, nextCh
}

func (c *MQTTClient) currentConnectedCh() <-chan struct{} {
	c.connMu.Lock()
	if c.connectedCh == nil {
		c.connectedCh = make(chan struct{})
	}
	ch := c.connectedCh
	c.connMu.Unlock()
	return ch
}

func (c *MQTTClient) connectionState() (uint64, <-chan struct{}) {
	c.connMu.Lock()
	if c.connectedCh == nil {
		c.connectedCh = make(chan struct{})
	}
	ch := c.connectedCh
	gen := c.connGen.Load()
	c.connMu.Unlock()
	return gen, ch
}

func (c *MQTTClient) waitForConnected() bool {
	for {
		if c.stopCh != nil {
			select {
			case <-c.stopCh:
				return false
			default:
			}
		}
		if c.isConnected() {
			return true
		}
		ch := c.currentConnectedCh()
		if c.isConnected() {
			return true
		}
		select {
		case <-c.stopCh:
			return false
		case <-ch:
		}
	}
}

// DeliveryDropped returns the cumulative number of payloads dropped due to marshaling failures,
// oversized MQTT packets (>1 MiB), repeated publish failures while connected, post-close publishes, or shutdown timeouts.
func (c *MQTTClient) DeliveryDropped() uint64 {
	return c.deliveryDropped.Load()
}

// DeliveryDroppedCount returns the cumulative number of payloads dropped during delivery or shutdown drain.
func (c *MQTTClient) DeliveryDroppedCount() uint64 {
	return c.deliveryDropped.Load()
}

func (c *MQTTClient) effectiveBatchTimeout() time.Duration {
	if c.batchTimeout > 0 {
		return c.batchTimeout
	}
	return defaultBatchTimeout
}

func (c *MQTTClient) effectiveBackoffBase() time.Duration {
	if c.backoffBase > 0 {
		return c.backoffBase
	}
	return defaultBackoffBase
}

func (c *MQTTClient) effectiveBackoffMax() time.Duration {
	if c.backoffMax > 0 {
		return c.backoffMax
	}
	return defaultBackoffMax
}

func (c *MQTTClient) effectiveMaxAttempts() int {
	if c.maxAttempts > 0 {
		return c.maxAttempts
	}
	return maxPublishAttempts
}

func (c *MQTTClient) effectiveDeadTransportTimeouts() int {
	if c.deadTransportTimeouts > 0 {
		return c.deadTransportTimeouts
	}
	return defaultDeadTransportTimeouts
}

func (c *MQTTClient) effectiveSubscribeTimeout() time.Duration {
	if c.subscribeTimeout > 0 {
		return c.subscribeTimeout
	}
	return defaultSubscribeWait
}

func (c *MQTTClient) recordDeliveryDrop(reason string, p *core.CTDPayload, err error) {
	dropped := c.deliveryDropped.Add(1)
	if dropped == 1 || dropped%100 == 0 {
		comp := ""
		if p != nil {
			comp = p.Component
		}
		log.Printf("[Relay] Dropped CTD payload (%s, component=%q, total_dropped=%d): %v", reason, comp, dropped, err)
	}
}

func (c *MQTTClient) nextBackoff(attempt int) time.Duration {
	base := c.effectiveBackoffBase()
	maxDur := c.effectiveBackoffMax()
	if maxDur < base {
		maxDur = base
	}
	delay := base
	for range attempt {
		if delay >= maxDur/2 {
			delay = maxDur
			break
		}
		delay *= 2
	}
	if delay > maxDur {
		delay = maxDur
	}
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}

func (c *MQTTClient) waitRetryBackoff() bool {
	attempt := int(c.backoffAttempt.Add(1) - 1)
	sleepDur := c.nextBackoff(attempt)
	connCh := c.currentConnectedCh()
	timer := time.NewTimer(sleepDur)
	defer timer.Stop()
	select {
	case <-c.stopCh:
		return false
	case <-connCh:
		c.backoffAttempt.Store(0)
		return true
	case <-timer.C:
		return true
	}
}

func (c *MQTTClient) forceReconnect() {
	if c.client == nil {
		return
	}
	select {
	case <-c.stopCh:
		return
	default:
	}

	if !c.isConnected() {
		return
	}

	log.Printf("[Relay] Dead transport detected across consecutive full-batch timeouts (0 acks); forcing MQTT reconnect")
	c.client.Disconnect(50)

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}
		tok := c.client.Connect()
		if tok == nil {
			return
		}
		select {
		case <-tok.Done():
			if err := tok.Error(); err != nil && time.Now().Before(deadline) {
				timer := time.NewTimer(20 * time.Millisecond)
				select {
				case <-c.stopCh:
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}
		default:
		}
		return
	}
}

// Publish enqueues the payload into the ring-buffer spool without ever blocking observers.
func (c *MQTTClient) Publish(payload *core.CTDPayload) error {
	if payload == nil {
		return nil
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		c.recordDeliveryDrop("marshal error", payload, err)
		return err
	}
	if len(raw) > maxMQTTPayloadBytes {
		sizeErr := fmt.Errorf("%d bytes > %d", len(raw), maxMQTTPayloadBytes)
		c.recordDeliveryDrop("payload exceeds max MQTT packet size", payload, sizeErr)
		return sizeErr
	}

	select {
	case <-c.stopCh:
		c.recordDeliveryDrop("relay closed", payload, errRelayClosed)
		return errRelayClosed
	default:
	}

	evicted, accepted := c.spool.PushSized(payload, int64(len(raw)))
	if !accepted {
		if c.spool.IsClosed() {
			c.recordDeliveryDrop("relay closed", payload, errRelayClosed)
			return errRelayClosed
		}
		capErr := fmt.Errorf("%d bytes > %d max spool bytes", len(raw), c.spool.MaxBytes())
		c.recordDeliveryDrop("payload exceeds max spool bytes", payload, capErr)
		return capErr
	}
	if evicted {
		dropped := c.spool.Dropped()
		if dropped == 1 || dropped%100 == 0 {
			log.Printf("[Relay] Ring spool saturated (%d slots / %d bytes max); evicted %d oldest telemetry events.",
				c.spool.Cap(), c.spool.MaxBytes(), dropped)
		}
	}

	return nil
}

// flushLoop waits for an active MQTT connection, pulls micro-batches from the ring buffer,
// and pipelines QoS 1 MQTT publishes to eliminate WAN RTT bottlenecks.
func (c *MQTTClient) flushLoop() {
	defer close(c.flushDone)
	for {
		if !c.waitForConnected() {
			return
		}
		batch, ok := c.spool.PopBatchBlocking(maxPipelinedPublishes, c.stopCh)
		if !ok {
			return
		}
		c.deliverBatchWithRetry(batch)
	}
}

func (c *MQTTClient) deliverBatchWithRetry(batch []*core.CTDPayload) {
	type pendingItem struct {
		payload  *core.CTDPayload
		topic    string
		raw      []byte
		failures int
	}
	pending := make([]pendingItem, 0, len(batch))
	for _, p := range batch {
		if p == nil {
			continue
		}
		raw, err := json.Marshal(p)
		if err != nil {
			c.recordDeliveryDrop("marshal error", p, err)
			continue
		}
		if len(raw) > maxMQTTPayloadBytes {
			c.recordDeliveryDrop("payload exceeds max MQTT packet size", p, fmt.Errorf("%d bytes > %d", len(raw), maxMQTTPayloadBytes))
			continue
		}
		pending = append(pending, pendingItem{
			payload: p,
			topic:   p.RoutingTopic(c.topicPrefix),
			raw:     raw,
		})
	}
	if len(pending) == 0 {
		return
	}

	requeueItems := func(items []pendingItem) {
		if len(items) == 0 {
			return
		}
		payloads := make([]*core.CTDPayload, 0, len(items))
		for _, item := range items {
			payloads = append(payloads, item.payload)
		}
		c.spool.PrependBatch(payloads)
	}

	maxAttempts := c.effectiveMaxAttempts()
	deadTransportThreshold := c.effectiveDeadTransportTimeouts()
	consecutiveFullTimeouts := 0
	batchHadSuccess := false

	for len(pending) > 0 {
		select {
		case <-c.stopCh:
			// Prepend undelivered items in FIFO order for best-effort final drain in Disconnect().
			requeueItems(pending)
			return
		default:
		}

		if !c.isConnected() {
			requeueItems(pending)
			return
		}

		tokens := make([]mqtt.Token, len(pending))
		for i, item := range pending {
			tokens[i] = c.client.Publish(item.topic, 1, false, item.raw)
		}

		type failedItem struct {
			item        pendingItem
			explicitErr bool
		}

		timer := time.NewTimer(c.effectiveBatchTimeout())
		var failed []failedItem
		succeeded := 0
		stopped := false
		timedOut := false
		hasExplicitErr := false
		stopIdx := 0

		for i, tok := range tokens {
			if timedOut {
				select {
				case <-tok.Done():
					if err := tok.Error(); err != nil {
						log.Printf("[Relay] Flight error publishing to %s, retrying: %v", pending[i].topic, err)
						failed = append(failed, failedItem{item: pending[i], explicitErr: true})
						hasExplicitErr = true
					} else {
						succeeded++
					}
				default:
					failed = append(failed, failedItem{item: pending[i], explicitErr: false})
				}
				continue
			}
			select {
			case <-c.stopCh:
				stopped = true
				stopIdx = i
			case <-tok.Done():
				if err := tok.Error(); err != nil {
					log.Printf("[Relay] Flight error publishing to %s, retrying: %v", pending[i].topic, err)
					failed = append(failed, failedItem{item: pending[i], explicitErr: true})
					hasExplicitErr = true
				} else {
					succeeded++
				}
			case <-timer.C:
				timedOut = true
				log.Printf("[Relay] Batch publish timed out waiting for %s (%d/%d remaining)", pending[i].topic, len(tokens)-i, len(tokens))
				failed = append(failed, failedItem{item: pending[i], explicitErr: false})
			}
			if stopped {
				break
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		if stopped {
			requeue := make([]pendingItem, 0, len(failed)+len(pending)-stopIdx)
			for _, f := range failed {
				requeue = append(requeue, f.item)
			}
			for j := stopIdx; j < len(pending); j++ {
				select {
				case <-tokens[j].Done():
					if tokens[j].Error() == nil {
						continue
					}
				default:
				}
				requeue = append(requeue, pending[j])
			}
			requeueItems(requeue)
			return
		}

		if len(failed) == 0 {
			c.backoffAttempt.Store(0)
			return
		}

		if succeeded > 0 {
			c.backoffAttempt.Store(0)
			consecutiveFullTimeouts = 0
			batchHadSuccess = true
		}

		if !c.isConnected() {
			requeue := make([]pendingItem, len(failed))
			for i, f := range failed {
				requeue[i] = f.item
			}
			requeueItems(requeue)
			return
		}

		// A full batch timing out with zero acks across the batch and no explicit errors indicates a
		// dead/half-open transport. Never evict valid telemetry as poison; force a transport reconnect
		// and return items to the bounded spool. If other items in this batch already succeeded
		// (batchHadSuccess == true), the transport is alive and remaining stalled items are poison.
		if !batchHadSuccess && succeeded == 0 && timedOut && !hasExplicitErr {
			consecutiveFullTimeouts++
			nextPending := make([]pendingItem, len(failed))
			for i, f := range failed {
				item := f.item
				item.failures = 0
				nextPending[i] = item
			}
			if consecutiveFullTimeouts >= deadTransportThreshold {
				c.forceReconnect()
				requeueItems(nextPending)
				_ = c.waitRetryBackoff()
				return
			}
			pending = nextPending
			if !c.waitRetryBackoff() {
				requeueItems(pending)
				return
			}
			continue
		}

		consecutiveFullTimeouts = 0
		nextPending := make([]pendingItem, 0, len(failed))
		for _, f := range failed {
			item := f.item
			item.failures++
			reason := "exceeded consecutive publish attempts"
			if !f.explicitErr {
				reason = "exceeded consecutive publish timeouts"
			}
			if item.failures >= maxAttempts {
				c.recordDeliveryDrop(reason, item.payload, fmt.Errorf("%d consecutive failures on %s", item.failures, item.topic))
				continue
			}
			nextPending = append(nextPending, item)
		}
		pending = nextPending
		if len(pending) == 0 {
			return
		}

		if !c.waitRetryBackoff() {
			requeueItems(pending)
			return
		}
	}
}

func countNonNilPayloads(items []*core.CTDPayload) uint64 {
	var n uint64
	for _, p := range items {
		if p != nil {
			n++
		}
	}
	return n
}

func (c *MQTTClient) drainRemainingBeforeClose(maxWait time.Duration) {
	remaining := c.spool.CloseAndDrain()
	if len(remaining) == 0 {
		return
	}
	if !c.isConnected() {
		if dropped := countNonNilPayloads(remaining); dropped > 0 {
			total := c.deliveryDropped.Add(dropped)
			log.Printf("[Relay] Disconnected during shutdown; dropped %d unsent spooled CTD events (total_dropped=%d)", dropped, total)
		}
		return
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	for start := 0; start < len(remaining); start += maxPipelinedPublishes {
		select {
		case <-timer.C:
			if unsent := countNonNilPayloads(remaining[start:]); unsent > 0 {
				total := c.deliveryDropped.Add(unsent)
				log.Printf("[Relay] Shutdown drain timed out; dropped %d unsent CTD events (total_dropped=%d)", unsent, total)
			}
			return
		default:
		}
		end := start + maxPipelinedPublishes
		if end > len(remaining) {
			end = len(remaining)
		}
		chunk := remaining[start:end]
		tokens := make([]mqtt.Token, 0, len(chunk))
		for _, p := range chunk {
			if p == nil {
				continue
			}
			raw, err := json.Marshal(p)
			if err != nil {
				c.recordDeliveryDrop("marshal error", p, err)
				continue
			}
			if len(raw) > maxMQTTPayloadBytes {
				c.recordDeliveryDrop("payload exceeds max MQTT packet size", p, fmt.Errorf("%d bytes > %d", len(raw), maxMQTTPayloadBytes))
				continue
			}
			tokens = append(tokens, c.client.Publish(p.RoutingTopic(c.topicPrefix), 1, false, raw))
		}
		for i, tok := range tokens {
			select {
			case <-tok.Done():
				if err := tok.Error(); err != nil {
					total := c.deliveryDropped.Add(1)
					log.Printf("[Relay] Shutdown drain publish error (total_dropped=%d): %v", total, err)
				}
			case <-timer.C:
				var unacked uint64
				for j := i; j < len(tokens); j++ {
					select {
					case <-tokens[j].Done():
						if tokens[j].Error() != nil {
							unacked++
						}
					default:
						unacked++
					}
				}
				unsent := countNonNilPayloads(remaining[end:])
				if dropped := unacked + unsent; dropped > 0 {
					total := c.deliveryDropped.Add(dropped)
					log.Printf("[Relay] Shutdown drain timed out; dropped %d CTD events (%d unacked, %d unsent, total_dropped=%d)",
						dropped, unacked, unsent, total)
				}
				return
			}
		}
	}
}

// Disconnect flushes pending spooled CTD events, announces OFFLINE state, and closes the socket.
func (c *MQTTClient) Disconnect() {
	c.stopOnce.Do(func() {
		c.onConnectMu.Lock()
		close(c.stopCh)
		c.onConnectMu.Unlock()

		<-c.flushDone
		c.drainRemainingBeforeClose(1500 * time.Millisecond)
		c.onConnectWG.Wait()
		c.subWG.Wait()
		if c.client != nil {
			if c.isConnected() {
				tok := c.client.Publish(c.brand.RegistryTopic(c.nodeID), 1, true, []byte("OFFLINE"))
				tok.WaitTimeout(500 * time.Millisecond)
			}
			c.client.Disconnect(250)
		}
	})
}
