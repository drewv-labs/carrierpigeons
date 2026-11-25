package relay

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
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
)

const (
	maxPipelinedPublishes     = 64
	maxMQTTPayloadBytes       = 1 << 20 // 1 MiB
	maxPublishAttempts        = 5
	maxTimeoutPublishAttempts = 10
	defaultBatchTimeout       = 5 * time.Second
	defaultBackoffBase        = 500 * time.Millisecond
	defaultBackoffMax         = 30 * time.Second
	defaultSubscribeWait      = 5 * time.Second
)

// Options configures a node's MQTT relay, ring-buffer spool, LWT presence, TLS, and NodeHello metadata.
type Options struct {
	BrokerURL       string
	NodeID          string
	ClientPrefix    string
	TopicPrefix     string
	Brand           brand.Spec
	SpoolCapacity   int
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

	subMu        sync.RWMutex
	onConnectSub func(mqtt.Client)

	runnerMu       sync.Mutex
	runnersStopped bool
	runnerWG       sync.WaitGroup

	deliveryDropped atomic.Uint64
	backoffAttempt  int

	batchTimeout       time.Duration
	backoffBase        time.Duration
	backoffMax         time.Duration
	maxAttempts        int
	maxTimeoutAttempts int
	subscribeTimeout   time.Duration
}

// NewMQTTClient initializes the broker connection with default CTD topic routing.
func NewMQTTClient(brokerURL, nodeID string) *MQTTClient {
	return NewMQTTClientWithOptions(Options{
		BrokerURL:     brokerURL,
		NodeID:        nodeID,
		TopicPrefix:   brand.Active().CTDPrefix(),
		SpoolCapacity: DefaultSpoolCapacity,
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
		RunnerPolicy:  runner.DefaultExecutionPolicy(),
	})
}

// NewMQTTClientWithOptions initializes the broker connection with configurable spool sizing, TLS, and NodeHello tags.
func NewMQTTClientWithOptions(cfg Options) *MQTTClient {
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
	pauseCtrl := cfg.PauseController
	if pauseCtrl == nil {
		pauseCtrl = observer.NewPauseController()
	}

	c := &MQTTClient{
		nodeID:       cfg.NodeID,
		topicPrefix:  cfg.TopicPrefix,
		brand:        bSpec,
		bootTags:     core.NormalizeTags(cfg.BootTags),
		meta:         cfg.Meta,
		runnerPolicy: cfg.RunnerPolicy,
		runnerReg:    cfg.RunnerRegistry,
		pauseCtrl:    pauseCtrl,
		spool:        NewRingSpool(cfg.SpoolCapacity),
		stopCh:       make(chan struct{}),
		flushDone:    make(chan struct{}),
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
	opts.SetOrderMatters(false)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(3 * time.Second)
	opts.SetMaxReconnectInterval(15 * time.Second)
	opts.SetWill(registryTopic, "OFFLINE", 1, true)

	opts.OnConnect = func(client mqtt.Client) {
		// 1. Announce node ONLINE status (retained)
		client.Publish(registryTopic, 1, true, []byte("ONLINE"))

		// 2. Announce hardware capabilities & boot tags via NodeHello (retained)
		hello := core.NodeHello{
			NodeID:    cfg.NodeID,
			Arch:      runtime.GOARCH,
			OS:        runtime.GOOS,
			NumCPU:    runtime.NumCPU(),
			BootTags:  c.bootTags,
			Meta:      c.meta,
			Timestamp: time.Now().UTC(),
		}
		if rawHello, err := json.Marshal(hello); err == nil {
			client.Publish(helloTopic, 1, true, rawHello)
		}

		// 3. Re-bind control subscriptions across broker restarts
		c.subMu.RLock()
		subFn := c.onConnectSub
		c.subMu.RUnlock()
		if subFn != nil {
			subFn(client)
		}
	}

	client := mqtt.NewClient(opts)
	c.client = client

	if token := client.Connect(); !token.WaitTimeout(3 * time.Second) {
		log.Printf("[Relay] Broker connect timed out after 3s on boot, spooling up to %d events", c.spool.Cap())
	} else if err := token.Error(); err != nil {
		log.Printf("[Relay] Broker unreachable on boot, spooling up to %d events (%v)", c.spool.Cap(), err)
	}

	go c.flushLoop()

	return c
}

// SpoolLen returns the number of queued CTD events awaiting broker delivery.
func (c *MQTTClient) SpoolLen() int {
	return c.spool.Len()
}

// SpoolDropped returns the total number of evicted events when the ring buffer saturated.
func (c *MQTTClient) SpoolDropped() uint64 {
	return c.spool.Dropped()
}

func (c *MQTTClient) isConnected() bool {
	return c.client != nil && c.client.IsConnected() && c.client.IsConnectionOpen()
}

// DeliveryDropped returns the cumulative number of payloads dropped due to marshaling failures,
// oversized MQTT packets (>1 MiB), repeated publish failures while connected, or shutdown timeouts.
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

func (c *MQTTClient) effectiveMaxTimeoutAttempts() int {
	if c.maxTimeoutAttempts > 0 {
		return c.maxTimeoutAttempts
	}
	if c.maxAttempts > 0 {
		return c.maxAttempts * 2
	}
	return maxTimeoutPublishAttempts
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
	sleepDur := c.nextBackoff(c.backoffAttempt)
	c.backoffAttempt++
	timer := time.NewTimer(sleepDur)
	defer timer.Stop()
	select {
	case <-c.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

// Publish enqueues the payload into the ring-buffer spool without ever blocking observers.
func (c *MQTTClient) Publish(payload *core.CTDPayload) error {
	if payload == nil {
		return nil
	}

	select {
	case <-c.stopCh:
		return nil
	default:
	}

	if evicted := c.spool.Push(payload); evicted {
		dropped := c.spool.Dropped()
		if dropped == 1 || dropped%100 == 0 {
			log.Printf("[Relay] Ring spool saturated (%d cap); evicted %d oldest telemetry events.", c.spool.Cap(), dropped)
		}
	}

	return nil
}

// flushLoop pulls micro-batches from the ring buffer and pipelines QoS 1 MQTT publishes to eliminate WAN RTT bottlenecks.
func (c *MQTTClient) flushLoop() {
	defer close(c.flushDone)
	for {
		select {
		case <-c.stopCh:
			return
		default:
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
	maxTimeoutAttempts := c.effectiveMaxTimeoutAttempts()

	for len(pending) > 0 {
		select {
		case <-c.stopCh:
			// Prepend undelivered items in FIFO order for best-effort final drain in Disconnect().
			requeueItems(pending)
			return
		default:
		}

		if !c.isConnected() {
			for i := range pending {
				pending[i].failures = 0
			}
			if !c.waitRetryBackoff() {
				requeueItems(pending)
				return
			}
			continue
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
		stopIdx := 0

		for i, tok := range tokens {
			if timedOut {
				select {
				case <-tok.Done():
					if err := tok.Error(); err != nil {
						log.Printf("[Relay] Flight error publishing to %s, retrying: %v", pending[i].topic, err)
						failed = append(failed, failedItem{item: pending[i], explicitErr: true})
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
			c.backoffAttempt = 0
			return
		}

		if succeeded > 0 {
			c.backoffAttempt = 0
		}

		connected := c.isConnected()
		nextPending := make([]pendingItem, 0, len(failed))
		for _, f := range failed {
			item := f.item
			if !connected {
				item.failures = 0
			} else {
				item.failures++
				threshold := maxTimeoutAttempts
				reason := "exceeded consecutive publish timeouts"
				if f.explicitErr || succeeded > 0 {
					threshold = maxAttempts
					reason = "exceeded consecutive publish attempts"
				}
				if item.failures >= threshold {
					c.recordDeliveryDrop(reason, item.payload, fmt.Errorf("%d consecutive failures on %s", item.failures, item.topic))
					continue
				}
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
	remaining := c.spool.DrainAll()
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
		close(c.stopCh)
		<-c.flushDone
		c.drainRemainingBeforeClose(1500 * time.Millisecond)
		if c.client != nil {
			if c.isConnected() {
				tok := c.client.Publish(c.brand.RegistryTopic(c.nodeID), 1, true, []byte("OFFLINE"))
				tok.WaitTimeout(500 * time.Millisecond)
			}
			c.client.Disconnect(250)
		}
	})
}
