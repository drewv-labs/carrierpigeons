package relay

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const maxPipelinedPublishes = 64

// Options configures a node's MQTT relay, ring-buffer spool, LWT presence, TLS, and NodeHello metadata.
type Options struct {
	BrokerURL      string
	NodeID         string
	ClientPrefix   string
	TopicPrefix    string
	SpoolCapacity  int
	BootTags       []string
	Username       string
	Password       string
	TLSConfig      *tls.Config
	Meta           map[string]string
	RunnerPolicy   runner.ExecutionPolicy
	RunnerRegistry *runner.Registry
}

// MQTTClient manages the MQTT connection, LWT presence, NodeHello boot announcement, and variable ring spool.
type MQTTClient struct {
	client       mqtt.Client
	nodeID       string
	topicPrefix  string
	bootTags     []string
	meta         map[string]string
	runnerPolicy runner.ExecutionPolicy
	runnerReg    *runner.Registry
	spool        *RingSpool
	stopCh       chan struct{}
	flushDone    chan struct{}
	stopOnce     sync.Once

	subMu        sync.RWMutex
	onConnectSub func(mqtt.Client)
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
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		cfg.BrokerURL = "tcp://127.0.0.1:1883"
	}
	if strings.TrimSpace(cfg.TopicPrefix) == "" {
		cfg.TopicPrefix = brand.Active().CTDPrefix()
	}
	clientPrefix := strings.TrimSpace(cfg.ClientPrefix)
	if clientPrefix == "" {
		clientPrefix = strings.ToLower(brand.Active().EdgeName)
	}
	if cfg.SpoolCapacity <= 0 {
		cfg.SpoolCapacity = DefaultSpoolCapacity
	}

	c := &MQTTClient{
		nodeID:       cfg.NodeID,
		topicPrefix:  cfg.TopicPrefix,
		bootTags:     core.NormalizeTags(cfg.BootTags),
		meta:         cfg.Meta,
		runnerPolicy: cfg.RunnerPolicy,
		runnerReg:    cfg.RunnerRegistry,
		spool:        NewRingSpool(cfg.SpoolCapacity),
		stopCh:       make(chan struct{}),
		flushDone:    make(chan struct{}),
	}

	registryTopic := core.RegistryTopic(cfg.NodeID)
	helloTopic := core.HelloTopic(cfg.NodeID)

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

	if token := client.Connect(); token.WaitTimeout(3*time.Second) && token.Error() != nil {
		log.Printf("[Relay] Broker unreachable on boot, spooling up to %d events (%v)", c.spool.Cap(), token.Error())
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
		batch, ok := c.spool.PopBatchBlocking(maxPipelinedPublishes, c.stopCh)
		if !ok {
			return
		}
		c.deliverBatchWithRetry(batch)
	}
}

func (c *MQTTClient) deliverBatchWithRetry(batch []*core.CTDPayload) {
	type pendingItem struct {
		payload *core.CTDPayload
		topic   string
		raw     []byte
	}
	pending := make([]pendingItem, 0, len(batch))
	for _, p := range batch {
		if p == nil {
			continue
		}
		raw, err := json.Marshal(p)
		if err != nil {
			log.Printf("[Relay] Failed to marshal payload: %v", err)
			continue
		}
		pending = append(pending, pendingItem{
			payload: p,
			topic:   p.RoutingTopic(c.topicPrefix),
			raw:     raw,
		})
	}

	for len(pending) > 0 {
		select {
		case <-c.stopCh:
			// Re-enqueue undelivered items for best-effort final drain in Disconnect().
			for _, item := range pending {
				c.spool.Push(item.payload)
			}
			return
		default:
		}

		if !c.client.IsConnected() {
			select {
			case <-c.stopCh:
				for _, item := range pending {
					c.spool.Push(item.payload)
				}
				return
			case <-time.After(1 * time.Second):
				continue
			}
		}

		tokens := make([]mqtt.Token, len(pending))
		for i, item := range pending {
			tokens[i] = c.client.Publish(item.topic, 1, false, item.raw)
		}

		var failed []pendingItem
		for i, tok := range tokens {
			if tok.WaitTimeout(5*time.Second) && tok.Error() == nil {
				continue
			}
			if tok.Error() != nil {
				log.Printf("[Relay] Flight error publishing to %s, retrying: %v", pending[i].topic, tok.Error())
			}
			failed = append(failed, pending[i])
		}
		if len(failed) == 0 {
			return
		}
		pending = failed

		select {
		case <-c.stopCh:
			for _, item := range pending {
				c.spool.Push(item.payload)
			}
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (c *MQTTClient) drainRemainingBeforeClose(maxWait time.Duration) {
	if c.client == nil || !c.client.IsConnected() {
		return
	}
	remaining := c.spool.DrainAll()
	if len(remaining) == 0 {
		return
	}
	deadline := time.Now().Add(maxWait)
	for start := 0; start < len(remaining) && time.Now().Before(deadline); start += maxPipelinedPublishes {
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
			if raw, err := json.Marshal(p); err == nil {
				tokens = append(tokens, c.client.Publish(p.RoutingTopic(c.topicPrefix), 1, false, raw))
			}
		}
		for _, tok := range tokens {
			rem := time.Until(deadline)
			if rem <= 0 {
				return
			}
			tok.WaitTimeout(rem)
		}
	}
}

// Disconnect flushes pending spooled CTD events, announces OFFLINE state, and closes the socket.
func (c *MQTTClient) Disconnect() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		<-c.flushDone
		if c.client != nil {
			c.drainRemainingBeforeClose(2 * time.Second)
			if c.client.IsConnected() {
				tok := c.client.Publish(core.RegistryTopic(c.nodeID), 1, true, []byte("OFFLINE"))
				tok.WaitTimeout(500 * time.Millisecond)
			}
			c.client.Disconnect(250)
		}
	})
}
