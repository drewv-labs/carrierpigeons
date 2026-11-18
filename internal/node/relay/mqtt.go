package relay

import (
	"encoding/json"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Options configures a node's MQTT relay, ring-buffer spool, LWT presence, and NodeHello metadata.
type Options struct {
	BrokerURL     string
	NodeID        string
	ClientPrefix  string
	TopicPrefix   string
	SpoolCapacity int
	BootTags      []string
	Username      string
	Password      string
	Meta          map[string]string
	RunnerPolicy  runner.ExecutionPolicy
}

// MQTTClient manages the MQTT connection, LWT presence, NodeHello boot announcement, and variable ring spool.
type MQTTClient struct {
	client       mqtt.Client
	nodeID       string
	topicPrefix  string
	bootTags     []string
	meta         map[string]string
	runnerPolicy runner.ExecutionPolicy
	spool        *RingSpool
	stopCh       chan struct{}
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

// NewMQTTClientWithOptions initializes the broker connection with configurable spool sizing and NodeHello tags.
func NewMQTTClientWithOptions(cfg Options) *MQTTClient {
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		cfg.BrokerURL = "tcp://localhost:1883"
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
		spool:        NewRingSpool(cfg.SpoolCapacity),
		stopCh:       make(chan struct{}),
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

// flushLoop pulls from the ring buffer in strict FIFO order and publishes when connected.
func (c *MQTTClient) flushLoop() {
	for {
		payload, ok := c.spool.PopBlocking(c.stopCh)
		if !ok {
			return
		}
		c.deliverWithRetry(payload)
	}
}

func (c *MQTTClient) deliverWithRetry(payload *core.CTDPayload) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[Relay] Failed to marshal payload: %v", err)
		return
	}

	topic := payload.RoutingTopic(c.topicPrefix)

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if !c.client.IsConnected() {
			select {
			case <-c.stopCh:
				return
			case <-time.After(1 * time.Second):
				continue
			}
		}

		token := c.client.Publish(topic, 1, false, data)
		if token.WaitTimeout(5*time.Second) && token.Error() == nil {
			return
		}
		if token.Error() != nil {
			log.Printf("[Relay] Flight error publishing to %s, retrying: %v", topic, token.Error())
		}

		select {
		case <-c.stopCh:
			return
		case <-time.After(1 * time.Second):
		}
	}
}

// Disconnect gracefully announces OFFLINE state, halts the flusher, and closes the socket.
func (c *MQTTClient) Disconnect() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		if c.client != nil {
			if c.client.IsConnected() {
				tok := c.client.Publish(core.RegistryTopic(c.nodeID), 1, true, []byte("OFFLINE"))
				tok.WaitTimeout(500 * time.Millisecond)
			}
			c.client.Disconnect(250)
		}
	})
}
