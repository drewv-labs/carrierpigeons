package relay

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTClient manages the MQTT connection, LWT presence, and offline telemetry spool.
type MQTTClient struct {
	client      mqtt.Client
	nodeID      string
	topicPrefix string
	spool       chan *core.CTDPayload
	stopCh      chan struct{}
	stopOnce    sync.Once

	subMu        sync.RWMutex
	onConnectSub func(mqtt.Client)
}

// NewMQTTClient initializes the broker connection with default CTD topic routing.
func NewMQTTClient(brokerURL, nodeID string) *MQTTClient {
	return NewMQTTClientWithPrefix(brokerURL, nodeID, core.DefaultTopicPrefix)
}

// NewMQTTClientWithPrefix initializes the broker connection, LWT will, and background FIFO flusher.
func NewMQTTClientWithPrefix(brokerURL, nodeID, topicPrefix string) *MQTTClient {
	if topicPrefix == "" {
		topicPrefix = core.DefaultTopicPrefix
	}

	c := &MQTTClient{
		nodeID:      nodeID,
		topicPrefix: topicPrefix,
		// 5000 event buffer gives ~7 hours of offline capacity at 5s intervals
		spool:  make(chan *core.CTDPayload, 5000),
		stopCh: make(chan struct{}),
	}

	registryTopic := core.RegistryTopic(nodeID)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("pigeoneer-" + nodeID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetMaxReconnectInterval(15 * time.Second)
	opts.SetWill(registryTopic, "OFFLINE", 1, true)

	opts.OnConnect = func(client mqtt.Client) {
		// Announce node ONLINE status (retained)
		client.Publish(registryTopic, 1, true, []byte("ONLINE"))

		// Re-bind control subscriptions across broker restarts
		c.subMu.RLock()
		subFn := c.onConnectSub
		c.subMu.RUnlock()
		if subFn != nil {
			subFn(client)
		}
	}

	client := mqtt.NewClient(opts)
	c.client = client

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("[Relay] Warning: Broker unreachable on boot, telemetry will spool (%v)", token.Error())
	}

	go c.flushLoop()

	return c
}

// Publish drops the payload into the spool channel without ever blocking observers.
func (c *MQTTClient) Publish(payload *core.CTDPayload) error {
	if payload == nil {
		return nil
	}

	select {
	case <-c.stopCh:
		return nil
	default:
	}

	select {
	case c.spool <- payload:
	default:
		// Buffer full: evict oldest event to preserve room for fresh telemetry.
		select {
		case <-c.spool:
		default:
		}
		select {
		case c.spool <- payload:
		default:
		}
		log.Printf("[Relay] Spool capacity reached; dropped oldest telemetry event.")
	}

	return nil
}

// flushLoop pulls from the spool in strict FIFO order and publishes when connected.
func (c *MQTTClient) flushLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		case payload := <-c.spool:
			c.deliverWithRetry(payload)
		}
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
		log.Println("[Relay] Disconnecting from broker...")
		if c.client != nil {
			if c.client.IsConnected() {
				tok := c.client.Publish(core.RegistryTopic(c.nodeID), 1, true, []byte("OFFLINE"))
				tok.WaitTimeout(500 * time.Millisecond)
			}
			c.client.Disconnect(250)
		}
	})
}
