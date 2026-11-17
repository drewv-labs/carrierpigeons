package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Ledger defines the contract for storing ingested telemetry.
type Ledger interface {
	InsertEvent(ctx context.Context, event *core.CTDPayload) error
}

// Subscriber manages the MQTT connection and routes data to the Ledger.
type Subscriber struct {
	client mqtt.Client
	ledger Ledger
}

// NewSubscriber creates a new MQTT listener and injects the storage dependency.
func NewSubscriber(brokerURL string, ledger Ledger) (*Subscriber, error) {
	sub := &Subscriber{ledger: ledger}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(fmt.Sprintf("pigeoncoop-ledger-injector-%d", os.Getpid()))
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetMaxReconnectInterval(15 * time.Second)

	opts.OnConnect = func(client mqtt.Client) {
		// 1. Subscribe to the canonical CTD telemetry firehose
		if tok := client.Subscribe(core.DefaultTelemetryWildcard, 1, sub.handleMessage); tok.Wait() && tok.Error() != nil {
			log.Printf("[Broker] Failed to subscribe to %s: %v", core.DefaultTelemetryWildcard, tok.Error())
		}

		// 2. Subscribe to LWT / node presence state changes
		if tok := client.Subscribe(core.RegistryWildcard, 1, sub.handleRegistryUpdate); tok.Wait() && tok.Error() != nil {
			log.Printf("[Broker] Failed to subscribe to %s: %v", core.RegistryWildcard, tok.Error())
		}

		log.Printf("Subscribed to MQTT telemetry (%s) & registry (%s)", core.DefaultTelemetryWildcard, core.RegistryWildcard)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	sub.client = client
	return sub, nil
}

// handleMessage unmarshals incoming CTD JSON envelopes and persists them to the Ledger.
func (s *Subscriber) handleMessage(_ mqtt.Client, msg mqtt.Message) {
	var event core.CTDPayload

	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		log.Printf("ERROR: Malformed CTD payload on %s dropped: %v", msg.Topic(), err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.ledger.InsertEvent(ctx, &event); err != nil {
		log.Printf("DB INSERT ERROR for %s/%s: %v", event.NodeID, event.Component, err)
		return
	}

	log.Printf("[LEDGER INJECT] %s/%s/%s -> PostgreSQL Insert Success", event.Pillar, event.NodeID, event.Component)
}

// Disconnect cleanly closes the network socket.
func (s *Subscriber) Disconnect() {
	if s.client != nil {
		s.client.Disconnect(250)
	}
}
