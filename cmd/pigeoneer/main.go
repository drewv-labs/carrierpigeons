package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/drewv-labs/carrier-pigeons/internal/pigeoneer/relay"
	"github.com/drewv-labs/carrier-pigeons/pkg/observer"

	// Blank imports register built-in hardware observers and remote runners
	_ "github.com/drewv-labs/carrier-pigeons/internal/pigeoneer/observers"
	_ "github.com/drewv-labs/carrier-pigeons/internal/pigeoneer/runners"
)

func resolveNodeID() string {
	for _, key := range []string{"PIGEONEER_NODE_ID", "PIGEONS_NODE_ID"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		return strings.TrimSpace(host)
	}
	return "edge-arm64-01"
}

func resolveBrokerURL() string {
	for _, key := range []string{"PIGEONEER_BROKER_URL", "PIGEONS_BROKER_URL", "MQTT_BROKER_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return "tcp://localhost:1883"
}

func main() {
	log.Println("Starting Pigeoneer: CTD Edge Agent...")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nodeID := resolveNodeID()
	brokerURL := resolveBrokerURL()
	topicPrefix := strings.TrimSpace(os.Getenv("PIGEONEER_TOPIC_PREFIX"))

	// 1. Initialize the network relay with LWT presence and offline spooler
	mqttRelay := relay.NewMQTTClientWithPrefix(brokerURL, nodeID, topicPrefix)
	defer mqttRelay.Disconnect()
	log.Printf("Configured relay for node %q -> %s", nodeID, brokerURL)

	// 2. Bind the control topic to listen for dynamic Runner dispatches (reconnect-safe)
	if err := mqttRelay.ListenForRunners(ctx, nodeID, mqttRelay); err != nil {
		log.Printf("[Runner Dispatch] Initial control bind deferred until broker reconnect: %v", err)
	}

	// 3. Deploy concurrent passive hardware observers bound to daemon lifecycle context
	observer.StartAll(ctx, nodeID, mqttRelay)

	// 4. Block until OS termination signal
	log.Printf("Observers (%v) & Runner agents active for %q. Press Ctrl+C to stop.", observer.RegistryNames(), nodeID)
	<-ctx.Done()

	log.Println("Shutting down Pigeoneer...")
}
