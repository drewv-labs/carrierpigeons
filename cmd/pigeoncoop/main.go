package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/drewv-labs/carrier-pigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

func resolveBrokerURL() string {
	for _, key := range []string{"PIGEONCOOP_BROKER_URL", "PIGEONS_BROKER_URL", "MQTT_BROKER_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return "tcp://localhost:1883"
}

func main() {
	log.Println("Starting PigeonCoop: CTD Ledger Injector...")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Initialize the PostgreSQL CTD Store & Schema
	dbURL := db.ResolveDBURL()
	store, err := db.NewPostgresStore(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to initialize PostgreSQL ledger: %v", err)
	}
	defer store.Close()
	log.Println("Connected to PostgreSQL Ledger.")

	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("Schema migration failed: %v", err)
	}
	log.Println("Verified PostgreSQL CTD schema (telemetric_ledger, ctd_artifacts, ctd_traces).")

	// 2. Initialize the MQTT Subscriber, injecting the store as the Ledger interface
	brokerURL := resolveBrokerURL()
	mqttSub, err := broker.NewSubscriber(brokerURL, store)
	if err != nil {
		log.Fatalf("Failed to connect to MQTT broker (%s): %v", brokerURL, err)
	}
	defer mqttSub.Disconnect()

	// 3. Block until OS termination signal
	log.Printf("PigeonCoop is running on %s. Waiting for telemetry...", brokerURL)
	<-ctx.Done()

	log.Println("Shutting down PigeonCoop gracefully...")
}
