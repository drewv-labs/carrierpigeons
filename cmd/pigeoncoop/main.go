package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/coop"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func main() {
	var (
		cfgPath        = flag.String("config", "", "Path to pigeoncoop.toml")
		meshName       = flag.String("mesh-name", "", "Override MNS mesh name (RFC 1123 lowercase kebab-case)")
		brokerMode     = flag.String("mqtt-mode", "", "Override MQTT mode ('embedded' or 'external')")
		dbURL          = flag.String("db-url", "", "Override PostgreSQL connection URL")
		provisionRoles = flag.Bool("provision-roles", false, "Provision ctd_ingestor & pigeoncare_ro PostgreSQL roles on startup")
	)
	flag.Parse()

	mqttModeFlagSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "mqtt-mode" {
			mqttModeFlagSet = true
		}
	})

	cfg, err := config.LoadCoopConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed loading PigeonCoop config: %v", err)
	}
	if strings.TrimSpace(*meshName) != "" {
		cfg.MeshName = strings.TrimSpace(*meshName)
		if err := core.ValidateMeshName(cfg.MeshName); err != nil {
			log.Fatalf("Invalid --mesh-name: %v", err)
		}
	}
	if mqttModeFlagSet {
		mode := strings.ToLower(strings.TrimSpace(*brokerMode))
		if mode != "embedded" && mode != "external" {
			log.Fatalf("Invalid --mqtt-mode %q: must be 'embedded' or 'external'", *brokerMode)
		}
		cfg.MQTT.Mode = mode
	} else {
		mode := strings.ToLower(strings.TrimSpace(cfg.MQTT.Mode))
		if mode == "" {
			mode = "embedded"
		}
		if mode != "embedded" && mode != "external" {
			log.Fatalf("Invalid mqtt.mode %q: must be 'embedded' or 'external'", cfg.MQTT.Mode)
		}
		cfg.MQTT.Mode = mode
	}
	if strings.TrimSpace(*dbURL) != "" {
		cfg.Database.URL = strings.TrimSpace(*dbURL)
	}

	log.Printf("Starting %s (mesh=%s, node=%s, mqtt=%s, self_observe=%v)...",
		cfg.Brand.CoopName, cfg.MeshName, cfg.NodeID, cfg.MQTT.Mode, cfg.SelfObserve)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := coop.New(cfg, coop.Options{
		ProvisionRoles: *provisionRoles,
	})
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("%s startup failed: %v", cfg.Brand.CoopName, err)
	}
	defer srv.Stop()

	log.Printf("%s active on %s [mesh=%s]. Press Ctrl+C to stop.", cfg.Brand.CoopName, config.RedactURL(srv.BrokerURL()), srv.MeshName())
	<-ctx.Done()

	log.Printf("Shutting down %s gracefully...", cfg.Brand.CoopName)
}
