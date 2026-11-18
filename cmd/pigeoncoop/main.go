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
)

func main() {
	var (
		cfgPath        = flag.String("config", "", "Path to pigeoncoop.toml")
		brokerMode     = flag.String("mqtt-mode", "", "Override MQTT mode ('embedded' or 'external')")
		dbURL          = flag.String("db-url", "", "Override PostgreSQL connection URL")
		provisionRoles = flag.Bool("provision-roles", false, "Provision ctd_ingestor & pigeoncare_ro PostgreSQL roles on startup")
	)
	flag.Parse()

	cfg, err := config.LoadCoopConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed loading PigeonCoop config: %v", err)
	}
	if strings.TrimSpace(*brokerMode) != "" {
		cfg.MQTT.Mode = strings.ToLower(strings.TrimSpace(*brokerMode))
	}
	if strings.TrimSpace(*dbURL) != "" {
		cfg.Database.URL = strings.TrimSpace(*dbURL)
	}

	log.Printf("Starting %s (%s, mqtt=%s, self_observe=%v)...",
		cfg.Brand.CoopName, cfg.NodeID, cfg.MQTT.Mode, cfg.SelfObserve)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := coop.New(cfg, coop.Options{
		ProvisionRoles: *provisionRoles,
	})
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("%s startup failed: %v", cfg.Brand.CoopName, err)
	}
	defer srv.Stop()

	log.Printf("%s active on %s. Press Ctrl+C to stop.", cfg.Brand.CoopName, srv.BrokerURL())
	<-ctx.Done()

	log.Printf("Shutting down %s gracefully...", cfg.Brand.CoopName)
}
