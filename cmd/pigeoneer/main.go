package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/edge"
)

func main() {
	var (
		cfgPath   = flag.String("config", "", "Path to pigeoneer.toml")
		nodeID    = flag.String("node-id", "", "Override node_id")
		brokerURL = flag.String("broker", "", "Override MQTT broker URL")
		tagsCSV   = flag.String("tags", "", "Comma-separated boot tags (e.g. riscv64,compute-bench)")
		spoolCap  = flag.Int("spool", 0, "Override ring-buffer spool capacity")
	)
	flag.Parse()

	cfg, err := config.LoadEdgeConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed loading configuration: %v", err)
	}
	if strings.TrimSpace(*nodeID) != "" {
		cfg.NodeID = strings.TrimSpace(*nodeID)
	}
	if strings.TrimSpace(*brokerURL) != "" {
		cfg.BrokerURL = strings.TrimSpace(*brokerURL)
	}
	if strings.TrimSpace(*tagsCSV) != "" {
		cfg.Tags = core.NormalizeTags(strings.Split(*tagsCSV, ","))
	}
	if *spoolCap > 0 {
		cfg.SpoolCapacity = *spoolCap
	}

	log.Printf("Starting %s: CTD Edge Agent (%s -> %s)...", cfg.Brand.EdgeName, cfg.NodeID, cfg.BrokerURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	agent := edge.New(cfg)
	if err := agent.Start(ctx); err != nil {
		log.Fatalf("Failed starting %s: %v", cfg.Brand.EdgeName, err)
	}
	defer agent.Stop()

	<-ctx.Done()
	log.Printf("Shutting down %s...", cfg.Brand.EdgeName)
}
