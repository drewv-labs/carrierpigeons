package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/config"
	"github.com/drewv-labs/carrier-pigeons/pkg/mcpserver"
)

func main() {
	var (
		cfgPath  = flag.String("config", "", "Optional path to pigeons.toml")
		httpAddr = flag.String("http", "", "Serve Streamable HTTP on host:port instead of stdio (e.g. 127.0.0.1:8089)")
		dbURL    = flag.String("db-url", "", "Override Read-Only PostgreSQL DSN")
	)
	flag.Parse()

	wsCfg, err := config.LoadWorkstationConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed loading config: %v", err)
	}
	brand.SetActive(wsCfg.Brand)

	dsn := strings.TrimSpace(*dbURL)
	if dsn == "" {
		dsn = wsCfg.ReadOnlyDatabaseURL
	}
	if dsn == "" {
		dsn = db.ResolveReadOnlyDBURL()
	}

	ctx := context.Background()
	store, err := db.NewReadOnlyAgentStore(ctx, dsn, strings.ToLower(wsCfg.Brand.MCPName))
	if err != nil {
		log.Fatalf("%s failed connecting to read-only ledger: %v", wsCfg.Brand.MCPName, err)
	}
	defer store.Close()

	optLLM := llm.New(llm.Config{
		Provider: wsCfg.LLM.Provider,
		Model:    wsCfg.LLM.Model,
		Endpoint: wsCfg.LLM.Endpoint,
		APIKey:   wsCfg.LLM.APIKey,
	})

	srv := mcpserver.New(mcpserver.Options{
		Store:     store,
		Brand:     wsCfg.Brand,
		Optimizer: optLLM,
	})

	if strings.TrimSpace(*httpAddr) != "" {
		log.Printf("Starting %s Streamable HTTP server on %s...", wsCfg.Brand.MCPName, *httpAddr)
		if err := mcpserver.ServeHTTP(srv, *httpAddr); err != nil {
			log.Fatalf("%s HTTP server error: %v", wsCfg.Brand.MCPName, err)
		}
		return
	}

	if err := mcpserver.ServeStdio(srv); err != nil {
		log.Fatalf("%s stdio server error: %v", wsCfg.Brand.MCPName, err)
	}
}
