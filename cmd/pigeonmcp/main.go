package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/mcpserver"
)

func main() {
	var (
		cfgPath         = flag.String("config", "", "Optional path to pigeons.toml")
		meshFlag        = flag.String("mesh", "", "Select named mesh profile from pigeons.toml (overrides PIGEONS_MESH and current_mesh)")
		httpAddr        = flag.String("http", "", "Serve Streamable HTTP on host:port instead of stdio (e.g. 127.0.0.1:8089)")
		authToken       = flag.String("auth-token", "", "Bearer token for --http Streamable HTTP mode (required on non-loopback)")
		dbURL           = flag.String("db-url", "", "Override Read-Only PostgreSQL DSN")
		allowWriteViews = flag.Bool("allow-write-views", false, "Register create_pigeonmath_view tool (disabled by default)")
	)
	flag.Parse()

	wsCfg, err := config.LoadWorkstationConfigForMesh(*cfgPath, *meshFlag)
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

	token := strings.TrimSpace(*authToken)
	if token == "" {
		token = wsCfg.MCPAuthToken
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := db.NewReadOnlyAgentStore(ctx, dsn, strings.ToLower(wsCfg.Brand.MCPName), db.ReadOnlyOptions{
		AllowSuperuser: wsCfg.AllowSuperuserReadOnly,
		MaxRows:        100,
		MathSchema:     wsCfg.Brand.MathSchema,
	})
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
		Store:           store,
		Brand:           wsCfg.Brand,
		Optimizer:       optLLM,
		AllowWriteViews: *allowWriteViews,
		ActiveMesh:      wsCfg.ActiveMesh,
	})

	if strings.TrimSpace(*httpAddr) != "" {
		log.Printf("Starting %s Streamable HTTP server on %s [mesh=%s]...", wsCfg.Brand.MCPName, *httpAddr, wsCfg.ActiveMesh)
		if err := mcpserver.ServeHTTPContext(ctx, srv, *httpAddr, mcpserver.HTTPOptions{
			AuthToken: token,
		}); err != nil {
			log.Fatalf("%s HTTP server error: %v", wsCfg.Brand.MCPName, err)
		}
		return
	}

	if err := mcpserver.ServeStdio(srv); err != nil {
		log.Fatalf("%s stdio server error: %v", wsCfg.Brand.MCPName, err)
	}
}
