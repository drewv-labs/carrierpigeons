package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/drewv-labs/carrierpigeons/internal/jsonutil"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/mcpserver"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runWithContext(ctx, os.Args[1:]); err != nil {
		log.Fatalf("%v", err)
	}
}

func run(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithContext(ctx, args)
}

func runWithContext(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pigeonmcp", flag.ContinueOnError)
	var (
		cfgPath            = fs.String("config", "", "Optional path to pigeons.toml")
		meshFlag           = fs.String("mesh", "", "Select named mesh profile from pigeons.toml (overrides PIGEONS_MESH and current_mesh)")
		httpAddr           = fs.String("http", "", "Serve Streamable HTTP on host:port instead of stdio (e.g. 127.0.0.1:8089)")
		authToken          = fs.String("auth-token", "", "Bearer token for --http Streamable HTTP mode (required on non-loopback)")
		dbURL              = fs.String("db-url", "", "Override Read-Only PostgreSQL DSN")
		allowWriteViews    = fs.Bool("allow-write-views", false, "Register create_pigeonmath_view tool (disabled by default)")
		noOptimize         = fs.Bool("no-optimize", false, "Disable LLM SQL optimization/certification during create_pigeonmath_view")
		tlsCert            = fs.String("tls-cert", "", "Path to TLS certificate PEM file for --http mode")
		tlsKey             = fs.String("tls-key", "", "Path to TLS private key PEM file for --http mode")
		allowPlaintextHTTP = fs.Bool("allow-plaintext-http", false, "Allow unencrypted HTTP when --http binds off loopback (insecure)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	wsCfg, err := config.LoadWorkstationConfigForMesh(*cfgPath, *meshFlag)
	if err != nil {
		return fmt.Errorf("failed loading config: %w", jsonutil.RedactError(err))
	}
	brand.SetActive(wsCfg.Brand)

	dsn := strings.TrimSpace(*dbURL)
	if dsn == "" {
		dsn = strings.TrimSpace(wsCfg.ReadOnlyDatabaseURL)
	}
	if !isDBExplicitlyConfigured(*dbURL, wsCfg) || dsn == "" {
		return fmt.Errorf("%s failed: read-only database DSN is not configured (pass --db-url, configure readonly_database_url / database_url, or set %s_RO_DB_URL)",
			wsCfg.Brand.MCPName, wsCfg.Brand.EnvPrefix)
	}

	token := strings.TrimSpace(*authToken)
	if token == "" {
		token = strings.TrimSpace(wsCfg.MCPAuthToken)
	}

	httpListen := strings.TrimSpace(*httpAddr)
	httpOpts := mcpserver.HTTPOptions{
		AuthToken:          token,
		TLSCertFile:        strings.TrimSpace(*tlsCert),
		TLSKeyFile:         strings.TrimSpace(*tlsKey),
		AllowPlaintextHTTP: *allowPlaintextHTTP,
	}
	if httpListen != "" {
		if err := mcpserver.ValidateHTTPOptions(httpListen, httpOpts); err != nil {
			return fmt.Errorf("%s HTTP configuration error: %w", wsCfg.Brand.MCPName, err)
		}
	} else if (httpOpts.TLSCertFile != "") != (httpOpts.TLSKeyFile != "") {
		return fmt.Errorf("%s configuration error: both --tls-cert and --tls-key must be provided together", wsCfg.Brand.MCPName)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	store, err := db.NewReadOnlyAgentStore(ctx, dsn, strings.ToLower(wsCfg.Brand.MCPName), db.ReadOnlyOptions{
		AllowSuperuser: wsCfg.AllowSuperuserReadOnly,
		MaxRows:        100,
		MathSchema:     wsCfg.Brand.MathSchema,
		EnvPrefix:      wsCfg.Brand.EnvPrefix,
	})
	if err != nil {
		return fmt.Errorf("%s failed connecting to read-only ledger: %w", wsCfg.Brand.MCPName, jsonutil.RedactError(err, dsn))
	}
	defer store.Close()

	var optLLM llm.BaseLLM
	if !*noOptimize && isLLMExplicitlyConfigured(wsCfg) {
		optLLM = llm.New(llm.Config{
			Provider: wsCfg.LLM.Provider,
			Model:    wsCfg.LLM.Model,
			Endpoint: wsCfg.LLM.Endpoint,
			APIKey:   wsCfg.LLM.APIKey,
		})
	}

	srv := mcpserver.New(mcpserver.Options{
		Store:           store,
		Brand:           wsCfg.Brand,
		Optimizer:       optLLM,
		AllowWriteViews: *allowWriteViews,
		ActiveMesh:      wsCfg.ActiveMesh,
	})

	if httpListen != "" {
		transport := "HTTP"
		if httpOpts.TLSCertFile != "" && httpOpts.TLSKeyFile != "" {
			transport = "HTTPS"
		}
		log.Printf("Starting %s Streamable %s server on %s [mesh=%s]...", wsCfg.Brand.MCPName, transport, httpListen, wsCfg.ActiveMesh)
		if err := mcpserver.ServeHTTPContext(ctx, srv, httpListen, httpOpts); err != nil {
			return fmt.Errorf("%s HTTP server error: %w", wsCfg.Brand.MCPName, err)
		}
		return nil
	}

	if err := mcpserver.ServeStdioContext(ctx, srv); err != nil {
		return fmt.Errorf("%s stdio server error: %w", wsCfg.Brand.MCPName, err)
	}
	return nil
}

func isDBExplicitlyConfigured(dbURLFlag string, wsCfg config.WorkstationConfig) bool {
	if strings.TrimSpace(dbURLFlag) != "" {
		return true
	}

	bp := wsCfg.Brand.MergeWithDefaults().EnvPrefix
	if config.ResolveWorkstationRODBURLEnv(bp) != "" || config.ResolveWorkstationDBURLEnv(bp) != "" {
		return true
	}

	if strings.TrimSpace(wsCfg.ConfigPath) != "" {
		var parsed struct {
			DatabaseURL         string `toml:"database_url"`
			ReadOnlyDatabaseURL string `toml:"readonly_database_url"`
			Meshes              map[string]struct {
				DatabaseURL         string `toml:"database_url"`
				ReadOnlyDatabaseURL string `toml:"readonly_database_url"`
			} `toml:"meshes"`
		}
		if md, err := toml.DecodeFile(wsCfg.ConfigPath, &parsed); err == nil {
			if (md.IsDefined("readonly_database_url") && strings.TrimSpace(parsed.ReadOnlyDatabaseURL) != "") ||
				(md.IsDefined("database_url") && strings.TrimSpace(parsed.DatabaseURL) != "") {
				return true
			}
			if wsCfg.ActiveMesh != "" {
				mp := parsed.Meshes[wsCfg.ActiveMesh]
				if (md.IsDefined("meshes", wsCfg.ActiveMesh, "readonly_database_url") && strings.TrimSpace(mp.ReadOnlyDatabaseURL) != "") ||
					(md.IsDefined("meshes", wsCfg.ActiveMesh, "database_url") && strings.TrimSpace(mp.DatabaseURL) != "") {
					return true
				}
			}
		}
	}

	return false
}

func isLLMExplicitlyConfigured(wsCfg config.WorkstationConfig) bool {
	provider := strings.ToLower(strings.TrimSpace(wsCfg.LLM.Provider))
	switch provider {
	case "", "none", "off", "disabled", "false":
		return false
	}

	bp := wsCfg.Brand.MergeWithDefaults().EnvPrefix
	for _, suffix := range []string{"LLM_PROVIDER", "LLM_MODEL", "LLM_ENDPOINT", "LLM_API_KEY"} {
		if config.PrefixedEnv(bp, suffix, "PIGEONS_"+suffix) != "" {
			return true
		}
	}

	if wsCfg.LLM != config.DefaultWorkstationConfig().LLM {
		return true
	}

	if strings.TrimSpace(wsCfg.ConfigPath) != "" {
		var raw any
		if md, err := toml.DecodeFile(wsCfg.ConfigPath, &raw); err == nil {
			for _, key := range []string{"provider", "model", "endpoint", "api_key"} {
				if md.IsDefined("llm", key) {
					return true
				}
				if wsCfg.ActiveMesh != "" && md.IsDefined("meshes", wsCfg.ActiveMesh, "llm", key) {
					return true
				}
			}
		}
	}

	return false
}
