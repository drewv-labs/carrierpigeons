package mcpserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Options configures a whitelabelable PigeonMCP server instance.
// SECURITY INVARIANT: Store is typed as `db.AgentSafeDB`, guaranteeing strictly Read-Only access
// to the public CTD Ledger for Zed, Google Antigravity, Claude Code/Desktop, and custom harnesses.
type Options struct {
	Store           db.AgentSafeDB
	Brand           brand.Spec
	ActiveMesh      string
	Optimizer       llm.BaseLLM
	Version         string
	AllowWriteViews bool
}

// ServerOptions is an alias for Options for callers referencing the explicit server options type name.
type ServerOptions = Options

// HTTPOptions configures Bearer authentication and DNS-rebinding guards for Streamable HTTP mode.
type HTTPOptions struct {
	AuthToken                       string
	AllowUnauthenticatedNonLoopback bool
	EnforceLoopbackHost             bool
}

// New constructs a configured `*server.MCPServer` ready for `ServeStdio` or `ServeHTTPWithOptions`.
func New(opts Options) *server.MCPServer {
	bSpec := opts.Brand.MergeWithDefaults()
	version := strings.TrimSpace(opts.Version)
	if version == "" {
		version = "0.2.0"
	}
	mathEngine := pigeonmath.NewEngineWithSchema(opts.Store, bSpec.MathSchema)

	s := server.NewMCPServer(
		bSpec.MCPName+" ("+bSpec.ProductName+" CTD Telemetry)",
		version,
		server.WithToolCapabilities(false),
		server.WithRecovery(),
	)

	// 0. get_mesh_identity
	meshTool := mcp.NewTool("get_mesh_identity",
		mcp.WithDescription("Return the registered Mesh Name System (MNS) identity of the connected PigeonCoop ledger and workstation active mesh."),
	)
	s.AddTool(meshTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		identity, err := opts.Store.GetMeshIdentity(ctx)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Database error: %v", err)), nil
		}
		resp := map[string]any{
			"workstation_active_mesh": opts.ActiveMesh,
			"coop_mesh_identity":      identity,
		}
		raw, _ := json.MarshalIndent(resp, "", "  ")
		return mcp.NewToolResultText(string(raw)), nil
	})

	// 1. get_cluster_status
	clusterTool := mcp.NewTool("get_cluster_status",
		mcp.WithDescription("List all registered mesh nodes with hardware architecture, tags, liveness, and latest metrics."),
	)
	s.AddTool(clusterTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		summaries, err := opts.Store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Database error: %v", err)), nil
		}
		raw, _ := json.MarshalIndent(summaries, "", "  ")
		return mcp.NewToolResultText(string(raw)), nil
	})

	// 2. get_node_telemetry
	telemetryTool := mcp.NewTool("get_node_telemetry",
		mcp.WithDescription("Fetch the latest CTD telemetry events for a specific node."),
		mcp.WithString("node_id", mcp.Required(), mcp.Description("Hardware node ID (e.g., rv64-coop-01, rv64-compute-01, arm64-gateway-01)")),
	)
	s.AddTool(telemetryTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		nodeID, err := req.RequireString("node_id")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid node_id: %v", err)), nil
		}
		rows, err := opts.Store.QueryReadOnlyMaps(ctx, `
			SELECT pillar, component, status, metrics, event_timestamp
			FROM telemetric_ledger
			WHERE node_id = $1
			ORDER BY event_timestamp DESC
			LIMIT 15
		`, nodeID)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Database error: %v", err)), nil
		}
		if len(rows) == 0 {
			return mcp.NewToolResultText("No telemetry found for " + nodeID), nil
		}
		var sb strings.Builder
		for _, r := range rows {
			tsStr := fmt.Sprint(r["event_timestamp"])
			if ts, ok := r["event_timestamp"].(time.Time); ok {
				tsStr = ts.UTC().Format(time.RFC3339)
			}
			metricsJSON, _ := json.Marshal(r["metrics"])
			fmt.Fprintf(&sb, "[%s] (%v) %v [%v]: %s\n", tsStr, r["pillar"], r["component"], r["status"], string(metricsJSON))
		}
		return mcp.NewToolResultText(sb.String()), nil
	})

	// 3. check_offline_nodes
	offlineTool := mcp.NewTool("check_offline_nodes",
		mcp.WithDescription("Identify any nodes that stopped reporting within the cluster health window or entered an unhealthy state."),
	)
	s.AddTool(offlineTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		summaries, err := opts.Store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Database error: %v", err)), nil
		}
		var sb strings.Builder
		for _, n := range summaries {
			if !n.IsHealthy {
				fmt.Fprintf(&sb, "WARNING %s (arch: %s, tags: %v, status: %s, component: %s, last seen: %s ago)\n",
					n.NodeID, n.Arch, n.Tags, n.Status, n.Component, time.Since(n.LastSeen).Round(time.Second))
			}
		}
		if sb.Len() == 0 {
			return mcp.NewToolResultText("All registered nodes are currently online and healthy."), nil
		}
		return mcp.NewToolResultText(sb.String()), nil
	})

	// 4. query_ctd_readonly
	sqlTool := mcp.NewTool("query_ctd_readonly",
		mcp.WithDescription("Execute a strictly read-only SELECT query against the CTD Ledger or certified pigeonmath.pmv_* views."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("Single read-only SELECT or WITH ... SELECT statement")),
	)
	s.AddTool(sqlTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		sqlStr, err := req.RequireString("sql")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid sql: %v", err)), nil
		}
		rows, err := opts.Store.QueryReadOnlyMaps(ctx, sqlStr)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Read-only query error: %v", err)), nil
		}
		if len(rows) > 100 {
			rows = rows[:100]
		}
		raw, _ := json.MarshalIndent(rows, "", "  ")
		return mcp.NewToolResultText(string(raw)), nil
	})

	// 5. get_lineage_dag
	lineageTool := mcp.NewTool("get_lineage_dag",
		mcp.WithDescription("Walk upstream and downstream CTD artifact provenance edges from a given artifact ID."),
		mcp.WithString("artifact_id", mcp.Required(), mcp.Description("Root artifact ID to inspect")),
	)
	s.AddTool(lineageTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		artifactID, err := req.RequireString("artifact_id")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid artifact_id: %v", err)), nil
		}
		dag, err := opts.Store.GetLineageDAG(ctx, artifactID, 10)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Lineage query error: %v", err)), nil
		}
		out, _ := json.MarshalIndent(dag, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	})

	// 6. get_ledger_activity
	activityTool := mcp.NewTool("get_ledger_activity",
		mcp.WithDescription("Inspect write/update actor attribution across the CTD Ledger."),
	)
	s.AddTool(activityTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		items, err := opts.Store.GetLedgerActivity(ctx, 25)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Activity query error: %v", err)), nil
		}
		out, _ := json.MarshalIndent(items, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	})

	// 7. list_pigeonmath_views
	listViewsTool := mcp.NewTool("list_pigeonmath_views",
		mcp.WithDescription("List cached and certified analytical views (pigeonmath.pmv_*) including original raw SQL intent."),
	)
	s.AddTool(listViewsTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		views, err := mathEngine.ListPMVs(ctx)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("List PMVs error: %v", err)), nil
		}
		out, _ := json.MarshalIndent(views, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	})

	// 8. create_pigeonmath_view (opt-in only via AllowWriteViews)
	if opts.AllowWriteViews {
		createViewTool := mcp.NewTool("create_pigeonmath_view",
			mcp.WithDescription(fmt.Sprintf("Cache raw SQL intent, validate read-only safety, and materialize an analytical view inside %s.pmv_<name>.", bSpec.MathSchema)),
			mcp.WithString("view_name", mcp.Required(), mcp.Description("Analytical view slug (e.g., cooling_headroom)")),
			mcp.WithString("sql", mcp.Required(), mcp.Description("Single read-only SELECT query")),
		)
		s.AddTool(createViewTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			viewName, err := req.RequireString("view_name")
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Invalid view_name: %v", err)), nil
			}
			rawSQL, err := req.RequireString("sql")
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Invalid sql: %v", err)), nil
			}
			rec, err := mathEngine.SubmitAndCertifyPMV(ctx, strings.ToLower(bSpec.MCPName), viewName, rawSQL, opts.Optimizer)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("PMV certification failed: %v", err)), nil
			}
			out, _ := json.MarshalIndent(rec, "", "  ")
			return mcp.NewToolResultText(string(out)), nil
		})
	}

	return s
}

func isLoopbackOrigin(origin string) bool {
	if strings.TrimSpace(origin) == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	return config.IsLoopbackListen(u.Host)
}

const maxMCPRequestBodyBytes = 1 << 20 // 1 MiB request body ceiling

// NewHTTPHandler wraps an MCPServer in a Streamable HTTP handler with Bearer token auth,
// loopback DNS-rebinding guards, and a 1 MiB request body limit.
func NewHTTPHandler(s *server.MCPServer, opts HTTPOptions) http.Handler {
	inner := server.NewStreamableHTTPServer(s)
	expectedToken := strings.TrimSpace(opts.AuthToken)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.EnforceLoopbackHost {
			if !config.IsLoopbackListen(r.Host) {
				http.Error(w, "Forbidden: non-loopback Host header rejected on loopback listener", http.StatusForbidden)
				return
			}
			if !isLoopbackOrigin(r.Header.Get("Origin")) {
				http.Error(w, "Forbidden: cross-origin request rejected", http.StatusForbidden)
				return
			}
		}

		if expectedToken != "" {
			authHdr := strings.TrimSpace(r.Header.Get("Authorization"))
			const prefix = "Bearer "
			if len(authHdr) <= len(prefix) || !strings.EqualFold(authHdr[:len(prefix)], prefix) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="pigeonmcp"`)
				http.Error(w, "Unauthorized: missing Bearer token", http.StatusUnauthorized)
				return
			}
			got := strings.TrimSpace(authHdr[len(prefix):])
			if subtle.ConstantTimeCompare([]byte(got), []byte(expectedToken)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="pigeonmcp"`)
				http.Error(w, "Unauthorized: invalid Bearer token", http.StatusUnauthorized)
				return
			}
		} else if !opts.EnforceLoopbackHost {
			// Loopback-only mode without token: enforce Host and Origin checks to defeat browser DNS-rebinding/CSRF.
			if !config.IsLoopbackListen(r.Host) {
				http.Error(w, "Forbidden: non-loopback Host header rejected without auth token", http.StatusForbidden)
				return
			}
			if !isLoopbackOrigin(r.Header.Get("Origin")) {
				http.Error(w, "Forbidden: cross-origin request rejected", http.StatusForbidden)
				return
			}
		}

		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxMCPRequestBodyBytes)
		}
		inner.ServeHTTP(w, r)
	})
}

// ServeStdio runs the MCP server over standard input/output (compatible with Zed, Antigravity, Claude Desktop/Code).
func ServeStdio(s *server.MCPServer) error {
	return server.ServeStdio(s)
}

// ServeHTTP runs the MCP server over Streamable HTTP on a loopback address (e.g., "127.0.0.1:8089").
func ServeHTTP(s *server.MCPServer, listenAddr string) error {
	return ServeHTTPWithOptions(s, listenAddr, HTTPOptions{})
}

// ServeHTTPWithOptions runs the MCP server over Streamable HTTP with Bearer authentication and timeouts.
func ServeHTTPWithOptions(s *server.MCPServer, listenAddr string, opts HTTPOptions) error {
	return ServeHTTPContext(context.Background(), s, listenAddr, opts)
}

// ServeHTTPContext runs the MCP server over Streamable HTTP and shuts down cleanly when ctx is cancelled.
func ServeHTTPContext(ctx context.Context, s *server.MCPServer, listenAddr string, opts HTTPOptions) error {
	listenAddr = strings.TrimSpace(listenAddr)
	isLoopback := config.IsLoopbackListen(listenAddr)
	if !isLoopback && strings.TrimSpace(opts.AuthToken) == "" && !opts.AllowUnauthenticatedNonLoopback {
		return fmt.Errorf(
			"refusing unauthenticated MCP HTTP listener on non-loopback %q: configure --auth-token / PIGEONS_MCP_AUTH_TOKEN or bind 127.0.0.1",
			listenAddr,
		)
	}
	if isLoopback {
		opts.EnforceLoopbackHost = true
	}
	httpSrv := &http.Server{
		Addr:              listenAddr,
		Handler:           NewHTTPHandler(s, opts),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		return <-errCh
	}
}
