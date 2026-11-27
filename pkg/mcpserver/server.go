package mcpserver

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/drewv-labs/carrierpigeons/internal/jsonutil"
	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const (
	defaultReadToolTimeout     = 10 * time.Second
	defaultPMVToolTimeout      = 60 * time.Second
	maxMCPOutputBytes          = 256 * 1024 // 256 KiB serialized tool output cap
	mcpTruncationMarker        = "\n...[output truncated at 256 KiB]"
	maxArtifactIDBytes         = 256
	maxMCPRequestBodyBytes     = 1 << 20 // 1 MiB request body ceiling
	defaultPOSTBodyReadTimeout = 15 * time.Second
	defaultHTTPShutdownTimeout = 5 * time.Second
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

// HTTPOptions configures Bearer authentication, DNS-rebinding guards, and TLS/plaintext policy for Streamable HTTP mode.
type HTTPOptions struct {
	AuthToken                       string
	AllowUnauthenticatedNonLoopback bool
	EnforceLoopbackHost             bool
	TLSCertFile                     string
	TLSKeyFile                      string
	AllowPlaintextHTTP              bool
}

// New constructs a configured `*server.MCPServer` ready for `ServeStdio`, `ServeStdioContext`, or `ServeHTTPWithOptions`.
func New(opts Options) *server.MCPServer {
	return newWithTimeouts(opts, defaultReadToolTimeout, defaultPMVToolTimeout)
}

func newWithTimeouts(opts Options, readTimeout, pmvTimeout time.Duration) *server.MCPServer {
	if readTimeout <= 0 {
		readTimeout = defaultReadToolTimeout
	}
	if pmvTimeout <= 0 {
		pmvTimeout = defaultPMVToolTimeout
	}

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
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		identity, err := opts.Store.GetMeshIdentity(tCtx)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Database error", err)), nil
		}
		resp := map[string]any{
			"workstation_active_mesh": opts.ActiveMesh,
			"coop_mesh_identity":      identity,
		}
		return marshalToolJSONResult(resp), nil
	})

	// 1. get_cluster_status
	clusterTool := mcp.NewTool("get_cluster_status",
		mcp.WithDescription("List all registered mesh nodes with hardware architecture, tags, liveness, and latest metrics."),
	)
	s.AddTool(clusterTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		summaries, err := opts.Store.GetClusterStatus(tCtx, config.DefaultHealthyWindow)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Database error", err)), nil
		}
		return marshalToolJSONResult(summaries), nil
	})

	// 2. get_node_telemetry
	telemetryTool := mcp.NewTool("get_node_telemetry",
		mcp.WithDescription("Fetch the latest CTD telemetry events for a specific node."),
		mcp.WithString("node_id", mcp.Required(), mcp.Description("Hardware node ID (e.g., rv64-coop-01, rv64-compute-01, arm64-gateway-01)")),
	)
	s.AddTool(telemetryTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		nodeID, err := req.RequireString("node_id")
		if err != nil {
			return mcp.NewToolResultError("Invalid node_id: missing or non-string value"), nil
		}
		if err := core.ValidateTopicToken(nodeID); err != nil {
			return mcp.NewToolResultError("Invalid node_id: must match ^[a-zA-Z0-9._:-]{1,128}$"), nil
		}

		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		rows, err := opts.Store.QueryReadOnlyMaps(tCtx, `
			SELECT pillar, component, status, metrics, event_timestamp
			FROM telemetric_ledger
			WHERE node_id = $1
			ORDER BY event_timestamp DESC
			LIMIT 15
		`, nodeID)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Database error", err)), nil
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
			metricsJSON, err := json.Marshal(jsonutil.SanitizeValue(r["metrics"]))
			if err != nil {
				return mcp.NewToolResultError("Serialization error: failed to encode telemetry metrics"), nil
			}
			fmt.Fprintf(&sb, "[%s] (%v) %v [%v]: %s\n", tsStr, r["pillar"], r["component"], r["status"], string(metricsJSON))
			if sb.Len() > maxMCPOutputBytes {
				break
			}
		}
		return mcp.NewToolResultText(capMCPOutput(sb.String())), nil
	})

	// 3. check_offline_nodes
	offlineTool := mcp.NewTool("check_offline_nodes",
		mcp.WithDescription("Identify any nodes that stopped reporting within the cluster health window or entered an unhealthy state."),
	)
	s.AddTool(offlineTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		summaries, err := opts.Store.GetClusterStatus(tCtx, config.DefaultHealthyWindow)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Database error", err)), nil
		}
		var sb strings.Builder
		for _, n := range summaries {
			if !n.IsHealthy {
				fmt.Fprintf(&sb, "WARNING %s (arch: %s, tags: %v, status: %s, component: %s, last seen: %s ago)\n",
					n.NodeID, n.Arch, n.Tags, n.Status, n.Component, time.Since(n.LastSeen).Round(time.Second))
				if sb.Len() > maxMCPOutputBytes {
					break
				}
			}
		}
		if sb.Len() == 0 {
			return mcp.NewToolResultText("All registered nodes are currently online and healthy."), nil
		}
		return mcp.NewToolResultText(capMCPOutput(sb.String())), nil
	})

	// 4. query_ctd_readonly
	sqlTool := mcp.NewTool("query_ctd_readonly",
		mcp.WithDescription("Execute a strictly read-only SELECT query against the CTD Ledger or certified pigeonmath.pmv_* views."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("Single read-only SELECT or WITH ... SELECT statement")),
	)
	s.AddTool(sqlTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		sqlStr, err := req.RequireString("sql")
		if err != nil {
			return mcp.NewToolResultError("Invalid sql: missing or non-string value"), nil
		}
		if _, err := db.ValidateSelectOnlySQL(sqlStr); err != nil {
			return mcp.NewToolResultError("Invalid sql: " + jsonutil.SanitizeExternalErrorText(err.Error())), nil
		}

		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		rows, err := opts.Store.QueryReadOnlyMaps(tCtx, sqlStr)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Read-only query error", err)), nil
		}
		if len(rows) > 100 {
			rows = rows[:100]
		}
		return marshalToolJSONResult(rows), nil
	})

	// 5. get_lineage_dag
	lineageTool := mcp.NewTool("get_lineage_dag",
		mcp.WithDescription("Walk upstream and downstream CTD artifact provenance edges from a given artifact ID."),
		mcp.WithString("artifact_id", mcp.Required(), mcp.Description("Root artifact ID to inspect")),
	)
	s.AddTool(lineageTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		artifactID, err := req.RequireString("artifact_id")
		if err != nil {
			return mcp.NewToolResultError("Invalid artifact_id: missing or non-string value"), nil
		}
		if err := validateArtifactID(artifactID); err != nil {
			return mcp.NewToolResultError("Invalid artifact_id: " + err.Error()), nil
		}

		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		dag, err := opts.Store.GetLineageDAG(tCtx, artifactID, 10)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Lineage query error", err)), nil
		}
		return marshalToolJSONResult(dag), nil
	})

	// 6. get_ledger_activity
	activityTool := mcp.NewTool("get_ledger_activity",
		mcp.WithDescription("Inspect write/update actor attribution across the CTD Ledger."),
	)
	s.AddTool(activityTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		items, err := opts.Store.GetLedgerActivity(tCtx, 25)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("Activity query error", err)), nil
		}
		return marshalToolJSONResult(items), nil
	})

	// 7. list_pigeonmath_views
	listViewsTool := mcp.NewTool("list_pigeonmath_views",
		mcp.WithDescription("List cached and certified analytical views (pigeonmath.pmv_*) including original raw SQL intent."),
	)
	s.AddTool(listViewsTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if opts.Store == nil {
			return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
		}
		tCtx, cancel := withBoundedTimeout(ctx, readTimeout)
		defer cancel()

		views, err := mathEngine.ListPMVs(tCtx)
		if err != nil {
			return mcp.NewToolResultError(classifyUpstreamError("List PMVs error", err)), nil
		}
		return marshalToolJSONResult(sanitizePMVRecords(views)), nil
	})

	// 8. create_pigeonmath_view (opt-in only via AllowWriteViews)
	if opts.AllowWriteViews {
		createViewTool := mcp.NewTool("create_pigeonmath_view",
			mcp.WithDescription(fmt.Sprintf("Cache raw SQL intent, validate read-only safety, and materialize an analytical view inside %s.pmv_<name>.", bSpec.MathSchema)),
			mcp.WithString("view_name", mcp.Required(), mcp.Description("Analytical view slug (e.g., cooling_headroom)")),
			mcp.WithString("sql", mcp.Required(), mcp.Description("Single read-only SELECT query")),
		)
		s.AddTool(createViewTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if opts.Store == nil {
				return mcp.NewToolResultError("Read-only CTD Ledger is not connected."), nil
			}
			viewName, err := req.RequireString("view_name")
			if err != nil {
				return mcp.NewToolResultError("Invalid view_name: missing or non-string value"), nil
			}
			if _, err := db.NormalizePMVName(viewName); err != nil {
				return mcp.NewToolResultError("Invalid view_name: " + jsonutil.SanitizeExternalErrorText(err.Error())), nil
			}
			rawSQL, err := req.RequireString("sql")
			if err != nil {
				return mcp.NewToolResultError("Invalid sql: missing or non-string value"), nil
			}
			if _, err := db.ValidateSelectOnlySQL(rawSQL); err != nil {
				return mcp.NewToolResultError("Invalid sql: " + jsonutil.SanitizeExternalErrorText(err.Error())), nil
			}

			tCtx, cancel := withBoundedTimeout(ctx, pmvTimeout)
			defer cancel()

			rec, err := mathEngine.SubmitAndCertifyPMV(tCtx, strings.ToLower(bSpec.MCPName), viewName, rawSQL, opts.Optimizer)
			if err != nil {
				return mcp.NewToolResultError(classifyPMVError(err)), nil
			}
			if rec != nil {
				clean := sanitizePMVRecord(*rec)
				rec = &clean
			}
			return marshalToolJSONResult(rec), nil
		})
	}

	return s
}

func withBoundedTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, d)
}

func validateArtifactID(id string) error {
	if id == "" {
		return errors.New("cannot be empty")
	}
	if len(id) > maxArtifactIDBytes {
		return fmt.Errorf("exceeds %d-byte limit", maxArtifactIDBytes)
	}
	if !utf8.ValidString(id) {
		return errors.New("must be valid UTF-8")
	}
	for _, r := range id {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return errors.New("cannot contain whitespace or control characters")
		}
	}
	return nil
}

func classifyUpstreamError(prefix string, err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return prefix + ": request timed out"
	case errors.Is(err, context.Canceled):
		return prefix + ": request canceled"
	case errors.Is(err, db.ErrReadOnlyByteLimitExceeded):
		return prefix + ": query result exceeded maximum byte limit"
	default:
		return prefix + ": upstream database operation failed"
	}
}

func classifyPMVError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "PMV certification failed: request timed out"
	case errors.Is(err, context.Canceled):
		return "PMV certification failed: request canceled"
	case errors.Is(err, db.ErrReadOnlyByteLimitExceeded):
		return "PMV certification failed: sample probe exceeded maximum byte limit"
	case strings.Contains(err.Error(), "certification failed closed"):
		return "PMV certification failed: candidate SQL rejected by certification check"
	default:
		return "PMV certification failed: upstream database or optimizer error"
	}
}

func sanitizePMVRecord(rec db.PMVRecord) db.PMVRecord {
	if rec.AgentNotes != "" {
		rec.AgentNotes = jsonutil.SanitizeExternalErrorText(rec.AgentNotes)
	}
	if len(rec.ValidationReport) > 0 {
		cloned := make(map[string]any, len(rec.ValidationReport))
		for k, v := range rec.ValidationReport {
			if (k == "error" || k == "verdict") && v != nil {
				if str, ok := v.(string); ok {
					cloned[k] = jsonutil.SanitizeExternalErrorText(str)
					continue
				}
			}
			cloned[k] = v
		}
		rec.ValidationReport = cloned
	}
	return rec
}

func sanitizePMVRecords(recs []db.PMVRecord) []db.PMVRecord {
	if len(recs) == 0 {
		return recs
	}
	out := make([]db.PMVRecord, len(recs))
	for i, r := range recs {
		out[i] = sanitizePMVRecord(r)
	}
	return out
}

func marshalToolJSONResult(v any) *mcp.CallToolResult {
	sanitized := jsonutil.SanitizeValue(v)
	raw, err := json.MarshalIndent(sanitized, "", "  ")
	if err != nil {
		return mcp.NewToolResultError("Serialization error: failed to encode tool output")
	}
	return mcp.NewToolResultText(capMCPOutput(string(raw)))
}

func capMCPOutput(s string) string {
	return capUTF8Output(s, maxMCPOutputBytes, mcpTruncationMarker)
}

func capUTF8Output(s string, maxBytes int, marker string) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	if len(marker) >= maxBytes {
		return jsonutil.TruncateValidUTF8(marker, maxBytes)
	}
	return jsonutil.TruncateValidUTF8(s, maxBytes-len(marker)) + marker
}

func isLoopbackOrigin(origin string) bool {
	trimmed := strings.TrimSpace(origin)
	if trimmed == "" {
		return true
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" || u.User != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	return config.IsLoopbackListen(u.Host)
}

// ValidateHTTPOptions enforces auth, TLS keypair completeness, and non-loopback transport safety before listening.
func ValidateHTTPOptions(listenAddr string, opts HTTPOptions) error {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		return errors.New("empty MCP HTTP listen address")
	}
	certFile := strings.TrimSpace(opts.TLSCertFile)
	keyFile := strings.TrimSpace(opts.TLSKeyFile)
	if (certFile != "") != (keyFile != "") {
		return errors.New("refusing MCP HTTP startup: both --tls-cert and --tls-key must be provided together")
	}
	isLoopback := config.IsLoopbackListen(listenAddr)
	if !isLoopback {
		if strings.TrimSpace(opts.AuthToken) == "" && !opts.AllowUnauthenticatedNonLoopback {
			return fmt.Errorf(
				"refusing unauthenticated MCP HTTP listener on non-loopback %q: configure --auth-token / PIGEONS_MCP_AUTH_TOKEN or bind 127.0.0.1",
				listenAddr,
			)
		}
		hasTLS := certFile != "" && keyFile != ""
		if !hasTLS && !opts.AllowPlaintextHTTP {
			return fmt.Errorf(
				"refusing plaintext MCP HTTP listener on non-loopback %q: configure --tls-cert and --tls-key, bind 127.0.0.1, or pass --allow-plaintext-http",
				listenAddr,
			)
		}
	}
	return nil
}

// NewHTTPHandler wraps an MCPServer in a Streamable HTTP handler with Bearer token auth,
// loopback DNS-rebinding guards, a 1 MiB request body limit, and bounded POST body read deadlines.
func NewHTTPHandler(s *server.MCPServer, opts HTTPOptions) http.Handler {
	return newHTTPHandlerWithTimeouts(s, opts, defaultPOSTBodyReadTimeout)
}

func newHTTPHandlerWithTimeouts(s *server.MCPServer, opts HTTPOptions, postBodyReadTimeout time.Duration) http.Handler {
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
		} else if !opts.EnforceLoopbackHost && !opts.AllowUnauthenticatedNonLoopback {
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

		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			// Clear any prior keep-alive read deadline so long-lived SSE GET streams remain open.
			_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxMCPRequestBodyBytes)
			}
		} else if r.Body != nil && r.Body != http.NoBody {
			body, statusCode, msg := readBoundedRequestBody(w, r, maxMCPRequestBodyBytes, postBodyReadTimeout)
			if statusCode != 0 {
				w.Header().Set("Connection", "close")
				http.Error(w, msg, statusCode)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}

		inner.ServeHTTP(w, r)
	})
}

func readBoundedRequestBody(w http.ResponseWriter, r *http.Request, maxBytes int64, timeout time.Duration) ([]byte, int, string) {
	if r.ContentLength > maxBytes {
		return nil, http.StatusRequestEntityTooLarge, "Request body exceeds 1 MiB limit"
	}
	rc := http.NewResponseController(w)
	var dlErr error
	if timeout > 0 {
		dlErr = rc.SetReadDeadline(time.Now().Add(timeout))
	}

	var timedOut atomic.Bool
	if timeout > 0 && errors.Is(dlErr, http.ErrNotSupported) && r.ProtoMajor >= 2 {
		timer := time.AfterFunc(timeout, func() {
			timedOut.Store(true)
			_ = r.Body.Close()
		})
		defer timer.Stop()
	}

	limited := http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(limited)
	if err == nil {
		// Only clear the socket read deadline once the full body has been read cleanly.
		if timeout > 0 {
			_ = rc.SetReadDeadline(time.Time{})
		}
		return body, 0, ""
	}

	// Keep the socket read deadline expired on failed/timed-out POST reads so net/http's
	// finishRequest body drain cannot block indefinitely before flushing the error response.
	if timeout > 0 {
		_ = rc.SetReadDeadline(time.Now())
	}

	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return nil, http.StatusRequestEntityTooLarge, "Request body exceeds 1 MiB limit"
	}
	if timedOut.Load() || isReadTimeoutError(err) {
		return nil, http.StatusRequestTimeout, "Request body read timed out"
	}
	return nil, http.StatusBadRequest, "Invalid request body"
}

func isReadTimeoutError(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// ServeStdio runs the MCP server over standard input/output (compatible with Zed, Antigravity, Claude Desktop/Code).
func ServeStdio(s *server.MCPServer) error {
	return server.ServeStdio(s)
}

// ServeStdioContext runs the MCP server over standard input/output and shuts down cleanly when ctx is canceled.
func ServeStdioContext(ctx context.Context, s *server.MCPServer) error {
	return serveStdioIO(ctx, s, os.Stdin, os.Stdout)
}

func serveStdioIO(ctx context.Context, s *server.MCPServer, stdin io.Reader, stdout io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := server.NewStdioServer(s).Listen(ctx, stdin, stdout)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
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
	return serveHTTPContextWithConfig(ctx, s, listenAddr, opts, defaultPOSTBodyReadTimeout, defaultHTTPShutdownTimeout)
}

func serveHTTPContextWithConfig(
	ctx context.Context,
	s *server.MCPServer,
	listenAddr string,
	opts HTTPOptions,
	postBodyReadTimeout time.Duration,
	shutdownTimeout time.Duration,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	listenAddr = strings.TrimSpace(listenAddr)
	if err := ValidateHTTPOptions(listenAddr, opts); err != nil {
		return err
	}
	if config.IsLoopbackListen(listenAddr) {
		opts.EnforceLoopbackHost = true
	}

	certFile := strings.TrimSpace(opts.TLSCertFile)
	keyFile := strings.TrimSpace(opts.TLSKeyFile)
	var tlsCfg *tls.Config
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load MCP TLS keypair: %w", err)
		}
		tlsCfg = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	return serveHTTPListener(ctx, ln, newHTTPHandlerWithTimeouts(s, opts, postBodyReadTimeout), tlsCfg, shutdownTimeout)
}

func serveHTTPListener(
	ctx context.Context,
	ln net.Listener,
	handler http.Handler,
	tlsCfg *tls.Config,
	shutdownTimeout time.Duration,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultHTTPShutdownTimeout
	}

	httpSrv := &http.Server{
		Addr:              ln.Addr().String(),
		Handler:           handler,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	errCh := make(chan error, 1)
	go func() {
		var err error
		if tlsCfg != nil {
			err = httpSrv.ServeTLS(ln, "", "")
		} else {
			err = httpSrv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		shutdownErr := httpSrv.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			_ = httpSrv.Close()
		}
		serveErr := <-errCh
		if serveErr != nil {
			return serveErr
		}
		return shutdownErr
	}
}
