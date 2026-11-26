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
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

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
	maxSanitizeDepth           = 64
	maxMCPRequestBodyBytes     = 1 << 20 // 1 MiB request body ceiling
	defaultPOSTBodyReadTimeout = 15 * time.Second
	defaultHTTPShutdownTimeout = 5 * time.Second
)

var (
	timeType       = reflect.TypeOf(time.Time{})
	rawMsgType     = reflect.TypeOf(json.RawMessage(nil))
	jsonNumberType = reflect.TypeOf(json.Number(""))
	anyType        = reflect.TypeOf((*any)(nil)).Elem()

	errURLRe        = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>]+`)
	errBearerRe     = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;]+`)
	errAuthHeaderRe = regexp.MustCompile(`(?i)\b(authorization|x-api-key)\s*[:=]\s*[^\r\n,;]+|\bx-api-key\s+[^\s,;]+`)
	errAPIKeyTokRe  = regexp.MustCompile(`\bsk-[a-zA-Z0-9_-]{8,}\b`)
	errKeyValRe     = regexp.MustCompile(`(?i)\b(password|passwd|pwd|api_key|apikey|token|secret|passfile|host|hostaddr|user)\s*=\s*(?:'[^']*'|"[^"]*"|\S+)`)
	errDialRe       = regexp.MustCompile(`(?i)\bdial\s+(?:tcp|udp|unix)\s+[^\s:]+(?::\d+)?`)
	errSQLStateRe   = regexp.MustCompile(`(?i)\bSQLSTATE\s+[A-Z0-9]{5}\b`)
	errIPPortRe     = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
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

// New constructs a configured `*server.MCPServer` ready for `ServeStdio` or `ServeHTTPWithOptions`.
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
			metricsJSON, err := json.Marshal(sanitizeJSONValue(r["metrics"]))
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
			return mcp.NewToolResultError("Invalid sql: " + sanitizeErrorText(err.Error())), nil
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
				return mcp.NewToolResultError("Invalid view_name: " + sanitizeErrorText(err.Error())), nil
			}
			rawSQL, err := req.RequireString("sql")
			if err != nil {
				return mcp.NewToolResultError("Invalid sql: missing or non-string value"), nil
			}
			if _, err := db.ValidateSelectOnlySQL(rawSQL); err != nil {
				return mcp.NewToolResultError("Invalid sql: " + sanitizeErrorText(err.Error())), nil
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

func sanitizeErrorText(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "unknown error"
	}
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = errURLRe.ReplaceAllString(s, "[redacted-url]")
	s = errAuthHeaderRe.ReplaceAllString(s, "[redacted-auth]")
	s = errBearerRe.ReplaceAllString(s, "Bearer [redacted]")
	s = errAPIKeyTokRe.ReplaceAllString(s, "[redacted-key]")
	s = errKeyValRe.ReplaceAllString(s, "$1=[redacted]")
	s = errDialRe.ReplaceAllString(s, "dial [redacted-endpoint]")
	s = errSQLStateRe.ReplaceAllString(s, "[sqlstate-redacted]")
	s = errIPPortRe.ReplaceAllString(s, "[redacted-addr]")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 512 {
		return truncateValidUTF8(s, 512)
	}
	return s
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
		rec.AgentNotes = sanitizeErrorText(rec.AgentNotes)
	}
	if len(rec.ValidationReport) > 0 {
		cloned := make(map[string]any, len(rec.ValidationReport))
		for k, v := range rec.ValidationReport {
			if (k == "error" || k == "verdict") && v != nil {
				if str, ok := v.(string); ok {
					cloned[k] = sanitizeErrorText(str)
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
	sanitized := sanitizeJSONValue(v)
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
		return truncateValidUTF8(marker, maxBytes)
	}
	return truncateValidUTF8(s, maxBytes-len(marker)) + marker
}

func truncateValidUTF8(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	for cut > 0 {
		r, size := utf8.DecodeLastRuneInString(s[:cut])
		if r != utf8.RuneError || size != 1 {
			break
		}
		cut--
	}
	return s[:cut]
}

// sanitizeJSONValue recursively replaces non-finite floats (NaN, +Inf, -Inf) with JSON-safe nil
// without mutating caller-owned maps, slices, or structs.
func sanitizeJSONValue(v any) any {
	if v == nil {
		return nil
	}
	sv, changed := sanitizeReflectValue(reflect.ValueOf(v), 0)
	if !changed {
		return v
	}
	if !sv.IsValid() {
		return nil
	}
	return sv.Interface()
}

func isNonFiniteFloat(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}

func canHoldNil(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return true
	default:
		return false
	}
}

func sanitizeReflectValue(v reflect.Value, depth int) (reflect.Value, bool) {
	if !v.IsValid() || depth > maxSanitizeDepth {
		return v, false
	}
	if v.Type() == timeType || v.Type() == rawMsgType {
		return v, false
	}

	if v.Type() == jsonNumberType {
		s := strings.TrimSpace(v.String())
		if f, err := strconv.ParseFloat(s, 64); err == nil && isNonFiniteFloat(f) {
			return reflect.Value{}, true
		}
		return v, false
	}

	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		if isNonFiniteFloat(v.Float()) {
			return reflect.Value{}, true
		}
		return v, false

	case reflect.Interface:
		if v.IsNil() {
			return v, false
		}
		elem, changed := sanitizeReflectValue(v.Elem(), depth+1)
		if !changed {
			return v, false
		}
		if !elem.IsValid() {
			return reflect.Zero(v.Type()), true
		}
		if elem.Type().AssignableTo(v.Type()) {
			out := reflect.New(v.Type()).Elem()
			out.Set(elem)
			return out, true
		}
		return v, false

	case reflect.Pointer:
		if v.IsNil() {
			return v, false
		}
		elem, changed := sanitizeReflectValue(v.Elem(), depth+1)
		if !changed {
			return v, false
		}
		if !elem.IsValid() {
			return reflect.Zero(v.Type()), true
		}
		if elem.Type().AssignableTo(v.Type().Elem()) {
			ptr := reflect.New(v.Type().Elem())
			ptr.Elem().Set(elem)
			return ptr, true
		}
		return elem, true

	case reflect.Map:
		if v.IsNil() || v.Len() == 0 {
			return v, false
		}
		elemType := v.Type().Elem()
		outElemType := elemType
		type kv struct {
			k reflect.Value
			v reflect.Value
		}
		var entries []kv
		changed := false

		iter := v.MapRange()
		for iter.Next() {
			mk := iter.Key()
			mv := iter.Value()
			sv, c := sanitizeReflectValue(mv, depth+1)
			if c {
				changed = true
				if !sv.IsValid() && !canHoldNil(outElemType) {
					outElemType = anyType
				} else if sv.IsValid() && !sv.Type().AssignableTo(outElemType) {
					outElemType = anyType
				}
			}
			entries = append(entries, kv{k: mk, v: sv})
		}
		if !changed {
			return v, false
		}
		out := reflect.MakeMapWithSize(reflect.MapOf(v.Type().Key(), outElemType), len(entries))
		for _, e := range entries {
			if !e.v.IsValid() {
				out.SetMapIndex(e.k, reflect.Zero(outElemType))
				continue
			}
			if e.v.Type().AssignableTo(outElemType) {
				out.SetMapIndex(e.k, e.v)
				continue
			}
			wrapped := reflect.New(outElemType).Elem()
			wrapped.Set(e.v)
			out.SetMapIndex(e.k, wrapped)
		}
		return out, true

	case reflect.Slice:
		if v.IsNil() || v.Len() == 0 || v.Type().Elem().Kind() == reflect.Uint8 {
			return v, false
		}
		return sanitizeSequence(v, depth)

	case reflect.Array:
		if v.Len() == 0 || v.Type().Elem().Kind() == reflect.Uint8 {
			return v, false
		}
		return sanitizeSequence(v, depth)

	case reflect.Struct:
		t := v.Type()
		n := v.NumField()
		sanitizedFields := make(map[int]reflect.Value)
		changed := false
		needsMap := false
		for i := 0; i < n; i++ {
			sf := t.Field(i)
			fv := v.Field(i)
			isEmbeddedStruct := sf.Anonymous && isStructOrPtrToStruct(fv.Type())
			if !sf.IsExported() && !isEmbeddedStruct {
				continue
			}
			if _, _, _, skip := parseJSONTag(sf.Tag.Get("json")); skip {
				continue
			}
			sv, c := sanitizeReflectValue(fv, depth+1)
			if !c {
				continue
			}
			changed = true
			sanitizedFields[i] = sv
			if !sf.IsExported() || (!sv.IsValid() && !canHoldNil(fv.Type())) || (sv.IsValid() && !sv.Type().AssignableTo(fv.Type())) {
				needsMap = true
			}
		}
		if !changed {
			return v, false
		}
		if needsMap {
			return reflect.ValueOf(structToJSONMap(v, sanitizedFields)), true
		}
		out := reflect.New(t).Elem()
		out.Set(v)
		for i, sv := range sanitizedFields {
			dst := out.Field(i)
			if !sv.IsValid() {
				dst.SetZero()
			} else {
				dst.Set(sv)
			}
		}
		return out, true

	default:
		return v, false
	}
}

func sanitizeSequence(v reflect.Value, depth int) (reflect.Value, bool) {
	n := v.Len()
	elemType := v.Type().Elem()
	outElemType := elemType
	items := make([]reflect.Value, n)
	changed := false

	for i := 0; i < n; i++ {
		sv, c := sanitizeReflectValue(v.Index(i), depth+1)
		if c {
			changed = true
			if !sv.IsValid() && !canHoldNil(outElemType) {
				outElemType = anyType
			} else if sv.IsValid() && !sv.Type().AssignableTo(outElemType) {
				outElemType = anyType
			}
		}
		items[i] = sv
	}
	if !changed {
		return v, false
	}

	if v.Kind() == reflect.Array && outElemType == elemType {
		out := reflect.New(v.Type()).Elem()
		for i, item := range items {
			if !item.IsValid() {
				out.Index(i).SetZero()
			} else {
				out.Index(i).Set(item)
			}
		}
		return out, true
	}

	out := reflect.MakeSlice(reflect.SliceOf(outElemType), n, n)
	for i, item := range items {
		if !item.IsValid() {
			continue
		}
		if item.Type().AssignableTo(outElemType) {
			out.Index(i).Set(item)
		} else {
			out.Index(i).Set(item)
		}
	}
	return out, true
}

func parseJSONTag(tag string) (name string, omitEmpty, omitZero, skip bool) {
	if tag == "-" {
		return "", false, false, true
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, opt := range parts[1:] {
		switch opt {
		case "omitempty":
			omitEmpty = true
		case "omitzero":
			omitZero = true
		}
	}
	return name, omitEmpty, omitZero, false
}

func isEmptyJSONReflectValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

func structToJSONMap(v reflect.Value, sanitizedFields map[int]reflect.Value) map[string]any {
	out := make(map[string]any, v.NumField())
	t := v.Type()

	// Process direct fields first so they take precedence over anonymous embedded struct fields.
	type embeddedEntry struct {
		val reflect.Value
	}
	var embedded []embeddedEntry

	for i := 0; i < v.NumField(); i++ {
		sf := t.Field(i)
		fv := v.Field(i)
		isEmbeddedStruct := sf.Anonymous && isStructOrPtrToStruct(fv.Type())
		if !sf.IsExported() && !isEmbeddedStruct {
			continue
		}
		tagName, omitEmpty, omitZero, skip := parseJSONTag(sf.Tag.Get("json"))
		if skip {
			continue
		}
		sv, hasSanitized := sanitizedFields[i]

		if sf.Anonymous && tagName == "" {
			eff := fv
			if hasSanitized {
				eff = sv
			}
			if eff.IsValid() && eff.Kind() == reflect.Pointer {
				if eff.IsNil() {
					continue
				}
				eff = eff.Elem()
			}
			if eff.IsValid() && (eff.Kind() == reflect.Struct || eff.Kind() == reflect.Map) {
				embedded = append(embedded, embeddedEntry{val: eff})
				continue
			}
		}
		if !sf.IsExported() {
			if hasSanitized && sv.IsValid() && sv.CanInterface() {
				if tagName == "" {
					tagName = sf.Name
				}
				out[tagName] = sv.Interface()
			} else if isEmbeddedStruct {
				eff := fv
				if eff.Kind() == reflect.Pointer {
					if eff.IsNil() {
						continue
					}
					eff = eff.Elem()
				}
				if tagName == "" {
					tagName = sf.Name
				}
				out[tagName] = structToJSONMap(eff, nil)
			}
			continue
		}

		if tagName == "" {
			tagName = sf.Name
		}
		if omitEmpty && !hasSanitized && isEmptyJSONReflectValue(fv) {
			continue
		}
		if omitEmpty && hasSanitized && sv.IsValid() && isEmptyJSONReflectValue(sv) && !isNonFiniteReflectFloat(fv) {
			continue
		}
		if omitZero && !hasSanitized && fv.IsZero() {
			continue
		}

		if !hasSanitized {
			out[tagName] = fv.Interface()
			continue
		}
		if !sv.IsValid() {
			out[tagName] = nil
			continue
		}
		out[tagName] = sv.Interface()
	}

	for _, emb := range embedded {
		var embMap map[string]any
		switch emb.val.Kind() {
		case reflect.Map:
			if emb.val.Type().Key().Kind() == reflect.String {
				embMap = make(map[string]any, emb.val.Len())
				iter := emb.val.MapRange()
				for iter.Next() {
					embMap[iter.Key().String()] = iter.Value().Interface()
				}
			}
		case reflect.Struct:
			embMap = structToJSONMap(emb.val, nil)
		}
		for k, val := range embMap {
			if _, exists := out[k]; !exists {
				out[k] = val
			}
		}
	}

	return out
}

func isStructOrPtrToStruct(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

func isNonFiniteReflectFloat(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	if v.Kind() == reflect.Float32 || v.Kind() == reflect.Float64 {
		return isNonFiniteFloat(v.Float())
	}
	return false
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
