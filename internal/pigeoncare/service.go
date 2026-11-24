package pigeoncare

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

// RequestDispatcher transmits a RunnerRequestCTD to PigeonCoop over MQTT (`pigeons/requests/runner`).
type RequestDispatcher func(ctx context.Context, req core.RunnerRequestCTD) error

// Service is the whitelabelable PigeonCare diagnostic & SRE agent service.
// SECURITY INVARIANT: Service holds ONLY `db.AgentSafeDB`—it physically lacks write methods to the public CTD Ledger.
type Service struct {
	store      db.AgentSafeDB
	math       *pigeonmath.Engine
	gate       *PolicyGate
	dispatcher RequestDispatcher
	llm        llm.BaseLLM
	plainLLM   llm.BaseLLM // tool-free handle used for internal PigeonMath SQL optimization passes
	brand      brand.Spec
}

// Options configures a PigeonCare Service instance.
type Options struct {
	Store      db.AgentSafeDB
	LLMConfig  config.WorkstationLLMConfig
	Permission config.PermissionConfig
	Prompter   Prompter
	Dispatcher RequestDispatcher
	Brand      brand.Spec
}

// NewService constructs a PigeonCare Service backed strictly by AgentSafeDB.
func NewService(opts Options) *Service {
	bSpec := opts.Brand.MergeWithDefaults()
	gate := NewPolicyGate(opts.Permission, opts.Prompter)

	svc := &Service{
		store:      opts.Store,
		math:       pigeonmath.NewEngineWithSchema(opts.Store, bSpec.MathSchema),
		gate:       gate,
		dispatcher: opts.Dispatcher,
		brand:      bSpec,
	}

	svc.plainLLM = llm.New(llm.Config{
		Provider: opts.LLMConfig.Provider,
		Model:    opts.LLMConfig.Model,
		Endpoint: opts.LLMConfig.Endpoint,
		APIKey:   opts.LLMConfig.APIKey,
	})
	svc.llm = llm.New(llm.Config{
		Provider: opts.LLMConfig.Provider,
		Model:    opts.LLMConfig.Model,
		Endpoint: opts.LLMConfig.Endpoint,
		APIKey:   opts.LLMConfig.APIKey,
		Tools:    svc,
	})

	return svc
}

func (s *Service) PolicyGate() *PolicyGate        { return s.gate }
func (s *Service) MathEngine() *pigeonmath.Engine { return s.math }
func (s *Service) LLM() llm.BaseLLM               { return s.llm }
func (s *Service) PlainLLM() llm.BaseLLM          { return s.plainLLM }

// SystemPrompt generates the active whitelabelable SRE system prompt.
func (s *Service) SystemPrompt() string {
	return fmt.Sprintf(`You are %s, the autonomous hardware diagnostics & SRE specialist for %s.
Capabilities & Boundaries:
1. You have STRICTLY READ-ONLY access to the public PostgreSQL CTD Ledger (telemetric_ledger, ctd_nodes, ctd_artifacts, ctd_traces). You can NEVER insert, update, or delete ledger records.
2. You can create certified analytical SQL views inside the isolated %s schema (%s.pmv_*) via propose_pigeonmath_view.
3. You can synthesize and deploy sandboxed ephemeral diagnostic Runners (sh, bash, python3) to heterogeneous nodes (riscv64, arm64, amd64) via dispatch_ephemeral_runner, subject to operator permission mode (%s).
Keep diagnostics concise, data-driven, and cite concrete metrics (CPU cooler RPM, thermals, load, NPU/GPU telemetry).`,
		s.brand.AgentName, s.brand.ProductName, s.brand.MathSchema, s.brand.MathSchema, s.gate.Mode().DisplayLabel())
}

// StreamChat streams an assistant turn with automatic read-only ledger & gated runner tool execution.
func (s *Service) StreamChat(ctx context.Context, history []llm.Message) (<-chan string, error) {
	return s.llm.StreamChat(ctx, s.SystemPrompt(), history)
}

// Definitions returns OpenAI/Ollama function schemas exposed to PigeonCare.
func (s *Service) Definitions() []map[string]any {
	return []map[string]any{
		{
			"type": "function",
			"function": map[string]any{
				"name":        "get_cluster_status",
				"description": "List all registered nodes in the mesh with hardware architecture, tags, liveness, and latest metrics.",
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        "get_node_telemetry",
				"description": "Fetch recent telemetry events for a specific node from the read-only PostgreSQL CTD Ledger.",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"node_id": map[string]any{
							"type":        "string",
							"description": "Target hardware node ID (e.g., rv64-coop-01, rv64-compute-01, arm64-gateway-01)",
						},
					},
					"required": []string{"node_id"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        "analyze_latest_benchmark",
				"description": "Inspect hardware observer telemetry captured between the latest runner-start and runner-end bookends for a node.",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"node_id": map[string]any{
							"type":        "string",
							"description": "Target hardware node ID (e.g., rv64-compute-01)",
						},
					},
					"required": []string{"node_id"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        "query_readonly_ledger",
				"description": "Execute a read-only SELECT query against telemetric_ledger, ctd_nodes, ctd_artifacts, or ctd_traces.",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"sql": map[string]any{
							"type":        "string",
							"description": "Single read-only SELECT or WITH ... SELECT statement",
						},
					},
					"required": []string{"sql"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        "propose_pigeonmath_view",
				"description": "Cache, optimize, verify, and materialize a certified analytical view (pigeonmath.pmv_<name>).",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"view_name": map[string]any{
							"type":        "string",
							"description": "Slug name for the view (e.g., thermal_efficiency -> pigeonmath.pmv_thermal_efficiency)",
						},
						"sql": map[string]any{
							"type":        "string",
							"description": "Raw SELECT query defining the analytical view",
						},
					},
					"required": []string{"view_name", "sql"},
				},
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        "dispatch_ephemeral_runner",
				"description": "Synthesize and deploy an ad-hoc sandboxed diagnostic script (sh, bash, python3) to target nodes or tags.",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title": map[string]any{
							"type":        "string",
							"description": "Concise human summary of what the ephemeral runner checks",
						},
						"interpreter": map[string]any{
							"type":        "string",
							"description": "Interpreter binary: 'sh', 'bash', or 'python3'",
						},
						"script_body": map[string]any{
							"type":        "string",
							"description": "Complete script source code to execute in an isolated temporary directory",
						},
						"nodes": map[string]any{
							"type":        "array",
							"items":       map[string]any{"type": "string"},
							"description": "Target node IDs (e.g., ['rv64-compute-01'])",
						},
						"tags": map[string]any{
							"type":        "array",
							"items":       map[string]any{"type": "string"},
							"description": "Target node tags (e.g., ['compute-bench'])",
						},
						"timeout_sec": map[string]any{
							"type":        "integer",
							"description": "Execution timeout in seconds (default 30, max 300)",
						},
					},
					"required": []string{"interpreter", "script_body"},
				},
			},
		},
	}
}

func toStringSlice(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := strings.TrimSpace(fmt.Sprint(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return strings.Split(v, ",")
	default:
		return nil
	}
}

func formatRowTimestamp(raw any, layout string) string {
	if ts, ok := raw.(time.Time); ok {
		return ts.UTC().Format(layout)
	}
	return fmt.Sprint(raw)
}

func formatRowMetrics(raw any) string {
	switch v := raw.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(b)
	}
}

// Execute implements llm.ToolExecutor strictly against AgentSafeDB and PolicyGate.
func (s *Service) Execute(ctx context.Context, name string, args map[string]any) string {
	if s.store == nil {
		return "Error: Read-only CTD Ledger is not connected."
	}

	switch name {
	case "get_cluster_status":
		summaries, err := s.store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return fmt.Sprintf("Database error: %v", err)
		}
		if len(summaries) == 0 {
			return "No nodes registered in the CTD Ledger yet."
		}
		raw, _ := json.MarshalIndent(summaries, "", "  ")
		return string(raw)

	case "get_node_telemetry":
		nodeID, _ := args["node_id"].(string)
		if strings.TrimSpace(nodeID) == "" {
			return "Error: Missing required argument 'node_id'"
		}
		rows, err := s.store.QueryReadOnlyMaps(ctx, `
			SELECT component, status, metrics, event_timestamp
			FROM telemetric_ledger
			WHERE node_id = $1
			ORDER BY event_timestamp DESC
			LIMIT 8
		`, nodeID)
		if err != nil {
			return fmt.Sprintf("Read-only ledger error: %v", err)
		}
		if len(rows) == 0 {
			return "No recent telemetry found for " + nodeID
		}
		var sb strings.Builder
		for _, r := range rows {
			fmt.Fprintf(&sb, "[%s] %v (%v): %s\n",
				formatRowTimestamp(r["event_timestamp"], "15:04:05"),
				r["component"],
				r["status"],
				formatRowMetrics(r["metrics"]))
		}
		return sb.String()

	case "analyze_latest_benchmark":
		nodeID, _ := args["node_id"].(string)
		if strings.TrimSpace(nodeID) == "" {
			return "Error: Missing required argument 'node_id'"
		}
		rows, err := s.store.QueryReadOnlyMaps(ctx, `
			WITH latest_run AS (
				SELECT
					metrics->>'run_id' AS run_id,
					event_timestamp AS start_time,
					COALESCE(
						(
							SELECT MIN(e.event_timestamp)
							FROM telemetric_ledger e
							WHERE e.node_id = $1
							  AND e.pillar = 'runner'
							  AND e.status IN ('runner-end', 'runner-aborted', 'runner-error')
							  AND e.metrics->>'run_id' = s.metrics->>'run_id'
						),
						s.event_timestamp + (COALESCE(NULLIF(s.metrics->>'duration_sec', '')::numeric, 60) || ' seconds')::interval
					) AS end_time
				FROM telemetric_ledger s
				WHERE s.node_id = $1
				  AND s.pillar = 'runner'
				  AND s.status = 'runner-start'
				ORDER BY s.event_timestamp DESC
				LIMIT 1
			)
			SELECT t.component, t.metrics, t.event_timestamp
			FROM telemetric_ledger t
			JOIN latest_run lr ON true
			WHERE t.node_id = $1
			  AND t.event_timestamp >= lr.start_time
			  AND t.event_timestamp <= lr.end_time
			  AND t.pillar = 'observer'
			ORDER BY t.event_timestamp ASC
			LIMIT 250
		`, nodeID)
		if err != nil {
			return fmt.Sprintf("Read-only ledger query failed: %v", err)
		}
		if len(rows) == 0 {
			return "No observer telemetry recorded during the latest runner window for " + nodeID
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "Found %d observer events during latest runner execution:\n", len(rows))
		for _, r := range rows {
			fmt.Fprintf(&sb, "[%s] %v: %s\n",
				formatRowTimestamp(r["event_timestamp"], "15:04:05.000"),
				r["component"],
				formatRowMetrics(r["metrics"]))
		}
		return sb.String()

	case "query_readonly_ledger":
		sqlQuery, _ := args["sql"].(string)
		rows, err := s.store.QueryReadOnlyMaps(ctx, sqlQuery)
		if err != nil {
			return fmt.Sprintf("Read-only SQL rejected/failed: %v", err)
		}
		if len(rows) > 50 {
			rows = rows[:50]
		}
		raw, _ := json.MarshalIndent(rows, "", "  ")
		return string(raw)

	case "propose_pigeonmath_view":
		viewName, _ := args["view_name"].(string)
		rawSQL, _ := args["sql"].(string)
		normName, err := db.NormalizePMVName(viewName)
		if err != nil {
			return fmt.Sprintf("Invalid view_name: %v", err)
		}

		proposal := ActionProposal{
			Kind:     ActionCreatePMV,
			Title:    fmt.Sprintf("Create %s view %s.%s", s.brand.MathName, s.brand.MathSchema, normName),
			ViewName: normName,
			RawSQL:   rawSQL,
		}
		if err := s.gate.Authorize(ctx, proposal); err != nil {
			return fmt.Sprintf("Permission denied: %v", err)
		}

		rec, err := s.math.SubmitAndCertifyPMV(ctx, strings.ToLower(s.brand.AgentName), normName, rawSQL, s.plainLLM)
		if err != nil {
			return fmt.Sprintf("PMV certification failed: %v", err)
		}
		return fmt.Sprintf("Certified %s.%s (status=%s): %s",
			s.brand.MathSchema, rec.ViewName, rec.Status, rec.AgentNotes)

	case "dispatch_ephemeral_runner":
		if s.dispatcher == nil {
			return "Error: MQTT RunnerRequestCTD dispatcher is not configured."
		}
		scriptBody, _ := args["script_body"].(string)
		if strings.TrimSpace(scriptBody) == "" {
			return "Error: Missing required argument 'script_body'"
		}
		interp, _ := args["interpreter"].(string)
		if strings.TrimSpace(interp) == "" {
			interp = "sh"
		}
		title, _ := args["title"].(string)
		if strings.TrimSpace(title) == "" {
			title = fmt.Sprintf("Ephemeral %s diagnostic runner", interp)
		}
		timeoutSec := 30
		if tFloat, ok := args["timeout_sec"].(float64); ok && int(tFloat) > 0 {
			timeoutSec = int(tFloat)
		} else if tInt, ok := args["timeout_sec"].(int); ok && tInt > 0 {
			timeoutSec = tInt
		}

		sel := core.RunnerSelector{
			Nodes: toStringSlice(args["nodes"]),
			Tags:  core.NormalizeTags(toStringSlice(args["tags"])),
		}
		if sel.IsEmpty() {
			return "Error: Specify at least one target in 'nodes' or 'tags'"
		}

		spec := runner.EphemeralSpec{
			Interpreter: interp,
			ScriptBody:  scriptBody,
			SHA256:      runner.ComputeScriptSHA256(scriptBody),
			TimeoutSec:  timeoutSec,
		}

		proposal := ActionProposal{
			Kind:        ActionEphemeralRunner,
			Title:       title,
			Runner:      "ephemeral",
			Selectors:   sel,
			Interpreter: spec.Interpreter,
			ScriptBody:  spec.ScriptBody,
			SHA256:      spec.SHA256,
			TimeoutSec:  spec.TimeoutSec,
		}
		if err := s.gate.Authorize(ctx, proposal); err != nil {
			return fmt.Sprintf("Ephemeral runner blocked: %v", err)
		}

		req := core.RunnerRequestCTD{
			RequestID:   fmt.Sprintf("care-%d", time.Now().UTC().UnixNano()),
			Action:      "start",
			Runner:      "ephemeral",
			Selectors:   sel,
			Params:      spec.ToParams(),
			RequestedBy: strings.ToLower(s.brand.AgentName),
			RequestedAt: time.Now().UTC(),
		}
		if err := s.dispatcher(ctx, req); err != nil {
			return fmt.Sprintf("Failed dispatching RunnerRequestCTD: %v", err)
		}
		return fmt.Sprintf("Dispatched ephemeral runner (request_id=%s, sha256=%s) to selectors nodes=%v tags=%v",
			req.RequestID, spec.SHA256[:12], sel.Nodes, sel.Tags)

	default:
		return fmt.Sprintf("Error: Unknown tool %q", name)
	}
}
