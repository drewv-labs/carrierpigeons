package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

// PigeonTools defines the SQL/CTD capabilities exposed to the local LLM.
var PigeonTools = []map[string]any{
	{
		"type": "function",
		"function": map[string]any{
			"name":        "get_node_telemetry",
			"description": "Fetch the latest telemetry metrics for a specific edge node from the PostgreSQL ledger.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"node_id": map[string]any{
						"type":        "string",
						"description": "The hardware node ID (e.g., edge-arm64-01, edge-rv64-01)",
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
			"description": "Fetch the hardware telemetry strictly bounded by the most recent runner execution for a given node.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"node_id": map[string]any{
						"type":        "string",
						"description": "The edge node ID (e.g., edge-arm64-01, edge-rv64-01)",
					},
				},
				"required": []string{"node_id"},
			},
		},
	},
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

// ExecuteTool routes the LLM's requested tool call through the db.BaseDB contract.
func ExecuteTool(ctx context.Context, name string, args map[string]any, store db.BaseDB) string {
	if store == nil {
		return "Error: Database ledger is not connected."
	}

	switch name {
	case "get_node_telemetry":
		nodeID, ok := args["node_id"].(string)
		if !ok || strings.TrimSpace(nodeID) == "" {
			return "Error: Missing required argument 'node_id'"
		}

		query := `
			SELECT component, metrics, event_timestamp
			FROM telemetric_ledger
			WHERE node_id = $1
			ORDER BY event_timestamp DESC
			LIMIT 5
		`
		rows, err := store.QueryMaps(ctx, query, nodeID)
		if err != nil {
			return fmt.Sprintf("Database error: %v", err)
		}
		if len(rows) == 0 {
			return "No recent telemetry found for " + nodeID
		}

		var sb strings.Builder
		for _, r := range rows {
			fmt.Fprintf(&sb, "[%s] %v: %s\n",
				formatRowTimestamp(r["event_timestamp"], "15:04:05"),
				r["component"],
				formatRowMetrics(r["metrics"]))
		}
		return sb.String()

	case "analyze_latest_benchmark":
		nodeID, ok := args["node_id"].(string)
		if !ok || strings.TrimSpace(nodeID) == "" {
			return "Error: Missing required argument 'node_id'"
		}

		query := `
			WITH latest_run AS (
				SELECT
					(metrics->>'run_id')::bigint AS run_id,
					event_timestamp AS start_time,
					COALESCE(
						(
							SELECT MIN(e.event_timestamp)
							FROM telemetric_ledger e
							WHERE e.node_id = $1
							  AND e.status IN ('runner-end', 'runner-aborted')
							  AND (e.metrics->>'run_id')::bigint = (s.metrics->>'run_id')::bigint
						),
						s.event_timestamp + (COALESCE(NULLIF(s.metrics->>'duration_sec', '')::numeric, 60) || ' seconds')::interval
					) AS end_time
				FROM telemetric_ledger s
				WHERE s.node_id = $1 AND s.status = 'runner-start'
				ORDER BY s.event_timestamp DESC
				LIMIT 1
			)
			SELECT t.component, t.metrics, t.event_timestamp
			FROM telemetric_ledger t
			JOIN latest_run lr ON true
			WHERE t.node_id = $1
			  AND t.event_timestamp >= lr.start_time
			  AND t.event_timestamp <= lr.end_time
			  AND t.component != 'runner:benchmark'
			ORDER BY t.event_timestamp ASC
		`
		rows, err := store.QueryMaps(ctx, query, nodeID)
		if err != nil {
			return fmt.Sprintf("Ledger query failed: %v", err)
		}
		if len(rows) == 0 {
			return "No telemetry was recorded during the last benchmark run for " + nodeID
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "Found %d hardware telemetry events during the benchmark:\n", len(rows))
		for _, r := range rows {
			fmt.Fprintf(&sb, "[%s] %v: %s\n",
				formatRowTimestamp(r["event_timestamp"], "15:04:05.000"),
				r["component"],
				formatRowMetrics(r["metrics"]))
		}
		return sb.String()

	default:
		return fmt.Sprintf("Error: Unknown tool %s", name)
	}
}
