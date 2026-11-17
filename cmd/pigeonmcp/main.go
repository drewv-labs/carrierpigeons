package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	ctx := context.Background()

	// 1. Connect to the PostgreSQL CTD Ledger
	store, err := db.NewPostgresStore(ctx, db.ResolveDBURL())
	if err != nil {
		log.Fatalf("MCP failed to connect to ledger: %v", err)
	}
	defer store.Close()

	// 2. Initialize the MCP Server
	s := server.NewMCPServer(
		"CarrierPigeons CTD Telemetry",
		"1.0.0",
		server.WithToolCapabilities(false),
		server.WithRecovery(),
	)

	// 3. Tool: Get Node Telemetry
	telemetryTool := mcp.NewTool("get_node_telemetry",
		mcp.WithDescription("Fetch the latest telemetry metrics for a specific edge node."),
		mcp.WithString("node_id", mcp.Required(), mcp.Description("The hardware node ID (e.g., edge-arm64-01, edge-rv64-01)")),
	)

	s.AddTool(telemetryTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		nodeID, err := req.RequireString("node_id")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid node_id: %v", err)), nil
		}

		query := `
			SELECT pillar, component, status, metrics, event_timestamp
			FROM telemetric_ledger
			WHERE node_id = $1
			ORDER BY event_timestamp DESC
			LIMIT 10
		`
		rows, err := store.QueryMaps(ctx, query, nodeID)
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

	// 4. Tool: Check Offline / Stale Nodes
	offlineTool := mcp.NewTool("check_offline_nodes",
		mcp.WithDescription("Identify any edge nodes that have stopped reporting data in the last 60 seconds or are in an unhealthy state."),
	)

	s.AddTool(offlineTool, func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		summaries, err := store.GetClusterStatus(ctx, 60*time.Second)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Database error: %v", err)), nil
		}

		var sb strings.Builder
		for _, n := range summaries {
			if !n.IsHealthy {
				fmt.Fprintf(&sb, "WARNING %s (status: %s, component: %s, last seen: %s ago)\n",
					n.NodeID, n.Status, n.Component, time.Since(n.LastSeen).Round(time.Second))
			}
		}

		if sb.Len() == 0 {
			return mcp.NewToolResultText("All registered nodes are currently online and healthy."), nil
		}
		return mcp.NewToolResultText(sb.String()), nil
	})

	// 5. Tool: Get Artifact Lineage DAG
	lineageTool := mcp.NewTool("get_lineage_dag",
		mcp.WithDescription("Walk upstream and downstream CTD artifact provenance edges from a given artifact ID."),
		mcp.WithString("artifact_id", mcp.Required(), mcp.Description("Root artifact ID to inspect")),
	)

	s.AddTool(lineageTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		artifactID, err := req.RequireString("artifact_id")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid artifact_id: %v", err)), nil
		}

		dag, err := store.GetLineageDAG(ctx, artifactID, 10)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Lineage query error: %v", err)), nil
		}

		out, err := json.MarshalIndent(dag, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to serialize DAG: %v", err)), nil
		}
		return mcp.NewToolResultText(string(out)), nil
	})

	// 6. Serve MCP over Standard I/O
	if err := server.ServeStdio(s); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
