package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/cobra"
)

func extractNodeTags(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return core.NormalizeTags(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := strings.TrimSpace(fmt.Sprint(item)); s != "" {
				out = append(out, s)
			}
		}
		return core.NormalizeTags(out)
	default:
		return nil
	}
}

var nodeCmd = &cobra.Command{
	Use:   "node <node_id>",
	Short: "Inspect latest component telemetry and tags for a specific node",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeID := strings.TrimSpace(args[0])
		if err := core.ValidateTopicToken(nodeID); err != nil {
			return fmt.Errorf("invalid node_id %q: %w", args[0], err)
		}

		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		out := cmd.OutOrStdout()

		store, err := openReadStoreForConfig(ctx, state.wsCfg, strings.ToLower(state.wsCfg.Brand.CLIName)+"-node")
		if err != nil {
			return fmt.Errorf("connect CTD Ledger: %w", err)
		}
		defer store.Close()

		return runNodeWithStore(ctx, out, store, nodeID)
	},
}

func runNodeWithStore(ctx context.Context, out io.Writer, store db.ReadOnlyDB, nodeID string) error {
	tagRows, tagErr := store.QueryReadOnlyMaps(ctx, `
		SELECT COALESCE(tags, '{}') AS tags
		FROM ctd_nodes
		WHERE node_id = $1
		LIMIT 1
	`, nodeID)
	if tagErr != nil {
		return fmt.Errorf("query node tags %q: %w", nodeID, tagErr)
	}
	var tags []string
	if len(tagRows) > 0 {
		tags = extractNodeTags(tagRows[0]["tags"])
	}

	rows, err := store.QueryReadOnlyMaps(ctx, `
		SELECT DISTINCT ON (component)
			pillar, component, status, metrics, event_timestamp
		FROM telemetric_ledger
		WHERE node_id = $1
		ORDER BY component, event_timestamp DESC
	`, nodeID)
	if err != nil {
		return fmt.Errorf("query node %q: %w", nodeID, err)
	}

	tagsStr := "-"
	if len(tags) > 0 {
		tagsStr = strings.Join(tags, ",")
	}
	if len(rows) == 0 {
		if len(tags) > 0 {
			fmt.Fprintf(out, "Node: %s\nTags: %s\n\nNo telemetry records found for node %q.\n", nodeID, tagsStr, nodeID)
			return nil
		}
		fmt.Fprintf(out, "No telemetry records found for node %q.\n", nodeID)
		return nil
	}

	fmt.Fprintf(out, "Node: %s\nTags: %s\n\n", nodeID, tagsStr)
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "component\t| pillar\t| status\t| last seen\t| metrics")
	fmt.Fprintln(w, "____________________________________________________________________________________\t")

	for _, r := range rows {
		tsStr := fmt.Sprint(r["event_timestamp"])
		if ts, ok := r["event_timestamp"].(time.Time); ok {
			tsStr = ts.UTC().Format(time.RFC3339)
		}
		metricsJSON, mErr := json.Marshal(r["metrics"])
		if mErr != nil {
			metricsJSON = []byte("{}")
		}
		fmt.Fprintf(w, "%v\t| %v\t| %v\t| %s\t| %s\n",
			r["component"], r["pillar"], r["status"], tsStr, string(metricsJSON))
	}
	return w.Flush()
}

func init() {
	rootCmd.AddCommand(nodeCmd)
}
