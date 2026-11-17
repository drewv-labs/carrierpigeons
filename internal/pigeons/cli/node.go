package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/spf13/cobra"
)

var nodeCmd = &cobra.Command{
	Use:   "node [node_id]",
	Short: "Inspect latest component telemetry for a specific Pigeoneer edge node",
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		nodeID := args[0]

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := db.NewPostgresStore(ctx, db.ResolveDBURL())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not connect to ledger: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()

		rows, err := store.QueryMaps(ctx, `
			SELECT DISTINCT ON (component)
				pillar, component, status, metrics, event_timestamp
			FROM telemetric_ledger
			WHERE node_id = $1
			ORDER BY component, event_timestamp DESC
		`, nodeID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Failed to query node %q: %v\n", nodeID, err)
			os.Exit(1)
		}

		if len(rows) == 0 {
			fmt.Printf("No telemetry records found for node %q.\n", nodeID)
			return
		}

		fmt.Printf("Node: %s\n\n", nodeID)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "component\t| pillar\t| status\t| last seen\t| metrics")
		fmt.Fprintln(w, "____________________________________________________________________________________\t")

		for _, r := range rows {
			tsStr := fmt.Sprint(r["event_timestamp"])
			if ts, ok := r["event_timestamp"].(time.Time); ok {
				tsStr = ts.UTC().Format(time.RFC3339)
			}
			metricsJSON, _ := json.Marshal(r["metrics"])
			fmt.Fprintf(w, "%v\t| %v\t| %v\t| %s\t| %s\n",
				r["component"], r["pillar"], r["status"], tsStr, string(metricsJSON))
		}
		_ = w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(nodeCmd)
}
