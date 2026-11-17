package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all registered edge nodes and their last known activity",
	Run: func(_ *cobra.Command, _ []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := db.NewPostgresStore(ctx, db.ResolveDBURL())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not connect to ledger: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()

		summaries, err := store.GetClusterStatus(ctx, 5*time.Minute)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Failed to query ledger: %v\n", err)
			os.Exit(1)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "node\t| last seen\t| status")
		fmt.Fprintln(w, "________________________________________________________\t")

		for _, n := range summaries {
			status := "active"
			if !n.IsHealthy {
				if time.Since(n.LastSeen) > 5*time.Minute {
					status = "offline"
				} else {
					status = n.Status
				}
			}
			fmt.Fprintf(w, "%s\t| %s\t| %s\n", n.NodeID, n.LastSeen.UTC().Format(time.RFC3339), status)
		}
		_ = w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}
