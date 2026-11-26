package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List registered mesh nodes, hardware architecture, tags, cooler RPM, and status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		tagFilter, _ := cmd.Flags().GetString("tag")

		store, err := openReadStoreForConfig(ctx, state.wsCfg, strings.ToLower(state.wsCfg.Brand.CLIName)+"-list")
		if err != nil {
			return fmt.Errorf("connect CTD Ledger: %w", err)
		}
		defer store.Close()

		summaries, err := store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return err
		}

		wantTag := strings.ToLower(strings.Trim(strings.TrimSpace(tagFilter), "{}"))

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "node\t| arch/os\t| tags\t| cpu fan rpm\t| last seen\t| status")
		fmt.Fprintln(w, "______________________________________________________________________________________\t")

		for _, n := range summaries {
			if wantTag != "" && !slices.Contains(n.Tags, wantTag) {
				continue
			}
			status := "active"
			if strings.EqualFold(n.Status, "paused") {
				status = "paused"
			} else if !n.IsHealthy {
				if time.Since(n.LastSeen) > config.DefaultHealthyWindow {
					status = "offline"
				} else {
					status = n.Status
				}
			}
			archOS := strings.Trim(n.Arch+"/"+n.OS, "/")
			if archOS == "" {
				archOS = "-"
			}
			tagsStr := strings.Join(n.Tags, ",")
			if tagsStr == "" {
				tagsStr = "-"
			}
			fanStr := "-"
			if rpm, ok := n.LatestMetrics["cpu_fan_rpm"]; ok {
				fanStr = fmt.Sprint(rpm)
			}
			fmt.Fprintf(w, "%s\t| %s\t| %s\t| %s\t| %s\t| %s\n",
				n.NodeID, archOS, tagsStr, fanStr, n.LastSeen.UTC().Format(time.RFC3339), status)
		}
		return w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
	listCmd.Flags().String("tag", "", "Filter listed nodes by tag")
}
