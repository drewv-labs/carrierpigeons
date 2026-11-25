package cli

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/spf13/cobra"
)

var psCmd = &cobra.Command{
	Use:   "ps",
	Short: "Show active cluster status and telemetry health",
	RunE: func(cmd *cobra.Command, _ []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		out := cmd.OutOrStdout()
		f := cmd.Flags()
		verydetailed, _ := f.GetBool("verydetailed")
		addSupport, _ := f.GetBool("add-support")

		store, err := openOperatorStoreForConfig(ctx, cfg)
		if err != nil {
			return fmt.Errorf("connect CTD Ledger: %w", err)
		}
		defer store.Close()

		summaries, err := store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return err
		}

		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		if verydetailed {
			fmt.Fprintln(w, "node\t| arch\t| tags\t| last component\t| state\t| detail")
		} else {
			fmt.Fprintln(w, "node\t| tags\t| last component\t| state")
		}
		fmt.Fprintln(w, "______________________________________________________________________________\t")

		var degradedNodes []string
		needsSupport := false

		for _, n := range summaries {
			state := "running"
			detail := "clean"

			timeSince := time.Since(n.LastSeen)
			if strings.EqualFold(n.Status, "paused") {
				state = "paused"
				detail = "observers paused (PauseRnr)"
			} else if timeSince > config.DefaultHealthyWindow {
				state = "offline"
				detail = fmt.Sprintf("unreachable (%.0fm ago)", timeSince.Minutes())
				needsSupport = true
				degradedNodes = append(degradedNodes, fmt.Sprintf("%s (%s)", n.NodeID, detail))
			} else if !n.IsHealthy {
				state = "degraded"
				detail = n.Status
				needsSupport = true
				degradedNodes = append(degradedNodes, fmt.Sprintf("%s (%s)", n.NodeID, detail))
			}

			tagStr := strings.Join(n.Tags, ",")
			if tagStr == "" {
				tagStr = "-"
			}
			arch := n.Arch
			if arch == "" {
				arch = "-"
			}

			if verydetailed {
				fmt.Fprintf(w, "%s\t| %s\t| %s\t| %s\t| %s\t| %s\n",
					n.NodeID, arch, tagStr, n.Component, state, detail)
			} else {
				fmt.Fprintf(w, "%s\t| %s\t| %s\t| %s\n",
					n.NodeID, tagStr, n.Component, state)
			}
		}
		_ = w.Flush()

		if addSupport && needsSupport {
			fmt.Fprintln(out, "\nAbnormal node states detected in the cluster.")
			fmt.Fprintf(out, "Contact %s Agent support? [Y/n] ", cfg.Brand.AgentName)

			reader := bufio.NewReader(cmd.InOrStdin())
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))

			if response == "" || response == "y" || response == "yes" {
				fmt.Fprintf(out, "\n[+] Handoff approved. Booting %s...\n", cfg.Brand.AgentName)

				var contextBuilder strings.Builder
				contextBuilder.WriteString("The following nodes are currently degraded or unreachable:\n")
				for _, issue := range degradedNodes {
					fmt.Fprintf(&contextBuilder, "- %s\n", issue)
				}

				return launchDashboardWithConfig(cfg, tui.TabPigeonCare, "", contextBuilder.String(), "")
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(psCmd)
	psCmd.Flags().BoolP("verydetailed", "v", false, "Include detailed hardware & status qualifiers")
	psCmd.Flags().Bool("add-support", false, "Prompt for PigeonCare support handoff on abnormal node states")
}
