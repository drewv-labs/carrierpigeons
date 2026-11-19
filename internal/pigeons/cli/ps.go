package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/spf13/cobra"
)

var (
	verydetailed bool
	addSupport   bool
)

var psCmd = &cobra.Command{
	Use:   "ps",
	Short: "Show active cluster status and telemetry health",
	RunE: func(_ *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := openOperatorStore(ctx)
		if err != nil {
			return fmt.Errorf("connect CTD Ledger: %w", err)
		}
		defer store.Close()

		summaries, err := store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return err
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
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
			if timeSince > config.DefaultHealthyWindow {
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
			fmt.Println("\nAbnormal node states detected in the cluster.")
			fmt.Printf("Contact %s Agent support? [Y/n] ", wsCfg.Brand.AgentName)

			reader := bufio.NewReader(os.Stdin)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))

			if response == "" || response == "y" || response == "yes" {
				fmt.Printf("\n[+] Handoff approved. Booting %s...\n", wsCfg.Brand.AgentName)

				var contextBuilder strings.Builder
				contextBuilder.WriteString("The following nodes are currently degraded or unreachable:\n")
				for _, issue := range degradedNodes {
					fmt.Fprintf(&contextBuilder, "- %s\n", issue)
				}

				return launchDashboard(tui.TabPigeonCare, "", contextBuilder.String(), "")
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(psCmd)
	psCmd.Flags().BoolVarP(&verydetailed, "verydetailed", "v", false, "Include detailed hardware & status qualifiers")
	psCmd.Flags().BoolVar(&addSupport, "add-support", false, "Prompt for PigeonCare support handoff on abnormal node states")
}
