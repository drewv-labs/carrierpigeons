package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/drewv-labs/carrier-pigeons/internal/pigeons/adapters"
	"github.com/drewv-labs/carrier-pigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/spf13/cobra"
)

var (
	verydetailed bool
	addSupport   bool
)

func envOrDefault(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

var psCmd = &cobra.Command{
	Use:   "ps",
	Short: "Show active cluster status and telemetry health",
	Run: func(_ *cobra.Command, _ []string) {
		ctx := context.Background()

		store, err := db.NewPostgresStore(ctx, db.ResolveDBURL())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not connect to ledger: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()

		summaries, err := store.GetClusterStatus(ctx, 3*time.Minute)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Failed to fetch cluster status: %v\n", err)
			os.Exit(1)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		if verydetailed {
			fmt.Fprintln(w, "node\t| last component\t| state\t| detail")
		} else {
			fmt.Fprintln(w, "node\t| last component\t| state")
		}
		fmt.Fprintln(w, "__________________________________________________________________\t")

		var degradedNodes []string
		needsSupport := false

		for _, n := range summaries {
			state := "running"
			detail := "clean"

			timeSince := time.Since(n.LastSeen)
			if timeSince > 3*time.Minute {
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

			if verydetailed {
				fmt.Fprintf(w, "%s\t| %s\t| %s\t| %s\n", n.NodeID, n.Component, state, detail)
			} else {
				fmt.Fprintf(w, "%s\t| %s\t| %s\n", n.NodeID, n.Component, state)
			}
		}
		_ = w.Flush()

		if addSupport && needsSupport {
			fmt.Println("\nAbnormal node states detected in the cluster.")
			fmt.Print("Contact PigeonCare+ Agent support? [Y/n] ")

			reader := bufio.NewReader(os.Stdin)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))

			if response == "" || response == "y" || response == "yes" {
				fmt.Println("\n[+] Handoff approved. Booting PigeonCare+...")

				var contextBuilder strings.Builder
				contextBuilder.WriteString("The following nodes are currently degraded or unreachable:\n")
				for _, issue := range degradedNodes {
					fmt.Fprintf(&contextBuilder, "- %s\n", issue)
				}

				llmCfg := adapters.LLMConfig{
					Provider: envOrDefault("PIGEONS_LLM_PROVIDER", "ollama"),
					Endpoint: envOrDefault("PIGEONS_LLM_ENDPOINT", "http://localhost:11434"),
					Model:    envOrDefault("PIGEONS_LLM_MODEL", "qwen2.5-coder"),
					APIKey:   os.Getenv("PIGEONS_LLM_API_KEY"),
					Store:    store,
				}
				agentLLM := adapters.NewLLMAdapter(llmCfg)

				p := tea.NewProgram(tui.NewAgentModel(contextBuilder.String(), agentLLM))
				if _, err := p.Run(); err != nil {
					fmt.Fprintf(os.Stderr, "Fatal error launching PigeonCare+: %v\n", err)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(psCmd)
	psCmd.Flags().BoolVarP(&verydetailed, "verydetailed", "v", false, "Include detailed status qualifiers")
	psCmd.Flags().BoolVar(&addSupport, "add-support", false, "Prompt for AI agent support on abnormal node states")
}
