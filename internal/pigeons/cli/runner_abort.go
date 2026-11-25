package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/cobra"
)

func buildAbortRunnerRequest(runnerName, operator string, all bool, flagNodes, positionalNodes, tags []string) (core.RunnerRequestCTD, error) {
	combinedNodes := make([]string, 0, len(flagNodes)+len(positionalNodes))
	combinedNodes = append(combinedNodes, flagNodes...)
	combinedNodes = append(combinedNodes, positionalNodes...)

	req := core.RunnerRequestCTD{
		RequestID: fmt.Sprintf("abort-%d", time.Now().UTC().UnixNano()),
		Action:    "abort",
		Runner:    strings.TrimSpace(runnerName),
		Selectors: core.RunnerSelector{
			All:   all,
			Nodes: cleanSelectorTokens(combinedNodes, false),
			Tags:  core.NormalizeTags(cleanSelectorTokens(tags, true)),
		},
		RequestedBy: operator,
		RequestedAt: time.Now().UTC(),
	}
	return req, req.Validate()
}

var runnerAbortCmd = &cobra.Command{
	Use:   "abort <runner-name> [node-ids...]",
	Short: "Send a stop/abort signal to an active runner (or resume PauseRnr) via PigeonCoop RunnerRequestCTD",
	Example: `  pigeons runner abort PauseRnr --all
  pigeons runner abort PauseRnr rv64-compute-01
  pigeons runner abort SyntheticComputeRnr --tag=compute-bench`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		f := cmd.Flags()
		all, _ := f.GetBool("all")
		nodes, _ := f.GetStringArray("node")
		tags, _ := f.GetStringArray("tag")

		req, err := buildAbortRunnerRequest(args[0], cfg.Operator, all, nodes, args[1:], tags)
		if err != nil {
			return err
		}

		brokerURL := resolveBrokerURLForConfig(cfg)
		if err := publishRunnerRequestWithConfig(cfg, brokerURL, req); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "[+] Submitted abort RunnerRequestCTD %s for %q -> (all=%v, nodes=%v, tags=%v)\n",
			req.RequestID, req.Runner, req.Selectors.All, req.Selectors.Nodes, req.Selectors.Tags)
		return nil
	},
}

func init() {
	runnerCmd.AddCommand(runnerAbortCmd)
	runnerAbortCmd.Flags().Bool("all", false, "Abort runner across all active nodes in the mesh")
	runnerAbortCmd.Flags().StringArray("node", nil, "Target node ID (repeatable, or pass node IDs positionally)")
	runnerAbortCmd.Flags().StringArray("tag", nil, "Target node tag (repeatable)")
}
