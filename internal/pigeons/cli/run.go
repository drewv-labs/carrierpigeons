package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/cobra"
)

func cleanSelectorTokens(raw []string, lowercase bool) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, item := range raw {
		for _, part := range strings.Split(item, ",") {
			cleaned := strings.TrimSpace(part)
			cleaned = strings.TrimPrefix(cleaned, "{")
			cleaned = strings.TrimSuffix(cleaned, "}")
			cleaned = strings.TrimSpace(cleaned)
			if lowercase {
				cleaned = strings.ToLower(cleaned)
			}
			if cleaned == "" {
				continue
			}
			if _, exists := seen[cleaned]; !exists {
				seen[cleaned] = struct{}{}
				out = append(out, cleaned)
			}
		}
	}
	return out
}

func parseRunnerKeyValueParams(pairs []string) map[string]any {
	params := make(map[string]any)
	for _, pair := range pairs {
		idx := strings.IndexByte(pair, '=')
		if idx <= 0 {
			continue
		}
		k := strings.TrimSpace(pair[:idx])
		v := strings.TrimSpace(pair[idx+1:])
		if k == "" {
			continue
		}
		if b, err := strconv.ParseBool(v); err == nil {
			params[k] = b
		} else if i, err := strconv.Atoi(v); err == nil {
			params[k] = i
		} else if f, err := strconv.ParseFloat(v, 64); err == nil {
			params[k] = f
		} else {
			params[k] = v
		}
	}
	return params
}

func isPauseRunnerName(name string) bool {
	trimmed := strings.TrimSpace(name)
	return strings.EqualFold(trimmed, "PauseRnr") || strings.EqualFold(trimmed, "pause")
}

func buildRunnerRequest(runnerName, operator string, all bool, nodes, tags, kvPairs []string, durationSec int) (core.RunnerRequestCTD, error) {
	trimmedRunner := strings.TrimSpace(runnerName)
	params := parseRunnerKeyValueParams(kvPairs)
	if isPauseRunnerName(trimmedRunner) {
		if durationSec < 0 {
			durationSec = 0
		}
		if _, exists := params["duration_sec"]; !exists {
			params["duration_sec"] = durationSec
		}
	} else if durationSec > 0 {
		if _, exists := params["duration_sec"]; !exists {
			params["duration_sec"] = durationSec
		}
	}

	req := core.RunnerRequestCTD{
		RequestID: fmt.Sprintf("req-%d", time.Now().UTC().UnixNano()),
		Action:    "start",
		Runner:    trimmedRunner,
		Selectors: core.RunnerSelector{
			All:   all,
			Nodes: cleanSelectorTokens(nodes, false),
			Tags:  core.NormalizeTags(cleanSelectorTokens(tags, true)),
		},
		Params:      params,
		RequestedBy: operator,
		RequestedAt: time.Now().UTC(),
	}
	return req, req.Validate()
}

var runCmd = &cobra.Command{
	Use:   "run <runner_name>",
	Short: "Send a RunnerRequestCTD to PigeonCoop (--all / --node / --tag) and launch RunnerManager TUI",
	Example: `  pigeons run PauseRnr --all
  pigeons run PauseRnr --node=rv64-compute-01 --secs=60
  pigeons run SyntheticComputeRnr --node=rv64-compute-01 --node=rv64-vision-02 --tag={compute-bench}
  pigeons run benchmark --all --secs 30 --param target_system=true`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		f := cmd.Flags()
		all, _ := f.GetBool("all")
		nodes, _ := f.GetStringArray("node")
		tags, _ := f.GetStringArray("tag")
		kv, _ := f.GetStringArray("param")
		secs, _ := f.GetInt("secs")
		noTUI, _ := f.GetBool("no-tui")

		req, err := buildRunnerRequest(args[0], cfg.Operator, all, nodes, tags, kv, secs)
		if err != nil {
			return err
		}

		brokerURL := resolveBrokerURLForConfig(cfg)
		if err := publishRunnerRequestWithConfig(cfg, brokerURL, req); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "[+] Submitted RunnerRequestCTD %s (%s) -> %s (all=%v, nodes=%v, tags=%v)\n",
			req.RequestID, req.Runner, cfg.Brand.CoopName, req.Selectors.All, req.Selectors.Nodes, req.Selectors.Tags)

		if noTUI {
			return nil
		}
		return launchDashboardWithConfig(cfg, tui.TabRunnerManager, req.RequestID, "", "")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().Bool("all", false, "Dispatch runner across all active nodes in the mesh")
	runCmd.Flags().StringArray("node", nil, "Target node ID (repeatable, chainable with --tag)")
	runCmd.Flags().StringArray("tag", nil, "Target node tag (repeatable, e.g. --tag=compute-bench or --tag={gpu-worker})")
	runCmd.Flags().StringArrayP("param", "p", nil, "Runner key=value parameter (repeatable)")
	runCmd.Flags().Int("secs", 0, "Optional duration_sec override (defaults to 0 = indefinite for PauseRnr)")
	runCmd.Flags().Bool("no-tui", false, "Return immediately after dispatching RunnerRequestCTD without opening TUI")
}
