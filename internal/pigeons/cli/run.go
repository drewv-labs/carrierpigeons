package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/spf13/cobra"
)

var (
	runAll   bool
	runNodes []string
	runTags  []string
	runKV    []string
	runSecs  int
	runNoTUI bool
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

func buildRunnerRequest(runnerName, operator string, all bool, nodes, tags, kvPairs []string, durationSec int) (core.RunnerRequestCTD, error) {
	params := parseRunnerKeyValueParams(kvPairs)
	if durationSec > 0 {
		if _, exists := params["duration_sec"]; !exists {
			params["duration_sec"] = durationSec
		}
	}

	req := core.RunnerRequestCTD{
		RequestID: fmt.Sprintf("req-%d", time.Now().UTC().UnixNano()),
		Action:    "start",
		Runner:    strings.TrimSpace(runnerName),
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
	Example: `  pigeons run SyntheticComputeRnr --node=rv64-compute-01 --node=rv64-vision-02 --tag={compute-bench}
  pigeons run benchmark --all --secs 30 --param target_system=true`,
	Args: cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		req, err := buildRunnerRequest(args[0], wsCfg.Operator, runAll, runNodes, runTags, runKV, runSecs)
		if err != nil {
			return err
		}

		brokerURL := resolveBrokerURL()
		if err := publishRunnerRequest(brokerURL, req); err != nil {
			return err
		}

		fmt.Printf("[+] Submitted RunnerRequestCTD %s (%s) -> %s (all=%v, nodes=%v, tags=%v)\n",
			req.RequestID, req.Runner, wsCfg.Brand.CoopName, req.Selectors.All, req.Selectors.Nodes, req.Selectors.Tags)

		if runNoTUI {
			return nil
		}
		return launchDashboard(tui.TabRunnerManager, req.RequestID, "", "")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().BoolVar(&runAll, "all", false, "Dispatch runner across all active nodes in the mesh")
	runCmd.Flags().StringArrayVar(&runNodes, "node", nil, "Target node ID (repeatable, chainable with --tag)")
	runCmd.Flags().StringArrayVar(&runTags, "tag", nil, "Target node tag (repeatable, e.g. --tag=compute-bench or --tag={gpu-worker})")
	runCmd.Flags().StringArrayVarP(&runKV, "param", "p", nil, "Runner key=value parameter (repeatable)")
	runCmd.Flags().IntVar(&runSecs, "secs", 0, "Optional duration_sec parameter override")
	runCmd.Flags().BoolVar(&runNoTUI, "no-tui", false, "Return immediately after dispatching RunnerRequestCTD without opening TUI")
}
