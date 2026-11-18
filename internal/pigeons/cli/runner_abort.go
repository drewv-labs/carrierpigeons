package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/cobra"
)

var runnerAbortCmd = &cobra.Command{
	Use:   "abort [runner-name] [node-ids...]",
	Short: "Send a hard kill signal to an active runner via PigeonCoop RunnerRequestCTD",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(_ *cobra.Command, args []string) error {
		runnerName := strings.TrimSpace(args[0])
		targetNodes := cleanSelectorTokens(args[1:], false)

		req := core.RunnerRequestCTD{
			RequestID: fmt.Sprintf("abort-%d", time.Now().UTC().UnixNano()),
			Action:    "abort",
			Runner:    runnerName,
			Selectors: core.RunnerSelector{
				Nodes: targetNodes,
			},
			RequestedBy: wsCfg.Operator,
			RequestedAt: time.Now().UTC(),
		}

		brokerURL := resolveBrokerURL()
		if err := publishRunnerRequest(brokerURL, req); err != nil {
			return err
		}

		fmt.Printf("[+] Submitted abort RunnerRequestCTD %s for %q -> %v\n", req.RequestID, runnerName, targetNodes)
		return nil
	},
}

func init() {
	runnerCmd.AddCommand(runnerAbortCmd)
}
