package cli

import (
	"fmt"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/cobra"
)

var (
	benchSecs   int
	benchSystem bool
	benchGPU    bool
	benchNPU    bool
)

var runnerBenchmarkCmd = &cobra.Command{
	Use:   "benchmark [node-ids...]",
	Short: "Dispatch a targeted synthetic benchmark via PigeonCoop RunnerRequestCTD",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		if !benchSystem && !benchGPU && !benchNPU {
			benchSystem = true
		}

		req := core.RunnerRequestCTD{
			RequestID: fmt.Sprintf("bench-%d", time.Now().UTC().UnixNano()),
			Action:    "start",
			Runner:    "benchmark",
			Selectors: core.RunnerSelector{
				Nodes: cleanSelectorTokens(args, false),
			},
			Params: map[string]any{
				"duration_sec":  benchSecs,
				"target_system": benchSystem,
				"target_gpu":    benchGPU,
				"target_npu":    benchNPU,
			},
			RequestedBy: wsCfg.Operator,
			RequestedAt: time.Now().UTC(),
		}

		brokerURL := resolveBrokerURL()
		if err := publishRunnerRequest(brokerURL, req); err != nil {
			return err
		}

		fmt.Printf("[+] Submitted RunnerRequestCTD %s ('benchmark') -> %v (Duration: %ds, Sys: %v, GPU: %v, NPU: %v)\n",
			req.RequestID, req.Selectors.Nodes, benchSecs, benchSystem, benchGPU, benchNPU)
		return nil
	},
}

func init() {
	runnerCmd.AddCommand(runnerBenchmarkCmd)

	runnerBenchmarkCmd.Flags().IntVar(&benchSecs, "secs", 60, "Duration of the workload in seconds")
	runnerBenchmarkCmd.Flags().BoolVar(&benchSystem, "system", false, "Pin the CPU cores")
	runnerBenchmarkCmd.Flags().BoolVar(&benchGPU, "gpu", false, "Trigger GPU load")
	runnerBenchmarkCmd.Flags().BoolVar(&benchNPU, "npu", false, "Trigger NPU accelerator load")
}
