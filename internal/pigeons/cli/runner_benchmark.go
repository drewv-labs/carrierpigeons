package cli

import (
	"fmt"
	"strings"
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
	Short: "Dispatch a targeted CPU synthetic benchmark via PigeonCoop RunnerRequestCTD",
	Long: "Dispatch a CPU-only synthetic benchmark workload via PigeonCoop RunnerRequestCTD.\n" +
		"GPU and NPU burn kernels are not yet implemented; passing --gpu or --npu fails fast client-side.",
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		f := cmd.Flags()
		secs, _ := f.GetInt("secs")
		sys, _ := f.GetBool("system")
		gpu, _ := f.GetBool("gpu")
		npu, _ := f.GetBool("npu")

		if gpu || npu {
			var unsupported []string
			if gpu {
				unsupported = append(unsupported, "--gpu")
			}
			if npu {
				unsupported = append(unsupported, "--npu")
			}
			return fmt.Errorf("%s not supported: benchmark is currently CPU-only until GPU/NPU burn kernels are implemented (use --system)",
				strings.Join(unsupported, ", "))
		}

		if !sys {
			sys = true
		}

		state := cliStateFromCommand(cmd)
		req := core.RunnerRequestCTD{
			RequestID: fmt.Sprintf("bench-%d", time.Now().UTC().UnixNano()),
			Action:    "start",
			Runner:    "benchmark",
			Selectors: core.RunnerSelector{
				Nodes: cleanSelectorTokens(args, false),
			},
			Params: map[string]any{
				"duration_sec":  secs,
				"target_system": sys,
				"target_gpu":    false,
				"target_npu":    false,
			},
			RequestedBy: state.wsCfg.Operator,
			RequestedAt: time.Now().UTC(),
		}

		brokerURL := resolveBrokerURLForConfig(state.wsCfg)
		if err := publishRunnerRequestWithConfig(state.wsCfg, brokerURL, req); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "[+] Submitted RunnerRequestCTD %s ('benchmark') -> %v (Duration: %ds, Sys: %v, GPU: %v, NPU: %v)\n",
			req.RequestID, req.Selectors.Nodes, secs, sys, false, false)
		return nil
	},
}

func init() {
	runnerCmd.AddCommand(runnerBenchmarkCmd)

	runnerBenchmarkCmd.Flags().IntVar(&benchSecs, "secs", 60, "Duration of the CPU workload in seconds")
	runnerBenchmarkCmd.Flags().BoolVar(&benchSystem, "system", false, "Pin and stress CPU cores (default CPU-only benchmark)")
	runnerBenchmarkCmd.Flags().BoolVar(&benchGPU, "gpu", false, "Unsupported: GPU burn kernels not yet implemented (fails fast; CPU-only benchmark)")
	runnerBenchmarkCmd.Flags().BoolVar(&benchNPU, "npu", false, "Unsupported: NPU burn kernels not yet implemented (fails fast; CPU-only benchmark)")
}
