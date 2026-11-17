package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
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
	Short: "Dispatch a targeted synthetic benchmark to edge nodes",
	Args:  cobra.MinimumNArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		if !benchSystem && !benchGPU && !benchNPU {
			benchSystem = true
		}

		brokerURL := resolveBrokerURL()
		opts := mqtt.NewClientOptions().
			AddBroker(brokerURL).
			SetClientID(fmt.Sprintf("pigeons-cli-dispatcher-%d", os.Getpid()))
		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not reach MQTT broker (%s): %v\n", brokerURL, token.Error())
			os.Exit(1)
		}
		defer client.Disconnect(250)

		env := runner.Envelope{
			Action: "start",
			Runner: "benchmark",
			Params: map[string]any{
				"duration_sec":  benchSecs,
				"target_system": benchSystem,
				"target_gpu":    benchGPU,
				"target_npu":    benchNPU,
			},
		}

		payload, err := json.Marshal(env)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Failed to marshal runner envelope: %v\n", err)
			os.Exit(1)
		}

		for _, nodeID := range args {
			topic := core.ControlTopic(nodeID)
			token := client.Publish(topic, 1, false, payload)
			token.Wait()
			if token.Error() != nil {
				fmt.Fprintf(os.Stderr, "[-] Failed to dispatch to %s: %v\n", nodeID, token.Error())
			} else {
				fmt.Printf("[+] Dispatched 'benchmark' runner to %s (Duration: %ds, Sys: %v, GPU: %v, NPU: %v)\n",
					nodeID, benchSecs, benchSystem, benchGPU, benchNPU)
			}
		}
	},
}

func init() {
	runnerCmd.AddCommand(runnerBenchmarkCmd)

	runnerBenchmarkCmd.Flags().IntVar(&benchSecs, "secs", 60, "Duration of the workload in seconds")
	runnerBenchmarkCmd.Flags().BoolVar(&benchSystem, "system", false, "Pin the CPU cores")
	runnerBenchmarkCmd.Flags().BoolVar(&benchGPU, "gpu", false, "Trigger GPU load")
	runnerBenchmarkCmd.Flags().BoolVar(&benchNPU, "npu", false, "Trigger NPU accelerator load")
}
