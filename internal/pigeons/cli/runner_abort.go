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

var runnerAbortCmd = &cobra.Command{
	Use:   "abort [runner-name] [node-ids...]",
	Short: "Send a hard kill signal to an active runner on edge nodes",
	Args:  cobra.MinimumNArgs(2),
	Run: func(_ *cobra.Command, args []string) {
		runnerName := args[0]
		targetNodes := args[1:]

		brokerURL := resolveBrokerURL()
		opts := mqtt.NewClientOptions().
			AddBroker(brokerURL).
			SetClientID(fmt.Sprintf("pigeons-cli-abort-%d", os.Getpid()))
		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not reach broker (%s): %v\n", brokerURL, token.Error())
			os.Exit(1)
		}
		defer client.Disconnect(250)

		env := runner.Envelope{
			Action: "abort",
			Runner: runnerName,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Failed to marshal abort envelope: %v\n", err)
			os.Exit(1)
		}

		for _, nodeID := range targetNodes {
			topic := core.ControlTopic(nodeID)
			token := client.Publish(topic, 1, false, payload)
			token.Wait()
			if token.Error() == nil {
				fmt.Printf("[+] Dispatched abort signal for %q to %s\n", runnerName, nodeID)
			} else {
				fmt.Fprintf(os.Stderr, "[-] Failed to dispatch to %s: %v\n", nodeID, token.Error())
			}
		}
	},
}

func init() {
	runnerCmd.AddCommand(runnerAbortCmd)
}
