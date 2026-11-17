package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pigeons",
	Short: "PigeonCommand: The Edge Observability CLI & TUI",
	Long:  `CarrierPigeons Master Control. Manage distributed telemetry, benchmark edge nodes, and interface with the PigeonCoop ledger.`,
	Run: func(cmd *cobra.Command, _ []string) {
		_ = cmd.Help()
	},
}

func resolveBrokerURL() string {
	for _, key := range []string{"PIGEONS_BROKER_URL", "PIGEONCOOP_BROKER_URL", "MQTT_BROKER_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return "tcp://localhost:1883"
}

// Execute is called by main.go to bootstrap the CLI.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
