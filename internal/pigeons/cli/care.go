package cli

import (
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/spf13/cobra"
)

var careCmd = &cobra.Command{
	Use:   "care",
	Short: "Launch interactive PigeonCare SRE diagnostics (Default / Auto Edit / YOLO permission modes)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		state := cliStateFromCommand(cmd)
		mode, _ := cmd.Flags().GetString("mode")
		return launchDashboardWithConfig(cmd.Context(), state.wsCfg, tui.TabPigeonCare, "", "", mode)
	},
}

func init() {
	rootCmd.AddCommand(careCmd)
	careCmd.Flags().String("mode", "", "Override permission mode ('default', 'auto_edit', or 'yolo')")
}
