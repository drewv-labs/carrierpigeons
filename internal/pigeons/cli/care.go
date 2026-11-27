package cli

import (
	"strings"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncare"
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/spf13/cobra"
)

var careCmd = &cobra.Command{
	Use:   "care",
	Short: "Launch interactive PigeonCare SRE diagnostics (Default / Auto Edit / YOLO permission modes)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		state := cliStateFromCommand(cmd)
		rawMode, _ := cmd.Flags().GetString("mode")
		modeOverride := ""
		if strings.TrimSpace(rawMode) != "" {
			parsed, err := pigeoncare.ParseModeStrict(rawMode)
			if err != nil {
				return err
			}
			modeOverride = string(parsed)
		}
		return launchDashboardWithConfig(cmd.Context(), state.wsCfg, tui.TabPigeonCare, "", "", modeOverride)
	},
}

func init() {
	rootCmd.AddCommand(careCmd)
	careCmd.Flags().String("mode", "", "Override permission mode ('default', 'auto_edit', or 'yolo')")
}
