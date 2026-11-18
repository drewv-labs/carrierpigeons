package cli

import (
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/spf13/cobra"
)

var careModeFlag string

var careCmd = &cobra.Command{
	Use:   "care",
	Short: "Launch interactive PigeonCare SRE diagnostics (Default / Auto Edit / YOLO permission modes)",
	RunE: func(_ *cobra.Command, _ []string) error {
		return launchDashboard(tui.TabPigeonCare, "", "", careModeFlag)
	},
}

func init() {
	rootCmd.AddCommand(careCmd)
	careCmd.Flags().StringVar(&careModeFlag, "mode", "", "Override permission mode ('default', 'auto_edit', or 'yolo')")
}
