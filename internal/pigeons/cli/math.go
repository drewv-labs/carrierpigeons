package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/spf13/cobra"
)

var mathCmd = &cobra.Command{
	Use:   "math",
	Short: "Manage PigeonMath analytical views (pigeonmath.pmv_*) or open the PigeonMath TUI",
	RunE: func(cmd *cobra.Command, _ []string) error {
		state := cliStateFromCommand(cmd)
		return launchDashboardWithConfig(state.wsCfg, tui.TabPigeonMath, "", "", "")
	},
}

var mathListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all cached and certified PigeonMath views (pigeonmath.pmv_*)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 8*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		out := cmd.OutOrStdout()

		store, err := openReadOnlyStoreForConfig(ctx, cfg, strings.ToLower(cfg.Brand.MathName))
		if err != nil {
			return err
		}
		defer store.Close()

		eng := pigeonmath.NewEngineWithSchema(store, cfg.Brand.MathSchema)
		views, err := eng.ListPMVs(ctx)
		if err != nil {
			return err
		}
		if len(views) == 0 {
			fmt.Fprintf(out, "No %s analytical views registered yet.\n", cfg.Brand.MathName)
			return nil
		}

		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "view\t| status\t| ver\t| submitted by\t| updated\t| notes")
		fmt.Fprintln(w, "______________________________________________________________________________________\t")
		for _, v := range views {
			fmt.Fprintf(w, "%s.%s\t| %s\t| v%d\t| %s\t| %s\t| %s\n",
				cfg.Brand.MathSchema, v.ViewName, v.Status, v.Version, v.SubmittedBy,
				v.UpdatedAt.UTC().Format(time.RFC3339), v.AgentNotes)
		}
		return w.Flush()
	},
}

var mathInspectCmd = &cobra.Command{
	Use:   "inspect <view_name>",
	Short: "Inspect cached raw SQL intent, optimized SQL, certification report, and sample rows for a PMV",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		out := cmd.OutOrStdout()

		store, err := openReadOnlyStoreForConfig(ctx, cfg, strings.ToLower(cfg.Brand.MathName))
		if err != nil {
			return err
		}
		defer store.Close()

		eng := pigeonmath.NewEngineWithSchema(store, cfg.Brand.MathSchema)
		rec, err := eng.GetPMV(ctx, args[0])
		if err != nil {
			return err
		}

		reportJSON, _ := json.MarshalIndent(rec.ValidationReport, "", "  ")
		fmt.Fprintf(out, "View:         %s.%s (v%d, %s)\n", cfg.Brand.MathSchema, rec.ViewName, rec.Version, rec.Status)
		fmt.Fprintf(out, "Submitted By: %s\n", rec.SubmittedBy)
		fmt.Fprintf(out, "Notes:        %s\n\n", rec.AgentNotes)
		fmt.Fprintf(out, "--- Cached Raw Intent SQL (Backtest Source) ---\n%s\n\n", rec.RawIntentSQL)
		fmt.Fprintf(out, "--- Materialized SQL ---\n%s\n\n", rec.OptimizedSQL)
		fmt.Fprintf(out, "--- Validation Report ---\n%s\n", string(reportJSON))

		if rec.Status == "certified" {
			rows, qErr := eng.QueryPMV(ctx, rec.ViewName, 5)
			if qErr == nil {
				sampleJSON, _ := json.MarshalIndent(rows, "", "  ")
				fmt.Fprintf(out, "\n--- Sample Output (LIMIT 5) ---\n%s\n", string(sampleJSON))
			}
		}
		return nil
	},
}

var mathCreateCmd = &cobra.Command{
	Use:   "create <view_name> <select_sql>",
	Short: "Cache raw SQL intent, tune/validate via PigeonCare, and materialize pigeonmath.pmv_<view_name>",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 45*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		noOptimize, _ := cmd.Flags().GetBool("no-optimize")

		store, err := openReadOnlyStoreForConfig(ctx, cfg, strings.ToLower(cfg.Brand.MathName))
		if err != nil {
			return err
		}
		defer store.Close()

		var optLLM llm.BaseLLM
		if !noOptimize {
			optLLM = llm.New(llm.Config{
				Provider: cfg.LLM.Provider,
				Model:    cfg.LLM.Model,
				Endpoint: cfg.LLM.Endpoint,
				APIKey:   cfg.LLM.APIKey,
			})
		}

		eng := pigeonmath.NewEngineWithSchema(store, cfg.Brand.MathSchema)
		rec, err := eng.SubmitAndCertifyPMV(ctx, cfg.Operator, args[0], args[1], optLLM)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "[+] Certified %s.%s (v%d)\n    Notes: %s\n",
			cfg.Brand.MathSchema, rec.ViewName, rec.Version, rec.AgentNotes)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(mathCmd)
	mathCmd.AddCommand(mathListCmd)
	mathCmd.AddCommand(mathInspectCmd)
	mathCmd.AddCommand(mathCreateCmd)
	mathCreateCmd.Flags().Bool("no-optimize", false, "Skip LLM query optimization and certify raw SQL directly")
}
