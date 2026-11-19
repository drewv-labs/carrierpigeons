package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/spf13/cobra"
)

var mathNoOptimize bool

var mathCmd = &cobra.Command{
	Use:   "math",
	Short: "Manage PigeonMath analytical views (pigeonmath.pmv_*) or open the PigeonMath TUI",
	RunE: func(_ *cobra.Command, _ []string) error {
		return launchDashboard(tui.TabPigeonMath, "", "", "")
	},
}

var mathListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all cached and certified PigeonMath views (pigeonmath.pmv_*)",
	RunE: func(_ *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		store, err := openReadOnlyStore(ctx, strings.ToLower(wsCfg.Brand.MathName))
		if err != nil {
			return err
		}
		defer store.Close()

		eng := pigeonmath.NewEngineWithSchema(store, wsCfg.Brand.MathSchema)
		views, err := eng.ListPMVs(ctx)
		if err != nil {
			return err
		}
		if len(views) == 0 {
			fmt.Printf("No %s analytical views registered yet.\n", wsCfg.Brand.MathName)
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "view\t| status\t| ver\t| submitted by\t| updated\t| notes")
		fmt.Fprintln(w, "______________________________________________________________________________________\t")
		for _, v := range views {
			fmt.Fprintf(w, "%s.%s\t| %s\t| v%d\t| %s\t| %s\t| %s\n",
				wsCfg.Brand.MathSchema, v.ViewName, v.Status, v.Version, v.SubmittedBy,
				v.UpdatedAt.UTC().Format(time.RFC3339), v.AgentNotes)
		}
		return w.Flush()
	},
}

var mathInspectCmd = &cobra.Command{
	Use:   "inspect <view_name>",
	Short: "Inspect cached raw SQL intent, optimized SQL, certification report, and sample rows for a PMV",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := openReadOnlyStore(ctx, strings.ToLower(wsCfg.Brand.MathName))
		if err != nil {
			return err
		}
		defer store.Close()

		eng := pigeonmath.NewEngineWithSchema(store, wsCfg.Brand.MathSchema)
		rec, err := eng.GetPMV(ctx, args[0])
		if err != nil {
			return err
		}

		reportJSON, _ := json.MarshalIndent(rec.ValidationReport, "", "  ")
		fmt.Printf("View:         %s.%s (v%d, %s)\n", wsCfg.Brand.MathSchema, rec.ViewName, rec.Version, rec.Status)
		fmt.Printf("Submitted By: %s\n", rec.SubmittedBy)
		fmt.Printf("Notes:        %s\n\n", rec.AgentNotes)
		fmt.Printf("--- Cached Raw Intent SQL (Backtest Source) ---\n%s\n\n", rec.RawIntentSQL)
		fmt.Printf("--- Materialized SQL ---\n%s\n\n", rec.OptimizedSQL)
		fmt.Printf("--- Validation Report ---\n%s\n", string(reportJSON))

		if rec.Status == "certified" {
			rows, qErr := eng.QueryPMV(ctx, rec.ViewName, 5)
			if qErr == nil {
				sampleJSON, _ := json.MarshalIndent(rows, "", "  ")
				fmt.Printf("\n--- Sample Output (LIMIT 5) ---\n%s\n", string(sampleJSON))
			}
		}
		return nil
	},
}

var mathCreateCmd = &cobra.Command{
	Use:   "create <view_name> <select_sql>",
	Short: "Cache raw SQL intent, tune/validate via PigeonCare, and materialize pigeonmath.pmv_<view_name>",
	Args:  cobra.ExactArgs(2),
	RunE: func(_ *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		store, err := openReadOnlyStore(ctx, strings.ToLower(wsCfg.Brand.MathName))
		if err != nil {
			return err
		}
		defer store.Close()

		var optLLM llm.BaseLLM
		if !mathNoOptimize {
			optLLM = llm.New(llm.Config{
				Provider: wsCfg.LLM.Provider,
				Model:    wsCfg.LLM.Model,
				Endpoint: wsCfg.LLM.Endpoint,
				APIKey:   wsCfg.LLM.APIKey,
			})
		}

		eng := pigeonmath.NewEngineWithSchema(store, wsCfg.Brand.MathSchema)
		rec, err := eng.SubmitAndCertifyPMV(ctx, wsCfg.Operator, args[0], args[1], optLLM)
		if err != nil {
			return err
		}
		fmt.Printf("[+] Certified %s.%s (v%d)\n    Notes: %s\n",
			wsCfg.Brand.MathSchema, rec.ViewName, rec.Version, rec.AgentNotes)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(mathCmd)
	mathCmd.AddCommand(mathListCmd)
	mathCmd.AddCommand(mathInspectCmd)
	mathCmd.AddCommand(mathCreateCmd)
	mathCreateCmd.Flags().BoolVar(&mathNoOptimize, "no-optimize", false, "Skip LLM query optimization and certify raw SQL directly")
}
