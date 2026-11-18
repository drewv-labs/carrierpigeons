package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var (
	activityLimit    int
	ingestRolePass   string
	readOnlyRolePass string
)

var coopCmd = &cobra.Command{
	Use:   "coop",
	Short: "Manage PigeonCoop, schema migrations, RBAC roles, and ledger attribution activity",
}

var coopStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "View live MQTT broker, CTD Ledger, and PigeonMath status",
	Run: func(_ *cobra.Command, _ []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		brokerURL := resolveBrokerURL()
		clientID := fmt.Sprintf("%s-coop-probe-%d", wsCfg.Brand.CLIName, os.Getpid())
		if mClient, err := dialMQTTClient(brokerURL, clientID, 3*time.Second); err != nil {
			fmt.Printf("[MQTT Broker]  DEGRADED (%v)\n", err)
		} else {
			fmt.Printf("[MQTT Broker]  ONLINE   (%s)\n", brokerURL)
			mClient.Disconnect(100)
		}

		store, err := openOperatorStore(ctx)
		if err != nil {
			fmt.Printf("[CTD Ledger]   OFFLINE  (%v)\n", err)
			os.Exit(1)
		}
		defer store.Close()

		fmt.Printf("[CTD Ledger]   ONLINE\n")

		counts, err := store.QueryMaps(ctx, fmt.Sprintf(`
			SELECT
				GREATEST(
					COALESCE((SELECT reltuples::bigint FROM pg_class WHERE oid = 'public.telemetric_ledger'::regclass), 0),
					(SELECT COUNT(*) FROM (SELECT 1 FROM telemetric_ledger LIMIT 100000) _bounded_ledger)
				) AS events,
				(SELECT COUNT(*) FROM ctd_nodes) AS registered_nodes,
				(SELECT COUNT(*) FROM ctd_artifacts) AS artifacts,
				(SELECT COUNT(*) FROM (SELECT 1 FROM ctd_traces LIMIT 100000) _bounded_traces) AS traces,
				(SELECT COUNT(*) FROM %s.pmv_catalog WHERE status = 'certified') AS certified_pmvs
		`, wsCfg.Brand.MathSchema))
		if err == nil && len(counts) > 0 {
			c := counts[0]
			fmt.Printf("  Nodes: %v | Events: %v | Artifacts: %v | Traces: %v | Certified PMVs: %v\n",
				c["registered_nodes"], c["events"], c["artifacts"], c["traces"], c["certified_pmvs"])
		}
	},
}

var coopMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Idempotently apply the CTD Ledger, Node/Tag Registry, and PigeonMath PostgreSQL schema",
	Run: func(_ *cobra.Command, _ []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := openOperatorStore(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not connect to ledger: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()

		if err := store.EnsureSchema(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Schema migration failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[+] Verified PostgreSQL schema (telemetric_ledger, ctd_nodes, ctd_artifacts, ctd_traces, %s.pmv_catalog).\n",
			wsCfg.Brand.MathSchema)
	},
}

var coopProvisionRolesCmd = &cobra.Command{
	Use:   "provision-roles",
	Short: "Provision least-privilege PostgreSQL roles (ctd_ingestor write role & pigeoncare_ro read-only role)",
	RunE: func(_ *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := openOperatorStore(ctx)
		if err != nil {
			return err
		}
		defer store.Close()

		if err := store.EnsureSchema(ctx); err != nil {
			return err
		}
		if err := store.ProvisionRoles(ctx, ingestRolePass, readOnlyRolePass); err != nil {
			return err
		}
		fmt.Println("[+] Provisioned PostgreSQL roles: ctd_ingestor (COPY/INSERT) & pigeoncare_ro (STRICT READ-ONLY on public.* + pigeonmath view privileges).")
		return nil
	},
}

var coopActivityCmd = &cobra.Command{
	Use:   "activity",
	Short: "Show CTD Ledger write/update actor attribution summary",
	RunE: func(_ *cobra.Command, _ []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		store, err := openOperatorStore(ctx)
		if err != nil {
			return err
		}
		defer store.Close()

		items, err := store.GetLedgerActivity(ctx, activityLimit)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			fmt.Println("No CTD Ledger write/update activity recorded yet.")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "table\t| action\t| db actor\t| app actor\t| target\t| records\t| last event")
		fmt.Fprintln(w, "__________________________________________________________________________________________\t")
		for _, a := range items {
			fmt.Fprintf(w, "%s\t| %s\t| %s\t| %s\t| %s\t| %d\t| %s\n",
				a.SourceTable, a.Action, a.DBActor, a.AppActor, a.Target, a.RecordCount,
				a.LastEventAt.UTC().Format(time.RFC3339))
		}
		return w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(coopCmd)
	coopCmd.AddCommand(coopStatusCmd)
	coopCmd.AddCommand(coopMigrateCmd)
	coopCmd.AddCommand(coopProvisionRolesCmd)
	coopCmd.AddCommand(coopActivityCmd)

	coopActivityCmd.Flags().IntVarP(&activityLimit, "limit", "n", 25, "Maximum attribution rows to display")
	coopProvisionRolesCmd.Flags().StringVar(&ingestRolePass, "ingest-password", "", "Password for ctd_ingestor role")
	coopProvisionRolesCmd.Flags().StringVar(&readOnlyRolePass, "readonly-password", "", "Password for pigeoncare_ro role")
}
