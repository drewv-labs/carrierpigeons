package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
)

var coopCmd = &cobra.Command{
	Use:   "coop",
	Short: "Manage the PigeonCoop injection engine and PostgreSQL ledger",
}

var coopStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "View live ingestion metrics and registry health",
	Run: func(_ *cobra.Command, _ []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		brokerURL := resolveBrokerURL()
		opts := mqtt.NewClientOptions().
			AddBroker(brokerURL).
			SetClientID(fmt.Sprintf("pigeons-coop-probe-%d", os.Getpid())).
			SetConnectTimeout(3 * time.Second)
		mClient := mqtt.NewClient(opts)
		if tok := mClient.Connect(); tok.Wait() && tok.Error() != nil {
			fmt.Printf("[MQTT Broker]  DEGRADED (%s: %v)\n", brokerURL, tok.Error())
		} else {
			fmt.Printf("[MQTT Broker]  ONLINE   (%s)\n", brokerURL)
			mClient.Disconnect(100)
		}

		store, err := db.NewPostgresStore(ctx, db.ResolveDBURL())
		if err != nil {
			fmt.Printf("[CTD Ledger]   OFFLINE  (%v)\n", err)
			os.Exit(1)
		}
		defer store.Close()

		fmt.Printf("[CTD Ledger]   ONLINE\n")

		counts, err := store.QueryMaps(ctx, `
			SELECT
				(SELECT COUNT(*) FROM telemetric_ledger) AS events,
				(SELECT COUNT(DISTINCT node_id) FROM telemetric_ledger) AS nodes,
				(SELECT COUNT(*) FROM ctd_artifacts) AS artifacts,
				(SELECT COUNT(*) FROM ctd_traces) AS traces
		`)
		if err == nil && len(counts) > 0 {
			c := counts[0]
			fmt.Printf("  Nodes: %v | Events: %v | Artifacts: %v | Traces: %v\n",
				c["nodes"], c["events"], c["artifacts"], c["traces"])
		}
	},
}

var coopMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Idempotently apply the CTD PostgreSQL schema (tables & GIN indexes)",
	Run: func(_ *cobra.Command, _ []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := db.NewPostgresStore(ctx, db.ResolveDBURL())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Could not connect to ledger: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()

		if err := store.EnsureSchema(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: Schema migration failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[+] CTD PostgreSQL schema verified (telemetric_ledger, ctd_artifacts, ctd_traces).")
	},
}

func init() {
	rootCmd.AddCommand(coopCmd)
	coopCmd.AddCommand(coopStatusCmd)
	coopCmd.AddCommand(coopMigrateCmd)
}
