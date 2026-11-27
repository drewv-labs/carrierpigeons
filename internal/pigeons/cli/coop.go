package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/spf13/cobra"
)

var coopCmd = &cobra.Command{
	Use:   "coop",
	Short: "Manage PigeonCoop, schema migrations, RBAC roles, retention pruning, and ledger attribution activity",
}

var coopStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "View live MNS identity, MQTT broker, CTD Ledger, and PigeonMath status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		out := cmd.OutOrStdout()

		activeMesh := cfg.ActiveMesh
		activeSource := cfg.ActiveMeshSource
		if activeMesh == "" {
			activeMesh = core.DefaultMeshName
			activeSource = "default"
		}
		fmt.Fprintf(out, "[Workstation]  mesh=%s (source=%s)\n", activeMesh, activeSource)

		brokerURL := resolveBrokerURLForConfig(cfg)
		clientID := fmt.Sprintf("%s-coop-probe-%d-%d", cfg.Brand.CLIName, os.Getpid(), time.Now().UnixNano())
		if mClient, err := dialMQTTClientWithConfig(ctx, cfg, brokerURL, clientID, 3*time.Second); err != nil {
			fmt.Fprintf(out, "[MQTT Broker]  DEGRADED (%v)\n", err)
		} else {
			fmt.Fprintf(out, "[MQTT Broker]  ONLINE   (%s)\n", config.RedactURL(brokerURL))
			mClient.Disconnect(100)
		}

		store, err := openReadStoreForConfig(ctx, cfg, strings.ToLower(cfg.Brand.CLIName)+"-coop-status")
		if err != nil {
			fmt.Fprintf(out, "[CTD Ledger]   OFFLINE  (%v)\n", err)
			return err
		}
		defer store.Close()

		fmt.Fprintf(out, "[CTD Ledger]   ONLINE\n")
		return queryAndPrintCoopStatus(ctx, out, cfg, store)
	},
}

func queryAndPrintCoopStatus(ctx context.Context, out io.Writer, cfg config.WorkstationConfig, store db.ReadOnlyDB) error {
	if meshID, mErr := store.GetMeshIdentity(ctx); mErr != nil {
		fmt.Fprintf(out, "[Mesh Warning] Failed querying MNS identity: %v\n", mErr)
	} else if meshID != nil {
		fmt.Fprintf(out, "[Mesh (MNS)]   %s (coop=%s, topic_root=%s)\n",
			meshID.MeshName, meshID.CoopNodeID, meshID.TopicRoot)
		if cfg.ActiveMesh != "" && meshID.MeshName != "" && meshID.MeshName != cfg.ActiveMesh {
			fmt.Fprintf(out, "[Mesh Warning] MISMATCH: workstation working mesh is %q, but Coop ledger reports %q!\n",
				cfg.ActiveMesh, meshID.MeshName)
		}
	}

	mathSchema := cfg.Brand.MathSchema
	if err := db.ValidateSchemaIdentifier(mathSchema); err != nil {
		return fmt.Errorf("invalid math_schema configuration: %w", err)
	}

	counts, err := store.QueryReadOnlyMaps(ctx, fmt.Sprintf(`
		SELECT
			GREATEST(
				COALESCE((SELECT reltuples::bigint FROM pg_class WHERE oid = 'public.telemetric_ledger'::regclass), 0),
				(SELECT COUNT(*) FROM (SELECT 1 FROM telemetric_ledger LIMIT 100000) _bounded_ledger)
			) AS events,
			(SELECT COUNT(*) FROM ctd_nodes) AS registered_nodes,
			(SELECT COUNT(*) FROM ctd_artifacts) AS artifacts,
			(SELECT COUNT(*) FROM (SELECT 1 FROM ctd_traces LIMIT 100000) _bounded_traces) AS traces,
			(SELECT COUNT(*) FROM %s.pmv_catalog WHERE status = 'certified') AS certified_pmvs
	`, mathSchema))
	if err != nil {
		fmt.Fprintf(out, "[Ledger Warning] Failed querying summary counts: %v\n", err)
	} else if len(counts) > 0 {
		c := counts[0]
		fmt.Fprintf(out, "  Nodes: %v | Events: %v | Artifacts: %v | Traces: %v | Certified PMVs: %v\n",
			c["registered_nodes"], c["events"], c["artifacts"], c["traces"], c["certified_pmvs"])
	}
	return nil
}

var coopMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Idempotently apply the CTD Ledger, MNS Registry, Node/Tag Registry, and PigeonMath PostgreSQL schema",
	RunE: func(cmd *cobra.Command, _ []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		out := cmd.OutOrStdout()

		store, err := openOperatorStoreForConfig(ctx, cfg)
		if err != nil {
			return fmt.Errorf("could not connect to ledger: %w", err)
		}
		defer store.Close()

		if err := store.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("schema migration failed: %w", err)
		}
		fmt.Fprintf(out, "[+] Verified PostgreSQL schema (telemetric_ledger, ctd_mesh, ctd_nodes, ctd_artifacts, ctd_traces, %s.pmv_catalog).\n",
			cfg.Brand.MathSchema)
		return nil
	},
}

var coopProvisionRolesCmd = &cobra.Command{
	Use:   "provision-roles",
	Short: "Provision least-privilege PostgreSQL roles (ctd_ingestor write role & pigeoncare_ro read-only role)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer cancel()

		state := cliStateFromCommand(cmd)
		cfg := state.wsCfg
		f := cmd.Flags()
		ingestPass, _ := f.GetString("ingest-password")
		roPass, _ := f.GetString("readonly-password")

		store, err := openOperatorStoreForConfig(ctx, cfg)
		if err != nil {
			return err
		}
		defer store.Close()

		if err := store.EnsureSchema(ctx); err != nil {
			return err
		}
		if err := store.ProvisionRoles(ctx, ingestPass, roPass); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "[+] Provisioned PostgreSQL roles: ctd_ingestor (COPY/INSERT) & pigeoncare_ro (STRICT READ-ONLY on public.* + pigeonmath view privileges).")
		return nil
	},
}

var coopActivityCmd = &cobra.Command{
	Use:   "activity",
	Short: "Show CTD Ledger write/update actor attribution summary",
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
		limit, _ := cmd.Flags().GetInt("limit")

		store, err := openReadStoreForConfig(ctx, cfg, strings.ToLower(cfg.Brand.CLIName)+"-coop-activity")
		if err != nil {
			return err
		}
		defer store.Close()

		items, err := store.GetLedgerActivity(ctx, limit)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			fmt.Fprintln(out, "No CTD Ledger write/update activity recorded yet.")
			return nil
		}

		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
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

type ledgerPruner interface {
	PruneLedgerBefore(ctx context.Context, cutoff time.Time, batchLimit int) (int64, error)
}

func parseRetentionDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("missing required --older-than duration (e.g. 720h or 30d)")
	}
	lower := strings.ToLower(trimmed)
	if strings.HasSuffix(lower, "d") && len(lower) > 1 {
		daysStr := strings.TrimSpace(lower[:len(lower)-1])
		days, err := strconv.ParseInt(daysStr, 10, 64)
		if err != nil || days <= 0 || days > 36500 {
			return 0, fmt.Errorf("invalid --older-than duration %q: expected positive Go duration (e.g. 720h) or whole days (e.g. 30d)", raw)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid --older-than duration %q: expected positive duration (e.g. 24h, 720h, 30d)", raw)
	}
	return d, nil
}

func runCoopPruneWithStore(ctx context.Context, out io.Writer, store ledgerPruner, now time.Time, olderThan time.Duration, batchLimit int) (int64, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("invalid --older-than duration %s: must be > 0", olderThan)
	}
	if batchLimit < 0 {
		return 0, fmt.Errorf("invalid --batch-size %d: must be >= 0", batchLimit)
	}
	effectiveBatch := batchLimit
	if effectiveBatch == 0 {
		effectiveBatch = db.DefaultLedgerPruneBatchLimit
	}
	cutoff := now.UTC().Add(-olderThan)

	var totalDeleted int64
	batches := 0
	for {
		if err := ctx.Err(); err != nil {
			return totalDeleted, err
		}
		deleted, err := store.PruneLedgerBefore(ctx, cutoff, effectiveBatch)
		if err != nil {
			return totalDeleted, err
		}
		totalDeleted += deleted
		if deleted > 0 {
			batches++
		}
		if deleted < int64(effectiveBatch) || deleted == 0 {
			break
		}
	}

	fmt.Fprintf(out, "[+] Pruned %d telemetric_ledger row(s) older than %s (cutoff=%s, batches=%d, batch_size=%d).\n",
		totalDeleted, olderThan, cutoff.Format(time.RFC3339), batches, effectiveBatch)
	return totalDeleted, nil
}

var coopPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Prune telemetric_ledger rows older than a retention duration in bounded batches",
	Example: `  pigeons coop prune --older-than 720h
  pigeons coop prune --older-than 30d --batch-size 5000`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		f := cmd.Flags()
		rawOlderThan, _ := f.GetString("older-than")
		batchSize, _ := f.GetInt("batch-size")

		olderThan, err := parseRetentionDuration(rawOlderThan)
		if err != nil {
			return err
		}
		if batchSize < 0 {
			return fmt.Errorf("invalid --batch-size %d: must be >= 0", batchSize)
		}

		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 5*time.Minute)
		defer cancel()

		state := cliStateFromCommand(cmd)
		store, err := openOperatorStoreForConfig(ctx, state.wsCfg)
		if err != nil {
			return fmt.Errorf("could not connect to operator ledger: %w", err)
		}
		defer store.Close()

		_, err = runCoopPruneWithStore(ctx, cmd.OutOrStdout(), store, time.Now().UTC(), olderThan, batchSize)
		return err
	},
}

func init() {
	rootCmd.AddCommand(coopCmd)
	coopCmd.AddCommand(coopStatusCmd)
	coopCmd.AddCommand(coopMigrateCmd)
	coopCmd.AddCommand(coopProvisionRolesCmd)
	coopCmd.AddCommand(coopActivityCmd)
	coopCmd.AddCommand(coopPruneCmd)

	coopActivityCmd.Flags().IntP("limit", "n", 25, "Maximum attribution rows to display")
	coopProvisionRolesCmd.Flags().String("ingest-password", "", "Password for ctd_ingestor role")
	coopProvisionRolesCmd.Flags().String("readonly-password", "", "Password for pigeoncare_ro role")
	coopPruneCmd.Flags().String("older-than", "", "Delete telemetric_ledger rows older than this duration (e.g. 24h, 720h, 30d)")
	coopPruneCmd.Flags().Int("batch-size", db.DefaultLedgerPruneBatchLimit, "Maximum rows deleted per transaction batch")
}
