package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
)

var (
	connectBrokerURL   string
	connectDBURL       string
	connectRODBURL     string
	connectMQTTUser    string
	connectMQTTPass    string
	connectTLSCA       string
	connectTLSCert     string
	connectTLSKey      string
	connectTLSInsecure bool
	connectTopicRoot   string
	connectSkipVerify  bool
)

var connectCmd = &cobra.Command{
	Use:   "connect <mesh-name|->",
	Short: "Switch current working mesh (`cwd`-style) and verify PigeonCoop's MNS registration",
	Long: "Switch the workstation's active CarrierPigeons mesh (persisted in ~/.config/pigeons/current_mesh).\n" +
		"Supply `--broker` / `--db` to register or update `[meshes.<mesh-name>]` in pigeons.toml.\n" +
		"Pass `-` (`pigeons connect -`) to toggle back to the previous working mesh.",
	Args: cobra.ExactArgs(1),
	RunE: runConnectMesh,
}

var disconnectCmd = &cobra.Command{
	Use:   "disconnect",
	Short: "Clear the persisted current working mesh and revert to top-level pigeons.toml defaults",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		prev, _ := config.ReadCurrentMesh()
		if err := config.ClearCurrentMesh(); err != nil {
			return err
		}
		out := cmd.OutOrStdout()
		if prev != "" {
			fmt.Fprintf(out, "[+] Disconnected from working mesh %q (reverted to top-level config defaults).\n", prev)
		} else {
			fmt.Fprintln(out, "[+] No active working mesh state set (using top-level config defaults).")
		}
		return nil
	},
}

var meshCmd = &cobra.Command{
	Use:   "mesh",
	Short: "Inspect configured MNS mesh profiles and current working mesh state",
	Args:  cobra.NoArgs,
	RunE:  runMeshList,
}

var meshListCmd = &cobra.Command{
	Use:   "list",
	Short: "List configured `[meshes.*]` profiles and highlight the active working mesh",
	Args:  cobra.NoArgs,
	RunE:  runMeshList,
}

var meshCurrentCmd = &cobra.Command{
	Use:   "current",
	Short: "Print the active working mesh name for shell prompts and scripts",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		effective, err := config.LoadWorkstationConfigForMesh(cfgFilePath, meshFlag)
		if err != nil {
			return err
		}
		name := effective.ActiveMesh
		if name == "" {
			name = core.DefaultMeshName
		}
		fmt.Fprintln(cmd.OutOrStdout(), name)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(connectCmd)
	rootCmd.AddCommand(disconnectCmd)
	rootCmd.AddCommand(meshCmd)
	meshCmd.AddCommand(meshListCmd)
	meshCmd.AddCommand(meshCurrentCmd)

	f := connectCmd.Flags()
	f.StringVar(&connectBrokerURL, "broker", "", "Register or update mesh MQTT broker URL (e.g. tcp://192.0.2.10:1883)")
	f.StringVar(&connectDBURL, "db", "", "Register or update mesh PostgreSQL operator DSN")
	f.StringVar(&connectRODBURL, "ro-db", "", "Register or update mesh Read-Only PostgreSQL DSN")
	f.StringVar(&connectMQTTUser, "mqtt-user", "", "Register or update mesh MQTT username")
	f.StringVar(&connectMQTTPass, "mqtt-pass", "", "Register or update mesh MQTT password")
	f.StringVar(&connectTLSCA, "tls-ca", "", "Register or update mesh MQTT TLS CA certificate path")
	f.StringVar(&connectTLSCert, "tls-cert", "", "Register or update mesh MQTT TLS client certificate path")
	f.StringVar(&connectTLSKey, "tls-key", "", "Register or update mesh MQTT TLS client key path")
	f.BoolVar(&connectTLSInsecure, "tls-insecure", false, "Skip TLS hostname/CA verification for lab self-signed brokers")
	f.StringVar(&connectTopicRoot, "topic-root", "", "Register or update mesh MQTT topic root override")
	f.BoolVar(&connectSkipVerify, "no-verify", false, "Switch working mesh offline without live MNS handshake verification")
}

func buildConnectProfilePatch(cmd *cobra.Command) config.MeshProfilePatch {
	var p config.MeshProfilePatch
	f := cmd.Flags()
	if f.Changed("broker") {
		v := connectBrokerURL
		p.BrokerURL = &v
	}
	if f.Changed("db") {
		v := connectDBURL
		p.DatabaseURL = &v
	}
	if f.Changed("ro-db") {
		v := connectRODBURL
		p.ReadOnlyDatabaseURL = &v
	}
	if f.Changed("mqtt-user") {
		v := connectMQTTUser
		p.MQTTUsername = &v
	}
	if f.Changed("mqtt-pass") {
		v := connectMQTTPass
		p.MQTTPassword = &v
	}
	if f.Changed("tls-ca") {
		v := connectTLSCA
		p.TLSCAFile = &v
	}
	if f.Changed("tls-cert") {
		v := connectTLSCert
		p.TLSCertFile = &v
	}
	if f.Changed("tls-key") {
		v := connectTLSKey
		p.TLSKeyFile = &v
	}
	if f.Changed("tls-insecure") {
		v := connectTLSInsecure
		p.TLSInsecureSkipVerify = &v
	}
	if f.Changed("topic-root") {
		v := connectTopicRoot
		p.TopicRoot = &v
	}
	return p
}

func runConnectMesh(cmd *cobra.Command, args []string) error {
	rawTarget := strings.TrimSpace(args[0])
	if rawTarget == "-" {
		prev, err := config.ReadPreviousMesh()
		if err != nil {
			return err
		}
		if prev == "" {
			return fmt.Errorf("no previous working mesh recorded; run `%s connect <mesh-name>` first", wsCfg.Brand.CLIName)
		}
		rawTarget = prev
	}

	if err := core.ValidateMeshName(rawTarget); err != nil {
		return err
	}
	targetMesh := rawTarget
	patch := buildConnectProfilePatch(cmd)

	baseCfg, err := config.LoadBaseWorkstationConfig(cfgFilePath)
	if err != nil {
		return err
	}

	existingProfile, hasProfile := baseCfg.Meshes[targetMesh]
	isTopLevelMesh := baseCfg.MeshName == targetMesh
	if patch.IsEmpty() && !hasProfile && !isTopLevelMesh {
		available := baseCfg.ConfiguredMeshNames()
		return fmt.Errorf(
			"mesh %q is not configured in pigeons.toml (available: %v; register it with `%s connect %s --broker <tcp://host:1883> [--db <postgres://...>]`)",
			targetMesh, available, baseCfg.Brand.CLIName, targetMesh,
		)
	}

	mergedProfile := patch.Apply(existingProfile)
	candidateBase := baseCfg
	if hasProfile || !patch.IsEmpty() {
		candidateBase = config.ApplyMeshProfile(baseCfg, targetMesh, mergedProfile)
	} else {
		candidateBase.ActiveMesh = targetMesh
		candidateBase.MeshName = targetMesh
	}
	candidateCfg, err := config.FinalizeWorkstationConfig(candidateBase)
	if err != nil {
		return err
	}

	// Probe PostgreSQL MNS table only when a DB URL belongs to this mesh profile (or top-level mesh)
	// so broker-only profiles never accidentally query an unrelated local loopback Postgres instance.
	probeDB := strings.TrimSpace(mergedProfile.DatabaseURL) != "" ||
		strings.TrimSpace(mergedProfile.ReadOnlyDatabaseURL) != "" ||
		isTopLevelMesh ||
		os.Getenv("PIGEONS_DB_URL") != "" ||
		os.Getenv("PIGEONS_RO_DB_URL") != ""

	verifySummary := "unverified (--no-verify)"
	if !connectSkipVerify {
		ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
		defer cancel()

		verified, err := verifyRemoteMeshIdentity(ctx, candidateCfg, targetMesh, probeDB)
		if err != nil {
			return err
		}
		verifySummary = verified
	}

	var savedConfigPath string
	if !patch.IsEmpty() {
		savedConfigPath, err = config.UpsertWorkstationMeshProfile(cfgFilePath, targetMesh, patch)
		if err != nil {
			return err
		}
	}

	statePath, err := config.WriteCurrentMesh(targetMesh)
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if savedConfigPath != "" {
		fmt.Fprintf(out, "[+] Registered [meshes.%s] in %s\n", targetMesh, savedConfigPath)
	}
	fmt.Fprintf(out, "[+] Connected current working mesh -> %s (%s) [broker=%s, state=%s]\n",
		targetMesh, verifySummary, candidateCfg.BrokerURL, statePath)
	return nil
}

func verifyRemoteMeshIdentity(ctx context.Context, cfg config.WorkstationConfig, expectedMesh string, probeDB bool) (string, error) {
	type probeResult struct {
		source   string
		identity *core.MeshIdentity
		err      error
	}

	var (
		wg      sync.WaitGroup
		results = make(chan probeResult, 2)
	)

	if strings.TrimSpace(cfg.BrokerURL) != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := probeMQTTMeshIdentity(ctx, cfg)
			results <- probeResult{source: "mqtt", identity: id, err: err}
		}()
	}

	if probeDB && (strings.TrimSpace(cfg.ReadOnlyDatabaseURL) != "" || strings.TrimSpace(cfg.DatabaseURL) != "") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := probePostgresMeshIdentity(ctx, cfg)
			results <- probeResult{source: "postgres", identity: id, err: err}
		}()
	}

	wg.Wait()
	close(results)

	var (
		verifiedSources []string
		coopNode        string
		probeErrors     []string
	)

	for r := range results {
		if r.identity != nil && strings.TrimSpace(r.identity.MeshName) != "" {
			got := strings.TrimSpace(r.identity.MeshName)
			if got != expectedMesh {
				nodeInfo := ""
				if r.identity.CoopNodeID != "" {
					nodeInfo = fmt.Sprintf(" on coop node %q", r.identity.CoopNodeID)
				}
				return "", fmt.Errorf(
					"MNS verification failed via %s: remote %s%s registered mesh_name=%q, expected %q (refusing to switch working mesh)",
					r.source, cfg.Brand.CoopName, nodeInfo, got, expectedMesh,
				)
			}
			verifiedSources = append(verifiedSources, r.source)
			if coopNode == "" && r.identity.CoopNodeID != "" {
				coopNode = r.identity.CoopNodeID
			}
			continue
		}
		if r.err != nil {
			probeErrors = append(probeErrors, fmt.Sprintf("%s: %v", r.source, r.err))
		} else {
			probeErrors = append(probeErrors, fmt.Sprintf("%s: no MNS identity registered yet", r.source))
		}
	}

	if len(verifiedSources) == 0 {
		detail := strings.Join(probeErrors, "; ")
		if detail == "" {
			detail = "no broker_url or database_url configured"
		}
		return "", fmt.Errorf(
			"could not verify remote MNS identity for mesh %q (%s) — ensure %s is running with mesh_name=%q or pass --no-verify for offline switching",
			expectedMesh, detail, cfg.Brand.CoopName, expectedMesh,
		)
	}

	// Canonical deterministic order ("mqtt+postgres").
	hasMQTT, hasPG := false, false
	for _, s := range verifiedSources {
		if s == "mqtt" {
			hasMQTT = true
		}
		if s == "postgres" {
			hasPG = true
		}
	}
	var ordered []string
	if hasMQTT {
		ordered = append(ordered, "mqtt")
	}
	if hasPG {
		ordered = append(ordered, "postgres")
	}
	summary := "verified via " + strings.Join(ordered, "+")
	if coopNode != "" {
		summary += ", coop=" + coopNode
	}
	return summary, nil
}

func probeMQTTMeshIdentity(ctx context.Context, cfg config.WorkstationConfig) (*core.MeshIdentity, error) {
	timeout := 2500 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem > 0 && rem < timeout {
			timeout = rem
		}
	}

	clientID := fmt.Sprintf("%s-mns-%d", cfg.Brand.CLIName, time.Now().UnixNano())
	client, err := dialMQTTClientWithConfig(cfg, cfg.BrokerURL, clientID, timeout)
	if err != nil {
		return nil, err
	}
	defer client.Disconnect(100)

	meshTopic := cfg.Brand.MeshTopic()
	msgCh := make(chan []byte, 1)
	tok := client.Subscribe(meshTopic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		payload := append([]byte(nil), msg.Payload()...)
		select {
		case msgCh <- payload:
		default:
		}
	})
	if !tok.WaitTimeout(timeout) {
		return nil, fmt.Errorf("timeout subscribing to %s", meshTopic)
	}
	if tok.Error() != nil {
		return nil, fmt.Errorf("subscribe %s: %w", meshTopic, tok.Error())
	}

	waitTimer := time.NewTimer(900 * time.Millisecond)
	defer waitTimer.Stop()

	select {
	case raw := <-msgCh:
		var id core.MeshIdentity
		if err := json.Unmarshal(raw, &id); err != nil {
			return nil, fmt.Errorf("decode retained MNS payload on %s: %w", meshTopic, err)
		}
		return &id, nil
	case <-waitTimer.C:
		return nil, fmt.Errorf("no retained MNS announcement on %s", meshTopic)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func probePostgresMeshIdentity(ctx context.Context, cfg config.WorkstationConfig) (*core.MeshIdentity, error) {
	actor := fmt.Sprintf("%s-mns:%s", cfg.Brand.CLIName, cfg.Operator)
	if roDSN := strings.TrimSpace(cfg.ReadOnlyDatabaseURL); roDSN != "" {
		roStore, roErr := db.NewReadOnlyAgentStore(ctx, roDSN, actor, db.ReadOnlyOptions{
			AllowSuperuser: cfg.AllowSuperuserReadOnly,
			MathSchema:     cfg.Brand.MathSchema,
		})
		if roErr == nil {
			id, qErr := roStore.GetMeshIdentity(ctx)
			roStore.Close()
			if qErr == nil {
				return id, nil
			}
		}
	}

	if opDSN := strings.TrimSpace(cfg.DatabaseURL); opDSN != "" {
		opStore, opErr := db.NewPostgresStoreWithOptions(ctx, opDSN, db.StoreOptions{
			AppActor:   actor,
			MaxConns:   2,
			MinConns:   0,
			MathSchema: cfg.Brand.MathSchema,
		})
		if opErr != nil {
			return nil, opErr
		}
		defer opStore.Close()
		return opStore.GetMeshIdentity(ctx)
	}

	return nil, fmt.Errorf("no database URL configured")
}

func redactDSN(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "-"
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	return u.Redacted()
}

func runMeshList(cmd *cobra.Command, _ []string) error {
	baseCfg, err := config.LoadBaseWorkstationConfig(cfgFilePath)
	if err != nil {
		return err
	}

	activeName := ""
	activeSource := "default"
	if eff, effErr := config.LoadWorkstationConfigForMesh(cfgFilePath, meshFlag); effErr == nil {
		activeName = eff.ActiveMesh
		if eff.ActiveMeshSource != "" {
			activeSource = eff.ActiveMeshSource
		}
	} else if stateMesh, _ := config.ReadCurrentMesh(); stateMesh != "" {
		activeName = stateMesh
		activeSource = "state (unconfigured!)"
	}

	out := cmd.OutOrStdout()
	names := baseCfg.ConfiguredMeshNames()
	if len(names) == 0 {
		displayActive := activeName
		if displayActive == "" {
			displayActive = core.DefaultMeshName
		}
		fmt.Fprintf(out, "Current working mesh: %s (source: %s)\n", displayActive, activeSource)
		fmt.Fprintf(out, "Broker: %s | Ledger: %s\n", baseCfg.BrokerURL, redactDSN(baseCfg.DatabaseURL))
		fmt.Fprintf(out, "No named `[meshes.<name>]` profiles configured yet. Register one via:\n")
		fmt.Fprintf(out, "  %s connect <mesh-name> --broker tcp://<host>:1883 --db postgres://<user>:<pass>@<host>:5432/edge_ledger\n",
			baseCfg.Brand.CLIName)
		return nil
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "active\t| mesh (MNS)\t| topic root\t| broker\t| ledger")
	fmt.Fprintln(w, "------\t| ----------\t| ----------\t| ------\t| ------")
	for _, name := range names {
		resolved := baseCfg
		if mp, ok := baseCfg.Meshes[name]; ok {
			resolved = config.ApplyMeshProfile(baseCfg, name, mp)
		}
		resolved, _ = config.FinalizeWorkstationConfig(resolved)
		marker := " "
		if name == activeName {
			marker = "*"
		}
		fmt.Fprintf(w, "  %s\t| %s\t| %s\t| %s\t| %s\n",
			marker, name, resolved.Brand.TopicRoot, resolved.BrokerURL, redactDSN(resolved.DatabaseURL))
	}
	if err := w.Flush(); err != nil {
		return err
	}
	displayActive := activeName
	if displayActive == "" {
		displayActive = "(top-level default)"
	}
	fmt.Fprintf(out, "\nActive working mesh: %s [source=%s]\n", displayActive, activeSource)
	return nil
}
