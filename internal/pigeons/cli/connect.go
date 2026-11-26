package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
)

const (
	defaultMNSRetainedMaxAge = 24 * time.Hour
	defaultMNSMaxFutureSkew  = 5 * time.Minute
)

// mnsRetainedMaxAge and mnsMaxFutureSkew bound how old or far ahead in the future an MNS
// announcement may be before `connect` rejects it as stale or skewed.
// Package-level vars for deterministic unit testing.
var (
	mnsRetainedMaxAge = defaultMNSRetainedMaxAge
	mnsMaxFutureSkew  = defaultMNSMaxFutureSkew
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
		state := cliStateFromCommand(cmd)
		cfgPath, _ := persistentConfigFlags(cmd)
		targetBrand := state.baseBrand
		if baseCfg, bErr := config.LoadBaseWorkstationConfigForBrand(cfgPath, state.baseBrand); bErr == nil {
			targetBrand = baseCfg.Brand
		}
		prev, _ := config.ReadCurrentMeshForBrand(targetBrand)
		if err := config.ClearCurrentMeshForBrand(targetBrand); err != nil {
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
		state := cliStateFromCommand(cmd)
		cfgPath, meshName := persistentConfigFlags(cmd)
		effective, err := loadWorkstationConfigForBrand(cfgPath, meshName, state.baseBrand, false)
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
	f.String("broker", "", "Register or update mesh MQTT broker URL (e.g. tcp://192.0.2.10:1883)")
	f.String("db", "", "Register or update mesh PostgreSQL operator DSN")
	f.String("ro-db", "", "Register or update mesh Read-Only PostgreSQL DSN")
	f.String("mqtt-user", "", "Register or update mesh MQTT username")
	f.String("mqtt-pass", "", "Register or update mesh MQTT password")
	f.String("tls-ca", "", "Register or update mesh MQTT TLS CA certificate path")
	f.String("tls-cert", "", "Register or update mesh MQTT TLS client certificate path")
	f.String("tls-key", "", "Register or update mesh MQTT TLS client key path")
	f.Bool("tls-insecure", false, "Skip TLS hostname/CA verification for lab self-signed brokers")
	f.String("topic-root", "", "Register or update mesh MQTT topic root override")
	f.Bool("no-verify", false, "Switch working mesh offline without live MNS handshake verification")
	f.Bool("allow-partial-verify", false, "Allow switching when at least one MNS source verifies and no source conflicts")
}

func buildConnectProfilePatch(cmd *cobra.Command) config.MeshProfilePatch {
	var p config.MeshProfilePatch
	f := cmd.Flags()
	if f.Changed("broker") {
		v, _ := f.GetString("broker")
		p.BrokerURL = &v
	}
	if f.Changed("db") {
		v, _ := f.GetString("db")
		p.DatabaseURL = &v
	}
	if f.Changed("ro-db") {
		v, _ := f.GetString("ro-db")
		p.ReadOnlyDatabaseURL = &v
	}
	if f.Changed("mqtt-user") {
		v, _ := f.GetString("mqtt-user")
		p.MQTTUsername = &v
	}
	if f.Changed("mqtt-pass") {
		v, _ := f.GetString("mqtt-pass")
		p.MQTTPassword = &v
	}
	if f.Changed("tls-ca") {
		v, _ := f.GetString("tls-ca")
		p.TLSCAFile = &v
	}
	if f.Changed("tls-cert") {
		v, _ := f.GetString("tls-cert")
		p.TLSCertFile = &v
	}
	if f.Changed("tls-key") {
		v, _ := f.GetString("tls-key")
		p.TLSKeyFile = &v
	}
	if f.Changed("tls-insecure") {
		v, _ := f.GetBool("tls-insecure")
		p.TLSInsecureSkipVerify = &v
	}
	if f.Changed("topic-root") {
		v, _ := f.GetString("topic-root")
		p.TopicRoot = &v
	}
	return p
}

func applyMeshProfilePatchToWorkstationConfig(cfg config.WorkstationConfig, baseBrand brand.Spec, patch config.MeshProfilePatch) (config.WorkstationConfig, error) {
	if patch.BrokerURL != nil {
		cfg.BrokerURL = strings.TrimSpace(*patch.BrokerURL)
	}
	if patch.DatabaseURL != nil {
		cfg.DatabaseURL = strings.TrimSpace(*patch.DatabaseURL)
		if patch.ReadOnlyDatabaseURL == nil {
			// Clear inherited/env RO DSN so `--db` probes the explicitly requested Postgres target.
			cfg.ReadOnlyDatabaseURL = ""
		}
	}
	if patch.ReadOnlyDatabaseURL != nil {
		cfg.ReadOnlyDatabaseURL = strings.TrimSpace(*patch.ReadOnlyDatabaseURL)
	}
	if patch.MQTTUsername != nil {
		u := strings.TrimSpace(*patch.MQTTUsername)
		cfg.MQTTUsername = u
		cfg.MQTT.Username = u
	}
	if patch.MQTTPassword != nil {
		cfg.MQTTPassword = *patch.MQTTPassword
		cfg.MQTT.Password = *patch.MQTTPassword
	}
	if patch.TLSCAFile != nil {
		v := strings.TrimSpace(*patch.TLSCAFile)
		cfg.TLSCAFile = v
		cfg.MQTT.TLSCAFile = v
	}
	if patch.TLSCertFile != nil {
		v := strings.TrimSpace(*patch.TLSCertFile)
		cfg.TLSCertFile = v
		cfg.MQTT.TLSCertFile = v
	}
	if patch.TLSKeyFile != nil {
		v := strings.TrimSpace(*patch.TLSKeyFile)
		cfg.TLSKeyFile = v
		cfg.MQTT.TLSKeyFile = v
	}
	if patch.TLSInsecureSkipVerify != nil {
		b := *patch.TLSInsecureSkipVerify
		cfg.TLSInsecureSkipVerify = b
		cfg.MQTT.TLSInsecureSkipVerify = b
	}
	if patch.TopicRoot != nil {
		tr := strings.Trim(strings.TrimSpace(*patch.TopicRoot), "/")
		if tr != "" {
			if err := core.ValidateTopicToken(tr); err != nil {
				return cfg, fmt.Errorf("invalid --topic-root %q: %w", *patch.TopicRoot, err)
			}
			cfg.Brand.TopicRoot = tr
		} else {
			cfg.Brand.TopicRoot = baseBrand.MergeWithDefaults().TopicRoot
		}
	}
	return cfg, nil
}

func runConnectMesh(cmd *cobra.Command, args []string) error {
	state := cliStateFromCommand(cmd)
	cfgPath, _ := persistentConfigFlags(cmd)

	baseCfg, err := config.LoadBaseWorkstationConfigForBrand(cfgPath, state.baseBrand)
	if err != nil {
		return err
	}

	rawTarget := strings.TrimSpace(args[0])
	if rawTarget == "-" {
		prev, err := config.ReadPreviousMeshForBrand(baseCfg.Brand)
		if err != nil {
			return err
		}
		if prev == "" {
			return fmt.Errorf("no previous working mesh recorded; run `%s connect <mesh-name>` first", baseCfg.Brand.CLIName)
		}
		rawTarget = prev
	}

	if err := core.ValidateMeshName(rawTarget); err != nil {
		return err
	}
	targetMesh := rawTarget
	patch := buildConnectProfilePatch(cmd)
	if patch.TopicRoot != nil {
		tr := strings.Trim(strings.TrimSpace(*patch.TopicRoot), "/")
		if tr != "" {
			if err := core.ValidateTopicToken(tr); err != nil {
				return fmt.Errorf("invalid --topic-root %q: %w", *patch.TopicRoot, err)
			}
		}
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
	candidateCfg, err = applyMeshProfilePatchToWorkstationConfig(candidateCfg, baseCfg.Brand, patch)
	if err != nil {
		return err
	}

	// Probe PostgreSQL MNS table only when a DB URL belongs to this mesh profile (or top-level mesh)
	// so broker-only profiles never accidentally query an unrelated local loopback Postgres instance.
	probeDB := strings.TrimSpace(mergedProfile.DatabaseURL) != "" ||
		strings.TrimSpace(mergedProfile.ReadOnlyDatabaseURL) != "" ||
		patch.DatabaseURL != nil ||
		patch.ReadOnlyDatabaseURL != nil ||
		isTopLevelMesh ||
		config.ResolveWorkstationDBURLEnv(candidateCfg.Brand.EnvPrefix) != "" ||
		config.ResolveWorkstationRODBURLEnv(candidateCfg.Brand.EnvPrefix) != ""

	skipVerify, _ := cmd.Flags().GetBool("no-verify")
	allowPartialVerify, _ := cmd.Flags().GetBool("allow-partial-verify")

	verifySummary := "unverified (--no-verify)"
	if !skipVerify {
		parentCtx := cmd.Context()
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
		defer cancel()

		verified, err := verifyRemoteMeshIdentity(ctx, candidateCfg, targetMesh, probeDB, allowPartialVerify)
		if err != nil {
			return err
		}
		verifySummary = verified
	}

	var savedConfigPath string
	if !patch.IsEmpty() {
		savedConfigPath, err = config.UpsertWorkstationMeshProfileForBrand(cfgPath, baseCfg.Brand, targetMesh, patch)
		if err != nil {
			return err
		}
	}

	statePath, err := config.WriteCurrentMeshForBrand(baseCfg.Brand, targetMesh)
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if savedConfigPath != "" {
		fmt.Fprintf(out, "[+] Registered [meshes.%s] in %s\n", targetMesh, savedConfigPath)
	}
	fmt.Fprintf(out, "[+] Connected current working mesh -> %s (%s) [broker=%s, state=%s]\n",
		targetMesh, verifySummary, config.RedactURL(candidateCfg.BrokerURL), statePath)
	return nil
}

type meshIdentityProber func(ctx context.Context, cfg config.WorkstationConfig) (*core.MeshIdentity, error)

func verifyRemoteMeshIdentity(ctx context.Context, cfg config.WorkstationConfig, expectedMesh string, probeDB, allowPartial bool) (string, error) {
	return verifyRemoteMeshIdentityWithProbers(ctx, cfg, expectedMesh, probeDB, allowPartial, probeMQTTMeshIdentity, probePostgresMeshIdentity)
}

func verifyRemoteMeshIdentityWithProbers(
	ctx context.Context,
	cfg config.WorkstationConfig,
	expectedMesh string,
	probeDB bool,
	allowPartial bool,
	probeMQTT meshIdentityProber,
	probePG meshIdentityProber,
) (string, error) {
	type probeResult struct {
		source   string
		identity *core.MeshIdentity
		err      error
	}

	var (
		wg      sync.WaitGroup
		results = make(chan probeResult, 2)
	)

	if probeMQTT != nil && strings.TrimSpace(cfg.BrokerURL) != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := probeMQTT(ctx, cfg)
			results <- probeResult{source: "mqtt", identity: id, err: err}
		}()
	}

	if probePG != nil && probeDB && (strings.TrimSpace(cfg.ReadOnlyDatabaseURL) != "" || strings.TrimSpace(cfg.DatabaseURL) != "") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := probePG(ctx, cfg)
			results <- probeResult{source: "postgres", identity: id, err: err}
		}()
	}

	wg.Wait()
	close(results)

	var (
		verifiedSources []string
		coopNode        string
		coopNodeSource  string
		topicRoot       string
		topicRootSource string
		probeErrors     []string
		mismatchErr     error
		conflictErr     error
	)

	var collected []probeResult
	for r := range results {
		collected = append(collected, r)
	}
	sort.Slice(collected, func(i, j int) bool {
		return collected[i].source < collected[j].source
	})

	for _, r := range collected {
		if r.err == nil && r.identity != nil && strings.TrimSpace(r.identity.MeshName) != "" {
			got := strings.TrimSpace(r.identity.MeshName)
			gotNode := strings.TrimSpace(r.identity.CoopNodeID)
			gotTopicRoot := strings.Trim(strings.TrimSpace(r.identity.TopicRoot), "/")
			if got != expectedMesh {
				nodeInfo := ""
				if gotNode != "" {
					nodeInfo = fmt.Sprintf(" on coop node %q", gotNode)
				}
				if mismatchErr == nil {
					mismatchErr = fmt.Errorf(
						"MNS verification failed via %s: remote %s%s registered mesh_name=%q, expected %q (refusing to switch working mesh)",
						r.source, cfg.Brand.CoopName, nodeInfo, got, expectedMesh,
					)
				}
				continue
			}
			if gotNode != "" {
				if coopNode == "" {
					coopNode = gotNode
					coopNodeSource = r.source
				} else if gotNode != coopNode && conflictErr == nil {
					conflictErr = fmt.Errorf(
						"MNS verification failed: conflicting coop_node_id across sources (%s=%q vs %s=%q)",
						coopNodeSource, coopNode, r.source, gotNode,
					)
				}
			}
			if gotTopicRoot != "" {
				if topicRoot == "" {
					topicRoot = gotTopicRoot
					topicRootSource = r.source
				} else if gotTopicRoot != topicRoot && conflictErr == nil {
					conflictErr = fmt.Errorf(
						"MNS verification failed: conflicting topic_root across sources (%s=%q vs %s=%q)",
						topicRootSource, topicRoot, r.source, gotTopicRoot,
					)
				}
			}
			verifiedSources = append(verifiedSources, r.source)
			continue
		}
		if r.err != nil {
			probeErrors = append(probeErrors, fmt.Sprintf("%s: %v", r.source, r.err))
		} else {
			probeErrors = append(probeErrors, fmt.Sprintf("%s: no MNS identity registered yet", r.source))
		}
	}

	if mismatchErr != nil {
		return "", mismatchErr
	}
	if conflictErr != nil {
		return "", conflictErr
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

	if !allowPartial && len(probeErrors) > 0 {
		return "", fmt.Errorf(
			"MNS verification incomplete for mesh %q (%s) — all probed sources must verify cleanly unless --allow-partial-verify is passed",
			expectedMesh, strings.Join(probeErrors, "; "),
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
	if len(probeErrors) > 0 {
		summary += " (partial)"
	}
	if coopNode != "" {
		summary += ", coop=" + coopNode
	}
	return summary, nil
}

type retainedMeshPayload struct {
	MeshName   string    `json:"mesh_name"`
	CoopNodeID string    `json:"coop_node_id"`
	TopicRoot  string    `json:"topic_root,omitempty"`
	Status     string    `json:"status"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func validateMeshIdentity(sourceDesc string, id *core.MeshIdentity, now time.Time) error {
	if id == nil {
		return nil
	}
	if strings.TrimSpace(id.MeshName) == "" {
		return fmt.Errorf("%s has empty mesh_name", sourceDesc)
	}
	if id.UpdatedAt.IsZero() {
		return fmt.Errorf("%s is missing updated_at", sourceDesc)
	}
	if mnsMaxFutureSkew > 0 && id.UpdatedAt.Sub(now) > mnsMaxFutureSkew {
		return fmt.Errorf("%s has future updated_at clock skew (updated_at=%s, ahead>%s)",
			sourceDesc, id.UpdatedAt.UTC().Format(time.RFC3339), mnsMaxFutureSkew)
	}
	if mnsRetainedMaxAge > 0 && now.Sub(id.UpdatedAt) > mnsRetainedMaxAge {
		return fmt.Errorf("%s is stale (updated_at=%s, age>%s)",
			sourceDesc, id.UpdatedAt.UTC().Format(time.RFC3339), mnsRetainedMaxAge)
	}
	return nil
}

func validateRetainedMeshPayload(raw []byte, meshTopic, coopName string, now time.Time) (*core.MeshIdentity, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("retained MNS announcement on %s is empty (cleared)", meshTopic)
	}
	var payload retainedMeshPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode retained MNS payload on %s: %w", meshTopic, err)
	}
	if strings.EqualFold(strings.TrimSpace(payload.Status), "OFFLINE") {
		return nil, fmt.Errorf("remote %s on %s is OFFLINE (LWT)", coopName, meshTopic)
	}
	id := &core.MeshIdentity{
		MeshName:   strings.TrimSpace(payload.MeshName),
		CoopNodeID: strings.TrimSpace(payload.CoopNodeID),
		TopicRoot:  strings.Trim(strings.TrimSpace(payload.TopicRoot), "/"),
		UpdatedAt:  payload.UpdatedAt,
	}
	if err := validateMeshIdentity(fmt.Sprintf("retained MNS payload on %s", meshTopic), id, now); err != nil {
		return nil, err
	}
	return id, nil
}

func probeMQTTMeshIdentity(ctx context.Context, cfg config.WorkstationConfig) (*core.MeshIdentity, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	timeout := 2500 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	clientID := fmt.Sprintf("%s-mns-%d", cfg.Brand.CLIName, time.Now().UnixNano())
	client, err := dialMQTTClientWithConfig(ctx, cfg, cfg.BrokerURL, clientID, timeout)
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

	subTimer := time.NewTimer(timeout)
	defer subTimer.Stop()
	select {
	case <-tok.Done():
		if tok.Error() != nil {
			if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, fmt.Errorf("subscribe %s: %w", meshTopic, tok.Error())
		}
		if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
			return nil, ctxErr
		}
	case <-subTimer.C:
		if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("timeout subscribing to %s", meshTopic)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	waitTimeout := 900 * time.Millisecond
	waitTimer := time.NewTimer(waitTimeout)
	defer waitTimer.Stop()

	select {
	case raw := <-msgCh:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return validateRetainedMeshPayload(raw, meshTopic, cfg.Brand.CoopName, time.Now().UTC())
	case <-waitTimer.C:
		if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("no retained MNS announcement on %s", meshTopic)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func probePostgresMeshIdentity(ctx context.Context, cfg config.WorkstationConfig) (*core.MeshIdentity, error) {
	actor := fmt.Sprintf("%s-mns:%s", cfg.Brand.CLIName, cfg.Operator)
	var roProbeErr error
	if roDSN := strings.TrimSpace(cfg.ReadOnlyDatabaseURL); roDSN != "" {
		roStore, roErr := db.NewReadOnlyAgentStore(ctx, roDSN, actor, db.ReadOnlyOptions{
			AllowSuperuser: cfg.AllowSuperuserReadOnly,
			MathSchema:     cfg.Brand.MathSchema,
			EnvPrefix:      cfg.Brand.EnvPrefix,
		})
		if roErr != nil {
			roProbeErr = roErr
		} else {
			id, qErr := roStore.GetMeshIdentity(ctx)
			roStore.Close()
			if qErr == nil {
				if err := validateMeshIdentity("postgres ctd_mesh", id, time.Now().UTC()); err != nil {
					return nil, err
				}
				return id, nil
			}
			roProbeErr = qErr
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
		id, qErr := opStore.GetMeshIdentity(ctx)
		if qErr != nil {
			return nil, qErr
		}
		if err := validateMeshIdentity("postgres ctd_mesh", id, time.Now().UTC()); err != nil {
			return nil, err
		}
		return id, nil
	}

	if roProbeErr != nil {
		return nil, roProbeErr
	}
	return nil, fmt.Errorf("no database URL configured")
}

func redactDSN(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "-"
	}
	return config.RedactURL(trimmed)
}

func runMeshList(cmd *cobra.Command, _ []string) error {
	state := cliStateFromCommand(cmd)
	cfgPath, meshName := persistentConfigFlags(cmd)

	baseCfg, err := config.LoadBaseWorkstationConfigForBrand(cfgPath, state.baseBrand)
	if err != nil {
		return err
	}

	activeName := ""
	activeSource := "default"
	if eff, effErr := loadWorkstationConfigForBrand(cfgPath, meshName, state.baseBrand, false); effErr == nil {
		activeName = eff.ActiveMesh
		if eff.ActiveMeshSource != "" {
			activeSource = eff.ActiveMeshSource
		}
	} else if stateMesh, _ := config.ReadCurrentMeshForBrand(baseCfg.Brand); stateMesh != "" {
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
		fmt.Fprintf(out, "Broker: %s | Ledger: %s\n", config.RedactURL(baseCfg.BrokerURL), redactDSN(baseCfg.DatabaseURL))
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
			marker, name, resolved.Brand.TopicRoot, config.RedactURL(resolved.BrokerURL), redactDSN(resolved.DatabaseURL))
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
