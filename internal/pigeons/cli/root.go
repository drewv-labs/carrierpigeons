package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncare"
	"github.com/drewv-labs/carrierpigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	cfgFilePath string
	meshFlag    string
	wsCfg       = config.DefaultWorkstationConfig()

	templateMu sync.Mutex
)

type cliState struct {
	baseBrand brand.Spec
	brand     brand.Spec
	wsCfg     config.WorkstationConfig
}

type cliStateContextKey struct{}

func withCLIState(ctx context.Context, state *cliState) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, cliStateContextKey{}, state)
}

func cliStateFromCommand(cmd *cobra.Command) *cliState {
	if cmd != nil && cmd.Context() != nil {
		if st, ok := cmd.Context().Value(cliStateContextKey{}).(*cliState); ok && st != nil {
			return st
		}
	}
	return &cliState{
		baseBrand: wsCfg.Brand,
		brand:     wsCfg.Brand,
		wsCfg:     wsCfg,
	}
}

func persistentConfigFlags(cmd *cobra.Command) (string, string) {
	if cmd == nil {
		return "", ""
	}
	pf := cmd.Root().PersistentFlags()
	var cfg, mesh string
	if f := pf.Lookup("config"); f != nil {
		cfg = f.Value.String()
	}
	if f := pf.Lookup("mesh"); f != nil {
		mesh = f.Value.String()
	}
	return cfg, mesh
}

func loadWorkstationConfigForBrand(cfgPath, meshName string, instanceBrand brand.Spec, allowBrokenCurrentMesh bool) (config.WorkstationConfig, error) {
	b := instanceBrand.MergeWithDefaults()
	effectiveMesh := strings.TrimSpace(meshName)

	loaded, err := config.LoadWorkstationConfigForMeshAndBrand(cfgPath, effectiveMesh, b)
	if err != nil {
		// Allow mesh recovery/switching commands (`connect`, `disconnect`, `mesh`) to run even if
		// `<cli>/current_mesh` points to a removed profile.
		if allowBrokenCurrentMesh && effectiveMesh == "" {
			base, baseErr := config.LoadBaseWorkstationConfigForBrand(cfgPath, b)
			if baseErr != nil {
				return config.WorkstationConfig{}, baseErr
			}
			return config.FinalizeWorkstationConfig(base)
		}
		return config.WorkstationConfig{}, err
	}
	return loaded, nil
}

func isMeshManagementCommand(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	switch cmd.Name() {
	case "connect", "disconnect", "mesh":
		return true
	}
	if p := cmd.Parent(); p != nil && p.Name() == "mesh" {
		return true
	}
	return false
}

var rootCmd = &cobra.Command{
	Use:   "pigeons",
	Short: "CarrierPigeons Control Plane CLI & TUI",
	Long:  "Manage heterogeneous telemetry meshes (RISC-V, ARM64, x86), node tags, remote Runners, PigeonMath views, and PigeonCare diagnostics.",
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		cfgPath, meshName := persistentConfigFlags(cmd)
		loaded, err := loadWorkstationConfigForBrand(cfgPath, meshName, brand.Default(), isMeshManagementCommand(cmd))
		if err != nil {
			return err
		}
		wsCfg = loaded
		cmd.SetContext(withCLIState(cmd.Context(), &cliState{
			baseBrand: brand.Default(),
			brand:     loaded.Brand,
			wsCfg:     loaded,
		}))
		return nil
	},
	Run: func(cmd *cobra.Command, _ []string) {
		_ = cmd.Help()
	},
}

var tuiCmd = &cobra.Command{
	Use:   "tui",
	Short: "Launch the interactive 4-tab Control Plane TUI (Overview, RunnerManager, PigeonMath, PigeonCare)",
	RunE: func(cmd *cobra.Command, _ []string) error {
		state := cliStateFromCommand(cmd)
		return launchDashboardWithConfig(cmd.Context(), state.wsCfg, tui.TabOverview, "", "", "")
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFilePath, "config", "", "Path to pigeons.toml")
	rootCmd.PersistentFlags().StringVar(&meshFlag, "mesh", "", "Override current working mesh for this command")
	rootCmd.AddCommand(tuiCmd)
}

func resolveBrokerURLForConfig(cfg config.WorkstationConfig) string {
	if strings.TrimSpace(cfg.BrokerURL) != "" {
		return cfg.BrokerURL
	}
	if val := config.PrefixedEnv(cfg.Brand.EnvPrefix, "BROKER_URL", "PIGEONS_BROKER_URL", "PIGEONCOOP_BROKER_URL", "MQTT_BROKER_URL"); val != "" {
		return val
	}
	return "tcp://127.0.0.1:1883"
}

func resolveBrokerURL() string {
	return resolveBrokerURLForConfig(wsCfg)
}

func isTLSMQTTScheme(scheme string) bool {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "tls", "ssl", "tcps", "mqtts", "mqtt+ssl", "wss":
		return true
	default:
		return false
	}
}

func waitForContextDeadlineErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func dialMQTTClientWithConfig(ctx context.Context, cfg config.WorkstationConfig, brokerURL, clientID string, timeout time.Duration) (mqtt.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	pahoTimeout := timeout
	if deadline, ok := ctx.Deadline(); ok {
		rem := time.Until(deadline)
		if rem <= 0 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		// Give Paho slightly more slack than ctx.Deadline so <-ctx.Done() wins deterministically
		// without racing a duplicate timer while still bounding socket teardown soon after ctx expiry.
		if bounded := rem + 15*time.Millisecond; bounded < pahoTimeout {
			pahoTimeout = bounded
		}
	}

	mqttAuth := cfg.EffectiveMQTTAuth()
	tlsCfg, err := config.BuildMQTTTLSConfig(
		mqttAuth.TLSCAFile,
		mqttAuth.TLSCertFile,
		mqttAuth.TLSKeyFile,
		mqttAuth.TLSInsecureSkipVerify,
	)
	if err != nil {
		return nil, err
	}

	if tlsCfg != nil {
		u, parseErr := url.Parse(strings.TrimSpace(brokerURL))
		if parseErr != nil {
			return nil, fmt.Errorf("invalid MQTT broker URL %s: %w", config.RedactURL(brokerURL), parseErr)
		}
		if !isTLSMQTTScheme(u.Scheme) {
			return nil, fmt.Errorf("MQTT TLS configuration requires a TLS broker URL scheme (tls://, ssl://, or wss://), got %s", config.RedactURL(brokerURL))
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectTimeout(pahoTimeout).
		SetWriteTimeout(pahoTimeout).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetDialer(&net.Dialer{Timeout: pahoTimeout})
	if mqttAuth.Username != "" {
		opts.SetUsername(mqttAuth.Username)
		opts.SetPassword(mqttAuth.Password)
	}
	if tlsCfg != nil {
		opts.SetTLSConfig(tlsCfg)
	}

	client := mqtt.NewClient(opts)
	tok := client.Connect()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-tok.Done():
		if tok.Error() != nil {
			client.Disconnect(0)
			if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, fmt.Errorf("connect MQTT broker %s: %w", config.RedactURL(brokerURL), tok.Error())
		}
		if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
			client.Disconnect(0)
			return nil, ctxErr
		}
		return client, nil
	case <-timer.C:
		client.Disconnect(0)
		if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("timeout connecting MQTT broker %s", config.RedactURL(brokerURL))
	case <-ctx.Done():
		client.Disconnect(0)
		return nil, ctx.Err()
	}
}

func dialMQTTClient(ctx context.Context, brokerURL, clientID string, timeout time.Duration) (mqtt.Client, error) {
	return dialMQTTClientWithConfig(ctx, wsCfg, brokerURL, clientID, timeout)
}

func openOperatorStoreForConfig(ctx context.Context, cfg config.WorkstationConfig) (*db.PostgresStore, error) {
	actor := fmt.Sprintf("%s-cli:%s", cfg.Brand.CLIName, cfg.Operator)
	return db.NewPostgresStoreWithOptions(ctx, cfg.DatabaseURL, db.StoreOptions{
		AppActor:   actor,
		MaxConns:   6,
		MinConns:   1,
		MathSchema: cfg.Brand.MathSchema,
	})
}

func openOperatorStore(ctx context.Context) (*db.PostgresStore, error) {
	return openOperatorStoreForConfig(ctx, wsCfg)
}

func openReadOnlyStoreForConfig(ctx context.Context, cfg config.WorkstationConfig, actor string) (*db.ReadOnlyAgentStore, error) {
	return db.NewReadOnlyAgentStore(ctx, cfg.ReadOnlyDatabaseURL, actor, db.ReadOnlyOptions{
		AllowSuperuser: cfg.AllowSuperuserReadOnly,
		MathSchema:     cfg.Brand.MathSchema,
		EnvPrefix:      cfg.Brand.EnvPrefix,
	})
}

func openReadStoreForConfig(ctx context.Context, cfg config.WorkstationConfig, actor string) (db.AgentSafeDB, error) {
	if strings.TrimSpace(cfg.ReadOnlyDatabaseURL) != "" {
		return openReadOnlyStoreForConfig(ctx, cfg, actor)
	}
	return openOperatorStoreForConfig(ctx, cfg)
}

func openReadOnlyStore(ctx context.Context, actor string) (*db.ReadOnlyAgentStore, error) {
	return openReadOnlyStoreForConfig(ctx, wsCfg, actor)
}

func publishRunnerRequestWithConfig(ctx context.Context, cfg config.WorkstationConfig, brokerURL string, req core.RunnerRequestCTD) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}

	clientID := fmt.Sprintf("%s-req-%d", cfg.Brand.CLIName, time.Now().UnixNano())
	client, err := dialMQTTClientWithConfig(ctx, cfg, brokerURL, clientID, 4*time.Second)
	if err != nil {
		return err
	}
	defer client.Disconnect(200)

	reqTopic := cfg.Brand.RunnerRequestTopic()
	tok := client.Publish(reqTopic, 1, false, raw)
	pubTimeout := 4 * time.Second
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	timer := time.NewTimer(pubTimeout)
	defer timer.Stop()

	select {
	case <-tok.Done():
		if tok.Error() != nil {
			if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
				return ctxErr
			}
			return tok.Error()
		}
		return waitForContextDeadlineErr(ctx)
	case <-timer.C:
		if ctxErr := waitForContextDeadlineErr(ctx); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("timeout publishing RunnerRequestCTD to %s", reqTopic)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func publishRunnerRequest(ctx context.Context, brokerURL string, req core.RunnerRequestCTD) error {
	return publishRunnerRequestWithConfig(ctx, wsCfg, brokerURL, req)
}

func launchDashboardWithConfig(parentCtx context.Context, cfg config.WorkstationConfig, initialTab tui.TabID, focusReq, faultContext, modeOverride string) error {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, 6*time.Second)
	roStore, err := openReadOnlyStoreForConfig(ctx, cfg, strings.ToLower(cfg.Brand.AgentName))
	cancel()
	if err != nil {
		return fmt.Errorf("connect read-only CTD Ledger: %w", err)
	}
	defer roStore.Close()

	permCfg := cfg.Permissions
	if strings.TrimSpace(modeOverride) != "" {
		permCfg.Mode = modeOverride
	}

	prompter, approvalInbox, closePrompter := tui.NewManagedChannelPrompter()
	defer closePrompter()
	brokerURL := resolveBrokerURLForConfig(cfg)
	dispatcher := func(dispCtx context.Context, req core.RunnerRequestCTD) error {
		return publishRunnerRequestWithConfig(dispCtx, cfg, brokerURL, req)
	}

	careSvc := pigeoncare.NewService(pigeoncare.Options{
		Store:      roStore,
		LLMConfig:  cfg.LLM,
		Permission: permCfg,
		Prompter:   prompter,
		Dispatcher: dispatcher,
		Brand:      cfg.Brand,
	})

	model := tui.NewDashboardModel(tui.DashboardOptions{
		Context:       parentCtx,
		Config:        cfg,
		Store:         roStore,
		Care:          careSvc,
		Dispatcher:    dispatcher,
		InitialTab:    initialTab,
		FocusRequest:  focusReq,
		FaultContext:  faultContext,
		ApprovalInbox: approvalInbox,
	})

	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithContext(parentCtx))
	_, runErr := p.Run()
	return runErr
}

func launchDashboard(ctx context.Context, initialTab tui.TabID, focusReq, faultContext, modeOverride string) error {
	return launchDashboardWithConfig(ctx, wsCfg, initialTab, focusReq, faultContext, modeOverride)
}

func isContextAwareCommand(src *cobra.Command) bool {
	switch src {
	case rootCmd, tuiCmd,
		connectCmd, disconnectCmd, meshCmd, meshListCmd, meshCurrentCmd,
		careCmd,
		coopCmd, coopStatusCmd, coopMigrateCmd, coopProvisionRolesCmd, coopActivityCmd,
		listCmd,
		mathCmd, mathListCmd, mathInspectCmd, mathCreateCmd,
		nodeCmd,
		psCmd,
		runCmd,
		runnerCmd, runnerBenchmarkCmd, runnerAbortCmd,
		tagCmd:
		return true
	default:
		return false
	}
}

func cloneFlagInto(dst *pflag.FlagSet, f *pflag.Flag) {
	if f == nil || dst.Lookup(f.Name) != nil {
		return
	}
	switch f.Value.Type() {
	case "bool":
		def, _ := strconv.ParseBool(f.DefValue)
		dst.BoolP(f.Name, f.Shorthand, def, f.Usage)
	case "string":
		dst.StringP(f.Name, f.Shorthand, f.DefValue, f.Usage)
	case "int":
		def, _ := strconv.Atoi(f.DefValue)
		dst.IntP(f.Name, f.Shorthand, def, f.Usage)
	case "stringArray":
		var def []string
		if trimmed := strings.TrimSpace(f.DefValue); trimmed != "" && trimmed != "[]" {
			trimmed = strings.TrimPrefix(strings.TrimSuffix(trimmed, "]"), "[")
			if trimmed != "" {
				def = strings.Split(trimmed, ",")
			}
		}
		dst.StringArrayP(f.Name, f.Shorthand, def, f.Usage)
	default:
		dst.StringP(f.Name, f.Shorthand, f.DefValue, f.Usage)
	}
	if nf := dst.Lookup(f.Name); nf != nil {
		nf.NoOptDefVal = f.NoOptDefVal
		nf.Deprecated = f.Deprecated
		nf.Hidden = f.Hidden
		nf.ShorthandDeprecated = f.ShorthandDeprecated
		nf.Annotations = maps.Clone(f.Annotations)
	}
}

func syncFlagsToTemplate(clonedFS, templateFS *pflag.FlagSet) func() {
	type savedFlag struct {
		flag      *pflag.Flag
		val       string
		sliceVal  []string
		hasSlice  bool
		changed   bool
	}
	var saved []savedFlag
	clonedFS.VisitAll(func(cf *pflag.Flag) {
		tf := templateFS.Lookup(cf.Name)
		if tf == nil {
			return
		}
		entry := savedFlag{
			flag:    tf,
			val:     tf.Value.String(),
			changed: tf.Changed,
		}
		if tsa, ok := tf.Value.(pflag.SliceValue); ok {
			entry.hasSlice = true
			entry.sliceVal = slices.Clone(tsa.GetSlice())
		}
		saved = append(saved, entry)

		if cf.Changed {
			if csa, ok := cf.Value.(pflag.SliceValue); ok {
				if tsa, tok := tf.Value.(pflag.SliceValue); tok {
					_ = tsa.Replace(csa.GetSlice())
				}
			} else {
				_ = tf.Value.Set(cf.Value.String())
			}
			tf.Changed = true
		} else {
			if tsa, ok := tf.Value.(pflag.SliceValue); ok {
				_ = tsa.Replace(nil)
			} else {
				_ = tf.Value.Set(tf.DefValue)
			}
			tf.Changed = false
		}
	})
	return func() {
		for _, s := range saved {
			if s.hasSlice {
				if tsa, ok := s.flag.Value.(pflag.SliceValue); ok {
					_ = tsa.Replace(s.sliceVal)
				}
			} else {
				_ = s.flag.Value.Set(s.val)
			}
			s.flag.Changed = s.changed
		}
	}
}

func wrapLegacyRunE(src *cobra.Command, state *cliState) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		templateMu.Lock()
		defer templateMu.Unlock()

		prevCfg := wsCfg
		wsCfg = state.wsCfg
		restoreLocal := syncFlagsToTemplate(cmd.LocalNonPersistentFlags(), src.LocalNonPersistentFlags())
		defer func() {
			restoreLocal()
			wsCfg = prevCfg
		}()
		return src.RunE(cmd, args)
	}
}

func wrapLegacyRun(src *cobra.Command, state *cliState) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		templateMu.Lock()
		defer templateMu.Unlock()

		prevCfg := wsCfg
		wsCfg = state.wsCfg
		restoreLocal := syncFlagsToTemplate(cmd.LocalNonPersistentFlags(), src.LocalNonPersistentFlags())
		defer func() {
			restoreLocal()
			wsCfg = prevCfg
		}()
		src.Run(cmd, args)
	}
}

func cloneCommandTree(src *cobra.Command, state *cliState, isRoot bool) *cobra.Command {
	dst := &cobra.Command{
		Use:                    src.Use,
		Aliases:                slices.Clone(src.Aliases),
		SuggestFor:             slices.Clone(src.SuggestFor),
		Short:                  src.Short,
		Long:                   src.Long,
		Example:                src.Example,
		ValidArgs:              slices.Clone(src.ValidArgs),
		ValidArgsFunction:      src.ValidArgsFunction,
		Args:                   src.Args,
		ArgAliases:             slices.Clone(src.ArgAliases),
		BashCompletionFunction: src.BashCompletionFunction,
		Deprecated:             src.Deprecated,
		Annotations:            maps.Clone(src.Annotations),
		Version:                src.Version,
		Hidden:                 src.Hidden,
		SilenceErrors:          src.SilenceErrors,
		SilenceUsage:           src.SilenceUsage,
		DisableFlagsInUseLine:  src.DisableFlagsInUseLine,
		DisableAutoGenTag:      src.DisableAutoGenTag,
		DisableFlagParsing:     src.DisableFlagParsing,
		DisableSuggestions:     src.DisableSuggestions,
		TraverseChildren:       src.TraverseChildren,
		FParseErrWhitelist:     src.FParseErrWhitelist,
		CompletionOptions:      src.CompletionOptions,
	}

	if isRoot {
		dst.Use = state.brand.CLIName
		dst.Short = fmt.Sprintf("%s: The %s Control Plane CLI & TUI", state.brand.CLIName, state.brand.ProductName)
		dst.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
			cfgPath, meshName := persistentConfigFlags(cmd)
			loaded, err := loadWorkstationConfigForBrand(cfgPath, meshName, state.baseBrand, isMeshManagementCommand(cmd))
			if err != nil {
				return err
			}
			state.brand = loaded.Brand
			state.wsCfg = loaded
			cmd.SetContext(withCLIState(cmd.Context(), state))
			return nil
		}
		dst.Run = src.Run
	} else if isContextAwareCommand(src) {
		dst.Run = src.Run
		dst.RunE = src.RunE
	} else {
		if src.RunE != nil {
			dst.RunE = wrapLegacyRunE(src, state)
		} else if src.Run != nil {
			dst.Run = wrapLegacyRun(src, state)
		}
	}

	src.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		cloneFlagInto(dst.PersistentFlags(), f)
	})
	src.LocalNonPersistentFlags().VisitAll(func(f *pflag.Flag) {
		cloneFlagInto(dst.Flags(), f)
	})

	for _, child := range src.Commands() {
		dst.AddCommand(cloneCommandTree(child, state, false))
	}
	return dst
}

// NewRootCommand constructs an isolated, per-invocation Cobra command tree customized with spec
// without mutating package-global command trees or process-global brand state.
func NewRootCommand(spec brand.Spec) *cobra.Command {
	b := spec.MergeWithDefaults()
	cfg := config.DefaultWorkstationConfig()
	cfg.Brand = b
	state := &cliState{
		baseBrand: b,
		brand:     b,
		wsCfg:     cfg,
	}

	templateMu.Lock()
	defer templateMu.Unlock()
	return cloneCommandTree(rootCmd, state, true)
}

// RootCommand exposes an isolated default Cobra command tree for SDK embedding.
func RootCommand() *cobra.Command {
	return NewRootCommand(brand.Default())
}

// ExecuteWithBrand runs an isolated CLI command tree customized with spec.
func ExecuteWithBrand(spec brand.Spec) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := NewRootCommand(spec).ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		stop()
		os.Exit(1)
	}
}

// Execute is called by main.go to bootstrap the CLI.
func Execute() {
	ExecuteWithBrand(brand.Default())
}
