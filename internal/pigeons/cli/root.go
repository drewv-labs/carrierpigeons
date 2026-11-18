package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/drewv-labs/carrier-pigeons/internal/pigeoncare"
	"github.com/drewv-labs/carrier-pigeons/internal/pigeons/tui"
	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/config"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
)

var (
	cfgFilePath string
	wsCfg       = config.DefaultWorkstationConfig()
)

var rootCmd = &cobra.Command{
	Use:   "pigeons",
	Short: "CarrierPigeons Control Plane CLI & TUI",
	Long:  "Manage heterogeneous telemetry meshes (RISC-V, ARM64, x86), node tags, remote Runners, PigeonMath views, and PigeonCare diagnostics.",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		loaded, err := config.LoadWorkstationConfig(cfgFilePath)
		if err != nil {
			return err
		}
		wsCfg = loaded
		brand.SetActive(wsCfg.Brand)
		return nil
	},
	Run: func(cmd *cobra.Command, _ []string) {
		_ = cmd.Help()
	},
}

var tuiCmd = &cobra.Command{
	Use:   "tui",
	Short: "Launch the interactive 4-tab Control Plane TUI (Overview, RunnerManager, PigeonMath, PigeonCare)",
	RunE: func(_ *cobra.Command, _ []string) error {
		return launchDashboard(tui.TabOverview, "", "", "")
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFilePath, "config", "", "Path to pigeons.toml")
	rootCmd.AddCommand(tuiCmd)
}

func resolveBrokerURL() string {
	if strings.TrimSpace(wsCfg.BrokerURL) != "" {
		return wsCfg.BrokerURL
	}
	for _, key := range []string{"PIGEONS_BROKER_URL", "PIGEONCOOP_BROKER_URL", "MQTT_BROKER_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return "tcp://localhost:1883"
}

func openOperatorStore(ctx context.Context) (*db.PostgresStore, error) {
	actor := fmt.Sprintf("%s-cli:%s", wsCfg.Brand.CLIName, wsCfg.Operator)
	return db.NewPostgresStoreWithOptions(ctx, wsCfg.DatabaseURL, db.StoreOptions{
		AppActor: actor,
		MaxConns: 6,
		MinConns: 1,
	})
}

func publishRunnerRequest(brokerURL string, req core.RunnerRequestCTD) error {
	if err := req.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}

	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(fmt.Sprintf("%s-req-%d", wsCfg.Brand.CLIName, time.Now().UnixNano())).
		SetConnectTimeout(4 * time.Second)

	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.WaitTimeout(4*time.Second) && tok.Error() != nil {
		return fmt.Errorf("connect MQTT broker %s: %w", brokerURL, tok.Error())
	}
	defer client.Disconnect(200)

	tok := client.Publish(core.RunnerRequestTopic(), 1, false, raw)
	if !tok.WaitTimeout(4 * time.Second) {
		return fmt.Errorf("timeout publishing RunnerRequestCTD to %s", core.RunnerRequestTopic())
	}
	return tok.Error()
}

func launchDashboard(initialTab tui.TabID, focusReq, faultContext, modeOverride string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	roStore, err := db.NewReadOnlyAgentStore(ctx, wsCfg.ReadOnlyDatabaseURL, strings.ToLower(wsCfg.Brand.AgentName))
	cancel()
	if err != nil {
		return fmt.Errorf("connect read-only CTD Ledger: %w", err)
	}
	defer roStore.Close()

	permCfg := wsCfg.Permissions
	if strings.TrimSpace(modeOverride) != "" {
		permCfg.Mode = modeOverride
	}

	prompter, approvalInbox := tui.NewChannelPrompter()
	brokerURL := resolveBrokerURL()
	dispatcher := func(_ context.Context, req core.RunnerRequestCTD) error {
		return publishRunnerRequest(brokerURL, req)
	}

	careSvc := pigeoncare.NewService(pigeoncare.Options{
		Store:      roStore,
		LLMConfig:  wsCfg.LLM,
		Permission: permCfg,
		Prompter:   prompter,
		Dispatcher: dispatcher,
		Brand:      wsCfg.Brand,
	})

	model := tui.NewDashboardModel(tui.DashboardOptions{
		Config:        wsCfg,
		Store:         roStore,
		Care:          careSvc,
		Dispatcher:    dispatcher,
		InitialTab:    initialTab,
		FocusRequest:  focusReq,
		FaultContext:  faultContext,
		ApprovalInbox: approvalInbox,
	})

	p := tea.NewProgram(model, tea.WithAltScreen())
	_, runErr := p.Run()
	return runErr
}

// ApplyBrand re-skins the root CLI command names and descriptions for whitelabeled SDK binaries.
func ApplyBrand(spec brand.Spec) {
	b := spec.MergeWithDefaults()
	brand.SetActive(b)
	wsCfg.Brand = b
	rootCmd.Use = b.CLIName
	rootCmd.Short = fmt.Sprintf("%s: The %s Control Plane CLI & TUI", b.CLIName, b.ProductName)
}

// RootCommand exposes the underlying Cobra command tree for SDK embedding.
func RootCommand() *cobra.Command {
	return rootCmd
}

// Execute is called by main.go to bootstrap the CLI.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
