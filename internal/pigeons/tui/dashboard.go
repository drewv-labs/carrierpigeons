package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncare"
	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

// TabID identifies the active view tab inside the Pigeons Control Plane TUI.
type TabID int

const (
	TabOverview TabID = iota
	TabRunnerManager
	TabPigeonMath
	TabPigeonCare
)

// RunnerRecord represents a recent runner dispatch or node bookend displayed in RunnerManager.
type RunnerRecord struct {
	Timestamp time.Time
	NodeID    string
	Targets   []string
	Runner    string
	Status    string
	RequestID string
	Detail    string
}

func truncateRunes(s string, maxRunes int, ellipsis string) string {
	if maxRunes <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	ellRunes := []rune(ellipsis)
	if len(ellRunes) >= maxRunes {
		return string(r[:maxRunes])
	}
	return string(r[:maxRunes-len(ellRunes)]) + ellipsis
}

// DashboardOptions configures the multi-tab Pigeons TUI control plane.
type DashboardOptions struct {
	Config        config.WorkstationConfig
	Store         db.AgentSafeDB
	Care          *pigeoncare.Service
	Dispatcher    pigeoncare.RequestDispatcher
	InitialTab    TabID
	FocusRequest  string
	FaultContext  string
	ApprovalInbox <-chan ApprovalPromptMsg
}

// ApprovalPromptMsg bridges an async PigeonCare PolicyGate prompt into the Bubble Tea event loop.
type ApprovalPromptMsg struct {
	Proposal pigeoncare.ActionProposal
	ReplyCh  chan<- pigeoncare.ConfirmationDecision
}

type overviewTickMsg time.Time
type mathTickMsg time.Time

type overviewDataMsg struct {
	nodes   []db.NodeSummary
	runners []RunnerRecord
	err     error
}

type mathDataMsg struct {
	snap *pigeonmath.DashboardSnapshot
	err  error
}

type abortResultMsg struct {
	status string
}

// DashboardModel is the whitelabelable 4-tab Bubble Tea TUI for Pigeons.
type DashboardModel struct {
	cfg           config.WorkstationConfig
	brand         brand.Spec
	store         db.AgentSafeDB
	care          *pigeoncare.Service
	math          *pigeonmath.Engine
	dispatcher    pigeoncare.RequestDispatcher
	approvalInbox <-chan ApprovalPromptMsg

	activeTab    TabID
	width        int
	height       int
	focusRequest string
	statusBanner string

	// Tab 1 & 2 state
	nodes        []db.NodeSummary
	runners      []RunnerRecord
	runnerCursor int

	// Tab 3 (PigeonMath) state
	mathSnap *pigeonmath.DashboardSnapshot
	mathErr  error

	// Tab 4 (PigeonCare) state
	chatHistory     []llm.Message
	uiHistory       []string
	userInput       string
	currentResponse *strings.Builder
	streamCancel    context.CancelFunc
	waiting         bool
	pendingApproval *ApprovalPromptMsg

	quitting bool
}

// NewChannelPrompter creates an async PolicyGate Prompter paired with a channel consumed by DashboardModel.
func NewChannelPrompter() (pigeoncare.Prompter, <-chan ApprovalPromptMsg) {
	ch := make(chan ApprovalPromptMsg, 4)
	prompter := func(ctx context.Context, proposal pigeoncare.ActionProposal) (pigeoncare.ConfirmationDecision, error) {
		replyCh := make(chan pigeoncare.ConfirmationDecision, 1)
		select {
		case ch <- ApprovalPromptMsg{Proposal: proposal, ReplyCh: replyCh}:
		case <-ctx.Done():
			return pigeoncare.DecisionDeny, ctx.Err()
		}
		select {
		case dec := <-replyCh:
			return dec, nil
		case <-ctx.Done():
			return pigeoncare.DecisionDeny, ctx.Err()
		}
	}
	return prompter, ch
}

// NewDashboardModel constructs the unified Pigeons multi-tab TUI.
func NewDashboardModel(opts DashboardOptions) DashboardModel {
	bSpec := opts.Config.Brand.MergeWithDefaults()
	var mathEng *pigeonmath.Engine
	if opts.Care != nil {
		mathEng = opts.Care.MathEngine()
	} else if opts.Store != nil {
		mathEng = pigeonmath.NewEngineWithSchema(opts.Store, bSpec.MathSchema)
	}

	agentGreeting := fmt.Sprintf("%s: Connected to %s read-only CTD Ledger. Ask about thermals, runners, or %s views.",
		bSpec.AgentName, bSpec.ProductName, bSpec.MathName)
	uiHist := []string{agentStyle.Render(agentGreeting)}
	var chatHist []llm.Message
	if fault := strings.TrimSpace(opts.FaultContext); fault != "" {
		uiHist = append([]string{systemStyle.Render("Fault Context:\n" + fault)}, uiHist...)
		chatHist = append(chatHist,
			llm.Message{Role: "user", Content: "Operator-supplied fault context for triage:\n" + fault},
			llm.Message{Role: "assistant", Content: "Fault context loaded into session memory. Ready to correlate against CTD Ledger telemetry."},
		)
	}

	banner := "Ready"
	if opts.FocusRequest != "" {
		banner = "Tracking RunnerRequestCTD: " + opts.FocusRequest
	}

	return DashboardModel{
		cfg:           opts.Config,
		brand:         bSpec,
		store:         opts.Store,
		care:          opts.Care,
		math:          mathEng,
		dispatcher:    opts.Dispatcher,
		approvalInbox: opts.ApprovalInbox,
		activeTab:     opts.InitialTab,
		width:         110,
		height:        32,
		focusRequest:  opts.FocusRequest,
		statusBanner:  banner,
		chatHistory:   chatHist,
		uiHistory:     uiHist,
	}
}

func (m DashboardModel) Init() tea.Cmd {
	cmds := []tea.Cmd{
		m.fetchOverviewCmd(),
		m.fetchMathCmd(),
		m.scheduleOverviewTick(),
		m.scheduleMathTick(),
	}
	if m.approvalInbox != nil {
		cmds = append(cmds, waitForApprovalCmd(m.approvalInbox))
	}
	return tea.Batch(cmds...)
}

func waitForApprovalCmd(inbox <-chan ApprovalPromptMsg) tea.Cmd {
	return func() tea.Msg {
		req, ok := <-inbox
		if !ok {
			return nil
		}
		return req
	}
}

func (m DashboardModel) scheduleOverviewTick() tea.Cmd {
	return tea.Tick(3*time.Second, func(t time.Time) tea.Msg {
		return overviewTickMsg(t)
	})
}

func (m DashboardModel) scheduleMathTick() tea.Cmd {
	interval := m.cfg.Math.ParsedRefreshInterval()
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return tea.Tick(interval, func(t time.Time) tea.Msg {
		return mathTickMsg(t)
	})
}

func extractTargetNodes(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := strings.TrimSpace(fmt.Sprint(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func (m DashboardModel) fetchOverviewCmd() tea.Cmd {
	store := m.store
	return func() tea.Msg {
		if store == nil {
			return overviewDataMsg{}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		nodes, err := store.GetClusterStatus(ctx, config.DefaultHealthyWindow)
		if err != nil {
			return overviewDataMsg{err: err}
		}

		rows, rErr := store.QueryReadOnlyMaps(ctx, `
			SELECT node_id, component, status, metrics, params, event_timestamp
			FROM telemetric_ledger
			WHERE pillar = 'runner'
			ORDER BY event_timestamp DESC
			LIMIT 25
		`)
		var recs []RunnerRecord
		if rErr == nil {
			for _, r := range rows {
				ts, _ := r["event_timestamp"].(time.Time)
				comp := fmt.Sprint(r["component"])
				runnerName := strings.TrimPrefix(comp, "runner:")
				runnerName = strings.TrimPrefix(runnerName, "runner-router:")
				reqID := ""
				if pMap, ok := r["params"].(map[string]any); ok {
					if idVal, exists := pMap["request_id"]; exists {
						reqID = fmt.Sprint(idVal)
					}
				}
				detail := ""
				var targets []string
				if mMap, ok := r["metrics"].(map[string]any); ok {
					targets = extractTargetNodes(mMap["targets"])
					if out, hasOut := mMap["stdout"].(string); hasOut && out != "" {
						line := strings.Split(out, "\n")[0]
						detail = truncateRunes(line, 48, "...")
					} else {
						raw, _ := json.Marshal(mMap)
						detail = truncateRunes(string(raw), 52, "...")
					}
				}
				recs = append(recs, RunnerRecord{
					Timestamp: ts,
					NodeID:    fmt.Sprint(r["node_id"]),
					Targets:   targets,
					Runner:    runnerName,
					Status:    fmt.Sprint(r["status"]),
					RequestID: reqID,
					Detail:    detail,
				})
			}
		}
		return overviewDataMsg{nodes: nodes, runners: recs}
	}
}

func (m DashboardModel) fetchMathCmd() tea.Cmd {
	eng := m.math
	return func() tea.Msg {
		if eng == nil {
			return mathDataMsg{}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		snap, err := eng.FetchDashboardSnapshot(ctx, 30)
		return mathDataMsg{snap: snap, err: err}
	}
}

func (m DashboardModel) dispatchAbortCmd(rec RunnerRecord) tea.Cmd {
	disp := m.dispatcher
	operator := m.cfg.Operator
	return func() tea.Msg {
		if disp == nil {
			return abortResultMsg{status: "MQTT dispatcher unavailable"}
		}
		targetNodes := rec.Targets
		if len(targetNodes) == 0 && strings.TrimSpace(rec.NodeID) != "" {
			targetNodes = []string{rec.NodeID}
		}
		if len(targetNodes) == 0 {
			return abortResultMsg{status: "No active target nodes on selected row"}
		}
		req := core.RunnerRequestCTD{
			RequestID: fmt.Sprintf("abort-%d", time.Now().UTC().UnixNano()),
			Action:    "abort",
			Runner:    rec.Runner,
			Selectors: core.RunnerSelector{
				Nodes: targetNodes,
			},
			RequestedBy: operator,
			RequestedAt: time.Now().UTC(),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if err := disp(ctx, req); err != nil {
			return abortResultMsg{status: fmt.Sprintf("Abort failed: %v", err)}
		}
		return abortResultMsg{status: fmt.Sprintf("Sent abort for %s -> %v", rec.Runner, targetNodes)}
	}
}

func (m DashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case overviewTickMsg:
		return m, tea.Batch(m.fetchOverviewCmd(), m.scheduleOverviewTick())

	case mathTickMsg:
		return m, tea.Batch(m.fetchMathCmd(), m.scheduleMathTick())

	case overviewDataMsg:
		if msg.err != nil {
			m.statusBanner = "Ledger warning: " + msg.err.Error()
		} else {
			m.nodes = msg.nodes
			m.runners = msg.runners
			if m.runnerCursor >= len(m.runners) {
				m.runnerCursor = max(0, len(m.runners)-1)
			}
		}
		return m, nil

	case mathDataMsg:
		m.mathErr = msg.err
		if msg.snap != nil {
			m.mathSnap = msg.snap
		}
		return m, nil

	case abortResultMsg:
		m.statusBanner = msg.status
		return m, m.fetchOverviewCmd()

	case ApprovalPromptMsg:
		m.pendingApproval = &msg
		m.activeTab = TabPigeonCare
		return m, nil

	case streamErrMsg:
		m.waiting = false
		if m.streamCancel != nil {
			m.streamCancel()
			m.streamCancel = nil
		}
		m.currentResponse = nil
		m.uiHistory[len(m.uiHistory)-1] = agentStyle.Render(m.brand.AgentName + " Error: " + msg.err.Error())
		return m, nil

	case streamTokenMsg:
		if m.currentResponse == nil {
			m.currentResponse = &strings.Builder{}
		}
		m.currentResponse.WriteString(msg.token)
		return m, waitForToken(msg.ch)

	case streamDoneMsg:
		m.waiting = false
		if m.streamCancel != nil {
			m.streamCancel()
			m.streamCancel = nil
		}
		reply := ""
		if m.currentResponse != nil {
			reply = m.currentResponse.String()
			m.currentResponse = nil
		}
		m.uiHistory[len(m.uiHistory)-1] = agentStyle.Render(m.brand.AgentName + ": " + reply)
		m.chatHistory = append(m.chatHistory, llm.Message{Role: "assistant", Content: reply})
		return m, tea.Batch(m.fetchOverviewCmd(), m.fetchMathCmd())

	case tea.KeyMsg:
		// Global quit on Ctrl+C
		if msg.Type == tea.KeyCtrlC {
			if m.streamCancel != nil {
				m.streamCancel()
			}
			m.quitting = true
			return m, tea.Quit
		}

		// Handle interactive Confirmation Modal first when pending!
		if m.pendingApproval != nil {
			key := strings.ToLower(msg.String())
			var decision pigeoncare.ConfirmationDecision
			switch key {
			case "1", "y":
				decision = pigeoncare.DecisionAllow
			case "2", "a":
				decision = pigeoncare.DecisionAlwaysAllow
			case "3", "n", "esc":
				decision = pigeoncare.DecisionDeny
			default:
				return m, nil
			}
			m.pendingApproval.ReplyCh <- decision
			m.statusBanner = fmt.Sprintf("Confirmation decision: %s (%s)", decision, m.pendingApproval.Proposal.Title)
			m.pendingApproval = nil
			if m.approvalInbox != nil {
				return m, waitForApprovalCmd(m.approvalInbox)
			}
			return m, nil
		}

		// Global Tab switching via Tab / Shift+Tab
		switch msg.Type {
		case tea.KeyTab:
			m.activeTab = (m.activeTab + 1) % 4
			return m, nil
		case tea.KeyShiftTab:
			m.activeTab = (m.activeTab + 3) % 4
			return m, nil
		}

		// Mode cycling in PigeonCare (or globally) via Ctrl+P / Ctrl+O
		if msg.String() == "ctrl+p" && m.care != nil {
			newMode := m.care.PolicyGate().CycleMode()
			m.statusBanner = fmt.Sprintf("%s permission mode -> %s", m.brand.AgentName, newMode.DisplayLabel())
			return m, nil
		}

		// Tab-specific key handling
		if m.activeTab == TabPigeonCare {
			return m.updateCareKeys(msg)
		}

		// Non-chat tabs allow quick number navigation & q/esc to quit
		switch msg.String() {
		case "q", "esc":
			m.quitting = true
			return m, tea.Quit
		case "1":
			m.activeTab = TabOverview
			return m, nil
		case "2":
			m.activeTab = TabRunnerManager
			return m, nil
		case "3":
			m.activeTab = TabPigeonMath
			return m, nil
		case "4":
			m.activeTab = TabPigeonCare
			return m, nil
		case "r":
			m.statusBanner = "Refreshing CTD telemetry & rollups..."
			return m, tea.Batch(m.fetchOverviewCmd(), m.fetchMathCmd())
		}

		if m.activeTab == TabRunnerManager {
			switch msg.String() {
			case "up", "k":
				if m.runnerCursor > 0 {
					m.runnerCursor--
				}
			case "down", "j":
				if m.runnerCursor < len(m.runners)-1 {
					m.runnerCursor++
				}
			case "x":
				if len(m.runners) > 0 && m.runnerCursor < len(m.runners) {
					return m, m.dispatchAbortCmd(m.runners[m.runnerCursor])
				}
			}
		}
	}

	return m, nil
}

func (m DashboardModel) updateCareKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.Type == tea.KeyEsc {
		if m.waiting && m.streamCancel != nil {
			m.streamCancel()
			m.streamCancel = nil
			m.waiting = false
			m.statusBanner = "Cancelled active stream."
			return m, nil
		}
		m.quitting = true
		return m, tea.Quit
	}

	if m.waiting {
		return m, nil
	}

	switch msg.Type {
	case tea.KeyEnter:
		userText := strings.TrimSpace(m.userInput)
		if userText == "" {
			return m, nil
		}
		if m.care == nil {
			m.uiHistory = append(m.uiHistory, agentStyle.Render(m.brand.AgentName+": LLM service is not configured."))
			m.userInput = ""
			return m, nil
		}

		m.chatHistory = append(m.chatHistory, llm.Message{Role: "user", Content: userText})
		m.uiHistory = append(m.uiHistory, userStyle.Render("Operator: "+userText))
		m.uiHistory = append(m.uiHistory, agentStyle.Render(m.brand.AgentName+": "))
		m.userInput = ""
		m.currentResponse = &strings.Builder{}
		m.waiting = true

		ctx, cancel := context.WithCancel(context.Background())
		m.streamCancel = cancel
		historySnap := slices.Clone(m.chatHistory)
		careSvc := m.care

		return m, func() tea.Msg {
			ch, err := careSvc.StreamChat(ctx, historySnap)
			if err != nil {
				return streamErrMsg{err}
			}
			return waitForToken(ch)()
		}

	case tea.KeyBackspace, tea.KeyDelete:
		runes := []rune(m.userInput)
		if len(runes) > 0 {
			m.userInput = string(runes[:len(runes)-1])
		}
	case tea.KeySpace:
		m.userInput += " "
	case tea.KeyRunes:
		m.userInput += string(msg.Runes)
	}
	return m, nil
}

func (m DashboardModel) View() string {
	if m.quitting {
		return fmt.Sprintf("%s control plane closed.\n", m.brand.ProductName)
	}

	accent := lipgloss.Color(m.brand.AccentColor)
	secondary := lipgloss.Color(m.brand.SecondaryColor)
	muted := lipgloss.Color(m.brand.MutedColor)

	activeTabStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0")).Background(accent).Padding(0, 2)
	inactiveTabStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Background(lipgloss.Color("236")).Padding(0, 2)
	headerStyle := lipgloss.NewStyle().Bold(true).Foreground(secondary)
	boxStyle := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(muted).Padding(0, 1)

	tabs := []struct {
		id    TabID
		label string
	}{
		{TabOverview, "1:Fleet Overview"},
		{TabRunnerManager, "2:RunnerManager"},
		{TabPigeonMath, "3:" + m.brand.MathName},
		{TabPigeonCare, "4:" + m.brand.AgentName},
	}

	var renderedTabs []string
	for _, t := range tabs {
		if t.id == m.activeTab {
			renderedTabs = append(renderedTabs, activeTabStyle.Render(t.label))
		} else {
			renderedTabs = append(renderedTabs, inactiveTabStyle.Render(t.label))
		}
	}

	modeBadge := ""
	if m.care != nil {
		mode := m.care.PolicyGate().Mode()
		badgeColor := accent
		if mode == pigeoncare.ModeYOLO {
			badgeColor = lipgloss.Color("196")
		} else if mode == pigeoncare.ModeAutoEdit {
			badgeColor = lipgloss.Color("214")
		}
		modeBadge = lipgloss.NewStyle().Bold(true).Foreground(badgeColor).
			Render(fmt.Sprintf(" [%s Mode: %s | Ctrl+P cycle]", m.brand.AgentName, mode.DisplayLabel()))
	}

	var sb strings.Builder
	sb.WriteString(headerStyle.Render(m.brand.ProductName+" Control Plane") + modeBadge + "\n")
	sb.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, renderedTabs...) + "\n\n")

	switch m.activeTab {
	case TabOverview:
		sb.WriteString(m.renderOverview(boxStyle))
	case TabRunnerManager:
		sb.WriteString(m.renderRunnerManager(boxStyle, accent))
	case TabPigeonMath:
		sb.WriteString(m.renderPigeonMath(boxStyle, accent, secondary))
	case TabPigeonCare:
		sb.WriteString(m.renderPigeonCare(boxStyle, accent))
	}

	footer := lipgloss.NewStyle().Foreground(muted).Render(
		fmt.Sprintf("\n[Tab] Switch Views | [r] Refresh | [Ctrl+P] Cycle Mode | [q/Ctrl+C] Quit  —  %s", m.statusBanner),
	)
	sb.WriteString(footer + "\n")
	return sb.String()
}

func (m DashboardModel) renderOverview(box lipgloss.Style) string {
	if len(m.nodes) == 0 {
		return box.Render("No nodes discovered in CTD Ledger yet. Start `pigeoncoop` or `pigeoneer` to stream telemetry.")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%-22s %-14s %-24s %-12s %-10s %-9s %-8s %s\n",
		"NODE ID", "ARCH/OS", "TAGS", "COMPONENT", "STATE", "FAN RPM", "TEMP °C", "LAST SEEN"))
	sb.WriteString(strings.Repeat("─", min(max(78, m.width-6), 118)) + "\n")

	for _, n := range m.nodes {
		archOS := strings.Trim(n.Arch+"/"+n.OS, "/")
		if archOS == "" {
			archOS = "-"
		}
		tagStr := strings.Join(n.Tags, ",")
		if tagStr == "" {
			tagStr = "-"
		}
		tagStr = truncateRunes(tagStr, 22, "...")
		fanRPM := "-"
		if v, ok := n.LatestMetrics["cpu_fan_rpm"]; ok {
			fanRPM = fmt.Sprint(v)
		}
		tempC := "-"
		if v, ok := n.LatestMetrics["cpu_temp_c"]; ok {
			tempC = fmt.Sprint(v)
		} else if v, ok := n.LatestMetrics["soc_temp_c"]; ok {
			tempC = fmt.Sprint(v)
		} else if v, ok := n.LatestMetrics["temperature_c"]; ok {
			tempC = fmt.Sprint(v)
		}
		state := "ONLINE"
		if !n.IsHealthy {
			state = strings.ToUpper(n.Status)
		}
		ago := time.Since(n.LastSeen).Round(time.Second)
		fmt.Fprintf(&sb, "%-22s %-14s %-24s %-12s %-10s %-9s %-8s %s ago\n",
			truncateRunes(n.NodeID, 22, ".."), archOS, tagStr, truncateRunes(n.Component, 12, ".."), state, fanRPM, tempC, ago)
	}
	return box.Render(sb.String())
}

func (m DashboardModel) renderRunnerManager(box lipgloss.Style, accent lipgloss.Color) string {
	var sb strings.Builder
	if m.focusRequest != "" {
		sb.WriteString(lipgloss.NewStyle().Foreground(accent).Bold(true).
			Render("Focused Request: "+m.focusRequest) + "  (Press [x] to abort selected runner, [j/k] to navigate)\n\n")
	} else {
		sb.WriteString("Runner Execution Ledger  (Press [x] to abort selected runner, [j/k] to navigate)\n\n")
	}

	if len(m.runners) == 0 {
		sb.WriteString("No runner executions recorded yet. Try: `pigeons run SyntheticComputeRnr --all`")
		return box.Render(sb.String())
	}

	sb.WriteString(fmt.Sprintf("  %-10s %-20s %-20s %-18s %-18s %s\n",
		"TIME", "NODE", "RUNNER", "STATUS", "REQUEST ID", "DETAIL"))
	sb.WriteString(strings.Repeat("─", min(max(78, m.width-6), 118)) + "\n")

	for i, r := range m.runners {
		cursor := " "
		if i == m.runnerCursor {
			cursor = "▸"
		}
		reqShort := truncateRunes(r.RequestID, 16, "")
		if reqShort == "" {
			reqShort = "-"
		}
		fmt.Fprintf(&sb, "%s %-10s %-20s %-20s %-18s %-18s %s\n",
			cursor, r.Timestamp.UTC().Format("15:04:05"), truncateRunes(r.NodeID, 20, ".."), truncateRunes(r.Runner, 20, ".."), r.Status, reqShort, r.Detail)
	}
	return box.Render(sb.String())
}

func (m DashboardModel) renderPigeonMath(box lipgloss.Style, accent, secondary lipgloss.Color) string {
	if m.mathErr != nil {
		return box.Render(fmt.Sprintf("%s query error: %v", m.brand.MathName, m.mathErr))
	}
	if m.mathSnap == nil || len(m.mathSnap.Points) == 0 {
		return box.Render(fmt.Sprintf("Loading %s 1-minute aggregated time-series...", m.brand.MathName))
	}

	pts := m.mathSnap.Points
	latest := pts[len(pts)-1]

	liveSeries := make([]float64, len(pts))
	quietSeries := make([]float64, len(pts))
	ctdTotalSeries := make([]float64, len(pts))
	obsSeries := make([]float64, len(pts))
	runSeries := make([]float64, len(pts))

	for i, p := range pts {
		liveSeries[i] = float64(p.LiveNodes)
		quietSeries[i] = float64(p.QuietNodes)
		ctdTotalSeries[i] = float64(p.TotalEvents)
		obsSeries[i] = float64(p.ObserverEvents)
		runSeries[i] = float64(p.RunnerEvents)
	}

	chartWidth := min(max(44, m.width-10), 96)

	var nBuf strings.Builder
	nBuf.WriteString(lipgloss.NewStyle().Bold(true).Foreground(accent).Render(
		fmt.Sprintf("▲ #nodes (%dm window, 1m refresh)  Total: %d | Live: %d | Quiet: %d",
			m.mathSnap.WindowMinutes, latest.TotalNodes, latest.LiveNodes, latest.QuietNodes),
	) + "\n")
	nBuf.WriteString(pigeonmath.RenderMultiLineGraph(liveSeries, chartWidth, 4, "") + "\n")
	nBuf.WriteString(fmt.Sprintf("  Quiet sparkline: %s", pigeonmath.Sparkline(quietSeries, min(40, chartWidth-18))))

	var cBuf strings.Builder
	cBuf.WriteString(lipgloss.NewStyle().Bold(true).Foreground(secondary).Render(
		fmt.Sprintf("▲ #CTD/min  Total: %d/m | Observer: %d/m | Runner: %d/m | Tracer: %d/m",
			latest.TotalEvents, latest.ObserverEvents, latest.RunnerEvents, latest.TracerEvents),
	) + "\n")
	cBuf.WriteString(pigeonmath.RenderMultiLineGraph(ctdTotalSeries, chartWidth, 4, "/m") + "\n")
	cBuf.WriteString(fmt.Sprintf("  Observer: %s   Runner: %s",
		pigeonmath.Sparkline(obsSeries, 24), pigeonmath.Sparkline(runSeries, 24)))

	var vBuf strings.Builder
	vBuf.WriteString(fmt.Sprintf("Certified %s Analytical Views (%s.pmv_*):\n", m.brand.MathName, m.brand.MathSchema))
	if len(m.mathSnap.Views) == 0 {
		vBuf.WriteString("  (none yet — run `pigeons math create <name> \"SELECT ...\"` or ask " + m.brand.AgentName + ")")
	} else {
		for idx, v := range m.mathSnap.Views {
			if idx >= 5 {
				break
			}
			fmt.Fprintf(&vBuf, "  • %s.%s [%s v%d by %s] — %s\n",
				m.brand.MathSchema, v.ViewName, strings.ToUpper(v.Status), v.Version, v.SubmittedBy, v.AgentNotes)
		}
	}

	return box.Render(nBuf.String()) + "\n" + box.Render(cBuf.String()) + "\n" + box.Render(vBuf.String())
}

func tailViewportLines(entries []string, maxLines int) []string {
	if maxLines <= 0 {
		maxLines = 18
	}
	var all []string
	for _, entry := range entries {
		all = append(all, strings.Split(entry, "\n")...)
	}
	if len(all) <= maxLines {
		return all
	}
	return all[len(all)-maxLines:]
}

func (m DashboardModel) renderPigeonCare(box lipgloss.Style, accent lipgloss.Color) string {
	var sb strings.Builder

	// Compute viewport line budget from terminal height so long streaming responses remain bounded.
	maxLines := max(12, m.height-11)
	start := max(0, len(m.uiHistory)-12)
	entries := make([]string, 0, len(m.uiHistory)-start)
	for i := start; i < len(m.uiHistory); i++ {
		if m.waiting && i == len(m.uiHistory)-1 && m.currentResponse != nil {
			entries = append(entries, agentStyle.Render(m.brand.AgentName+": "+m.currentResponse.String()))
		} else {
			entries = append(entries, m.uiHistory[i])
		}
	}
	for _, line := range tailViewportLines(entries, maxLines) {
		sb.WriteString(line + "\n")
	}

	if m.pendingApproval != nil {
		prop := m.pendingApproval.Proposal
		modalStyle := lipgloss.NewStyle().
			Border(lipgloss.DoubleBorder()).
			BorderForeground(lipgloss.Color("214")).
			Padding(0, 1)

		var mText strings.Builder
		fmt.Fprintf(&mText, "⚠ %s Action Confirmation Required: %s\n", m.brand.AgentName, prop.Title)
		if prop.Kind == pigeoncare.ActionEphemeralRunner {
			fmt.Fprintf(&mText, "Interpreter: %s | Timeout: %ds | SHA-256: %s\n",
				prop.Interpreter, prop.TimeoutSec, truncateRunes(prop.SHA256, 16, ""))
			fmt.Fprintf(&mText, "Targets: nodes=%v tags=%v\n", prop.Selectors.Nodes, prop.Selectors.Tags)
			fmt.Fprintf(&mText, "Script:\n%s\n", strings.TrimSpace(prop.ScriptBody))
		} else if prop.Kind == pigeoncare.ActionCreatePMV {
			fmt.Fprintf(&mText, "View: %s.%s\nSQL: %s\n", m.brand.MathSchema, prop.ViewName, prop.RawSQL)
		}
		mText.WriteString("\nChoose: [1 / y] Allow   [2 / a] Always Allow   [3 / n] Deny")
		sb.WriteString("\n" + modalStyle.Render(mText.String()) + "\n")
		return box.Render(sb.String())
	}

	if m.waiting {
		sb.WriteString("\n" + waitStyle.Render("Streaming diagnostic response... (Esc to cancel)"))
	} else {
		pStyle := lipgloss.NewStyle().Foreground(accent).Bold(true)
		sb.WriteString("\n" + pStyle.Render(fmt.Sprintf("Ask %s > ", m.brand.AgentName)) + m.userInput)
	}

	return box.Render(sb.String())
}
