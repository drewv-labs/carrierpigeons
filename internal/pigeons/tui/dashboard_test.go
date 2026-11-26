package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncare"
	"github.com/drewv-labs/carrierpigeons/internal/pigeonmath"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func TestTruncateRunesUTF8Safety(t *testing.T) {
	in := "αβγδεζηθικ"
	got := truncateRunes(in, 5, "…")
	if got != "αβγδ…" {
		t.Fatalf("expected 'αβγδ…', got %q", got)
	}
}

func TestDashboardFaultContextAndAbortDispatchTargets(t *testing.T) {
	var lastReq core.RunnerRequestCTD
	m := NewDashboardModel(DashboardOptions{
		Config: config.DefaultWorkstationConfig(),
		Dispatcher: func(_ context.Context, req core.RunnerRequestCTD) error {
			lastReq = req
			return nil
		},
		FaultContext: "Thermal spike detected on node edge-rv64-01",
	})

	if len(m.chatHistory) != 2 || !strings.Contains(m.chatHistory[0].Content, "edge-rv64-01") {
		t.Fatalf("expected FaultContext to seed alternating user/assistant chatHistory for LLM context, got %+v", m.chatHistory)
	}

	cmd := m.dispatchAbortCmd(RunnerRecord{
		NodeID:    "pigeoncoop",
		Targets:   []string{"edge-rv64-01", "edge-rv64-02"},
		Runner:    "SyntheticComputeRnr",
		Status:    "request-dispatched",
		RequestID: "req-1234",
	})
	msg := cmd()
	res, ok := msg.(abortResultMsg)
	if !ok {
		t.Fatalf("expected abortResultMsg, got %T", msg)
	}
	if !strings.Contains(res.status, "SyntheticComputeRnr") || !strings.Contains(res.status, "edge-rv64-01") {
		t.Fatalf("unexpected abort status message: %q", res.status)
	}
	if len(lastReq.Selectors.Nodes) != 2 || lastReq.Selectors.Nodes[0] != "edge-rv64-01" {
		t.Fatalf("expected abort dispatch to target original edge nodes, got %+v", lastReq.Selectors.Nodes)
	}
}

func TestDashboardQuitCancelsStreamOnNonChatTabs(t *testing.T) {
	tabs := []struct {
		name string
		tab  TabID
	}{
		{name: "overview", tab: TabOverview},
		{name: "runner_manager", tab: TabRunnerManager},
		{name: "pigeonmath", tab: TabPigeonMath},
	}
	keys := []struct {
		name string
		msg  tea.KeyMsg
	}{
		{name: "q", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}}},
		{name: "esc", msg: tea.KeyMsg{Type: tea.KeyEsc}},
	}

	for _, tb := range tabs {
		for _, k := range keys {
			t.Run(tb.name+"_"+k.name, func(t *testing.T) {
				m := NewDashboardModel(DashboardOptions{
					Config:     config.DefaultWorkstationConfig(),
					InitialTab: tb.tab,
				})
				ctx, cancel := context.WithCancel(context.Background())
				m.streamCancel = cancel
				m.waiting = true

				updated, cmd := m.Update(k.msg)
				if !errors.Is(ctx.Err(), context.Canceled) {
					t.Fatalf("expected streamCancel to be invoked on %s quit from %s", k.name, tb.name)
				}
				um, ok := updated.(DashboardModel)
				if !ok {
					t.Fatalf("expected DashboardModel, got %T", updated)
				}
				if !um.quitting {
					t.Fatalf("expected quitting=true after %s on %s", k.name, tb.name)
				}
				if um.streamCancel != nil {
					t.Fatalf("expected streamCancel to be cleared after quit")
				}
				if cmd == nil {
					t.Fatal("expected non-nil tea.Quit command")
				}
				if _, isQuit := cmd().(tea.QuitMsg); !isQuit {
					t.Fatalf("expected tea.QuitMsg from quit command")
				}
			})
		}
	}
}

func TestDashboardStreamGenerationGuardAfterEscCancel(t *testing.T) {
	m := NewDashboardModel(DashboardOptions{
		Config:     config.DefaultWorkstationConfig(),
		Care:       pigeoncare.NewService(pigeoncare.Options{}),
		InitialTab: TabPigeonCare,
	})

	typeText := func(model DashboardModel, text string) DashboardModel {
		for _, r := range text {
			updated, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
			model = updated.(DashboardModel)
		}
		return model
	}

	// 1. Submit first prompt -> activeStreamID becomes 1.
	m = typeText(m, "first")
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(DashboardModel)
	if !m.waiting || m.activeStreamID != 1 {
		t.Fatalf("after first prompt: waiting=%v activeStreamID=%d, want true, 1", m.waiting, m.activeStreamID)
	}
	firstCtx, firstCancel := context.WithCancel(context.Background())
	if m.streamCancel != nil {
		m.streamCancel()
	}
	m.streamCancel = firstCancel

	ch1 := make(chan string, 2)
	updated, cmd := m.Update(streamTokenMsg{streamID: 1, token: "partial ", ch: ch1})
	m = updated.(DashboardModel)
	if cmd == nil {
		t.Fatal("expected waitForToken cmd for active stream 1")
	}
	if m.currentResponse == nil || m.currentResponse.String() != "partial " {
		t.Fatalf("unexpected currentResponse for stream 1: %v", m.currentResponse)
	}

	// 2. Press Esc to cancel active stream -> activeStreamID advances to 2.
	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = updated.(DashboardModel)
	if !errors.Is(firstCtx.Err(), context.Canceled) {
		t.Fatal("expected Esc to cancel stream 1 context")
	}
	if m.waiting || m.activeStreamID != 2 {
		t.Fatalf("after Esc cancel: waiting=%v activeStreamID=%d, want false, 2", m.waiting, m.activeStreamID)
	}

	// Stale messages from stream 1 while idle must be ignored.
	historyLenBefore := len(m.chatHistory)
	uiBefore := append([]string(nil), m.uiHistory...)
	for _, stale := range []tea.Msg{
		streamTokenMsg{streamID: 1, token: "stale-idle", ch: ch1},
		streamDoneMsg{streamID: 1},
		streamErrMsg{streamID: 1, err: errors.New("stale-idle-err")},
	} {
		updated, cmd = m.Update(stale)
		m = updated.(DashboardModel)
		if cmd != nil {
			t.Fatalf("expected nil cmd for stale message %T", stale)
		}
		if m.waiting || m.currentResponse != nil {
			t.Fatalf("stale message %T mutated waiting/currentResponse", stale)
		}
		if len(m.chatHistory) != historyLenBefore || len(m.uiHistory) != len(uiBefore) {
			t.Fatalf("stale message %T mutated chat/ui history", stale)
		}
		for i := range uiBefore {
			if m.uiHistory[i] != uiBefore[i] {
				t.Fatalf("stale message %T overwrote uiHistory[%d]", stale, i)
			}
		}
	}

	// 3. Submit second prompt -> activeStreamID advances to 3.
	m = typeText(m, "second")
	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(DashboardModel)
	if !m.waiting || m.activeStreamID != 3 {
		t.Fatalf("after second prompt: waiting=%v activeStreamID=%d, want true, 3", m.waiting, m.activeStreamID)
	}
	if m.streamCancel != nil {
		m.streamCancel()
		m.streamCancel = nil
	}

	// Stale token/done/err from stream 1 (and 2) MUST NOT corrupt stream 3.
	for _, stale := range []tea.Msg{
		streamTokenMsg{streamID: 1, token: "CORRUPT ", ch: ch1},
		streamDoneMsg{streamID: 1},
		streamErrMsg{streamID: 1, err: errors.New("CORRUPT ERR")},
		streamTokenMsg{streamID: 2, token: "CORRUPT2 ", ch: ch1},
	} {
		updated, cmd = m.Update(stale)
		m = updated.(DashboardModel)
		if cmd != nil {
			t.Fatalf("expected nil cmd for stale message %T during stream 3", stale)
		}
		if !m.waiting || m.activeStreamID != 3 {
			t.Fatalf("stale message %T disrupted active stream 3 (waiting=%v, activeStreamID=%d)", stale, m.waiting, m.activeStreamID)
		}
		if m.currentResponse.String() != "" {
			t.Fatalf("stale message %T leaked into stream 3 response: %q", stale, m.currentResponse.String())
		}
	}

	// 4. Valid messages for stream 3 complete cleanly.
	ch3 := make(chan string, 1)
	updated, cmd = m.Update(streamTokenMsg{streamID: 3, token: "fresh answer", ch: ch3})
	m = updated.(DashboardModel)
	if cmd == nil {
		t.Fatal("expected waitForToken cmd for active stream 3")
	}
	updated, _ = m.Update(streamDoneMsg{streamID: 3})
	m = updated.(DashboardModel)
	if m.waiting {
		t.Fatal("expected waiting=false after streamDoneMsg for stream 3")
	}
	lastChat := m.chatHistory[len(m.chatHistory)-1]
	if lastChat.Role != "assistant" || lastChat.Content != "fresh answer" {
		t.Fatalf("unexpected final assistant chatHistory entry: %+v", lastChat)
	}
	if !strings.Contains(m.uiHistory[len(m.uiHistory)-1], "fresh answer") {
		t.Fatalf("expected uiHistory to contain 'fresh answer', got %q", m.uiHistory[len(m.uiHistory)-1])
	}
}

func TestManagedChannelPrompterCloseTerminatesWaitForApprovalCmd(t *testing.T) {
	prompter, inbox, closePrompter := NewManagedChannelPrompter()

	waitDone := make(chan tea.Msg, 1)
	go func() {
		waitDone <- waitForApprovalCmd(inbox)()
	}()

	// Also verify a pending prompter call waiting on ReplyCh unblocks cleanly on closePrompter().
	prompterDone := make(chan error, 1)
	go func() {
		_, err := prompter(context.Background(), pigeoncare.ActionProposal{Title: "pending-action"})
		prompterDone <- err
	}()

	// Drain the queued proposal from inbox before closing so the prompter goroutine is blocked on replyCh.
	select {
	case msg := <-waitDone:
		if _, ok := msg.(ApprovalPromptMsg); !ok {
			t.Fatalf("expected ApprovalPromptMsg, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ApprovalPromptMsg")
	}

	// Start a second waitForApprovalCmd (as DashboardModel does after handling an approval) and closePrompter.
	waitDone2 := make(chan tea.Msg, 1)
	go func() {
		waitDone2 <- waitForApprovalCmd(inbox)()
	}()

	// closePrompter must be idempotent.
	closePrompter()
	closePrompter()

	select {
	case msg := <-waitDone2:
		if msg != nil {
			t.Fatalf("expected nil tea.Msg after closePrompter(), got %#v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForApprovalCmd leaked/hung after closePrompter()")
	}

	select {
	case err := <-prompterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected blocked prompter to return context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked prompter hung after closePrompter()")
	}

	// Subsequent prompter invocations after closePrompter() must return immediately without panic.
	dec, err := prompter(context.Background(), pigeoncare.ActionProposal{Title: "after-close"})
	if dec != pigeoncare.DecisionDeny || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected (DecisionDeny, context.Canceled) after close, got (%v, %v)", dec, err)
	}
}

func TestDashboardQuitCancelsModelContextAndCtxPrompter(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	m := NewDashboardModel(DashboardOptions{
		Context:    parentCtx,
		Config:     config.DefaultWorkstationConfig(),
		InitialTab: TabOverview,
	})
	modelCtx := m.baseContext()
	if modelCtx.Err() != nil {
		t.Fatalf("expected fresh model context to be active, got %v", modelCtx.Err())
	}

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	um := updated.(DashboardModel)
	if !um.quitting {
		t.Fatal("expected quitting=true")
	}
	if !errors.Is(modelCtx.Err(), context.Canceled) {
		t.Fatalf("expected quit() to cancel model-scoped ctx, got %v", modelCtx.Err())
	}
	if parentCtx.Err() != nil {
		t.Fatalf("expected quit() not to cancel caller parentCtx, got %v", parentCtx.Err())
	}
	if cmd == nil {
		t.Fatal("expected tea.Quit command")
	}

	// Verify waitForApprovalCmdWithContext unblocks cleanly on ctx cancel even if channel remains open.
	openInbox := make(chan ApprovalPromptMsg)
	waitCh := make(chan tea.Msg, 1)
	go func() {
		waitCh <- waitForApprovalCmdWithContext(modelCtx, openInbox)()
	}()
	select {
	case msg := <-waitCh:
		if msg != nil {
			t.Fatalf("expected nil msg on canceled model ctx, got %#v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForApprovalCmdWithContext hung after model ctx cancel")
	}

	// Verify NewChannelPrompterWithContext closes inbox when ctx cancels.
	pCtx, pCancel := context.WithCancel(context.Background())
	prompter, inbox := NewChannelPrompterWithContext(pCtx)
	pCancel()

	select {
	case _, ok := <-inbox:
		if ok {
			t.Fatal("expected NewChannelPrompterWithContext inbox to close after ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("NewChannelPrompterWithContext inbox did not close on ctx cancel")
	}
	if dec, err := prompter(context.Background(), pigeoncare.ActionProposal{Title: "after-cancel"}); dec != pigeoncare.DecisionDeny || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled ctx prompter to return (DecisionDeny, context.Canceled), got (%v, %v)", dec, err)
	}
}

func TestDashboardApprovalStreamIDCorrelationAndLifecycleClear(t *testing.T) {
	prompter, inbox, closePrompter := NewManagedChannelPrompter()
	defer closePrompter()

	m := NewDashboardModel(DashboardOptions{
		Config:        config.DefaultWorkstationConfig(),
		Care:          pigeoncare.NewService(pigeoncare.Options{Prompter: prompter}),
		ApprovalInbox: inbox,
		InitialTab:    TabOverview,
	})

	// Prompter attaches streamID from ctx into ApprovalPromptMsg.
	replyDone := make(chan pigeoncare.ConfirmationDecision, 1)
	go func() {
		dec, _ := prompter(withApprovalStreamID(context.Background(), 99), pigeoncare.ActionProposal{Title: "correlated"})
		replyDone <- dec
	}()
	var correlated ApprovalPromptMsg
	select {
	case correlated = <-inbox:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for correlated ApprovalPromptMsg")
	}
	if correlated.StreamID != 99 {
		t.Fatalf("expected StreamID=99 from context, got %d", correlated.StreamID)
	}
	correlated.ReplyCh <- pigeoncare.DecisionDeny
	<-replyDone

	// Start stream 1 in TabPigeonCare.
	m.activeTab = TabPigeonCare
	for _, r := range "check thermals" {
		u, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = u.(DashboardModel)
	}
	u, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = u.(DashboardModel)
	if !m.waiting || m.activeStreamID != 1 {
		t.Fatalf("expected active stream 1, got waiting=%v activeStreamID=%d", m.waiting, m.activeStreamID)
	}
	if m.streamCancel != nil {
		m.streamCancel()
		m.streamCancel = nil
	}

	// Cancel stream 1 via Esc -> activeStreamID becomes 2.
	u, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = u.(DashboardModel)
	m.activeTab = TabOverview

	// Stale ApprovalPromptMsg from stream 1 must be denied immediately and re-arm waitForApprovalCmd.
	staleReply := make(chan pigeoncare.ConfirmationDecision, 1)
	u, cmd := m.Update(ApprovalPromptMsg{
		StreamID: 1,
		Proposal: pigeoncare.ActionProposal{Title: "stale-approval"},
		ReplyCh:  staleReply,
	})
	m = u.(DashboardModel)
	if m.pendingApproval != nil {
		t.Fatal("expected stale ApprovalPromptMsg not to set pendingApproval")
	}
	if m.activeTab != TabOverview {
		t.Fatalf("expected stale ApprovalPromptMsg not to switch tabs, got %v", m.activeTab)
	}
	if cmd == nil {
		t.Fatal("expected stale ApprovalPromptMsg handling to re-arm waitForApprovalCmd")
	}
	select {
	case dec := <-staleReply:
		if dec != pigeoncare.DecisionDeny {
			t.Fatalf("expected stale ApprovalPromptMsg to receive DecisionDeny, got %v", dec)
		}
	default:
		t.Fatal("expected stale ApprovalPromptMsg ReplyCh to be denied immediately")
	}

	// Start stream 2 -> activeStreamID increments; matching ApprovalPromptMsg sets pendingApproval,
	// and streamErrMsg / streamDoneMsg / quit() clear and deny pendingApproval cleanly.
	for _, tc := range []struct {
		name string
		kind string
	}{
		{name: "stream_err", kind: "err"},
		{name: "stream_done", kind: "done"},
		{name: "quit", kind: "quit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subM := NewDashboardModel(DashboardOptions{
				Config:        config.DefaultWorkstationConfig(),
				Care:          pigeoncare.NewService(pigeoncare.Options{Prompter: prompter}),
				ApprovalInbox: inbox,
				InitialTab:    TabPigeonCare,
			})
			subM.userInput = "run probe"
			u, _ := subM.Update(tea.KeyMsg{Type: tea.KeyEnter})
			subM = u.(DashboardModel)
			if subM.streamCancel != nil {
				subM.streamCancel()
				subM.streamCancel = nil
			}
			curID := subM.activeStreamID

			activeReply := make(chan pigeoncare.ConfirmationDecision, 1)
			u, _ = subM.Update(ApprovalPromptMsg{
				StreamID: curID,
				Proposal: pigeoncare.ActionProposal{Title: "active-approval"},
				ReplyCh:  activeReply,
			})
			subM = u.(DashboardModel)
			if subM.pendingApproval == nil {
				t.Fatal("expected matching ApprovalPromptMsg to populate pendingApproval")
			}

			var finCmd tea.Cmd
			switch tc.kind {
			case "quit":
				u, finCmd = subM.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
			case "err":
				u, finCmd = subM.Update(streamErrMsg{streamID: curID, err: errors.New("boom")})
			case "done":
				u, finCmd = subM.Update(streamDoneMsg{streamID: curID})
			}
			subM = u.(DashboardModel)
			if subM.pendingApproval != nil {
				t.Fatalf("expected pendingApproval to be cleared on %s", tc.name)
			}
			select {
			case dec := <-activeReply:
				if dec != pigeoncare.DecisionDeny {
					t.Fatalf("expected cleared pendingApproval on %s to receive DecisionDeny, got %v", tc.name, dec)
				}
			default:
				t.Fatalf("expected cleared pendingApproval ReplyCh to be unblocked on %s", tc.name)
			}
			if tc.kind != "quit" && finCmd == nil {
				t.Fatalf("expected %s clearing pendingApproval to re-arm waitForApprovalCmd", tc.name)
			}
			_ = llm.Message{}
		})
	}
}

type blockingDashboardStore struct {
	mu           sync.Mutex
	clusterGate  chan struct{}
	clusterActive atomic.Int64
	clusterCalls  atomic.Int64
	clusterMax    atomic.Int64
	nodes         []db.NodeSummary

	mathGate   chan struct{}
	mathActive atomic.Int64
	mathCalls  atomic.Int64
	mathMax    atomic.Int64
	pmvs       []db.PMVRecord
}

func (b *blockingDashboardStore) recordClusterConcurrency() {
	b.clusterCalls.Add(1)
	cur := b.clusterActive.Add(1)
	for {
		prev := b.clusterMax.Load()
		if cur <= prev || b.clusterMax.CompareAndSwap(prev, cur) {
			break
		}
	}
}

func (b *blockingDashboardStore) GetMeshIdentity(_ context.Context) (*core.MeshIdentity, error) {
	return &core.MeshIdentity{MeshName: "test-mesh", CoopNodeID: "rv64-coop-01"}, nil
}

func (b *blockingDashboardStore) GetClusterStatus(ctx context.Context, _ time.Duration) ([]db.NodeSummary, error) {
	b.recordClusterConcurrency()
	defer b.clusterActive.Add(-1)
	if b.clusterGate != nil {
		select {
		case <-b.clusterGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]db.NodeSummary, len(b.nodes))
	copy(out, b.nodes)
	return out, nil
}

func (b *blockingDashboardStore) ResolveNodes(_ context.Context, _ core.RunnerSelector) ([]string, error) {
	return nil, nil
}
func (b *blockingDashboardStore) GetLineageDAG(_ context.Context, _ string, _ int) (*db.DAG, error) {
	return nil, nil
}
func (b *blockingDashboardStore) GetLedgerActivity(_ context.Context, _ int) ([]db.LedgerActivity, error) {
	return nil, nil
}
func (b *blockingDashboardStore) QueryReadOnlyMaps(ctx context.Context, sql string, _ ...any) ([]map[string]any, error) {
	if strings.Contains(sql, "date_trunc('minute'") {
		b.mathCalls.Add(1)
		cur := b.mathActive.Add(1)
		for {
			prev := b.mathMax.Load()
			if cur <= prev || b.mathMax.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer b.mathActive.Add(-1)
		if b.mathGate != nil {
			select {
			case <-b.mathGate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []map[string]any{{
			"bucket":          time.Now().UTC().Truncate(time.Minute),
			"live_nodes":      int64(1),
			"observer_events": int64(2),
			"runner_events":   int64(1),
			"tracer_events":   int64(0),
			"total_events":    int64(3),
		}}, nil
	}
	return nil, nil
}
func (b *blockingDashboardStore) ExplainReadOnly(_ context.Context, _ string) (string, error) {
	return "", nil
}
func (b *blockingDashboardStore) EnsureMathSchema(_ context.Context) error { return nil }
func (b *blockingDashboardStore) SavePMVDraft(_ context.Context, rec db.PMVRecord) (*db.PMVRecord, error) {
	return &rec, nil
}
func (b *blockingDashboardStore) UpdatePMVState(_ context.Context, _ db.PMVRecord) error { return nil }
func (b *blockingDashboardStore) GetPMV(_ context.Context, _ string) (*db.PMVRecord, error) {
	return nil, nil
}
func (b *blockingDashboardStore) ListPMVs(_ context.Context) ([]db.PMVRecord, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]db.PMVRecord(nil), b.pmvs...), nil
}
func (b *blockingDashboardStore) MaterializePMV(_ context.Context, _, _ string) error { return nil }
func (b *blockingDashboardStore) DropPMV(_ context.Context, _ string) error          { return nil }
func (b *blockingDashboardStore) Close()                                             {}

func TestDashboardPollingInFlightAndMonotonicSequenceGuards(t *testing.T) {
	gateOverview := make(chan struct{})
	gateMath := make(chan struct{})
	store := &blockingDashboardStore{
		clusterGate: gateOverview,
		mathGate:    gateMath,
		nodes:       []db.NodeSummary{{NodeID: "node-initial", IsHealthy: true}},
	}

	m := NewDashboardModel(DashboardOptions{
		Config: config.DefaultWorkstationConfig(),
		Store:  store,
	})

	// 1. In-flight guard prevents concurrent overview/math polls from piling up.
	ovCmd1 := m.fetchOverviewCmd()
	mathCmd1 := m.fetchMathCmd()
	if ovCmd1 == nil || mathCmd1 == nil {
		t.Fatal("expected initial overview and math commands to be non-nil")
	}

	// While first polls have not completed, duplicate calls / ticks / 'r' must not launch concurrent polls.
	if dup := m.fetchOverviewCmd(); dup != nil {
		t.Fatal("expected concurrent fetchOverviewCmd while in-flight to return nil")
	}
	if dup := m.fetchMathCmd(); dup != nil {
		t.Fatal("expected concurrent fetchMathCmd while in-flight to return nil")
	}

	var wg sync.WaitGroup
	ovMsgCh := make(chan tea.Msg, 1)
	mathMsgCh := make(chan tea.Msg, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		ovMsgCh <- ovCmd1()
	}()
	go func() {
		defer wg.Done()
		mathMsgCh <- mathCmd1()
	}()

	// Trigger extra ticks & manual refresh while goroutines are blocked on gates.
	u, _ := m.Update(overviewTickMsg(time.Now()))
	m = u.(DashboardModel)
	u, _ = m.Update(mathTickMsg(time.Now()))
	m = u.(DashboardModel)
	u, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	m = u.(DashboardModel)

	close(gateOverview)
	close(gateMath)
	wg.Wait()

	if store.clusterMax.Load() > 1 || store.mathMax.Load() > 1 {
		t.Fatalf("expected max concurrent overview/math polls <= 1, got clusterMax=%d mathMax=%d",
			store.clusterMax.Load(), store.mathMax.Load())
	}

	ovMsg := (<-ovMsgCh).(overviewDataMsg)
	mathMsg := (<-mathMsgCh).(mathDataMsg)
	if ovMsg.seq != 1 || mathMsg.seq != 1 {
		t.Fatalf("expected seq=1 on initial polls, got overview=%d math=%d", ovMsg.seq, mathMsg.seq)
	}

	// 2. Monotonic sequence guard prevents stale poll results from overwriting newer state.
	newerOverview := overviewDataMsg{
		seq:   5,
		nodes: []db.NodeSummary{{NodeID: "node-newer", IsHealthy: true}},
	}
	u, _ = m.Update(newerOverview)
	m = u.(DashboardModel)
	if len(m.nodes) != 1 || m.nodes[0].NodeID != "node-newer" {
		t.Fatalf("expected newer overview state to apply, got %+v", m.nodes)
	}

	// Stale seq=1 (with error and older nodes) must be ignored!
	u, _ = m.Update(overviewDataMsg{
		seq:   1,
		nodes: []db.NodeSummary{{NodeID: "node-stale", IsHealthy: true}},
		err:   errors.New("stale error"),
	})
	m = u.(DashboardModel)
	if len(m.nodes) != 1 || m.nodes[0].NodeID != "node-newer" {
		t.Fatalf("stale overviewDataMsg overwrote newer nodes: %+v", m.nodes)
	}
	if strings.Contains(m.statusBanner, "stale error") {
		t.Fatalf("stale overviewDataMsg overwrote statusBanner: %q", m.statusBanner)
	}

	// Same monotonic sequence guard for PigeonMath polling.
	newerSnap := &pigeonmath.DashboardSnapshot{WindowMinutes: 60}
	u, _ = m.Update(mathDataMsg{seq: 5, snap: newerSnap})
	m = u.(DashboardModel)
	if m.mathSnap == nil || m.mathSnap.WindowMinutes != 60 {
		t.Fatalf("expected newer mathSnap WindowMinutes=60, got %+v", m.mathSnap)
	}

	staleSnap := &pigeonmath.DashboardSnapshot{WindowMinutes: 15}
	u, _ = m.Update(mathDataMsg{seq: 1, snap: staleSnap, err: errors.New("stale math err")})
	m = u.(DashboardModel)
	if m.mathSnap == nil || m.mathSnap.WindowMinutes != 60 || m.mathErr != nil {
		t.Fatalf("stale mathDataMsg overwrote newer state: snap=%+v err=%v", m.mathSnap, m.mathErr)
	}
}
