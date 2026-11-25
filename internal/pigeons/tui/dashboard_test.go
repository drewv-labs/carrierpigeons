package tui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncare"
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
