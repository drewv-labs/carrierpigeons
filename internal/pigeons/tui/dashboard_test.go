package tui

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
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
