package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncare"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func TestDispatchAbortCmdSetsTargetRequestID(t *testing.T) {
	var captured core.RunnerRequestCTD
	m := NewDashboardModel(DashboardOptions{
		Config: config.DefaultWorkstationConfig(),
		Dispatcher: func(_ context.Context, req core.RunnerRequestCTD) error {
			captured = req
			return nil
		},
	})

	cmd := m.dispatchAbortCmd(RunnerRecord{
		NodeID:    "rv64-node-01",
		Targets:   []string{"rv64-node-01"},
		Runner:    "SyntheticComputeRnr",
		RequestID: "  req-orig-42  ",
	})
	_ = cmd()

	if captured.Action != "abort" {
		t.Fatalf("Action = %q, want abort", captured.Action)
	}
	if captured.TargetRequestID != "req-orig-42" {
		t.Fatalf("TargetRequestID = %q, want req-orig-42", captured.TargetRequestID)
	}
	if captured.RequestID == "" || captured.RequestID == captured.TargetRequestID {
		t.Fatalf("expected fresh unique transport RequestID separate from TargetRequestID, got %q", captured.RequestID)
	}
}

func TestRenderPigeonCareSanitizesANSIAndRendersAllAndEnv(t *testing.T) {
	m := NewDashboardModel(DashboardOptions{
		Config:     config.DefaultWorkstationConfig(),
		InitialTab: TabPigeonCare,
	})
	m.pendingApproval = &ApprovalPromptMsg{
		Proposal: pigeoncare.ActionProposal{
			Kind:        pigeoncare.ActionEphemeralRunner,
			Title:       "Diag\x1b[31mRED\x1b[0m\x1b]52;c;ZXZpbA==\x07Title",
			Interpreter: "sh\x1b[2J",
			SHA256:      "abcdef0123456789\x1b[31m",
			TimeoutSec:  30,
			Selectors: core.RunnerSelector{
				All:   true,
				Nodes: []string{"rv64-01\x1b]0;pwn\x1b\\"},
				Tags:  []string{"compute\u009b31m"},
			},
			Env: map[string]string{
				"Z_VAR": "val\x1b[1mZ",
				"A_VAR": "val\r\x07A",
			},
			ScriptBody: "echo hi\x1b[31m\n\x1b]52;c;cHdu\x07uname -m",
		},
	}

	rendered := m.renderPigeonCare(lipgloss.NewStyle(), lipgloss.Color("12"))
	if !strings.Contains(rendered, "Targets: all=true nodes=[rv64-01] tags=[compute]") {
		t.Fatalf("expected sanitized Targets line with all=true, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "Env: A_VAR=valA Z_VAR=valZ") {
		t.Fatalf("expected sorted sanitized Env line, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "DiagREDTitle") {
		t.Fatalf("expected ANSI-stripped title DiagREDTitle, got:\n%s", rendered)
	}
	if strings.Contains(rendered, "ZXZpbA==") || strings.Contains(rendered, "cHdu") || strings.Contains(rendered, "pwn") {
		t.Fatalf("expected OSC payloads to be stripped from modal render, got:\n%s", rendered)
	}
	if strings.Contains(rendered, "\x1b[31m") || strings.Contains(rendered, "\x1b[2J") || strings.Contains(rendered, "\u009b") {
		t.Fatalf("expected untrusted ANSI CSI/C1 sequences to be stripped, got:\n%s", rendered)
	}
	idxEcho := strings.Index(rendered, "echo hi")
	idxUname := strings.Index(rendered, "uname -m")
	if idxEcho < 0 || idxUname < 0 || idxEcho >= idxUname {
		t.Fatalf("expected sanitized multiline script body lines in order, got:\n%s", rendered)
	}
}

func TestDashboardHistoryBoundsResponseCapAndTrailingUserPop(t *testing.T) {
	m := NewDashboardModel(DashboardOptions{
		Config:       config.DefaultWorkstationConfig(),
		Care:         pigeoncare.NewService(pigeoncare.Options{}),
		InitialTab:   TabPigeonCare,
		FaultContext: "Thermal excursion on rv64-node-01",
	})

	// 1. Esc cancel and streamErrMsg must pop dangling trailing user turn without touching FaultContext.
	m.userInput = "cancelled question"
	u, _ := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = u.(DashboardModel)
	if len(m.chatHistory) != 3 {
		t.Fatalf("expected 3 chatHistory entries (2 fault + 1 user), got %d", len(m.chatHistory))
	}
	u, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = u.(DashboardModel)
	if len(m.chatHistory) != 2 {
		t.Fatalf("expected Esc cancel to pop dangling user turn leaving 2 fault turns, got %d (%+v)", len(m.chatHistory), m.chatHistory)
	}

	m.userInput = "errored question"
	u, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = u.(DashboardModel)
	errStreamID := m.activeStreamID
	u, _ = m.Update(streamErrMsg{streamID: errStreamID, err: errors.New("upstream 502")})
	m = u.(DashboardModel)
	if len(m.chatHistory) != 2 {
		t.Fatalf("expected streamErrMsg to pop dangling user turn leaving 2 fault turns, got %d (%+v)", len(m.chatHistory), m.chatHistory)
	}

	// 2. Ring bounds: uiHistory <= 200 and chatHistory <= 50 preserving initial FaultContext.
	for i := 0; i < 150; i++ {
		m.appendUIHistory(fmt.Sprintf("ui-line-%d", i), fmt.Sprintf("ui-reply-%d", i))
		m.appendChatHistory(
			llm.Message{Role: "user", Content: fmt.Sprintf("q-%d", i)},
			llm.Message{Role: "assistant", Content: fmt.Sprintf("a-%d", i)},
		)
	}
	if len(m.uiHistory) > maxUIHistoryEntries {
		t.Fatalf("uiHistory exceeded cap %d: got %d", maxUIHistoryEntries, len(m.uiHistory))
	}
	if len(m.chatHistory) > maxChatHistoryEntries {
		t.Fatalf("chatHistory exceeded cap %d: got %d", maxChatHistoryEntries, len(m.chatHistory))
	}
	if !strings.Contains(m.chatHistory[0].Content, "Thermal excursion on rv64-node-01") || m.chatHistory[1].Role != "assistant" {
		t.Fatalf("expected initial FaultContext turns to be preserved at chatHistory[0:2], got %+v", m.chatHistory[:2])
	}
	if m.chatHistory[len(m.chatHistory)-1].Content != "a-149" {
		t.Fatalf("expected newest assistant turn a-149 at end of chatHistory, got %+v", m.chatHistory[len(m.chatHistory)-1])
	}

	// 3. currentResponse capped at <= 256 KiB.
	m.waiting = true
	m.activeStreamID++
	capID := m.activeStreamID
	m.currentResponse = &strings.Builder{}
	m.responseTruncated = false
	ch := make(chan string, 1)
	chunk := strings.Repeat("a", 64*1024)
	for i := 0; i < 8; i++ {
		u, _ = m.Update(streamTokenMsg{streamID: capID, token: chunk, ch: ch})
		m = u.(DashboardModel)
	}
	if m.currentResponse.Len() > maxCurrentResponseBytes {
		t.Fatalf("currentResponse exceeded %d bytes: got %d", maxCurrentResponseBytes, m.currentResponse.Len())
	}
	if !m.responseTruncated || !strings.Contains(m.currentResponse.String(), "truncated at 256 KiB") {
		t.Fatalf("expected responseTruncated=true with truncation marker, len=%d", m.currentResponse.Len())
	}
}

func TestScheduleTicksExitPromptlyOnQuitContextCancel(t *testing.T) {
	m := NewDashboardModel(DashboardOptions{
		Config: config.DefaultWorkstationConfig(),
	})

	ovTickCmd := m.scheduleOverviewTick()
	mathTickCmd := m.scheduleMathTick()

	ovDone := make(chan tea.Msg, 1)
	mathDone := make(chan tea.Msg, 1)
	go func() { ovDone <- ovTickCmd() }()
	go func() { mathDone <- mathTickCmd() }()

	_, _ = m.quit()

	for name, ch := range map[string]chan tea.Msg{"overview": ovDone, "math": mathDone} {
		select {
		case msg := <-ch:
			if msg != nil {
				t.Fatalf("expected nil msg from canceled %s tick, got %#v", name, msg)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("%s tick command did not exit promptly after quit() canceled model ctx", name)
		}
	}
}
