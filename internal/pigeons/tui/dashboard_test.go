package tui

import (
	"context"
	"strings"
	"testing"

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
