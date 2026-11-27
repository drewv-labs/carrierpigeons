package runner

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type stubRunner struct {
	name string
}

func (s *stubRunner) Name() string { return s.name }
func (s *stubRunner) Run(_ context.Context, _ string, _ map[string]any, _ core.TelemetryPublisher) error {
	return nil
}

func TestRegistryAndEnvelopeValidation(t *testing.T) {
	Register(&stubRunner{name: "unit-stub"})

	got, ok := Get("unit-stub")
	if !ok || got.Name() != "unit-stub" {
		t.Fatalf("expected registered runner unit-stub, got ok=%v", ok)
	}
	if !slices.Contains(RegistryNames(), "unit-stub") {
		t.Fatalf("RegistryNames missing unit-stub: %v", RegistryNames())
	}

	validCases := []Envelope{
		{Runner: "benchmark"},
		{Action: "start", Runner: "benchmark"},
		{Action: "abort", Runner: "benchmark"},
		{RequestID: "req-abort-1", TargetRequestID: "req-start-1", Action: "abort", Runner: "benchmark"},
	}
	for _, env := range validCases {
		if err := env.Validate(); err != nil {
			t.Fatalf("expected valid envelope %+v, got err: %v", env, err)
		}
	}

	rawNoTarget, err := json.Marshal(Envelope{RequestID: "req-1", Action: "start", Runner: "benchmark"})
	if err != nil {
		t.Fatalf("marshal Envelope failed: %v", err)
	}
	if strings.Contains(string(rawNoTarget), "target_request_id") {
		t.Fatalf("expected empty TargetRequestID omitted, got %s", string(rawNoTarget))
	}

	rawAbort, err := json.Marshal(Envelope{
		RequestID:       "req-abort-2",
		TargetRequestID: "req-start-1",
		Action:          "abort",
		Runner:          "benchmark",
	})
	if err != nil {
		t.Fatalf("marshal abort Envelope failed: %v", err)
	}
	var decodedAbort Envelope
	if err := json.Unmarshal(rawAbort, &decodedAbort); err != nil {
		t.Fatalf("unmarshal abort Envelope failed: %v", err)
	}
	if decodedAbort.RequestID != "req-abort-2" || decodedAbort.TargetRequestID != "req-start-1" {
		t.Fatalf("unexpected decoded abort Envelope: %+v", decodedAbort)
	}

	invalidCases := []Envelope{
		{Action: "start"},
		{Action: "restart", Runner: "benchmark"},
		{Action: "start", Runner: "+"},
		{Action: "start", Runner: "#"},
		{Action: "start", Runner: "ephemeral/#"},
		{Action: "start", Runner: "ephemeral/+"},
		{Action: "start", Runner: "bench/mark"},
		{Action: "start", Runner: "bench mark"},
		{Action: "start", Runner: "bench\x00mark"},
	}
	for _, env := range invalidCases {
		if err := env.Validate(); err == nil {
			t.Fatalf("expected error for invalid envelope %+v", env)
		}
	}
}

func TestNormalizeInterpreterAndBoundScriptHash(t *testing.T) {
	if got := NormalizeInterpreter(""); got != "sh" {
		t.Fatalf("NormalizeInterpreter(\"\") = %q, want sh", got)
	}
	if got := NormalizeInterpreter("/usr/bin/Python3"); got != "python3" {
		t.Fatalf("NormalizeInterpreter(/usr/bin/Python3) = %q, want python3", got)
	}
	if got := NormalizeInterpreter(".."); got != "" {
		t.Fatalf("NormalizeInterpreter(..) = %q, want empty", got)
	}
	if got := NormalizeInterpreter("."); got != "" {
		t.Fatalf("NormalizeInterpreter(.) = %q, want empty", got)
	}

	script := "print('ok')\n"
	shHash := ComputeBoundScriptSHA256("sh", script)
	pyHash := ComputeBoundScriptSHA256("python3", script)
	rawHash := ComputeScriptSHA256(script)
	if shHash == pyHash || shHash == rawHash || pyHash == rawHash {
		t.Fatalf("expected distinct bound hashes across interpreters and raw body: sh=%s py=%s raw=%s", shHash, pyHash, rawHash)
	}

	if got, ok := NormalizeAllowedScriptHashEntry(" /usr/bin/sh : ABCD "); !ok || got != "sh:abcd" {
		t.Fatalf("NormalizeAllowedScriptHashEntry normalized path/space failed: got=(%q,%v)", got, ok)
	}
	for _, invalid := range []string{"", "   ", ":abcd", "..:abcd", ".:abcd", "sh:"} {
		if got, ok := NormalizeAllowedScriptHashEntry(invalid); ok {
			t.Fatalf("expected NormalizeAllowedScriptHashEntry(%q) invalid, got %q", invalid, got)
		}
	}
}
