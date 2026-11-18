package runner

import (
	"context"
	"slices"
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
	}
	for _, env := range validCases {
		if err := env.Validate(); err != nil {
			t.Fatalf("expected valid envelope %+v, got err: %v", env, err)
		}
	}

	invalidCases := []Envelope{
		{Action: "start"},
		{Action: "restart", Runner: "benchmark"},
	}
	for _, env := range invalidCases {
		if err := env.Validate(); err == nil {
			t.Fatalf("expected error for invalid envelope %+v", env)
		}
	}
}
