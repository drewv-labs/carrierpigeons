package brand

import "testing"

func TestMergeWithDefaultsAndTopics(t *testing.T) {
	custom := Spec{
		ProductName: "AcmeTelemetry",
		TopicRoot:   "/acme/fleet/",
		EnvPrefix:   "acme_",
	}.MergeWithDefaults()

	if custom.ProductName != "AcmeTelemetry" {
		t.Fatalf("unexpected ProductName: %q", custom.ProductName)
	}
	if custom.CoopName != "PigeonCoop" {
		t.Fatalf("expected CoopName default fallback, got %q", custom.CoopName)
	}
	if custom.TopicRoot != "acme/fleet" {
		t.Fatalf("expected trimmed TopicRoot 'acme/fleet', got %q", custom.TopicRoot)
	}
	if custom.EnvPrefix != "ACME" {
		t.Fatalf("expected normalized EnvPrefix 'ACME', got %q", custom.EnvPrefix)
	}
	if got := custom.CTDPrefix(); got != "acme/fleet/ctd" {
		t.Fatalf("unexpected CTDPrefix: %q", got)
	}
	if got := custom.TelemetryWildcard(); got != "acme/fleet/ctd/#" {
		t.Fatalf("unexpected TelemetryWildcard: %q", got)
	}
	if got := custom.ControlTopic("edge-01"); got != "acme/fleet/control/edge-01" {
		t.Fatalf("unexpected ControlTopic: %q", got)
	}
	if got := custom.RunnerRequestTopic(); got != "acme/fleet/requests/runner" {
		t.Fatalf("unexpected RunnerRequestTopic: %q", got)
	}
}
