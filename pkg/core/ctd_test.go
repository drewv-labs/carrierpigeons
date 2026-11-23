package core

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCTDPayloadLifecycleAndRouting(t *testing.T) {
	p := NewCTDPayload("rv64-coop-01", "host-os", "active").
		AddMetric("load_1m", 1.42).
		AddParam("governor", "performance")

	if p.NodeID != "rv64-coop-01" {
		t.Fatalf("expected NodeID rv64-coop-01, got %q", p.NodeID)
	}
	if p.Component != "host-os" {
		t.Fatalf("expected Component host-os, got %q", p.Component)
	}
	if p.Status != "active" {
		t.Fatalf("expected Status active, got %q", p.Status)
	}
	if p.Pillar != PillarObserver {
		t.Fatalf("expected default Pillar %q, got %q", PillarObserver, p.Pillar)
	}
	if got := p.RoutingTopic(""); got != "pigeons/ctd/observer/rv64-coop-01/host-os" {
		t.Fatalf("unexpected default routing topic: %q", got)
	}
	if got := p.RoutingTopic("custom/prefix"); got != "custom/prefix/observer/rv64-coop-01/host-os" {
		t.Fatalf("unexpected custom routing topic: %q", got)
	}

	raw, err := p.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	var decoded CTDPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.Component != "host-os" || decoded.Params["governor"] != "performance" {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
}

func TestCTDPayloadArtifactAndTrace(t *testing.T) {
	p := NewCTDPayload("rv64-compute-01", "pipeline", "completed").
		WithArtifact(Artifact{
			ID:   "dataset:telemetry-001",
			Kind: "dataset",
		}).
		WithTrace("dataset:raw-001", "transformed_into", "dataset:telemetry-001", map[string]string{"arch": "riscv64"})

	if p.Pillar != PillarTracer {
		t.Fatalf("WithTrace should set PillarTracer, got %q", p.Pillar)
	}
	if p.Artifact == nil || p.Artifact.CreatedAt.IsZero() {
		t.Fatalf("expected hydrated Artifact timestamp, got %+v", p.Artifact)
	}
	if p.Trace == nil || p.Trace.Verb != "transformed_into" || p.Trace.NodeID != "rv64-compute-01" {
		t.Fatalf("unexpected TraceEdge: %+v", p.Trace)
	}
	if got := MeshTopic(); got != "pigeons/registry/mesh" {
		t.Fatalf("unexpected MeshTopic: %q", got)
	}
	if got := RegistryTopic("node-a"); got != "pigeons/registry/node-a/status" {
		t.Fatalf("unexpected RegistryTopic: %q", got)
	}
	if got := HelloTopic("node-a"); got != "pigeons/registry/node-a/hello" {
		t.Fatalf("unexpected HelloTopic: %q", got)
	}
	if got := ControlTopic("node-a"); got != "pigeons/control/node-a" {
		t.Fatalf("unexpected ControlTopic: %q", got)
	}
	if got := RunnerRequestTopic(); got != "pigeons/requests/runner" {
		t.Fatalf("unexpected RunnerRequestTopic: %q", got)
	}
}

func TestValidateMeshNameRFC1123KebabCase(t *testing.T) {
	valid := []string{
		"a",
		"0",
		"default",
		"rv64-lab",
		"compute-mesh-01",
		"sensor-fabric-west",
		strings.Repeat("a", 63),
	}
	for _, name := range valid {
		if err := ValidateMeshName(name); err != nil {
			t.Fatalf("expected ValidateMeshName(%q) to succeed, got %v", name, err)
		}
	}

	invalid := []string{
		"",
		"-leading",
		"trailing-",
		"-",
		"UpperCase",
		"Mixed-Case",
		"under_score",
		"dot.domain",
		"slash/path",
		"space name",
		"wild+card",
		"wild#card",
		strings.Repeat("a", 64),
	}
	for _, name := range invalid {
		if err := ValidateMeshName(name); err == nil {
			t.Fatalf("expected ValidateMeshName(%q) to fail", name)
		}
	}
}

func TestNormalizeTagsAndRunnerRequestValidate(t *testing.T) {
	tags := NormalizeTags([]string{" RISC-V ", "compute-bench", "risc-v", "", "DataBroker"})
	want := []string{"compute-bench", "databroker", "risc-v"}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("NormalizeTags = %v, want %v", tags, want)
	}

	req := RunnerRequestCTD{
		RequestID:   "req-001",
		Action:      "start",
		Runner:      "SyntheticComputeRnr",
		Selectors:   RunnerSelector{Nodes: []string{"rv64-compute-01"}, Tags: []string{"compute-bench"}},
		RequestedBy: "operator",
		RequestedAt: time.Now().UTC(),
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("expected valid RunnerRequestCTD, got %v", err)
	}

	emptySel := req
	emptySel.Selectors = RunnerSelector{}
	if err := emptySel.Validate(); err == nil {
		t.Fatal("expected empty RunnerSelector to fail validation")
	}

	for _, bad := range []string{"", "bad/slash", "bad+plus", "bad#hash", "has space", "null\x00byte", "ctrl\nchar", strings.Repeat("a", 129)} {
		if err := ValidateTopicToken(bad); err == nil {
			t.Fatalf("expected ValidateTopicToken(%q) to fail", bad)
		}
	}
	for _, good := range []string{"rv64-compute-01", "runner:SyntheticComputeRnr", "host_os.01", "192.0.2.1"} {
		if err := ValidateTopicToken(good); err != nil {
			t.Fatalf("expected ValidateTopicToken(%q) to succeed, got %v", good, err)
		}
	}

	badRunner := req
	badRunner.Runner = "bad/runner"
	if err := badRunner.Validate(); err == nil {
		t.Fatal("expected RunnerRequestCTD with slash in runner to fail")
	}
	badNode := req
	badNode.Selectors = RunnerSelector{Nodes: []string{"node+#"}}
	if err := badNode.Validate(); err == nil {
		t.Fatal("expected RunnerRequestCTD with wildcard in node selector to fail")
	}
	badReqBy := req
	badReqBy.RequestedBy = "bad operator"
	if err := badReqBy.Validate(); err == nil {
		t.Fatal("expected RunnerRequestCTD with space in requested_by to fail")
	}
}

func TestDecodeWireCTDPayloadPreservesRawJSON(t *testing.T) {
	wire := []byte(`{"node_id":"rv64-compute-01","pillar":"observer","component":"host-os","status":"active","metrics":{"load_1m":1.25,"temp":42},"params":{"arch":"riscv64"}}`)
	ev, err := DecodeWireCTDPayload(wire)
	if err != nil {
		t.Fatalf("DecodeWireCTDPayload failed: %v", err)
	}
	if ev.NodeID != "rv64-compute-01" || ev.Pillar != PillarObserver || ev.Component != "host-os" {
		t.Fatalf("unexpected decoded envelope: %+v", ev)
	}
	if ev.Metrics != nil || ev.Params != nil {
		t.Fatal("expected DecodeWireCTDPayload to skip map allocation on fast path")
	}
	if string(ev.RawMetrics()) != `{"load_1m":1.25,"temp":42}` {
		t.Fatalf("unexpected RawMetrics: %s", string(ev.RawMetrics()))
	}
	if string(ev.RawParams()) != `{"arch":"riscv64"}` {
		t.Fatalf("unexpected RawParams: %s", string(ev.RawParams()))
	}
	// Mutating metrics invalidates raw cache
	ev.AddMetric("extra", 1)
	if ev.RawMetrics() != nil {
		t.Fatal("expected AddMetric to invalidate cached RawMetrics")
	}
}
