package core

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestCTDPayloadLifecycleAndRouting(t *testing.T) {
	p := NewCTDPayload("rv64-coop-01", "host-os", "active").
		AddMetric("load_1m", 1.42).
		AddParam("io_method", "epoll")

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
	if decoded.Component != "host-os" || decoded.Params["io_method"] != "epoll" {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
}

func TestCTDPayloadArtifactAndTrace(t *testing.T) {
	p := NewCTDPayload("rv64-compute-01", "pipeline", "completed").
		WithArtifact(Artifact{
			ID:   "dataset:clean-001",
			Kind: "dataset",
		}).
		WithTrace("dataset:raw-001", "transformed_into", "dataset:clean-001", map[string]string{"profile": "rva23"})

	if p.Pillar != PillarTracer {
		t.Fatalf("WithTrace should set PillarTracer, got %q", p.Pillar)
	}
	if p.Artifact == nil || p.Artifact.CreatedAt.IsZero() {
		t.Fatalf("expected hydrated Artifact timestamp, got %+v", p.Artifact)
	}
	if p.Trace == nil || p.Trace.Verb != "transformed_into" || p.Trace.NodeID != "rv64-compute-01" {
		t.Fatalf("unexpected TraceEdge: %+v", p.Trace)
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
		RequestedBy: "drew",
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
}
