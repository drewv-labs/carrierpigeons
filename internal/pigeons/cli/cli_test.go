package cli

import (
	"reflect"
	"testing"
)

func TestBuildRunnerRequestChainedNodesAndBraceTags(t *testing.T) {
	req, err := buildRunnerRequest(
		"SyntheticComputeRnr",
		"operator",
		false,
		[]string{"rv64-compute-01", "rv64-vision-02"},
		[]string{"{compute-bench}", "DataBroker,riscv64"},
		[]string{"target_system=true", "threads=8", "threshold=0.95"},
		45,
	)
	if err != nil {
		t.Fatalf("buildRunnerRequest failed: %v", err)
	}

	wantNodes := []string{"rv64-compute-01", "rv64-vision-02"}
	if !reflect.DeepEqual(req.Selectors.Nodes, wantNodes) {
		t.Fatalf("Nodes = %v, want %v", req.Selectors.Nodes, wantNodes)
	}

	wantTags := []string{"compute-bench", "databroker", "riscv64"}
	if !reflect.DeepEqual(req.Selectors.Tags, wantTags) {
		t.Fatalf("Tags = %v, want %v", req.Selectors.Tags, wantTags)
	}

	if req.Params["duration_sec"] != 45 || req.Params["target_system"] != true || req.Params["threads"] != 8 {
		t.Fatalf("unexpected parsed params: %+v", req.Params)
	}

	if _, err := buildRunnerRequest("SyntheticComputeRnr", "operator", false, nil, nil, nil, 0); err == nil {
		t.Fatal("expected missing selectors to return validation error")
	}
}
