package mcpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

type stubAgentSafeDB struct{}

func (stubAgentSafeDB) GetClusterStatus(context.Context, time.Duration) ([]db.NodeSummary, error) {
	return []db.NodeSummary{{NodeID: "edge-01", Status: "ONLINE"}}, nil
}
func (stubAgentSafeDB) ResolveNodes(context.Context, core.RunnerSelector) ([]string, error) {
	return []string{"edge-01"}, nil
}
func (stubAgentSafeDB) GetLineageDAG(context.Context, string, int) (*db.DAG, error) {
	return &db.DAG{}, nil
}
func (stubAgentSafeDB) GetLedgerActivity(context.Context, int) ([]db.LedgerActivity, error) {
	return nil, nil
}
func (stubAgentSafeDB) QueryReadOnlyMaps(context.Context, string, ...any) ([]map[string]any, error) {
	return nil, nil
}
func (stubAgentSafeDB) ExplainReadOnly(context.Context, string) (string, error) {
	return `[{"Plan":{}}]`, nil
}
func (stubAgentSafeDB) EnsureMathSchema(context.Context) error { return nil }
func (stubAgentSafeDB) SavePMVDraft(_ context.Context, r db.PMVRecord) (*db.PMVRecord, error) {
	cp := r
	return &cp, nil
}
func (stubAgentSafeDB) UpdatePMVState(context.Context, db.PMVRecord) error { return nil }
func (stubAgentSafeDB) GetPMV(context.Context, string) (*db.PMVRecord, error) {
	return &db.PMVRecord{}, nil
}
func (stubAgentSafeDB) ListPMVs(context.Context) ([]db.PMVRecord, error) { return nil, nil }
func (stubAgentSafeDB) MaterializePMV(context.Context, string, string) error {
	return nil
}
func (stubAgentSafeDB) DropPMV(context.Context, string) error { return nil }
func (stubAgentSafeDB) Close()                                {}

func TestMCPHTTPAuthAndDNSRebindingProtection(t *testing.T) {
	srv := New(Options{Store: stubAgentSafeDB{}})

	// Non-loopback ServeHTTPWithOptions without token must fail closed immediately.
	if err := ServeHTTPWithOptions(srv, "0.0.0.0:18089", HTTPOptions{}); err == nil {
		t.Fatal("expected ServeHTTPWithOptions on 0.0.0.0 without AuthToken to fail closed")
	}

	// Loopback handler without token enforces Host and Origin checks against DNS rebinding.
	loopbackHandler := NewHTTPHandler(srv, HTTPOptions{})

	evilHostReq := httptest.NewRequest(http.MethodPost, "http://evil.attacker.invalid/mcp", strings.NewReader(`{}`))
	evilHostReq.Host = "evil.attacker.invalid:8089"
	rec := httptest.NewRecorder()
	loopbackHandler.ServeHTTP(rec, evilHostReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for DNS-rebound Host header, got %d", rec.Code)
	}

	evilOriginReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
	evilOriginReq.Host = "127.0.0.1:8089"
	evilOriginReq.Header.Set("Origin", "https://evil.example.com")
	rec = httptest.NewRecorder()
	loopbackHandler.ServeHTTP(rec, evilOriginReq)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for cross-origin browser request, got %d", rec.Code)
	}

	// Handler with AuthToken enforces constant-time Bearer authentication.
	authedHandler := NewHTTPHandler(srv, HTTPOptions{AuthToken: "super-secret-mcp-token"})

	unauthReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
	rec = httptest.NewRecorder()
	authedHandler.ServeHTTP(rec, unauthReq)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized when missing Bearer token, got %d", rec.Code)
	}

	wrongReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
	wrongReq.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	authedHandler.ServeHTTP(rec, wrongReq)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for invalid Bearer token, got %d", rec.Code)
	}

	initBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`
	validReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(initBody))
	validReq.Header.Set("Authorization", "Bearer super-secret-mcp-token")
	validReq.Header.Set("Content-Type", "application/json")
	validReq.Header.Set("Accept", "application/json, text/event-stream")
	rec = httptest.NewRecorder()
	authedHandler.ServeHTTP(rec, validReq)
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("expected authenticated MCP request to pass auth middleware, got %d", rec.Code)
	}
}
