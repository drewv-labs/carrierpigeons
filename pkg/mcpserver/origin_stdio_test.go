package mcpserver

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIsLoopbackOriginRestrictsToHTTPAndHTTPSSchemes(t *testing.T) {
	allowed := []string{
		"",
		"   ",
		"http://127.0.0.1:8089",
		"https://127.0.0.1:8443",
		"http://localhost:3000",
		"HTTPS://[::1]:8089",
	}
	for _, origin := range allowed {
		if !isLoopbackOrigin(origin) {
			t.Fatalf("expected Origin %q to be allowed", origin)
		}
	}

	rejected := []string{
		"javascript://127.0.0.1/%0aalert(1)",
		"file://localhost/etc/passwd",
		"ws://127.0.0.1:8089",
		"wss://localhost:8089",
		"ftp://127.0.0.1:21",
		"data://127.0.0.1",
		"http://user:pass@127.0.0.1:8089",
		"null",
		"http://evil.example.com",
	}
	srv := New(Options{Store: stubAgentSafeDB{}})
	handler := NewHTTPHandler(srv, HTTPOptions{EnforceLoopbackHost: true})

	for _, origin := range rejected {
		if isLoopbackOrigin(origin) {
			t.Fatalf("expected Origin %q to be rejected", origin)
		}
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8089/mcp", strings.NewReader(`{}`))
		req.Host = "127.0.0.1:8089"
		req.Header.Set("Origin", origin)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("Origin %q: expected 403 Forbidden, got %d", origin, rec.Code)
		}
	}
}

func TestServeStdioContextExitsCleanlyOnCancel(t *testing.T) {
	srv := New(Options{Store: stubAgentSafeDB{}})
	pr, pw := io.Pipe()
	defer pw.Close()
	defer pr.Close()

	var stdout bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- serveStdioIO(ctx, srv, pr, &stdout)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on canceled stdio context, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveStdioIO did not exit promptly after context cancellation")
	}
}
