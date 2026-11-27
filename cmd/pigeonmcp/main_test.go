package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunWithContextRedactsDSNCredentialsOnConnectError(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "pigeons.toml")
	secretDSN := "postgres://pigeoncare_ro:superSecretMCPPass99@127.0.0.1:1/edge_ledger\n?sslmode=disable"

	err := runWithContext(context.Background(), []string{
		"--config", cfgPath,
		"--db-url", secretDSN,
	})
	if err == nil {
		t.Fatal("expected DB parse/connect error for invalid DSN")
	}
	msg := err.Error()
	if strings.Contains(msg, "superSecretMCPPass99") {
		t.Fatalf("pigeonmcp startup error leaked DSN password: %s", msg)
	}
}
