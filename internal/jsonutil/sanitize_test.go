package jsonutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestSanitizeValueNonFiniteFloatsStructsAndImmutability(t *testing.T) {
	nanPtr := new(float64)
	*nanPtr = math.NaN()

	type embeddedMetrics struct {
		AmbientC float64 `json:"ambient_c"`
	}
	type sensorSnapshot struct {
		embeddedMetrics
		TempC      float64            `json:"temp_c"`
		TempF32    float32            `json:"temp_f32"`
		Series     []float64          `json:"series"`
		Pair       [2]float64         `json:"pair"`
		PerCore    map[string]float64 `json:"per_core"`
		PtrSeries  *[]float64         `json:"ptr_series"`
		Ignored    float64            `json:"-"`
		LiteralHyp float64            `json:"-,"`
		OmitZero   float64            `json:"omit_zero,omitempty"`
		KeepEmpty  float64            `json:"keep_empty"`
		OmitNaN    float64            `json:"omit_nan,omitempty"`
		Timestamp  time.Time          `json:"timestamp"`
		Raw        json.RawMessage    `json:"raw"`
	}

	ptrSlice := []float64{3.5, math.Inf(-1)}
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	snap := sensorSnapshot{
		embeddedMetrics: embeddedMetrics{AmbientC: math.NaN()},
		TempC:           math.NaN(),
		TempF32:         float32(math.Inf(1)),
		Series:          []float64{1.5, math.NaN()},
		Pair:            [2]float64{2.0, math.Inf(1)},
		PerCore:         map[string]float64{"cpu0": math.NaN(), "cpu1": 55.25},
		PtrSeries:       &ptrSlice,
		Ignored:         math.NaN(),
		LiteralHyp:      math.Inf(1),
		OmitZero:        0,
		KeepEmpty:       0,
		OmitNaN:         math.NaN(),
		Timestamp:       now,
		Raw:             json.RawMessage(`{"ok":true}`),
	}

	in := map[string]any{
		"ptr_nan":      nanPtr,
		"json_num_nan": json.Number("NaN"),
		"json_num_ok":  json.Number("42.25"),
		"snap_a":       snap,
		"snap_b":       snap, // shared non-cyclic DAG reference
	}

	sanitized := SanitizeValue(in)
	raw, err := json.Marshal(sanitized)
	if err != nil {
		t.Fatalf("json.Marshal(SanitizeValue) failed: %v", err)
	}
	if !json.Valid(raw) {
		t.Fatalf("expected valid JSON, got %s", string(raw))
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if v, ok := decoded["ptr_nan"]; !ok || v != nil {
		t.Fatalf("expected ptr_nan=null, got %#v", decoded["ptr_nan"])
	}
	if v, ok := decoded["json_num_nan"]; !ok || v != nil {
		t.Fatalf("expected json_num_nan=null, got %#v", decoded["json_num_nan"])
	}
	if decoded["json_num_ok"] != 42.25 {
		t.Fatalf("expected json_num_ok=42.25, got %#v", decoded["json_num_ok"])
	}

	for _, snapKey := range []string{"snap_a", "snap_b"} {
		sm, ok := decoded[snapKey].(map[string]any)
		if !ok {
			t.Fatalf("expected %s to decode as object, got %T", snapKey, decoded[snapKey])
		}
		if _, hasIgnored := sm["Ignored"]; hasIgnored {
			t.Fatalf("expected json:\"-\" field omitted in %s: %v", snapKey, sm)
		}
		if _, hasOmitZero := sm["omit_zero"]; hasOmitZero {
			t.Fatalf("expected omit_zero omitted in %s: %v", snapKey, sm)
		}
		for _, nullField := range []string{"ambient_c", "temp_c", "temp_f32", "-", "omit_nan"} {
			val, exists := sm[nullField]
			if !exists || val != nil {
				t.Fatalf("expected %s.%s = null, got exists=%v val=%#v", snapKey, nullField, exists, val)
			}
		}
	}

	if !math.IsNaN(*nanPtr) || !math.IsNaN(snap.Series[1]) || !math.IsNaN(snap.PerCore["cpu0"]) {
		t.Fatal("SanitizeValue mutated caller-owned values in place")
	}
}

func TestSanitizeValueHandlesPointerMapAndSliceCycles(t *testing.T) {
	type cyclicNode struct {
		Name string      `json:"name"`
		Load float64     `json:"load"`
		Next *cyclicNode `json:"next"`
	}
	n1 := &cyclicNode{Name: "rv64-compute-01", Load: math.NaN()}
	n2 := &cyclicNode{Name: "rv64-compute-02", Load: 1.5}
	n1.Next = n2
	n2.Next = n1

	cyclicMap := map[string]any{"ok": 7.5}
	cyclicMap["self"] = cyclicMap

	cyclicSlice := make([]any, 2)
	cyclicSlice[0] = "head"
	cyclicSlice[1] = cyclicSlice

	out := SanitizeValue(map[string]any{
		"node":  n1,
		"map":   cyclicMap,
		"slice": cyclicSlice,
	})
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("expected cyclic structures to marshal cleanly after SanitizeValue, got: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal cyclic output failed: %v (%s)", err, string(raw))
	}

	if m, ok := decoded["map"].(map[string]any); !ok || m["ok"] != 7.5 || m["self"] != nil {
		t.Fatalf("unexpected sanitized cyclic map: %#v", decoded["map"])
	}
	if s, ok := decoded["slice"].([]any); !ok || len(s) != 2 || s[0] != "head" || s[1] != nil {
		t.Fatalf("unexpected sanitized cyclic slice: %#v", decoded["slice"])
	}
	if n1.Next != n2 || n2.Next != n1 {
		t.Fatal("SanitizeValue mutated caller-owned cyclic struct pointers")
	}
}

func TestRedactErrorAndCredentials(t *testing.T) {
	_, rawParseErr := url.Parse("postgres://ctd_admin:ultraSecretPW@192.0.2.10:badport/edge_ledger?token=tokSecret99")
	if rawParseErr == nil {
		t.Fatal("expected url.Parse error for invalid port")
	}
	wrapped := fmt.Errorf("cannot parse `postgres://ctd_admin:ultraSecretPW@192.0.2.10:badport/edge_ledger?token=tokSecret99`: %w", rawParseErr)

	redacted := RedactError(wrapped)
	if redacted == nil {
		t.Fatal("expected non-nil redacted error")
	}
	msg := redacted.Error()
	for _, secret := range []string{"ultraSecretPW", "tokSecret99"} {
		if strings.Contains(msg, secret) {
			t.Fatalf("RedactError leaked %q in %q", secret, msg)
		}
	}
	var uErr *url.Error
	if !errors.As(redacted, &uErr) {
		t.Fatalf("expected RedactError to preserve *url.Error unwrapping, got %T", redacted)
	}
	if strings.Contains(uErr.URL, "ultraSecretPW") || strings.Contains(uErr.URL, "tokSecret99") {
		t.Fatalf("unwrapped *url.Error.URL leaked secret: %q", uErr.URL)
	}

	// Verify libpq keyword/value and extraSecrets redaction plus sentinel preservation.
	dsnErr := fmt.Errorf("dsn error host=192.0.2.10 password='quoted secret pw' customHdr=customTopSecret: %w", context.DeadlineExceeded)
	redactedDSN := RedactError(dsnErr, "customTopSecret")
	if !errors.Is(redactedDSN, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded to remain matchable via errors.Is, got %v", redactedDSN)
	}
	for _, secret := range []string{"quoted secret pw", "customTopSecret"} {
		if strings.Contains(redactedDSN.Error(), secret) {
			t.Fatalf("RedactError leaked %q in %q", secret, redactedDSN.Error())
		}
	}

	ext := SanitizeExternalErrorText("connect postgres://op:secretPass@192.0.2.44:5432/ctd (SQLSTATE 28P01) Authorization: Bearer myJwtTok sk-ant-1234567890")
	for _, tok := range []string{"secretPass", "192.0.2.44", "28P01", "SQLSTATE", "myJwtTok", "sk-ant-1234567890", "postgres://"} {
		if strings.Contains(ext, tok) {
			t.Fatalf("SanitizeExternalErrorText leaked %q in %q", tok, ext)
		}
	}
}
