package jsonutil

import (
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const maxSanitizeDepth = 64

var (
	timeType       = reflect.TypeOf(time.Time{})
	rawMsgType     = reflect.TypeOf(json.RawMessage(nil))
	jsonNumberType = reflect.TypeOf(json.Number(""))
	anyType        = reflect.TypeOf((*any)(nil)).Elem()

	errURLRe          = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"'` + "`" + `<>]+`)
	errBearerRe       = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;'` + "`" + `"]+`)
	errAuthHeaderRe   = regexp.MustCompile(`(?i)\b(authorization|x-api-key)\s*[:=]\s*[^\r\n,;]+|\bx-api-key\s+[^\s,;'` + "`" + `"]+`)
	errAPIKeyTokRe    = regexp.MustCompile(`\bsk-[a-zA-Z0-9_-]{8,}\b`)
	errSecretKeyValRe = regexp.MustCompile(`(?i)\b(password|passwd|pwd|api_key|apikey|token|secret|passfile|client_secret)\s*=\s*(?:'(?:\\.|[^'])*'|"(?:\\.|[^"])*"|\S+)`)
	errAllKeyValRe    = regexp.MustCompile(`(?i)\b(password|passwd|pwd|api_key|apikey|token|secret|passfile|client_secret|host|hostaddr|user)\s*=\s*(?:'(?:\\.|[^'])*'|"(?:\\.|[^"])*"|\S+)`)
	errDialRe         = regexp.MustCompile(`(?i)\bdial\s+(?:tcp|udp|unix)\s+[^\s:]+(?::\d+)?`)
	errSQLStateRe     = regexp.MustCompile(`(?i)\bSQLSTATE\s+[A-Z0-9]{5}\b`)
	errIPPortRe       = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
)

type visitKey struct {
	typ reflect.Type
	ptr uintptr
	len int
}

type sanitizeState struct {
	visiting map[visitKey]struct{}
}

func (s *sanitizeState) enter(k visitKey) bool {
	if s.visiting == nil {
		s.visiting = make(map[visitKey]struct{}, 8)
	}
	if _, exists := s.visiting[k]; exists {
		return false
	}
	s.visiting[k] = struct{}{}
	return true
}

func (s *sanitizeState) leave(k visitKey) {
	delete(s.visiting, k)
}

// SanitizeValue recursively replaces non-finite floats (NaN, +Inf, -Inf) and reference cycles
// with JSON-safe nil without mutating caller-owned maps, slices, or structs.
func SanitizeValue(v any) any {
	if v == nil {
		return nil
	}
	var st sanitizeState
	sv, changed := st.sanitizeReflectValue(reflect.ValueOf(v), 0)
	if !changed {
		return v
	}
	if !sv.IsValid() {
		return nil
	}
	return sv.Interface()
}

// IsNonFiniteFloat reports whether f is NaN or positive/negative infinity.
func IsNonFiniteFloat(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}

func canHoldNil(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return true
	default:
		return false
	}
}

func (s *sanitizeState) sanitizeReflectValue(v reflect.Value, depth int) (reflect.Value, bool) {
	if !v.IsValid() || depth > maxSanitizeDepth {
		return v, false
	}
	if v.Type() == timeType || v.Type() == rawMsgType {
		return v, false
	}

	if v.Type() == jsonNumberType {
		str := strings.TrimSpace(v.String())
		if f, err := strconv.ParseFloat(str, 64); err == nil && IsNonFiniteFloat(f) {
			return reflect.Value{}, true
		}
		return v, false
	}

	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		if IsNonFiniteFloat(v.Float()) {
			return reflect.Value{}, true
		}
		return v, false

	case reflect.Interface:
		if v.IsNil() {
			return v, false
		}
		elem, changed := s.sanitizeReflectValue(v.Elem(), depth+1)
		if !changed {
			return v, false
		}
		if !elem.IsValid() {
			return reflect.Zero(v.Type()), true
		}
		if elem.Type().AssignableTo(v.Type()) {
			out := reflect.New(v.Type()).Elem()
			out.Set(elem)
			return out, true
		}
		return v, false

	case reflect.Pointer:
		if v.IsNil() {
			return v, false
		}
		trackCycle := v.Type().Elem().Size() > 0
		var vk visitKey
		if trackCycle {
			vk = visitKey{typ: v.Type(), ptr: v.Pointer()}
			if !s.enter(vk) {
				return reflect.Zero(v.Type()), true
			}
			defer s.leave(vk)
		}
		elem, changed := s.sanitizeReflectValue(v.Elem(), depth+1)
		if !changed {
			return v, false
		}
		if !elem.IsValid() {
			return reflect.Zero(v.Type()), true
		}
		if elem.Type().AssignableTo(v.Type().Elem()) {
			ptr := reflect.New(v.Type().Elem())
			ptr.Elem().Set(elem)
			return ptr, true
		}
		return elem, true

	case reflect.Map:
		if v.IsNil() || v.Len() == 0 {
			return v, false
		}
		vk := visitKey{typ: v.Type(), ptr: v.Pointer()}
		if !s.enter(vk) {
			return reflect.Value{}, true
		}
		defer s.leave(vk)

		elemType := v.Type().Elem()
		outElemType := elemType
		type kv struct {
			k reflect.Value
			v reflect.Value
		}
		entries := make([]kv, 0, v.Len())
		changed := false

		iter := v.MapRange()
		for iter.Next() {
			mk := iter.Key()
			mv := iter.Value()
			sv, c := s.sanitizeReflectValue(mv, depth+1)
			if c {
				changed = true
				if !sv.IsValid() && !canHoldNil(outElemType) {
					outElemType = anyType
				} else if sv.IsValid() && !sv.Type().AssignableTo(outElemType) {
					outElemType = anyType
				}
			}
			entries = append(entries, kv{k: mk, v: sv})
		}
		if !changed {
			return v, false
		}
		out := reflect.MakeMapWithSize(reflect.MapOf(v.Type().Key(), outElemType), len(entries))
		for _, e := range entries {
			if !e.v.IsValid() {
				out.SetMapIndex(e.k, reflect.Zero(outElemType))
				continue
			}
			if e.v.Type().AssignableTo(outElemType) {
				out.SetMapIndex(e.k, e.v)
				continue
			}
			wrapped := reflect.New(outElemType).Elem()
			wrapped.Set(e.v)
			out.SetMapIndex(e.k, wrapped)
		}
		return out, true

	case reflect.Slice:
		if v.IsNil() || v.Len() == 0 || v.Type().Elem().Kind() == reflect.Uint8 {
			return v, false
		}
		trackCycle := v.Type().Elem().Size() > 0
		if trackCycle {
			vk := visitKey{typ: v.Type(), ptr: v.Pointer(), len: v.Len()}
			if !s.enter(vk) {
				return reflect.Value{}, true
			}
			defer s.leave(vk)
		}
		return s.sanitizeSequence(v, depth)

	case reflect.Array:
		if v.Len() == 0 || v.Type().Elem().Kind() == reflect.Uint8 {
			return v, false
		}
		return s.sanitizeSequence(v, depth)

	case reflect.Struct:
		t := v.Type()
		n := v.NumField()
		var sanitizedFields map[int]reflect.Value
		changed := false
		needsMap := false
		for i := 0; i < n; i++ {
			sf := t.Field(i)
			fv := v.Field(i)
			isEmbeddedStruct := sf.Anonymous && isStructOrPtrToStruct(fv.Type())
			if !sf.IsExported() && !isEmbeddedStruct {
				continue
			}
			if _, _, _, skip := parseJSONTag(sf.Tag.Get("json")); skip {
				continue
			}
			sv, c := s.sanitizeReflectValue(fv, depth+1)
			if !c {
				continue
			}
			changed = true
			if sanitizedFields == nil {
				sanitizedFields = make(map[int]reflect.Value)
			}
			sanitizedFields[i] = sv
			if !sf.IsExported() || (!sv.IsValid() && !canHoldNil(fv.Type())) || (sv.IsValid() && !sv.Type().AssignableTo(fv.Type())) {
				needsMap = true
			}
		}
		if !changed {
			return v, false
		}
		if needsMap {
			return reflect.ValueOf(structToJSONMap(v, sanitizedFields)), true
		}
		out := reflect.New(t).Elem()
		out.Set(v)
		for i, sv := range sanitizedFields {
			dst := out.Field(i)
			if !sv.IsValid() {
				dst.SetZero()
			} else {
				dst.Set(sv)
			}
		}
		return out, true

	default:
		return v, false
	}
}

func (s *sanitizeState) sanitizeSequence(v reflect.Value, depth int) (reflect.Value, bool) {
	n := v.Len()
	elemType := v.Type().Elem()
	outElemType := elemType
	items := make([]reflect.Value, n)
	changed := false

	for i := 0; i < n; i++ {
		sv, c := s.sanitizeReflectValue(v.Index(i), depth+1)
		if c {
			changed = true
			if !sv.IsValid() && !canHoldNil(outElemType) {
				outElemType = anyType
			} else if sv.IsValid() && !sv.Type().AssignableTo(outElemType) {
				outElemType = anyType
			}
		}
		items[i] = sv
	}
	if !changed {
		return v, false
	}

	if v.Kind() == reflect.Array && outElemType == elemType {
		out := reflect.New(v.Type()).Elem()
		for i, item := range items {
			if !item.IsValid() {
				out.Index(i).SetZero()
			} else {
				out.Index(i).Set(item)
			}
		}
		return out, true
	}

	out := reflect.MakeSlice(reflect.SliceOf(outElemType), n, n)
	for i, item := range items {
		if !item.IsValid() {
			continue
		}
		out.Index(i).Set(item)
	}
	return out, true
}

func parseJSONTag(tag string) (name string, omitEmpty, omitZero, skip bool) {
	if tag == "-" {
		return "", false, false, true
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, opt := range parts[1:] {
		switch opt {
		case "omitempty":
			omitEmpty = true
		case "omitzero":
			omitZero = true
		}
	}
	return name, omitEmpty, omitZero, false
}

func isEmptyJSONReflectValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

func structToJSONMap(v reflect.Value, sanitizedFields map[int]reflect.Value) map[string]any {
	out := make(map[string]any, v.NumField())
	t := v.Type()

	// Process direct fields first so they take precedence over anonymous embedded struct fields.
	type embeddedEntry struct {
		val reflect.Value
	}
	var embedded []embeddedEntry

	for i := 0; i < v.NumField(); i++ {
		sf := t.Field(i)
		fv := v.Field(i)
		isEmbeddedStruct := sf.Anonymous && isStructOrPtrToStruct(fv.Type())
		if !sf.IsExported() && !isEmbeddedStruct {
			continue
		}
		tagName, omitEmpty, omitZero, skip := parseJSONTag(sf.Tag.Get("json"))
		if skip {
			continue
		}
		sv, hasSanitized := sanitizedFields[i]

		if sf.Anonymous && tagName == "" {
			eff := fv
			if hasSanitized {
				eff = sv
			}
			if eff.IsValid() && eff.Kind() == reflect.Pointer {
				if eff.IsNil() {
					continue
				}
				eff = eff.Elem()
			}
			if eff.IsValid() && (eff.Kind() == reflect.Struct || eff.Kind() == reflect.Map) {
				embedded = append(embedded, embeddedEntry{val: eff})
				continue
			}
		}
		if !sf.IsExported() {
			if hasSanitized && sv.IsValid() && sv.CanInterface() {
				if tagName == "" {
					tagName = sf.Name
				}
				out[tagName] = sv.Interface()
			} else if isEmbeddedStruct {
				eff := fv
				if eff.Kind() == reflect.Pointer {
					if eff.IsNil() {
						continue
					}
					eff = eff.Elem()
				}
				if tagName == "" {
					tagName = sf.Name
				}
				out[tagName] = structToJSONMap(eff, nil)
			}
			continue
		}

		if tagName == "" {
			tagName = sf.Name
		}
		if omitEmpty && !hasSanitized && isEmptyJSONReflectValue(fv) {
			continue
		}
		if omitEmpty && hasSanitized && sv.IsValid() && isEmptyJSONReflectValue(sv) && !isNonFiniteReflectFloat(fv) {
			continue
		}
		if omitZero && !hasSanitized && fv.IsZero() {
			continue
		}

		if !hasSanitized {
			out[tagName] = fv.Interface()
			continue
		}
		if !sv.IsValid() {
			out[tagName] = nil
			continue
		}
		out[tagName] = sv.Interface()
	}

	for _, emb := range embedded {
		var embMap map[string]any
		switch emb.val.Kind() {
		case reflect.Map:
			if emb.val.Type().Key().Kind() == reflect.String {
				embMap = make(map[string]any, emb.val.Len())
				iter := emb.val.MapRange()
				for iter.Next() {
					embMap[iter.Key().String()] = iter.Value().Interface()
				}
			}
		case reflect.Struct:
			embMap = structToJSONMap(emb.val, nil)
		}
		for k, val := range embMap {
			if _, exists := out[k]; !exists {
				out[k] = val
			}
		}
	}

	return out
}

func isStructOrPtrToStruct(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

func isNonFiniteReflectFloat(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	if v.Kind() == reflect.Float32 || v.Kind() == reflect.Float64 {
		return IsNonFiniteFloat(v.Float())
	}
	return false
}

func isSensitiveQueryKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	switch k {
	case "pass", "pwd", "auth", "apikey":
		return true
	}
	return strings.Contains(k, "password") ||
		strings.Contains(k, "passwd") ||
		strings.Contains(k, "secret") ||
		strings.Contains(k, "token") ||
		strings.Contains(k, "api_key")
}

func redactURLQuery(u *url.URL) {
	if u.RawQuery == "" {
		return
	}
	q := u.Query()
	changed := false
	for k := range q {
		if isSensitiveQueryKey(k) {
			q.Set(k, "xxxxx")
			changed = true
		}
	}
	if changed {
		u.RawQuery = q.Encode()
	}
}

// RedactURL masks userinfo passwords and sensitive query parameters in raw before logging or returning errors.
// Malformed or schemeless inputs are masked fail-closed so credentials are never echoed verbatim.
func RedactURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err == nil && u.Scheme != "" && u.Opaque == "" {
			redactURLQuery(u)
			return u.Redacted()
		}
		if schemeIdx := strings.Index(trimmed, "://"); schemeIdx >= 0 {
			rest := trimmed[schemeIdx+3:]
			pathIdx := strings.IndexAny(rest, "/?#")
			authority := rest
			pathOnly := ""
			if pathIdx >= 0 {
				authority = rest[:pathIdx]
				if rest[pathIdx] == '/' {
					endPath := strings.IndexAny(rest[pathIdx:], "?#")
					if endPath >= 0 {
						pathOnly = rest[pathIdx : pathIdx+endPath]
					} else {
						pathOnly = rest[pathIdx:]
					}
				}
			}
			if atIdx := strings.LastIndexByte(authority, '@'); atIdx >= 0 {
				userInfo := authority[:atIdx]
				host := authority[atIdx+1:]
				if colonIdx := strings.IndexByte(userInfo, ':'); colonIdx >= 0 {
					userInfo = userInfo[:colonIdx] + ":xxxxx"
				}
				return trimmed[:schemeIdx+3] + userInfo + "@" + host + pathOnly
			}
		}
		return "[redacted-invalid-url]"
	}
	if !strings.ContainsAny(trimmed, " \t\r\n=?#") {
		if atIdx := strings.LastIndexByte(trimmed, '@'); atIdx > 0 {
			userInfo := trimmed[:atIdx]
			hostPart := trimmed[atIdx+1:]
			if colonIdx := strings.IndexByte(userInfo, ':'); colonIdx >= 0 {
				return userInfo[:colonIdx] + ":xxxxx@" + hostPart
			}
		}
	}
	return "[redacted-invalid-url]"
}

func redactInlineURLMatch(match string) string {
	end := len(match)
	for end > 0 {
		ch := match[end-1]
		if ch == ')' || ch == ',' || ch == ';' {
			if ch == ')' && strings.Contains(match[:end-1], "(") {
				break
			}
			end--
			continue
		}
		break
	}
	return RedactURL(match[:end]) + match[end:]
}

func scrubExtraSecrets(s string, extraSecrets []string) string {
	for _, raw := range extraSecrets {
		if raw == "" {
			continue
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed != "" {
			s = strings.ReplaceAll(s, trimmed, "[redacted]")
		}
		if raw != trimmed && strings.TrimSpace(raw) != "" {
			s = strings.ReplaceAll(s, raw, "[redacted]")
		}
	}
	return s
}

// RedactCredentials scrubs URL userinfo/query secrets, auth headers, bearer tokens, sk-* keys,
// libpq/DSN secret key-value pairs, and any explicit extraSecrets from raw while keeping host/error context.
func RedactCredentials(raw string, extraSecrets ...string) string {
	if raw == "" {
		return ""
	}
	s := scrubExtraSecrets(raw, extraSecrets)
	s = errURLRe.ReplaceAllStringFunc(s, redactInlineURLMatch)
	s = errAuthHeaderRe.ReplaceAllString(s, "[redacted-auth]")
	s = errBearerRe.ReplaceAllString(s, "Bearer [redacted]")
	s = errAPIKeyTokRe.ReplaceAllString(s, "[redacted-key]")
	s = errSecretKeyValRe.ReplaceAllString(s, "$1=[redacted]")
	return s
}

type redactedWrapError struct {
	msg string
	err error
}

func (e *redactedWrapError) Error() string { return e.msg }
func (e *redactedWrapError) Unwrap() error { return e.err }

type redactedJoinError struct {
	msg  string
	errs []error
}

func (e *redactedJoinError) Error() string   { return e.msg }
func (e *redactedJoinError) Unwrap() []error { return e.errs }

type redactedTimeoutError struct {
	msg string
}

func (e *redactedTimeoutError) Error() string   { return e.msg }
func (e *redactedTimeoutError) Timeout() bool   { return true }
func (e *redactedTimeoutError) Temporary() bool { return true }

// RedactError unwraps *url.Error and wrapped error chains, redacting embedded URL userinfo/query secrets,
// DSN credentials, auth headers, and any explicit extraSecrets while preserving sentinel unwrapping
// (such as context.Canceled, context.DeadlineExceeded, and *url.Error).
func RedactError(err error, extraSecrets ...string) error {
	if err == nil {
		return nil
	}

	if uErr, ok := err.(*url.Error); ok {
		cleanURL := scrubExtraSecrets(RedactURL(uErr.URL), extraSecrets)
		cleanInner := RedactError(uErr.Err, extraSecrets...)
		if cleanURL == uErr.URL && cleanInner == uErr.Err {
			return err
		}
		return &url.Error{
			Op:  uErr.Op,
			URL: cleanURL,
			Err: cleanInner,
		}
	}

	if u, ok := err.(interface{ Unwrap() error }); ok {
		inner := u.Unwrap()
		cleanInner := RedactError(inner, extraSecrets...)
		cleanMsg := RedactCredentials(err.Error(), extraSecrets...)
		if cleanMsg == err.Error() && cleanInner == inner {
			return err
		}
		return &redactedWrapError{msg: cleanMsg, err: cleanInner}
	}

	if jm, ok := err.(interface{ Unwrap() []error }); ok {
		orig := jm.Unwrap()
		cleanChildren := make([]error, len(orig))
		changed := false
		for i, child := range orig {
			c := RedactError(child, extraSecrets...)
			if c != child {
				changed = true
			}
			cleanChildren[i] = c
		}
		cleanMsg := RedactCredentials(err.Error(), extraSecrets...)
		if !changed && cleanMsg == err.Error() {
			return err
		}
		return &redactedJoinError{msg: cleanMsg, errs: cleanChildren}
	}

	cleanMsg := RedactCredentials(err.Error(), extraSecrets...)
	if cleanMsg == err.Error() {
		return err
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return &redactedTimeoutError{msg: cleanMsg}
	}
	return errors.New(cleanMsg)
}

// SanitizeExternalErrorText scrubs URLs, auth headers, bearer/API tokens, DSN/host/IP endpoints,
// SQLSTATE codes, and control characters from raw error text before exposing it to external LLMs or MCP clients.
func SanitizeExternalErrorText(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "unknown error"
	}
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = errURLRe.ReplaceAllString(s, "[redacted-url]")
	s = errAuthHeaderRe.ReplaceAllString(s, "[redacted-auth]")
	s = errBearerRe.ReplaceAllString(s, "Bearer [redacted]")
	s = errAPIKeyTokRe.ReplaceAllString(s, "[redacted-key]")
	s = errAllKeyValRe.ReplaceAllString(s, "$1=[redacted]")
	s = errDialRe.ReplaceAllString(s, "dial [redacted-endpoint]")
	s = errSQLStateRe.ReplaceAllString(s, "[sqlstate-redacted]")
	s = errIPPortRe.ReplaceAllString(s, "[redacted-addr]")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 512 {
		return TruncateValidUTF8(s, 512)
	}
	return s
}

// TruncateValidUTF8 truncates s to at most maxBytes without splitting multi-byte UTF-8 runes.
func TruncateValidUTF8(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	for cut > 0 {
		r, size := utf8.DecodeLastRuneInString(s[:cut])
		if r != utf8.RuneError || size != 1 {
			break
		}
		cut--
	}
	return s[:cut]
}
