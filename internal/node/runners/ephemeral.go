package runners

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
)

const (
	maxCapturedStreamBytes  = 16 * 1024
	maxEphemeralScriptBytes = 256 * 1024
	sandboxPATH             = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

var safeEnvKeyRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

var blockedEnvKeys = map[string]struct{}{
	"PATH":               {},
	"HOME":               {},
	"TMPDIR":             {},
	"TMP":                {},
	"TEMP":               {},
	"USER":               {},
	"LOGNAME":            {},
	"SHELL":              {},
	"SHELLOPTS":          {},
	"BASHOPTS":           {},
	"BASH_ENV":           {},
	"ENV":                {},
	"SHINIT":             {},
	"IFS":                {},
	"CDPATH":             {},
	"GLOBIGNORE":         {},
	"FPATH":              {},
	"ZDOTDIR":            {},
	"PS1":                {},
	"PS2":                {},
	"PS3":                {},
	"PS4":                {},
	"PROMPT_COMMAND":     {},
	"NODE_OPTIONS":       {},
	"PERL5OPT":           {},
	"PERL5LIB":           {},
	"PERL5DB":            {},
	"PERLLIB":            {},
	"RUBYOPT":            {},
	"RUBYLIB":            {},
	"GCONV_PATH":         {},
	"LOCPATH":            {},
	"NLSPATH":            {},
	"HOSTALIASES":        {},
	"SSL_CERT_FILE":      {},
	"SSL_CERT_DIR":       {},
	"SSLKEYLOGFILE":      {},
	"REQUESTS_CA_BUNDLE": {},
	"JAVA_TOOL_OPTIONS":  {},
	"JDK_JAVA_OPTIONS":   {},
	"AWKPATH":            {},
	"AWKLIBPATH":         {},
	"GOFLAGS":            {},
	"GOPATH":             {},
	"GOBIN":              {},
	"GOCACHE":            {},
	"GOMODCACHE":         {},
}

var blockedEnvPrefixes = []string{
	"LD_",
	"DYLD_",
	"BASH_",
	"PYTHON",
	"OPENSSL_",
	"PIGEONS_",
	"XDG_",
	"GIT_",
	"CURL_",
	"PG",
	"PERL",
	"RUBY",
	"NODE_",
	"LUA_",
}

func validateSandboxEnvVar(key, val string) (string, error) {
	k := strings.TrimSpace(key)
	if !safeEnvKeyRe.MatchString(k) {
		return "", fmt.Errorf("disallowed environment variable name %q (must match ^[A-Z][A-Z0-9_]{0,63}$)", key)
	}
	if _, blocked := blockedEnvKeys[k]; blocked {
		return "", fmt.Errorf("environment variable %q is blocked by sandbox policy", k)
	}
	for _, prefix := range blockedEnvPrefixes {
		if strings.HasPrefix(k, prefix) {
			return "", fmt.Errorf("environment variable %q (prefix %s*) is blocked by sandbox policy", k, prefix)
		}
	}
	if strings.ContainsRune(val, 0) {
		return "", fmt.Errorf("environment variable %q contains NUL byte", k)
	}
	return k + "=" + val, nil
}

func normalizeAllowedInterpreters(raw []string) []string {
	if len(raw) == 0 {
		return []string{"sh", "bash", "python3"}
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		norm := runner.NormalizeInterpreter(item)
		if norm == "" {
			continue
		}
		if _, exists := seen[norm]; !exists {
			seen[norm] = struct{}{}
			out = append(out, norm)
		}
	}
	if len(out) == 0 {
		return []string{"sh", "bash", "python3"}
	}
	return out
}

func resolveSandboxInterpreter(interp string) (string, error) {
	if interp == "" || interp == "." || interp == ".." || strings.ContainsAny(interp, `/\`) {
		return "", fmt.Errorf("invalid interpreter name %q", interp)
	}
	for _, dir := range strings.Split(sandboxPATH, ":") {
		candidate := filepath.Join(dir, interp)
		if runtime.GOOS == "windows" {
			for _, winPath := range []string{candidate, candidate + ".exe"} {
				if info, err := os.Stat(winPath); err == nil && !info.IsDir() {
					return winPath, nil
				}
			}
			continue
		}
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			continue
		}
		if info.Mode()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("executable file not found in fixed sandbox PATH (%s)", sandboxPATH)
}

func interpreterArgs(interp, scriptPath string) []string {
	switch {
	case interp == "bash":
		return []string{"--norc", "--noprofile", scriptPath}
	case interp == "python3" || strings.HasPrefix(interp, "python"):
		return []string{"-I", scriptPath}
	default:
		return []string{scriptPath}
	}
}

// isScriptHashAllowed verifies script execution against policy.AllowedScriptHashes.
// Supported entry formats:
//   - "<interpreter>:<sha256>" where <sha256> matches sha256(scriptBody) (or bound sha256(interpreter + "\x00" + scriptBody))
//   - "<sha256>" matching bound sha256(interpreter + "\x00" + scriptBody)
//   - legacy raw "<sha256(scriptBody)>" ONLY when allowedInterpreters unambiguously pins a single interpreter matching interp.
func isScriptHashAllowed(interp, bodyHash, boundHash string, allowedInterpreters, allowedHashes []string) bool {
	var singleAllowed string
	if len(allowedInterpreters) == 1 {
		singleAllowed = allowedInterpreters[0]
	}

	for _, h := range allowedHashes {
		norm, ok := runner.NormalizeAllowedScriptHashEntry(h)
		if !ok {
			continue
		}
		if entryInterp, entryHash, hasInterp := strings.Cut(norm, ":"); hasInterp {
			if entryInterp == interp && (entryHash == bodyHash || entryHash == boundHash) {
				return true
			}
			continue
		}
		if norm == boundHash {
			return true
		}
		if singleAllowed != "" && singleAllowed == interp && norm == bodyHash {
			return true
		}
	}
	return false
}

func runCommandWithPipes(
	ctx context.Context,
	cmd *exec.Cmd,
	stdoutDst io.Writer,
	stderrDst io.Writer,
	startAndWait func(ctx context.Context, cmd *exec.Cmd, onStarted func()) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		return fmt.Errorf("create stderr pipe: %w", err)
	}

	closeFile := func(f **os.File) {
		if *f != nil {
			_ = (*f).Close()
			*f = nil
		}
	}
	defer func() {
		closeFile(&stdoutW)
		closeFile(&stderrW)
		closeFile(&stdoutR)
		closeFile(&stderrR)
	}()

	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	var pipeWG sync.WaitGroup
	pipeErrCh := make(chan error, 2)

	onStarted := func() {
		// Close parent write ends immediately so EOF is observed once all process-group writers exit.
		closeFile(&stdoutW)
		closeFile(&stderrW)

		copyStream := func(dst io.Writer, src *os.File) {
			defer pipeWG.Done()
			if dst == nil {
				dst = io.Discard
			}
			if _, copyErr := io.Copy(dst, src); copyErr != nil && !errors.Is(copyErr, os.ErrClosed) {
				pipeErrCh <- copyErr
			}
		}
		pipeWG.Add(2)
		go copyStream(stdoutDst, stdoutR)
		go copyStream(stderrDst, stderrR)
	}

	runErr := startAndWait(ctx, cmd, onStarted)

	pipesDone := make(chan struct{})
	go func() {
		pipeWG.Wait()
		close(pipesDone)
	}()

	const waitDelay = 2 * time.Second
	timer := time.NewTimer(waitDelay)
	select {
	case <-pipesDone:
		timer.Stop()
	case <-timer.C:
		closeFile(&stdoutR)
		closeFile(&stderrR)
		<-pipesDone
	}

	if runErr != nil {
		return runErr
	}
	select {
	case pErr := <-pipeErrCh:
		return pErr
	default:
		return nil
	}
}

func evalProcessWaitResult(ctx context.Context, state *os.ProcessState, waitErr error) error {
	if waitErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return waitErr
	}
	if state != nil && !state.Success() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &exec.ExitError{ProcessState: state}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

type boundedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	total := len(p)
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return total, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, _ = b.buf.Write(p)
	return total, nil
}

func (b *boundedBuffer) String() string {
	s := b.buf.String()
	if b.truncated {
		return s + "\n...[truncated]"
	}
	return s
}

// EphemeralRunner executes ad-hoc diagnostic scripts synthesized by PigeonCare or an operator
// inside an isolated temporary directory with strict interpreter allowlists, SHA-256 verification,
// env-var sanitization, and process-group termination on timeout/abort.
type EphemeralRunner struct{}

func init() {
	runner.Register(&EphemeralRunner{})
}

func (e *EphemeralRunner) Name() string {
	return "ephemeral"
}

func (e *EphemeralRunner) Run(ctx context.Context, nodeID string, params map[string]any, pub core.TelemetryPublisher) error {
	policy := runner.PolicyFromContext(ctx)
	reqID, _ := params["request_id"].(string)

	emitErr := func(reason string) error {
		ev := core.NewCTDPayload(nodeID, "runner:ephemeral", "runner-error").
			WithPillar(core.PillarRunner).
			AddParam("error", reason)
		if reqID != "" {
			ev.AddParam("request_id", reqID)
		}
		if pub != nil {
			_ = pub.Publish(ev)
		}
		return errors.New(reason)
	}

	if !policy.AllowEphemeral {
		return emitErr("ephemeral runners are disabled by node policy")
	}

	scriptBody, _ := params["script_body"].(string)
	if strings.TrimSpace(scriptBody) == "" {
		return emitErr("missing script_body parameter")
	}
	if len(scriptBody) > maxEphemeralScriptBytes {
		return emitErr(fmt.Sprintf("ephemeral script_body exceeds %d byte limit (%d bytes)", maxEphemeralScriptBytes, len(scriptBody)))
	}

	rawInterp, _ := params["interpreter"].(string)
	interp := runner.NormalizeInterpreter(rawInterp)
	if interp == "" {
		return emitErr(fmt.Sprintf("invalid interpreter %q", rawInterp))
	}

	allowed := normalizeAllowedInterpreters(policy.AllowedInterpreters)
	if !slices.Contains(allowed, interp) {
		return emitErr(fmt.Sprintf("interpreter %q is not in node allowlist (%v)", interp, allowed))
	}

	interpBin, err := resolveSandboxInterpreter(interp)
	if err != nil {
		return emitErr(fmt.Sprintf("interpreter %q not installed on node: %v", interp, err))
	}

	actualHash := runner.ComputeScriptSHA256(scriptBody)
	boundHash := runner.ComputeBoundScriptSHA256(interp, scriptBody)
	if expectedHash, _ := params["sha256"].(string); strings.TrimSpace(expectedHash) != "" {
		exp := strings.ToLower(strings.TrimSpace(expectedHash))
		if exp != actualHash && exp != boundHash {
			return emitErr("ephemeral script SHA-256 digest mismatch")
		}
	}
	if len(policy.AllowedScriptHashes) > 0 {
		if !isScriptHashAllowed(interp, actualHash, boundHash, allowed, policy.AllowedScriptHashes) {
			return emitErr("ephemeral script SHA-256 is not in node allowed_script_hashes policy")
		}
	}

	var customEnv []string
	if envMap, ok := params["env"].(map[string]any); ok {
		for k, v := range envMap {
			kv, envErr := validateSandboxEnvVar(k, fmt.Sprint(v))
			if envErr != nil {
				return emitErr(envErr.Error())
			}
			customEnv = append(customEnv, kv)
		}
	} else if envStrMap, ok := params["env"].(map[string]string); ok {
		for k, v := range envStrMap {
			kv, envErr := validateSandboxEnvVar(k, v)
			if envErr != nil {
				return emitErr(envErr.Error())
			}
			customEnv = append(customEnv, kv)
		}
	}
	slices.Sort(customEnv)

	timeoutSec := parseIntParam(params["timeout_sec"], 30)
	maxTimeout := policy.MaxTimeoutSec
	if maxTimeout <= 0 {
		maxTimeout = 300
	}
	if timeoutSec > maxTimeout {
		timeoutSec = maxTimeout
	}

	workDir, err := os.MkdirTemp("", "cp-ephemeral-*")
	if err != nil {
		return emitErr(fmt.Sprintf("failed creating ephemeral workspace: %v", err))
	}
	defer os.RemoveAll(workDir)

	ext := ".sh"
	if strings.HasPrefix(interp, "python") {
		ext = ".py"
	}
	scriptPath := filepath.Join(workDir, "task"+ext)
	if err := os.WriteFile(scriptPath, []byte(scriptBody), 0o700); err != nil {
		return emitErr(fmt.Sprintf("failed writing ephemeral script: %v", err))
	}

	shortHash := actualHash
	if len(shortHash) > 12 {
		shortHash = shortHash[:12]
	}
	artifactID := "ephemeral:" + shortHash
	runID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	startedAt := time.Now()

	startEv := core.NewCTDPayload(nodeID, "runner:ephemeral", "runner-start").
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("timeout_sec", timeoutSec).
		AddParam("interpreter", interp).
		AddParam("sha256", actualHash).
		WithArtifact(core.Artifact{
			ID:     artifactID,
			Kind:   "ephemeral_runner",
			NodeID: nodeID,
			Meta: map[string]string{
				"interpreter": interp,
				"sha256":      actualHash,
			},
		})
	if reqID != "" {
		startEv.AddParam("request_id", reqID)
	}
	if pub != nil {
		_ = pub.Publish(startEv)
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	cmd := exec.Command(interpBin, interpreterArgs(interp, scriptPath)...)
	cmd.Dir = workDir
	cmdEnv := make([]string, 0, 4+len(customEnv))
	cmdEnv = append(cmdEnv,
		"PATH="+sandboxPATH,
		"HOME="+workDir,
		"TMPDIR="+workDir,
	)
	cmdEnv = append(cmdEnv, customEnv...)
	cmdEnv = append(cmdEnv, "PIGEONS_NODE_ID="+nodeID)
	cmd.Env = cmdEnv

	stdoutBuf := &boundedBuffer{limit: maxCapturedStreamBytes}
	stderrBuf := &boundedBuffer{limit: maxCapturedStreamBytes}

	runErr := runProcessGroupCmd(execCtx, cmd, stdoutBuf, stderrBuf)
	exitCode := 0
	endStatus := "runner-end"

	if runErr != nil {
		if ctx.Err() != nil {
			endStatus = "runner-aborted"
			exitCode = -1
		} else if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			endStatus = "runner-error"
			exitCode = 124
		} else {
			endStatus = "runner-error"
			var exitErr *exec.ExitError
			if errors.As(runErr, &exitErr) {
				exitCode = exitErr.ExitCode()
			} else {
				exitCode = 1
			}
		}
	}

	endEv := core.NewCTDPayload(nodeID, "runner:ephemeral", endStatus).
		WithPillar(core.PillarRunner).
		AddMetric("run_id", runID).
		AddMetric("exit_code", exitCode).
		AddMetric("elapsed_ms", time.Since(startedAt).Milliseconds()).
		AddMetric("stdout", strings.TrimSpace(stdoutBuf.String())).
		AddMetric("stderr", strings.TrimSpace(stderrBuf.String())).
		AddParam("interpreter", interp).
		AddParam("sha256", actualHash)
	if reqID != "" {
		endEv.AddParam("request_id", reqID)
	}
	if pub != nil {
		_ = pub.Publish(endEv)
	}

	return runErr
}
