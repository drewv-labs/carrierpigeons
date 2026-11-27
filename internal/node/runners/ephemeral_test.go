package runners

import (
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"
)

type fakeFileInfo struct {
	name  string
	mode  fs.FileMode
	isDir bool
}

func (f fakeFileInfo) Name() string       { return f.name }
func (f fakeFileInfo) Size() int64        { return 1024 }
func (f fakeFileInfo) Mode() fs.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.isDir }
func (f fakeFileInfo) Sys() any           { return nil }

func TestResolveSandboxInterpreterWindowsDriveColonAndPATHEXT(t *testing.T) {
	winEnv := map[string]string{
		"SystemRoot":        `D:\Windows`,
		"ProgramFiles":      `D:\Program Files`,
		"ProgramFiles(x86)": `D:\Program Files (x86)`,
	}
	pathList := sandboxPathForOS("windows", func(k string) string { return winEnv[k] })
	if strings.Contains(pathList, "/usr/bin") {
		t.Fatalf("expected Windows sandbox PATH not to use POSIX paths, got %q", pathList)
	}
	if !strings.Contains(pathList, `D:\Windows\System32`) || !strings.Contains(pathList, ";") {
		t.Fatalf("expected semicolon-delimited Windows sandbox PATH containing System32, got %q", pathList)
	}

	// Inject relative and drive-relative entries alongside valid Windows drive paths;
	// splitSandboxPathList MUST ignore relative/drive-relative paths without mangling drive colons.
	poisonedList := `.;relative\bin;C:drive-relative;` + pathList
	dirs := splitSandboxPathList("windows", poisonedList)
	for _, d := range dirs {
		if d == "." || strings.Contains(d, "relative") {
			t.Fatalf("expected relative/drive-relative entry %q to be rejected from Windows sandbox dirs", d)
		}
	}

	files := map[string]fakeFileInfo{
		`d:\program files\git\usr\bin\bash.exe`: {name: "bash.exe", mode: 0o644},
		`d:\program files\python312\python.exe`: {name: "python.exe", mode: 0o644},
		`d:\windows\system32\customtool.cmd`:    {name: "customtool.cmd", mode: 0o644},
		`c:drive-relative\bash.exe`:             {name: "bash.exe", mode: 0o644},
		`relative\bin\bash.exe`:                 {name: "bash.exe", mode: 0o644},
	}
	statFn := func(p string) (os.FileInfo, error) {
		if info, ok := files[strings.ToLower(p)]; ok {
			return info, nil
		}
		return nil, os.ErrNotExist
	}

	gotBash, err := resolveSandboxInterpreterWithStat("windows", poisonedList, ".EXE;.CMD", "bash", statFn)
	if err != nil {
		t.Fatalf("resolve bash on Windows failed: %v", err)
	}
	if !strings.EqualFold(gotBash, `D:\Program Files\Git\usr\bin\bash.exe`) {
		t.Fatalf("got bash=%q, want D:\\Program Files\\Git\\usr\\bin\\bash.exe", gotBash)
	}

	gotPy3, err := resolveSandboxInterpreterWithStat("windows", poisonedList, ".EXE;.CMD", "python3", statFn)
	if err != nil {
		t.Fatalf("resolve python3 on Windows failed: %v", err)
	}
	if !strings.EqualFold(gotPy3, `D:\Program Files\Python312\python.exe`) {
		t.Fatalf("got python3=%q, want D:\\Program Files\\Python312\\python.exe", gotPy3)
	}

	gotCmd, err := resolveSandboxInterpreterWithStat("windows", poisonedList, ".EXE;.CMD", "customtool", statFn)
	if err != nil {
		t.Fatalf("resolve .CMD via PATHEXT failed: %v", err)
	}
	if !strings.EqualFold(gotCmd, `D:\Windows\System32\customtool.cmd`) {
		t.Fatalf("got customtool=%q, want D:\\Windows\\System32\\customtool.cmd", gotCmd)
	}

	// Reject drive-letter/path-separator traversal attempts in interpreter name.
	for _, bad := range []string{"", ".", "..", `C:python3`, `..\bash`, `/bin/sh`, `sub\python`} {
		if _, err := resolveSandboxInterpreterWithStat("windows", pathList, ".EXE", bad, statFn); err == nil {
			t.Fatalf("expected invalid interpreter %q to be rejected", bad)
		}
	}
}

func TestResolveSandboxInterpreterPOSIXRequiresExecBitAndBlocksWindowsEnvInjection(t *testing.T) {
	files := map[string]fakeFileInfo{
		"/usr/local/bin/python3": {name: "python3", mode: 0o644}, // non-executable -> skip
		"/usr/bin/python3":       {name: "python3", mode: 0o755}, // executable -> match
	}
	statFn := func(p string) (os.FileInfo, error) {
		if info, ok := files[p]; ok {
			return info, nil
		}
		return nil, os.ErrNotExist
	}

	got, err := resolveSandboxInterpreterWithStat("linux", posixSandboxPATH, "", "python3", statFn)
	if err != nil {
		t.Fatalf("resolve python3 on POSIX failed: %v", err)
	}
	if got != "/usr/bin/python3" {
		t.Fatalf("expected /usr/bin/python3, got %q", got)
	}

	for _, key := range []string{"SYSTEMROOT", "WINDIR", "PATHEXT", "COMSPEC", "USERPROFILE", "APPDATA", "LOCALAPPDATA"} {
		if _, err := validateSandboxEnvVar(key, `C:\evil`); err == nil {
			t.Fatalf("expected Windows sandbox env key %q to be blocked", key)
		}
	}
}
