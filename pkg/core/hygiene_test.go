package core

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepositoryUpstreamVendorNeutrality(t *testing.T) {
	repoRoot := filepath.Join("..", "..")

	// Assemble downstream appliance & sibling-project tokens dynamically so this test file stays clean.
	forbidden := []string{
		"braker" + "rak",
		"braker" + "box",
		"brak" + "sky",
		"braker" + "bench",
		"bracker" + "bench",
		"vision" + "bench",
		"vision" + "ferret",
		"ada" + "lovespace",
		"ham" + "stak",
		"babbage" + "box",
		"rv" + "dk",
		"riscv-" + "data-kit",
		"riscv_" + "data_kit",
		"fits " + "frame",
		"calibrated_" + "into",
		"io_" + "uring",
		"lil" + "blu",
		"big" + "red",
	}

	skipExt := map[string]bool{
		".png": true, ".gif": true, ".jpg": true, ".jpeg": true, ".ico": true, ".sum": true,
		".exe": true, ".bin": true,
	}

	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "dist" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if skipExt[strings.ToLower(filepath.Ext(d.Name()))] {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lower := strings.ToLower(string(raw))
		for _, bad := range forbidden {
			if strings.Contains(lower, bad) {
				t.Errorf("downstream appliance/sibling token %q found in %s", bad, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir failed: %v", err)
	}
}
