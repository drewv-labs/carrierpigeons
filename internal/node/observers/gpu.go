package observers

import (
	"context"
	"log"
	"os/exec"
	"runtime"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

func resolveNvidiaSMIBin() string {
	if p, err := exec.LookPath("nvidia-smi"); err == nil {
		return p
	}
	if runtime.GOOS == "windows" {
		const winPath = `C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe`
		if pathExists(winPath) {
			return winPath
		}
	}
	return ""
}

// observeGPU detects local GPU hardware and launches matching vendor observers, blocking until all exit.
func observeGPU(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if observeGPUPlatform(ctx, pub, nodeID) {
		return
	}
	var wg sync.WaitGroup
	if hasSysfsAMDGPU("/sys") {
		log.Println("[GPU Observer] AMD drm/amdgpu sysfs hardware detected. Booting zero-fork sysfs observer.")
		wg.Add(1)
		go func() {
			defer wg.Done()
			observeGPUAMDSysfs(ctx, pub, nodeID)
		}()
	} else if isInstalled("rocm-smi") {
		log.Println("[GPU Observer] AMD ROCm CLI detected. Booting rocm-smi observer.")
		wg.Add(1)
		go func() {
			defer wg.Done()
			observeGPUAMD(ctx, pub, nodeID)
		}()
	}
	if isInstalled("intel_gpu_top") {
		log.Println("[GPU Observer] Intel hardware detected. Booting intel_gpu_top observer.")
		wg.Add(1)
		go func() {
			defer wg.Done()
			observeGPUIntel(ctx, pub, nodeID)
		}()
	}
	if nvBin := resolveNvidiaSMIBin(); nvBin != "" {
		log.Printf("[GPU Observer] Nvidia hardware detected (%s). Booting RTX/CUDA observer.", nvBin)
		wg.Add(1)
		go func() {
			defer wg.Done()
			observeGPUNvidiaWithBin(ctx, pub, nodeID, nvBin)
		}()
	}
	wg.Wait()
}
