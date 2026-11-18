package observers

import (
	"context"
	"log"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// observeGPU detects local GPU hardware and launches matching vendor observers.
func observeGPU(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if observeGPUPlatform(ctx, pub, nodeID) {
		return
	}
	if isInstalled("rocm-smi") {
		log.Println("[GPU Observer] AMD hardware detected. Booting ROCm observer.")
		go observeGPUAMD(ctx, pub, nodeID)
	}
	if isInstalled("intel_gpu_top") {
		log.Println("[GPU Observer] Intel hardware detected. Booting intel_gpu_top observer.")
		go observeGPUIntel(ctx, pub, nodeID)
	}
	if isInstalled("nvidia-smi") {
		log.Println("[GPU Observer] Nvidia hardware detected. Booting nvidia-smi observer.")
		go observeGPUNvidia(ctx, pub, nodeID)
	}
}
