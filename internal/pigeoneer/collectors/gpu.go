package collectors

import (
	"context"
	"log"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

// monitorGPU detects local GPU hardware and launches matching vendor collectors.
func monitorGPU(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	if monitorGPUPlatform(ctx, pub, nodeID) {
		return
	}
	if isInstalled("rocm-smi") {
		log.Println("[GPU Router] AMD hardware detected. Booting ROCm collector.")
		go monitorGPUAMD(ctx, pub, nodeID)
	}
	if isInstalled("intel_gpu_top") {
		log.Println("[GPU Router] Intel hardware detected. Booting intel_gpu_top collector.")
		go monitorGPUIntel(ctx, pub, nodeID)
	}
	if isInstalled("nvidia-smi") {
		log.Println("[GPU Router] Nvidia hardware detected. Booting nvidia-smi collector.")
		go monitorGPUNvidia(ctx, pub, nodeID)
	}
}
