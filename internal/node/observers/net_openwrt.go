//go:build linux

package observers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
)

func observeNetOpenWrt(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	type UbusDevice struct {
		Up         bool `json:"up"`
		Statistics struct {
			RxBytes float64 `json:"rx_bytes"`
			TxBytes float64 `json:"tx_bytes"`
		} `json:"statistics"`
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			out, err := runCommand(ctx, defaultCmdTimeout, "ubus", "call", "network.device", "status")
			if err != nil {
				continue
			}

			var devices map[string]UbusDevice
			if err := json.Unmarshal(out, &devices); err != nil {
				log.Printf("[Net Observer] Failed to parse ubus output: %v", err)
				continue
			}

			for iface, data := range devices {
				if !data.Up || iface == "lo" {
					continue
				}

				componentID := "net-" + iface
				event := core.NewCTDPayload(nodeID, componentID, "active").
					AddMetric("rx_bytes", data.Statistics.RxBytes).
					AddMetric("tx_bytes", data.Statistics.TxBytes)

				if err := pub.Publish(event); err != nil {
					log.Printf("[Net Observer] Publish failed for %s: %v", componentID, err)
				}
			}
		}
	}
}
