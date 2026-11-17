package broker

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ntfyHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
}

func resolveNtfyURL() string {
	for _, key := range []string{"PIGEONCOOP_NTFY_URL", "NTFY_URL"} {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return "http://localhost:2586/carrier-pigeons"
}

// handleRegistryUpdate intercepts LWT / presence messages and triggers Ntfy alerts.
func (s *Subscriber) handleRegistryUpdate(_ mqtt.Client, msg mqtt.Message) {
	// Topic structure: pigeons/registry/<node_id>/status
	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 3 {
		return
	}

	nodeID := strings.TrimPrefix(parts[len(parts)-2], "pigeoneer-")
	status := strings.TrimSpace(string(msg.Payload()))

	log.Printf("[Registry] %s state changed to: %s", nodeID, status)

	if status == "OFFLINE" || status == "ONLINE" {
		go sendNtfyAlert(nodeID, status)
	}
}

// sendNtfyAlert pushes a notification to the configured Ntfy instance.
func sendNtfyAlert(nodeID, status string) {
	url := resolveNtfyURL()
	if url == "off" || url == "disabled" || url == "none" {
		return
	}

	var message string
	if status == "OFFLINE" {
		message = "CRITICAL: " + nodeID + " has dropped off the network."
	} else {
		message = "RECOVERY: " + nodeID + " is back online."
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(message))
	if err != nil {
		log.Printf("[Ntfy] Invalid request URL %q: %v", url, err)
		return
	}
	req.Header.Set("Title", "CarrierPigeons Hardware Alert")

	if status == "OFFLINE" {
		req.Header.Set("Tags", "warning,skull")
		req.Header.Set("Priority", "high")
	} else {
		req.Header.Set("Tags", "white_check_mark")
	}

	resp, err := ntfyHTTPClient.Do(req)
	if err != nil {
		log.Printf("[Ntfy] Failed to send alert: %v", err)
		return
	}
	defer resp.Body.Close()
}
