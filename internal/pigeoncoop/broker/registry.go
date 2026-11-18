package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ntfyHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
}

// handleHelloUpdate ingests NodeHello hardware capabilities & boot tags on `pigeons/registry/+/hello`.
func (s *Subscriber) handleHelloUpdate(_ mqtt.Client, msg mqtt.Message) {
	var hello core.NodeHello
	if err := json.Unmarshal(msg.Payload(), &hello); err != nil {
		log.Printf("[Registry] Malformed NodeHello on %s: %v", msg.Topic(), err)
		return
	}
	if strings.TrimSpace(hello.NodeID) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tags, err := s.ledger.UpsertNodeHello(ctx, hello)
	if err != nil {
		log.Printf("[Registry] Failed upserting NodeHello for %s: %v", hello.NodeID, err)
		return
	}
	log.Printf("[Registry] Registered %s (%s/%s, cpus=%d, tags=%v)",
		hello.NodeID, hello.OS, hello.Arch, hello.NumCPU, tags)
}

// handleRegistryUpdate intercepts LWT / presence messages, persists presence in PostgreSQL, and triggers Ntfy alerts.
func (s *Subscriber) handleRegistryUpdate(_ mqtt.Client, msg mqtt.Message) {
	// Topic structure: <root>/registry/<node_id>/status
	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 3 {
		return
	}

	nodeID := strings.TrimPrefix(parts[len(parts)-2], "pigeoneer-")
	status := strings.TrimSpace(string(msg.Payload()))

	log.Printf("[Registry] %s state changed to: %s", nodeID, status)

	if status == "OFFLINE" || status == "ONLINE" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.ledger.UpdateNodePresence(ctx, nodeID, status); err != nil {
			log.Printf("[Registry] Failed updating presence for %s: %v", nodeID, err)
		}
		cancel()

		if s.ntfyEnabled && s.ntfyURL != "" {
			go s.sendNtfyAlert(nodeID, status)
		}
	}
}

// sendNtfyAlert pushes a notification to the configured Ntfy instance.
func (s *Subscriber) sendNtfyAlert(nodeID, status string) {
	url := strings.TrimSpace(s.ntfyURL)
	if url == "" || url == "off" || url == "disabled" || url == "none" {
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
	req.Header.Set("Title", brand.Active().ProductName+" Hardware Alert")

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
