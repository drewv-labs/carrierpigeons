package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var ntfyHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
}

// processHelloUpdate ingests NodeHello hardware capabilities & boot tags on `<root>/registry/+/hello`.
func (s *Subscriber) processHelloUpdate(topic string, payload []byte) {
	var hello core.NodeHello
	if err := json.Unmarshal(payload, &hello); err != nil {
		log.Printf("[Registry] Malformed NodeHello on %s: %v", topic, err)
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

// processRegistryUpdate intercepts LWT / presence messages, persists presence in PostgreSQL,
// and triggers bounded Ntfy alerts strictly on genuine state transitions.
func (s *Subscriber) processRegistryUpdate(topic string, payload []byte) {
	// Topic structure: <root>/registry/<node_id>/status
	parts := strings.Split(topic, "/")
	if len(parts) < 3 {
		return
	}

	nodeID := strings.TrimSpace(parts[len(parts)-2])
	if nodeID == "" {
		return
	}
	status := strings.TrimSpace(string(payload))
	if status != "OFFLINE" && status != "ONLINE" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	changed := true
	if tl, ok := s.ledger.(TransitionLedger); ok {
		prev, didChange, err := tl.UpdateNodePresenceTransition(ctx, nodeID, status)
		if err != nil {
			log.Printf("[Registry] Failed updating presence for %s: %v", nodeID, err)
			changed = false
		} else {
			// Alert on genuine transitions (OFFLINE always alerts if changed; ONLINE alerts when recovering from OFFLINE).
			changed = didChange && (status == "OFFLINE" || prev == "OFFLINE")
		}
	} else {
		if err := s.ledger.UpdateNodePresence(ctx, nodeID, status); err != nil {
			log.Printf("[Registry] Failed updating presence for %s: %v", nodeID, err)
			changed = false
		}
	}
	cancel()

	if changed {
		log.Printf("[Registry] %s state changed to: %s", nodeID, status)
	}

	if changed && s.ntfyEnabled && s.ntfyURL != "" {
		select {
		case s.ntfySem <- struct{}{}:
			go func() {
				defer func() { <-s.ntfySem }()
				s.sendNtfyAlert(nodeID, status)
			}()
		default:
			log.Printf("[Ntfy] Alert concurrency limit reached; skipping duplicate notification for %s (%s)", nodeID, status)
		}
	}
}

// sendNtfyAlert pushes a notification to the configured Ntfy instance and drains the HTTP body for keep-alive reuse.
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
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
}
