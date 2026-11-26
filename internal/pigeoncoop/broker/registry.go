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

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var ntfyHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
}

func (s *Subscriber) parseRegistryNodeID(topic, suffix string) (string, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != s.topicRoot || parts[1] != "registry" || parts[3] != suffix {
		return "", false
	}
	nodeID := parts[2]
	if err := core.ValidateTopicToken(nodeID); err != nil {
		log.Printf("[Registry] Invalid node_id in topic %q: %v", topic, err)
		return "", false
	}
	return nodeID, true
}

// processHelloUpdate ingests NodeHello hardware capabilities & boot tags on `<root>/registry/<node_id>/hello`.
func (s *Subscriber) processHelloUpdate(topic string, payload []byte) {
	topicNodeID, ok := s.parseRegistryNodeID(topic, "hello")
	if !ok {
		log.Printf("[Registry] Malformed NodeHello topic %q dropped", topic)
		return
	}

	var hello core.NodeHello
	if err := json.Unmarshal(payload, &hello); err != nil {
		log.Printf("[Registry] Malformed NodeHello on %s: %v", topic, err)
		return
	}
	if hello.NodeID != topicNodeID {
		log.Printf("[Registry] Mismatched NodeHello node_id %q on topic %q dropped", hello.NodeID, topic)
		return
	}

	ctx, cancel := context.WithTimeout(s.workContext(), 5*time.Second)
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
	nodeID, ok := s.parseRegistryNodeID(topic, "status")
	if !ok {
		return
	}
	status := strings.TrimSpace(string(payload))
	if status != "OFFLINE" && status != "ONLINE" {
		return
	}

	ctx, cancel := context.WithTimeout(s.workContext(), 5*time.Second)
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
			s.ntfyMu.Lock()
			closed := s.ntfyClosed || (s.ntfyCtx != nil && s.ntfyCtx.Err() != nil)
			if !closed {
				s.ntfyWG.Add(1)
			}
			s.ntfyMu.Unlock()
			if closed {
				<-s.ntfySem
				return
			}
			go func() {
				defer s.ntfyWG.Done()
				defer func() { <-s.ntfySem }()
				s.sendNtfyAlert(s.ntfyCtx, nodeID, status)
			}()
		default:
			log.Printf("[Ntfy] Alert concurrency limit reached; skipping duplicate notification for %s (%s)", nodeID, status)
		}
	}
}

func (s *Subscriber) reaperContext() context.Context {
	if s.reaperCtx != nil {
		return s.reaperCtx
	}
	return s.workContext()
}

func (s *Subscriber) reaperLoop() {
	defer s.reaperWG.Done()

	if _, ok := s.ledger.(ClusterStatusLedger); !ok {
		return
	}

	interval := s.staleNodeInterval
	if interval <= 0 {
		interval = defaultStaleNodeInterval
	}

	initialDelay := min(interval, 5*time.Second)
	initialTimer := time.NewTimer(initialDelay)
	select {
	case <-s.reaperStop:
		initialTimer.Stop()
		return
	case <-s.reaperContext().Done():
		initialTimer.Stop()
		return
	case <-initialTimer.C:
	}

	s.reapStaleNodes()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.reaperStop:
			return
		case <-s.reaperContext().Done():
			return
		case <-ticker.C:
			s.reapStaleNodes()
		}
	}
}

func (s *Subscriber) reapStaleNodes() {
	csl, ok := s.ledger.(ClusterStatusLedger)
	if !ok {
		return
	}
	threshold := s.staleNodeThreshold
	if threshold <= 0 {
		threshold = defaultStaleNodeThreshold
	}

	ctx, cancel := context.WithTimeout(s.reaperContext(), 5*time.Second)
	defer cancel()

	summaries, err := csl.GetClusterStatus(ctx, threshold)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[Registry Reaper] Failed checking stale nodes: %v", err)
		}
		return
	}

	cutoff := time.Now().UTC().Add(-threshold)
	for _, ns := range summaries {
		if ctx.Err() != nil {
			return
		}
		if ns.NodeID == "" || strings.EqualFold(ns.Status, "OFFLINE") || strings.EqualFold(ns.Status, "paused") {
			continue
		}
		if ns.LastSeen.After(cutoff) {
			continue
		}

		if tl, ok := s.ledger.(TransitionLedger); ok {
			prev, didChange, err := tl.UpdateNodePresenceTransition(ctx, ns.NodeID, "OFFLINE")
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[Registry Reaper] Failed marking stale node %s OFFLINE: %v", ns.NodeID, err)
				}
				continue
			}
			if didChange {
				log.Printf("[Registry Reaper] Reconciled stale node %s -> OFFLINE (prev=%s, last_seen=%s)",
					ns.NodeID, prev, ns.LastSeen.UTC().Format(time.RFC3339))
			}
			continue
		}

		if err := s.ledger.UpdateNodePresence(ctx, ns.NodeID, "OFFLINE"); err != nil {
			if ctx.Err() == nil {
				log.Printf("[Registry Reaper] Failed marking stale node %s OFFLINE: %v", ns.NodeID, err)
			}
			continue
		}
		log.Printf("[Registry Reaper] Reconciled stale node %s -> OFFLINE (last_seen=%s)",
			ns.NodeID, ns.LastSeen.UTC().Format(time.RFC3339))
	}
}

// sendNtfyAlert pushes a notification to the configured Ntfy instance and drains the HTTP body for keep-alive reuse.
func (s *Subscriber) sendNtfyAlert(parentCtx context.Context, nodeID, status string) {
	url := strings.TrimSpace(s.ntfyURL)
	if url == "" || url == "off" || url == "disabled" || url == "none" {
		return
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	var message string
	if status == "OFFLINE" {
		message = "CRITICAL: " + nodeID + " has dropped off the network."
	} else {
		message = "RECOVERY: " + nodeID + " is back online."
	}

	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(message))
	if err != nil {
		log.Printf("[Ntfy] Invalid request URL %q: %v", url, err)
		return
	}
	req.Header.Set("Title", s.brand.ProductName+" Hardware Alert")

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
