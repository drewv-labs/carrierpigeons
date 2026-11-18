package relay

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/drewv-labs/carrier-pigeons/pkg/core"
	"github.com/drewv-labs/carrier-pigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type activeRun struct {
	id     uint64
	cancel context.CancelFunc
}

// ListenForRunners binds the MQTT client to the node's exclusive control topic and re-subscribes on reconnect.
func (c *MQTTClient) ListenForRunners(parentCtx context.Context, nodeID string, pub core.TelemetryPublisher) error {
	topic := core.ControlTopic(nodeID)

	var (
		mu            sync.Mutex
		activeRunners = make(map[string]*activeRun)
		seq           atomic.Uint64
	)

	// Cancel all active runners if the daemon shuts down.
	go func() {
		<-parentCtx.Done()
		mu.Lock()
		defer mu.Unlock()
		for key, entry := range activeRunners {
			entry.cancel()
			delete(activeRunners, key)
		}
	}()

	handler := func(_ mqtt.Client, msg mqtt.Message) {
		var env runner.Envelope
		if err := json.Unmarshal(msg.Payload(), &env); err != nil {
			log.Printf("[Runner Dispatch] Invalid envelope dropped: %v", err)
			return
		}

		if err := env.Validate(); err != nil {
			log.Printf("[Runner Dispatch] Envelope validation failed: %v", err)
			return
		}

		action := env.Action
		if action == "" {
			action = "start"
		}

		runnerKey := strings.ToLower(strings.TrimSpace(env.Runner))

		if action == "abort" {
			mu.Lock()
			entry, active := activeRunners[runnerKey]
			if active {
				delete(activeRunners, runnerKey)
			}
			mu.Unlock()

			if active {
				log.Printf("[Runner Dispatch] ABORT SIGNAL RECEIVED: Killing %q on %s", env.Runner, nodeID)
				entry.cancel()
			} else {
				log.Printf("[Runner Dispatch] Abort ignored: %q is not currently running.", env.Runner)
			}
			return
		}

		task, exists := runner.Get(env.Runner)
		if !exists {
			log.Printf("[Runner Dispatch] Unknown runner requested: %q", env.Runner)
			errEv := core.NewCTDPayload(nodeID, "runner:"+env.Runner, "runner-error").
				WithPillar(core.PillarRunner).
				AddParam("error", "unknown runner")
			if env.RequestID != "" {
				errEv.AddParam("request_id", env.RequestID)
			}
			_ = pub.Publish(errEv)
			return
		}

		mu.Lock()
		if _, active := activeRunners[runnerKey]; active {
			mu.Unlock()
			log.Printf("[Runner Dispatch] Runner %q is already active. Ignoring duplicate start.", env.Runner)
			return
		}

		baseCtx := runner.WithExecutionPolicy(parentCtx, c.runnerPolicy)
		runCtx, cancel := context.WithCancel(baseCtx)
		runID := seq.Add(1)
		activeRunners[runnerKey] = &activeRun{id: runID, cancel: cancel}
		mu.Unlock()

		params := make(map[string]any, len(env.Params)+2)
		maps.Copy(params, env.Params)
		if env.RequestID != "" {
			params["request_id"] = env.RequestID
		}
		params["invoked_as"] = env.Runner

		go func() {
			defer func() {
				cancel()
				mu.Lock()
				if cur, ok := activeRunners[runnerKey]; ok && cur.id == runID {
					delete(activeRunners, runnerKey)
				}
				mu.Unlock()
			}()

			log.Printf("[Runner Dispatch] Executing %q (request=%s) on %s", env.Runner, env.RequestID, nodeID)
			if err := task.Run(runCtx, nodeID, params, pub); err != nil {
				log.Printf("[Runner Dispatch] Runner %q failed: %v", env.Runner, err)
			}
		}()
	}

	subscribeFn := func(client mqtt.Client) {
		if tok := client.Subscribe(topic, 1, handler); tok.Wait() && tok.Error() != nil {
			log.Printf("[Runner Dispatch] Failed subscribing to %s: %v", topic, tok.Error())
		} else {
			log.Printf("[Runner Dispatch] Listening on control topic: %s", topic)
		}
	}

	c.subMu.Lock()
	c.onConnectSub = subscribeFn
	c.subMu.Unlock()

	if c.client != nil && c.client.IsConnected() {
		tok := c.client.Subscribe(topic, 1, handler)
		tok.Wait()
		if err := tok.Error(); err != nil {
			return err
		}
		log.Printf("[Runner Dispatch] Listening on control topic: %s", topic)
	}

	return nil
}
