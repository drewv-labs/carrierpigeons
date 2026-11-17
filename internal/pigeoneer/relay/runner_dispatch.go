package relay

import (
	"context"
	"encoding/json"
	"log"
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
		for name, entry := range activeRunners {
			entry.cancel()
			delete(activeRunners, name)
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

		if action == "abort" {
			mu.Lock()
			entry, active := activeRunners[env.Runner]
			if active {
				delete(activeRunners, env.Runner)
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
			return
		}

		mu.Lock()
		if _, active := activeRunners[env.Runner]; active {
			mu.Unlock()
			log.Printf("[Runner Dispatch] Runner %q is already active. Ignoring duplicate start.", env.Runner)
			return
		}

		runCtx, cancel := context.WithCancel(parentCtx)
		runID := seq.Add(1)
		activeRunners[env.Runner] = &activeRun{id: runID, cancel: cancel}
		mu.Unlock()

		go func() {
			defer func() {
				cancel()
				mu.Lock()
				if cur, ok := activeRunners[env.Runner]; ok && cur.id == runID {
					delete(activeRunners, env.Runner)
				}
				mu.Unlock()
			}()

			log.Printf("[Runner Dispatch] Executing %q on %s", env.Runner, nodeID)
			if err := task.Run(runCtx, nodeID, env.Params, pub); err != nil {
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
