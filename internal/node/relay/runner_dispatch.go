package relay

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type activeRun struct {
	id     uint64
	cancel context.CancelFunc
}

func (c *MQTTClient) lookupRunner(name string) (runner.Runner, string, bool) {
	if c.runnerReg != nil {
		task, ok := c.runnerReg.Get(name)
		return task, c.runnerReg.CanonicalKey(name), ok
	}
	task, ok := runner.Get(name)
	return task, runner.CanonicalKey(name), ok
}

// ListenForRunners binds the MQTT client to the node's exclusive control topic and re-subscribes on reconnect.
func (c *MQTTClient) ListenForRunners(parentCtx context.Context, nodeID string, pub core.TelemetryPublisher) error {
	topic := c.brand.ControlTopic(nodeID)

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

		task, runnerKey, exists := c.lookupRunner(env.Runner)

		if action == "abort" {
			mu.Lock()
			entry, active := activeRunners[runnerKey]
			if active {
				delete(activeRunners, runnerKey)
			}
			mu.Unlock()

			if active {
				log.Printf("[Runner Dispatch] ABORT SIGNAL RECEIVED: Killing %q (%s) on %s", env.Runner, runnerKey, nodeID)
				entry.cancel()
			} else {
				log.Printf("[Runner Dispatch] Abort ignored: %q (%s) is not currently running.", env.Runner, runnerKey)
			}
			return
		}

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
			log.Printf("[Runner Dispatch] Runner %q (%s) is already active. Ignoring duplicate start.", env.Runner, runnerKey)
			return
		}

		baseCtx := runner.WithExecutionPolicy(parentCtx, c.runnerPolicy)
		if observer.PauseControllerFromContext(baseCtx) == nil && c.pauseCtrl != nil {
			baseCtx = observer.WithPauseController(baseCtx, c.pauseCtrl)
		}
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

			log.Printf("[Runner Dispatch] Executing %q (canonical=%s, request=%s) on %s", env.Runner, runnerKey, env.RequestID, nodeID)
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
