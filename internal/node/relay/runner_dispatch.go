package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	"github.com/drewv-labs/carrierpigeons/pkg/runner"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type activeRun struct {
	id        uint64
	requestID string
	cancel    context.CancelFunc
	aborting  bool
}

func (c *MQTTClient) lookupRunner(name string) (runner.Runner, string, bool) {
	if c.runnerReg != nil {
		task, ok := c.runnerReg.Get(name)
		return task, c.runnerReg.CanonicalKey(name), ok
	}
	task, ok := runner.Get(name)
	return task, runner.CanonicalKey(name), ok
}

func publishRunnerError(pub core.TelemetryPublisher, nodeID, runnerName, requestID, errMsg string) {
	if pub == nil {
		return
	}
	comp := "runner:" + runnerName
	if err := core.ValidateTopicToken(comp); err != nil {
		log.Printf("[Runner Dispatch] Dropped runner-error event due to invalid component token %q: %v", comp, err)
		return
	}
	errEv := core.NewCTDPayload(nodeID, comp, "runner-error").
		WithPillar(core.PillarRunner).
		AddParam("error", errMsg)
	if requestID != "" {
		errEv.AddParam("request_id", requestID)
	}
	_ = pub.Publish(errEv)
}

// WaitRunners stops accepting new runner dispatches and waits up to timeout for active runner
// goroutines to finish emitting terminal CTD bookends before MQTT socket teardown.
func (c *MQTTClient) WaitRunners(timeout time.Duration) bool {
	c.runnerMu.Lock()
	c.runnersStopped = true
	c.runnerMu.Unlock()

	done := make(chan struct{})
	go func() {
		c.runnerWG.Wait()
		close(done)
	}()

	if timeout <= 0 {
		<-done
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// ListenForRunners binds the MQTT client to the node's exclusive control topic and re-subscribes on reconnect.
func (c *MQTTClient) ListenForRunners(parentCtx context.Context, nodeID string, pub core.TelemetryPublisher) error {
	topic := c.brand.ControlTopic(nodeID)

	c.runnerMu.Lock()
	c.runnersStopped = false
	c.runnerMu.Unlock()

	var (
		mu            sync.Mutex
		activeRunners = make(map[string]*activeRun)
		seq           atomic.Uint64
	)

	// Cancel all active runners if the daemon shuts down.
	go func() {
		select {
		case <-parentCtx.Done():
		case <-c.stopCh:
		}
		mu.Lock()
		defer mu.Unlock()
		for _, entry := range activeRunners {
			entry.aborting = true
			entry.cancel()
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
		if err := core.ValidateTopicToken(env.Runner); err != nil {
			log.Printf("[Runner Dispatch] Invalid runner topic token %q dropped: %v", env.Runner, err)
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
			if !active {
				mu.Unlock()
				log.Printf("[Runner Dispatch] Abort ignored: %q (%s) is not currently running.", env.Runner, runnerKey)
				return
			}
			if env.RequestID != "" && entry.requestID != env.RequestID {
				activeReqID := entry.requestID
				mu.Unlock()
				log.Printf("[Runner Dispatch] Abort ignored: %q (%s) request_id mismatch (abort=%q, active=%q).", env.Runner, runnerKey, env.RequestID, activeReqID)
				return
			}
			alreadyAborting := entry.aborting
			entry.aborting = true
			cancelFn := entry.cancel
			mu.Unlock()

			if !alreadyAborting {
				log.Printf("[Runner Dispatch] ABORT SIGNAL RECEIVED: Killing %q (%s) on %s", env.Runner, runnerKey, nodeID)
			}
			cancelFn()
			return
		}

		if !exists {
			log.Printf("[Runner Dispatch] Unknown runner requested: %q", env.Runner)
			publishRunnerError(pub, nodeID, env.Runner, env.RequestID, "unknown runner")
			return
		}

		c.runnerMu.Lock()
		if c.runnersStopped || parentCtx.Err() != nil {
			c.runnerMu.Unlock()
			log.Printf("[Runner Dispatch] Ignoring start for %q during shutdown", env.Runner)
			return
		}

		mu.Lock()
		if cur, active := activeRunners[runnerKey]; active {
			aborting := cur.aborting
			mu.Unlock()
			c.runnerMu.Unlock()
			if aborting {
				log.Printf("[Runner Dispatch] Runner %q (%s) is currently aborting teardown. Ignoring concurrent start.", env.Runner, runnerKey)
			} else {
				log.Printf("[Runner Dispatch] Runner %q (%s) is already active. Ignoring duplicate start.", env.Runner, runnerKey)
			}
			return
		}

		baseCtx := runner.WithExecutionPolicy(parentCtx, c.runnerPolicy)
		if observer.PauseControllerFromContext(baseCtx) == nil && c.pauseCtrl != nil {
			baseCtx = observer.WithPauseController(baseCtx, c.pauseCtrl)
		}
		runCtx, cancel := context.WithCancel(baseCtx)
		runID := seq.Add(1)
		activeRunners[runnerKey] = &activeRun{
			id:        runID,
			requestID: env.RequestID,
			cancel:    cancel,
		}
		mu.Unlock()

		c.runnerWG.Add(1)
		c.runnerMu.Unlock()

		params := make(map[string]any, len(env.Params)+2)
		maps.Copy(params, env.Params)
		if env.RequestID != "" {
			params["request_id"] = env.RequestID
		}
		params["invoked_as"] = env.Runner

		go func() {
			defer c.runnerWG.Done()
			defer func() {
				cancel()
				mu.Lock()
				if cur, ok := activeRunners[runnerKey]; ok && cur.id == runID {
					delete(activeRunners, runnerKey)
				}
				mu.Unlock()
				if r := recover(); r != nil {
					log.Printf("[Runner Dispatch] Runner %q panicked: %v", env.Runner, r)
					publishRunnerError(pub, nodeID, env.Runner, env.RequestID, fmt.Sprintf("panic: %v", r))
				}
			}()

			log.Printf("[Runner Dispatch] Executing %q (canonical=%s, request=%s) on %s", env.Runner, runnerKey, env.RequestID, nodeID)
			if err := task.Run(runCtx, nodeID, params, pub); err != nil {
				log.Printf("[Runner Dispatch] Runner %q failed: %v", env.Runner, err)
			}
		}()
	}

	subTimeout := c.effectiveSubscribeTimeout()

	subscribeOnce := func(client mqtt.Client, nextConnCh <-chan struct{}) error {
		tok := client.Subscribe(topic, 1, handler)
		timer := time.NewTimer(subTimeout)
		defer timer.Stop()
		select {
		case <-parentCtx.Done():
			return parentCtx.Err()
		case <-c.stopCh:
			return errRelayClosed
		case <-nextConnCh:
			return fmt.Errorf("superseded by new connection generation")
		case <-timer.C:
			return fmt.Errorf("subscribe %s timed out after %s", topic, subTimeout)
		case <-tok.Done():
			if err := tok.Error(); err != nil {
				return err
			}
			return nil
		}
	}

	retrySubscribe := func(client mqtt.Client, gen uint64, nextConnCh <-chan struct{}, startAttempt int) {
		attempt := startAttempt
		for {
			if parentCtx.Err() != nil {
				return
			}
			select {
			case <-c.stopCh:
				return
			case <-nextConnCh:
				return
			default:
			}
			if gen != 0 && c.connGen.Load() != gen {
				return
			}

			if attempt > startAttempt || startAttempt > 0 {
				backoff := c.nextBackoff(attempt - 1)
				sleepTimer := time.NewTimer(backoff)
				select {
				case <-parentCtx.Done():
					sleepTimer.Stop()
					return
				case <-c.stopCh:
					sleepTimer.Stop()
					return
				case <-nextConnCh:
					sleepTimer.Stop()
					return
				case <-sleepTimer.C:
				}
			}

			if gen != 0 && c.connGen.Load() != gen {
				return
			}

			err := subscribeOnce(client, nextConnCh)
			if err == nil {
				log.Printf("[Runner Dispatch] Listening on control topic: %s", topic)
				return
			}
			if parentCtx.Err() != nil {
				return
			}
			select {
			case <-c.stopCh:
				return
			case <-nextConnCh:
				return
			default:
			}
			log.Printf("[Runner Dispatch] Subscribe to %s failed (gen=%d, attempt=%d): %v", topic, gen, attempt+1, err)
			attempt++
		}
	}

	c.subMu.Lock()
	c.onConnectSub = func(client mqtt.Client, gen uint64, nextConnCh <-chan struct{}) {
		retrySubscribe(client, gen, nextConnCh, 0)
	}
	c.subMu.Unlock()

	if c.client != nil && c.client.IsConnected() {
		gen, nextConnCh := c.connectionState()
		if err := subscribeOnce(c.client, nextConnCh); err != nil {
			c.onConnectMu.Lock()
			select {
			case <-c.stopCh:
				c.onConnectMu.Unlock()
				return err
			default:
			}
			c.subWG.Add(1)
			c.onConnectMu.Unlock()

			go func() {
				defer c.subWG.Done()
				retrySubscribe(c.client, gen, nextConnCh, 1)
			}()
			return err
		}
		log.Printf("[Runner Dispatch] Listening on control topic: %s", topic)
	}

	return nil
}
