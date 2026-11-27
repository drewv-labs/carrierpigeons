package broker

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
)

func TestStartEmbeddedPropagatesFatalServeErrorAndReleasesPort(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen failed: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	injectedErr := errors.New("injected mochi serve failure")
	emb, err := startEmbeddedWithServe(EmbeddedOptions{ListenAddr: addr}, func(_ *mochi.Server) error {
		return injectedErr
	})
	if err == nil {
		if emb != nil {
			emb.Close()
		}
		t.Fatal("expected startEmbeddedWithServe to return error when Serve() fails")
	}
	if !errors.Is(err, injectedErr) || !strings.Contains(err.Error(), "serve embedded MQTT broker") {
		t.Fatalf("unexpected wrapped error: %v", err)
	}

	// Verify bound TCP listener was cleanly closed so subsequent start on the same port succeeds.
	rebound, err := StartEmbedded(EmbeddedOptions{ListenAddr: addr})
	if err != nil {
		t.Fatalf("expected port %s to be released after failed Serve(), got %v", addr, err)
	}
	defer rebound.Close()

	cli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(rebound.LocalClientURL()).
		SetClientID("rebound-verify-cli").
		SetConnectRetry(false))
	if tok := cli.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("expected live embedded MQTT broker on %s, got %v", rebound.LocalClientURL(), tok.Error())
	}
	cli.Disconnect(50)
}
