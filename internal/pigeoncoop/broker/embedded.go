package broker

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// EmbeddedOptions configures PigeonCoop's built-in pure-Go MQTT v3.1.1/v5.0 broker.
type EmbeddedOptions struct {
	ListenAddr string
	Username   string
	Password   string
}

// EmbeddedBroker hosts an in-process pure-Go MQTT server (`CGO_ENABLED=0`).
type EmbeddedBroker struct {
	server    *mochi.Server
	boundAddr string
	closeOnce sync.Once
}

// StartEmbedded launches an embedded MQTT TCP listener and begins serving packets.
func StartEmbedded(opts EmbeddedOptions) (*EmbeddedBroker, error) {
	listenAddr := strings.TrimSpace(opts.ListenAddr)
	if listenAddr == "" {
		listenAddr = "0.0.0.0:1883"
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("bind embedded MQTT listener %s: %w", listenAddr, err)
	}

	quietLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := mochi.New(&mochi.Options{
		InlineClient: true,
		Logger:       quietLogger,
	})

	if strings.TrimSpace(opts.Username) != "" {
		err = srv.AddHook(new(auth.Hook), &auth.Options{
			Ledger: &auth.Ledger{
				Auth: auth.AuthRules{
					{
						Username: auth.RString(opts.Username),
						Password: auth.RString(opts.Password),
						Allow:    true,
					},
					{
						Allow: false,
					},
				},
			},
		})
	} else {
		err = srv.AddHook(new(auth.AllowHook), nil)
	}
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("configure embedded MQTT auth hook: %w", err)
	}

	tcpListener := listeners.NewNet("coop-tcp", ln)
	if err := srv.AddListener(tcpListener); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("attach embedded MQTT listener: %w", err)
	}

	go func() {
		_ = srv.Serve()
	}()

	return &EmbeddedBroker{
		server:    srv,
		boundAddr: ln.Addr().String(),
	}, nil
}

// BoundAddr returns the actual TCP `host:port` bound by the listener.
func (b *EmbeddedBroker) BoundAddr() string {
	return b.boundAddr
}

// LocalClientURL returns a dialable `tcp://127.0.0.1:<port>` URL for local Coop & self-observer clients.
func (b *EmbeddedBroker) LocalClientURL() string {
	_, port, err := net.SplitHostPort(b.boundAddr)
	if err != nil || port == "" {
		return "tcp://127.0.0.1:1883"
	}
	return "tcp://127.0.0.1:" + port
}

// Close gracefully terminates the embedded MQTT broker and closes all active client sockets.
func (b *EmbeddedBroker) Close() {
	b.closeOnce.Do(func() {
		if b.server != nil {
			_ = b.server.Close()
		}
	})
}
