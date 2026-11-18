package broker

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// EmbeddedOptions configures PigeonCoop's built-in pure-Go MQTT v3.1.1/v5.0 broker.
type EmbeddedOptions struct {
	ListenAddr   string
	Username     string
	Password     string
	EdgeUsername string
	EdgePassword string
	TLSCertFile  string
	TLSKeyFile   string
	TopicRoot    string
}

// EmbeddedBroker hosts an in-process pure-Go MQTT server (`CGO_ENABLED=0`).
type EmbeddedBroker struct {
	server    *mochi.Server
	boundAddr string
	tlsActive bool
	closeOnce sync.Once
}

// StartEmbedded launches an embedded MQTT TCP/TLS listener with packet-size/connection limits and optional topic ACLs.
func StartEmbedded(opts EmbeddedOptions) (*EmbeddedBroker, error) {
	listenAddr := strings.TrimSpace(opts.ListenAddr)
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1883"
	}

	var (
		ln        net.Listener
		err       error
		tlsActive bool
	)
	certFile := strings.TrimSpace(opts.TLSCertFile)
	keyFile := strings.TrimSpace(opts.TLSKeyFile)
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("embedded MQTT TLS requires both tls_cert_file and tls_key_file")
		}
		cert, loadErr := tls.LoadX509KeyPair(certFile, keyFile)
		if loadErr != nil {
			return nil, fmt.Errorf("load embedded MQTT TLS keypair: %w", loadErr)
		}
		tlsCfg := &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
		ln, err = tls.Listen("tcp", listenAddr, tlsCfg)
		tlsActive = true
	} else {
		ln, err = net.Listen("tcp", listenAddr)
	}
	if err != nil {
		return nil, fmt.Errorf("bind embedded MQTT listener %s: %w", listenAddr, err)
	}

	caps := mochi.NewDefaultServerCapabilities()
	caps.MaximumPacketSize = 1 << 20 // 1 MiB ceiling prevents OOM packet floods
	caps.MaximumClients = 4096
	caps.MaximumClientWritesPending = 8192

	warnLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	srv := mochi.New(&mochi.Options{
		InlineClient: true,
		Capabilities: caps,
		Logger:       warnLogger,
	})

	adminUser := strings.TrimSpace(opts.Username)
	edgeUser := strings.TrimSpace(opts.EdgeUsername)
	topicRoot := strings.Trim(strings.TrimSpace(opts.TopicRoot), "/")
	if topicRoot == "" {
		topicRoot = strings.Trim(brand.Active().TopicRoot, "/")
		if topicRoot == "" {
			topicRoot = "pigeons"
		}
	}

	if adminUser != "" || edgeUser != "" {
		var authRules auth.AuthRules
		var aclRules auth.ACLRules

		if adminUser != "" {
			authRules = append(authRules, auth.AuthRule{
				Username: auth.RString(adminUser),
				Password: auth.RString(opts.Password),
				Allow:    true,
			})
			aclRules = append(aclRules, auth.ACLRule{
				Username: auth.RString(adminUser),
			})
		}

		if edgeUser != "" && edgeUser != adminUser {
			authRules = append(authRules, auth.AuthRule{
				Username: auth.RString(edgeUser),
				Password: auth.RString(opts.EdgePassword),
				Allow:    true,
			})
			// Least-privilege edge node ACL: may publish telemetry + presence/hello and subscribe to control/<node>,
			// but cannot publish RunnerRequestCTD or dispatch control envelopes to sibling nodes.
			aclRules = append(aclRules, auth.ACLRule{
				Username: auth.RString(edgeUser),
				Filters: auth.Filters{
					auth.RString(topicRoot + "/ctd/#"):      auth.WriteOnly,
					auth.RString(topicRoot + "/registry/#"): auth.WriteOnly,
					auth.RString(topicRoot + "/control/+"):  auth.ReadOnly,
					"#":                                     auth.Deny,
				},
			})
		}

		authRules = append(authRules, auth.AuthRule{Allow: false})
		err = srv.AddHook(new(auth.Hook), &auth.Options{
			Ledger: &auth.Ledger{
				Auth: authRules,
				ACL:  aclRules,
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
		tlsActive: tlsActive,
	}, nil
}

// BoundAddr returns the actual TCP `host:port` bound by the listener.
func (b *EmbeddedBroker) BoundAddr() string {
	return b.boundAddr
}

// LocalClientURL returns a dialable `tcp://127.0.0.1:<port>` (or `tls://`) URL for local Coop & self-observer clients.
func (b *EmbeddedBroker) LocalClientURL() string {
	scheme := "tcp://"
	if b.tlsActive {
		scheme = "tls://"
	}
	_, port, err := net.SplitHostPort(b.boundAddr)
	if err != nil || port == "" {
		return scheme + "127.0.0.1:1883"
	}
	return scheme + "127.0.0.1:" + port
}

// Close gracefully terminates the embedded MQTT broker and closes all active client sockets.
func (b *EmbeddedBroker) Close() {
	b.closeOnce.Do(func() {
		if b.server != nil {
			_ = b.server.Close()
		}
	})
}
