package broker

import (
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

// EmbeddedOptions configures PigeonCoop's built-in pure-Go MQTT v3.1.1/v5.0 broker.
type EmbeddedOptions struct {
	ListenAddr       string
	Username         string
	Password         string
	EdgeUsername     string
	EdgePassword     string
	EdgeClientPrefix string
	TLSCertFile      string
	TLSKeyFile       string
	TopicRoot        string
}

type perNodeAuthHook struct {
	mochi.HookBase
	adminUser  string
	adminPass  string
	edgeUser   string
	edgePass   string
	edgePrefix string
	topicRoot  string
}

func (h *perNodeAuthHook) ID() string {
	return "per-node-acl"
}

func (h *perNodeAuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{mochi.OnConnectAuthenticate, mochi.OnACLCheck}, []byte{b})
}

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func extractEdgeNodeID(clientID, edgePrefix string) string {
	id := strings.TrimSpace(clientID)
	if edgePrefix != "" && strings.HasPrefix(id, edgePrefix) {
		trimmed := strings.TrimPrefix(id, edgePrefix)
		if trimmed != "" {
			return trimmed
		}
	}
	return id
}

func (h *perNodeAuthHook) OnConnectAuthenticate(cl *mochi.Client, pk packets.Packet) bool {
	user := string(pk.Connect.Username)
	pass := string(pk.Connect.Password)

	if h.adminUser != "" && constantTimeEqual(user, h.adminUser) && constantTimeEqual(pass, h.adminPass) {
		return true
	}
	if h.edgeUser != "" && constantTimeEqual(user, h.edgeUser) && constantTimeEqual(pass, h.edgePass) {
		nodeID := extractEdgeNodeID(cl.ID, h.edgePrefix)
		if err := core.ValidateTopicToken(nodeID); err != nil {
			return false
		}
		if pk.Connect.WillFlag && !h.allowEdgeWrite(nodeID, pk.Connect.WillTopic) {
			return false
		}
		return true
	}
	return false
}

func (h *perNodeAuthHook) allowEdgeWrite(nodeID, topic string) bool {
	if strings.ContainsAny(topic, "+#") {
		return false
	}
	if topic == h.topicRoot+"/registry/"+nodeID+"/status" || topic == h.topicRoot+"/registry/"+nodeID+"/hello" {
		return true
	}
	parts := strings.Split(topic, "/")
	if len(parts) == 5 && parts[0] == h.topicRoot && parts[1] == "ctd" && parts[3] == nodeID {
		return core.ValidateTopicToken(parts[2]) == nil && core.ValidateTopicToken(parts[4]) == nil
	}
	return false
}

func (h *perNodeAuthHook) OnACLCheck(cl *mochi.Client, topic string, write bool) bool {
	if cl == nil {
		return false
	}
	if cl.Net.Inline {
		return true
	}

	user := string(cl.Properties.Username)
	if h.adminUser != "" && constantTimeEqual(user, h.adminUser) {
		return topic == h.topicRoot || strings.HasPrefix(topic, h.topicRoot+"/") || topic == "#"
	}
	if h.edgeUser != "" && constantTimeEqual(user, h.edgeUser) {
		nodeID := extractEdgeNodeID(cl.ID, h.edgePrefix)
		if err := core.ValidateTopicToken(nodeID); err != nil {
			return false
		}
		if !write {
			// Strict per-node ReadOnly: disallow control/+ or sibling node control topics.
			return topic == h.topicRoot+"/control/"+nodeID
		}
		return h.allowEdgeWrite(nodeID, topic)
	}
	return false
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

	edgePrefix := strings.TrimSpace(opts.EdgeClientPrefix)
	if edgePrefix == "" {
		edgePrefix = strings.ToLower(brand.Active().EdgeName)
	}
	if edgePrefix != "" && !strings.HasSuffix(edgePrefix, "-") {
		edgePrefix += "-"
	}

	if adminUser != "" || edgeUser != "" {
		hook := &perNodeAuthHook{
			adminUser:  adminUser,
			adminPass:  opts.Password,
			edgeUser:   edgeUser,
			edgePass:   opts.EdgePassword,
			edgePrefix: edgePrefix,
			topicRoot:  topicRoot,
		}
		err = srv.AddHook(hook, nil)
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
