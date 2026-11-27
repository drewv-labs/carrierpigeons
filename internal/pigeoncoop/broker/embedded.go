package broker

import (
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
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
	ListenAddr        string
	Username          string
	Password          string
	EdgeUsername      string
	EdgePassword      string
	EdgeClientPrefix  string
	TLSCertFile       string
	TLSKeyFile        string
	TLSClientCAFile   string
	RequireClientCert bool
	TopicRoot         string
}

type perNodeAuthHook struct {
	mochi.HookBase
	adminUser         string
	adminPass         string
	edgeUser          string
	edgePass          string
	edgePrefix        string
	topicRoot         string
	requireClientCert bool
}

func (h *perNodeAuthHook) ID() string {
	return "per-node-acl"
}

func (h *perNodeAuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{mochi.OnConnectAuthenticate, mochi.OnACLCheck, mochi.OnPublish}, []byte{b})
}

func isCTDTopic(topic, topicRoot string) bool {
	if topicRoot == "" {
		return false
	}
	prefix := topicRoot + "/ctd"
	return topic == prefix || strings.HasPrefix(topic, prefix+"/")
}

func rejectRetainedCTD(pk packets.Packet, topicRoot string) (packets.Packet, error) {
	if pk.FixedHeader.Retain && isCTDTopic(pk.TopicName, topicRoot) {
		return pk, packets.ErrRejectPacket
	}
	return pk, nil
}

func (h *perNodeAuthHook) OnPublish(_ *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	return rejectRetainedCTD(pk, h.topicRoot)
}

type ctdRetainGuardHook struct {
	mochi.HookBase
	topicRoot string
}

func (h *ctdRetainGuardHook) ID() string {
	return "ctd-retain-guard"
}

func (h *ctdRetainGuardHook) Provides(b byte) bool {
	return b == mochi.OnPublish
}

func (h *ctdRetainGuardHook) OnPublish(_ *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	return rejectRetainedCTD(pk, h.topicRoot)
}

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func extractEdgeNodeID(clientID, edgePrefix string) (string, bool) {
	if clientID == "" || strings.TrimSpace(clientID) != clientID {
		return "", false
	}
	nodeID := clientID
	if edgePrefix != "" {
		if !strings.HasPrefix(clientID, edgePrefix) {
			return "", false
		}
		nodeID = strings.TrimPrefix(clientID, edgePrefix)
	}
	if nodeID == "" {
		return "", false
	}
	if err := core.ValidateTopicToken(nodeID); err != nil {
		return "", false
	}
	return nodeID, true
}

func certMatchesNodeID(cert *x509.Certificate, nodeID string) bool {
	if cert == nil || nodeID == "" {
		return false
	}
	if cert.Subject.CommonName == nodeID || slices.Contains(cert.DNSNames, nodeID) {
		return true
	}
	if ip := net.ParseIP(nodeID); ip != nil {
		for _, certIP := range cert.IPAddresses {
			if ip.Equal(certIP) {
				return true
			}
		}
	}
	return false
}

func verifiedClientCert(cl *mochi.Client) *x509.Certificate {
	if cl == nil {
		return nil
	}
	tlsConn, ok := cl.Net.Conn.(*tls.Conn)
	if !ok {
		return nil
	}
	state := tlsConn.ConnectionState()
	if state.HandshakeComplete && len(state.VerifiedChains) > 0 && len(state.PeerCertificates) > 0 {
		return state.PeerCertificates[0]
	}
	return nil
}

func (h *perNodeAuthHook) OnConnectAuthenticate(cl *mochi.Client, pk packets.Packet) bool {
	if cl == nil {
		return false
	}

	verifiedCert := verifiedClientCert(cl)
	if h.requireClientCert && verifiedCert == nil {
		return false
	}

	user := string(pk.Connect.Username)
	pass := string(pk.Connect.Password)

	if h.adminUser != "" && constantTimeEqual(user, h.adminUser) {
		return h.adminPass != "" && constantTimeEqual(pass, h.adminPass)
	}

	edgeCredMatch := h.edgeUser != "" && h.edgePass != "" && constantTimeEqual(user, h.edgeUser) && constantTimeEqual(pass, h.edgePass)
	certOnlyEdge := h.edgeUser == "" && h.edgePass == "" && h.requireClientCert
	if !edgeCredMatch && !certOnlyEdge {
		return false
	}

	nodeID, ok := extractEdgeNodeID(cl.ID, h.edgePrefix)
	if !ok {
		return false
	}
	if verifiedCert != nil && !certMatchesNodeID(verifiedCert, nodeID) {
		return false
	}
	if pk.Connect.WillFlag {
		if pk.Connect.WillRetain && isCTDTopic(pk.Connect.WillTopic, h.topicRoot) {
			return false
		}
		if !h.allowEdgeWrite(nodeID, pk.Connect.WillTopic) {
			return false
		}
	}
	return true
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
	if h.adminUser != "" && h.adminPass != "" && constantTimeEqual(user, h.adminUser) {
		return topic == h.topicRoot || strings.HasPrefix(topic, h.topicRoot+"/") || topic == "#"
	}

	edgeCredMatch := h.edgeUser != "" && h.edgePass != "" && constantTimeEqual(user, h.edgeUser)
	certOnlyEdge := false
	if h.edgeUser == "" && h.edgePass == "" && h.requireClientCert {
		if cert := verifiedClientCert(cl); cert != nil {
			if nodeID, ok := extractEdgeNodeID(cl.ID, h.edgePrefix); ok {
				certOnlyEdge = certMatchesNodeID(cert, nodeID)
			}
		}
	}
	if edgeCredMatch || certOnlyEdge {
		nodeID, ok := extractEdgeNodeID(cl.ID, h.edgePrefix)
		if !ok {
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
	return startEmbeddedWithServe(opts, (*mochi.Server).Serve)
}

func startEmbeddedWithServe(opts EmbeddedOptions, serveFn func(*mochi.Server) error) (*EmbeddedBroker, error) {
	if serveFn == nil {
		serveFn = (*mochi.Server).Serve
	}
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
	clientCAFile := strings.TrimSpace(opts.TLSClientCAFile)
	if clientCAFile != "" || opts.RequireClientCert {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("embedded MQTT client-cert mTLS requires tls_cert_file and tls_key_file")
		}
		if opts.RequireClientCert && clientCAFile == "" {
			return nil, fmt.Errorf("embedded MQTT require_client_cert requires tls_client_ca_file")
		}
	}
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
		if clientCAFile != "" {
			caPEM, caErr := os.ReadFile(clientCAFile)
			if caErr != nil {
				return nil, fmt.Errorf("read embedded MQTT client CA file %s: %w", clientCAFile, caErr)
			}
			clientCAPool := x509.NewCertPool()
			if !clientCAPool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("no valid PEM certificates in embedded MQTT client CA file %s", clientCAFile)
			}
			tlsCfg.ClientCAs = clientCAPool
			if opts.RequireClientCert {
				tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
			} else {
				tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
			}
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
	caps.MaximumSessionExpiryInterval = 600
	caps.MaximumInflight = 1024
	caps.MaximumClientWritesPending = 2048
	caps.MaximumClients = 512

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

	if adminUser != "" || edgeUser != "" || opts.RequireClientCert {
		hook := &perNodeAuthHook{
			adminUser:         adminUser,
			adminPass:         opts.Password,
			edgeUser:          edgeUser,
			edgePass:          opts.EdgePassword,
			edgePrefix:        edgePrefix,
			topicRoot:         topicRoot,
			requireClientCert: opts.RequireClientCert,
		}
		err = srv.AddHook(hook, nil)
	} else {
		if err = srv.AddHook(new(auth.AllowHook), nil); err == nil {
			err = srv.AddHook(&ctdRetainGuardHook{topicRoot: topicRoot}, nil)
		}
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

	if serveErr := serveFn(srv); serveErr != nil {
		warnLogger.Error("embedded MQTT server failed to start", "err", serveErr)
		_ = srv.Close()
		_ = ln.Close()
		return nil, fmt.Errorf("serve embedded MQTT broker: %w", serveErr)
	}

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

// LocalClientURL returns a dialable `tcp://` (or `tls://`) URL for local Coop & self-observer clients.
// Empty or unspecified bound hosts are rewritten to `127.0.0.1` (or `::1` for `[::]`); specific bound hosts are preserved.
func (b *EmbeddedBroker) LocalClientURL() string {
	scheme := "tcp://"
	if b.tlsActive {
		scheme = "tls://"
	}
	host, port, err := net.SplitHostPort(b.boundAddr)
	if err != nil || port == "" {
		return scheme + "127.0.0.1:1883"
	}
	if host == "" {
		host = "127.0.0.1"
	} else if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() == nil {
			host = "::1"
		} else {
			host = "127.0.0.1"
		}
	}
	return scheme + net.JoinHostPort(host, port)
}

// Close gracefully terminates the embedded MQTT broker and closes all active client sockets.
func (b *EmbeddedBroker) Close() {
	b.closeOnce.Do(func() {
		if b.server != nil {
			_ = b.server.Close()
		}
	})
}
