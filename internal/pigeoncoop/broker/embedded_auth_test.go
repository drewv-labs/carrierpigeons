package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

func TestPerNodeAuthHookRejectsEmptyPasswordsAndInvalidEdgeConnect(t *testing.T) {
	hook := &perNodeAuthHook{
		adminUser:  "coop_admin",
		adminPass:  "",
		edgeUser:   "edge_node",
		edgePass:   "",
		edgePrefix: "pigeoneer-",
		topicRoot:  "pigeons",
	}

	adminClient := &mochi.Client{ID: "coop-client"}
	adminEmptyPacket := packets.Packet{
		Connect: packets.ConnectParams{
			Username: []byte("coop_admin"),
			Password: []byte(""),
		},
	}
	if hook.OnConnectAuthenticate(adminClient, adminEmptyPacket) {
		t.Fatal("expected OnConnectAuthenticate to reject admin login when configured adminPass is empty")
	}
	if hook.OnACLCheck(&mochi.Client{
		ID:         "coop-client",
		Properties: mochi.ClientProperties{Username: []byte("coop_admin")},
	}, "pigeons/ctd/#", false) {
		t.Fatal("expected OnACLCheck to reject admin when configured adminPass is empty")
	}

	edgeClient := &mochi.Client{ID: "pigeoneer-rv64-edge-01"}
	edgeEmptyPacket := packets.Packet{
		Connect: packets.ConnectParams{
			Username: []byte("edge_node"),
			Password: []byte(""),
		},
	}
	if hook.OnConnectAuthenticate(edgeClient, edgeEmptyPacket) {
		t.Fatal("expected OnConnectAuthenticate to reject edge login when configured edgePass is empty")
	}
	if hook.OnACLCheck(&mochi.Client{
		ID:         "pigeoneer-rv64-edge-01",
		Properties: mochi.ClientProperties{Username: []byte("edge_node")},
	}, "pigeons/control/rv64-edge-01", false) {
		t.Fatal("expected OnACLCheck to reject edge when configured edgePass is empty")
	}

	if hook.OnConnectAuthenticate(nil, adminEmptyPacket) {
		t.Fatal("expected OnConnectAuthenticate to reject nil client")
	}

	hook.adminPass = "admin_secret"
	hook.edgePass = "edge_secret"

	if !hook.OnConnectAuthenticate(adminClient, packets.Packet{
		Connect: packets.ConnectParams{
			Username: []byte("coop_admin"),
			Password: []byte("admin_secret"),
		},
	}) {
		t.Fatal("expected valid admin credentials to authenticate")
	}

	if hook.OnConnectAuthenticate(adminClient, packets.Packet{
		Connect: packets.ConnectParams{
			Username: []byte("coop_admin"),
			Password: []byte("wrong_secret"),
		},
	}) {
		t.Fatal("expected wrong admin password to fail authentication")
	}

	// Valid edge login with own LWT will topic must succeed.
	if !hook.OnConnectAuthenticate(edgeClient, packets.Packet{
		Connect: packets.ConnectParams{
			Username:  []byte("edge_node"),
			Password:  []byte("edge_secret"),
			WillFlag:  true,
			WillTopic: "pigeons/registry/rv64-edge-01/status",
		},
	}) {
		t.Fatal("expected valid edge credentials and own LWT topic to authenticate")
	}

	// Edge login with invalid node ID token must fail closed.
	if hook.OnConnectAuthenticate(&mochi.Client{ID: "pigeoneer-bad+node"}, packets.Packet{
		Connect: packets.ConnectParams{
			Username: []byte("edge_node"),
			Password: []byte("edge_secret"),
		},
	}) {
		t.Fatal("expected invalid edge client_id topic token to fail authentication")
	}

	// Edge login attempting to register LWT on another node's or MNS topic must fail closed.
	for _, badWill := range []string{
		"pigeons/registry/mesh",
		"pigeons/registry/rv64-edge-02/status",
		"pigeons/requests/runner",
	} {
		if hook.OnConnectAuthenticate(edgeClient, packets.Packet{
			Connect: packets.ConnectParams{
				Username:  []byte("edge_node"),
				Password:  []byte("edge_secret"),
				WillFlag:  true,
				WillTopic: badWill,
			},
		}) {
			t.Fatalf("expected unauthorized edge LWT will topic %q to fail authentication", badWill)
		}
	}
}

func TestEmbeddedBrokerRejectsEmptyPasswordsAndUnauthorizedWill(t *testing.T) {
	embEmpty, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:   "127.0.0.1:0",
		Username:     "coop_admin",
		Password:     "",
		EdgeUsername: "edge_node",
		EdgePassword: "",
		TopicRoot:    "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer embEmpty.Close()

	for _, tc := range []struct {
		name string
		id   string
		user string
	}{
		{name: "admin empty password", id: "coop-empty-pass", user: "coop_admin"},
		{name: "edge empty password", id: "pigeoneer-edge-01", user: "edge_node"},
	} {
		cli := mqtt.NewClient(mqtt.NewClientOptions().
			AddBroker(embEmpty.LocalClientURL()).
			SetClientID(tc.id).
			SetUsername(tc.user).
			SetPassword("").
			SetConnectRetry(false))
		if tok := cli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
			cli.Disconnect(50)
			t.Fatalf("expected %s connection to be rejected", tc.name)
		}
	}

	embAuth, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:   "127.0.0.1:0",
		Username:     "coop_admin",
		Password:     "admin_secret",
		EdgeUsername: "edge_node",
		EdgePassword: "edge_secret",
		TopicRoot:    "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded with auth failed: %v", err)
	}
	defer embAuth.Close()

	forgedWillCli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(embAuth.LocalClientURL()).
		SetClientID("pigeoneer-edge-01").
		SetUsername("edge_node").
		SetPassword("edge_secret").
		SetWill("pigeons/registry/victim-node/status", "OFFLINE", 1, false).
		SetConnectRetry(false))
	if tok := forgedWillCli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
		forgedWillCli.Disconnect(50)
		t.Fatal("expected edge connection with forged sibling LWT will topic to be rejected")
	}
}

func writeTestCertFiles(t *testing.T, dir, prefix string, tmpl *x509.Certificate, parent *x509.Certificate, signer *ecdsa.PrivateKey) (certPath, keyPath string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}
	if parent == nil {
		parent = tmpl
		signer = priv
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &priv.PublicKey, signer)
	if err != nil {
		t.Fatalf("CreateCertificate failed: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate failed: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey failed: %v", err)
	}
	certPath = filepath.Join(dir, prefix+".crt")
	keyPath = filepath.Join(dir, prefix+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert failed: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key failed: %v", err)
	}
	return certPath, keyPath, parsed, priv
}

func TestEmbeddedBrokerMutualTLSNodeIDBinding(t *testing.T) {
	tmp := t.TempDir()
	now := time.Now().Add(-1 * time.Minute)
	exp := now.Add(24 * time.Hour)

	caPath, _, caCert, caKey := writeTestCertFiles(t, tmp, "ca", &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-mesh-ca"},
		NotBefore:             now,
		NotAfter:              exp,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}, nil, nil)

	srvCertPath, srvKeyPath, _, _ := writeTestCertFiles(t, tmp, "server", &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "coop-server"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, caCert, caKey)

	edge1CertPath, edge1KeyPath, _, _ := writeTestCertFiles(t, tmp, "edge1", &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "rv64-edge-01"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"rv64-edge-01"},
	}, caCert, caKey)

	adminCertPath, adminKeyPath, _, _ := writeTestCertFiles(t, tmp, "admin", &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject:      pkix.Name{CommonName: "coop-admin"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caCert, caKey)

	// Invalid mTLS options must fail closed.
	if _, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:        "127.0.0.1:0",
		RequireClientCert: true,
	}); err == nil {
		t.Fatal("expected StartEmbedded to reject RequireClientCert without server cert/key")
	}
	if _, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:        "127.0.0.1:0",
		TLSCertFile:       srvCertPath,
		TLSKeyFile:        srvKeyPath,
		RequireClientCert: true,
	}); err == nil {
		t.Fatal("expected StartEmbedded to reject RequireClientCert without TLSClientCAFile")
	}

	emb, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:        "127.0.0.1:0",
		Username:          "coop_admin",
		Password:          "admin_secret",
		EdgeUsername:      "edge_node",
		EdgePassword:      "edge_secret",
		TLSCertFile:       srvCertPath,
		TLSKeyFile:        srvKeyPath,
		TLSClientCAFile:   caPath,
		RequireClientCert: true,
		TopicRoot:         "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded with mTLS failed: %v", err)
	}
	defer emb.Close()

	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read CA file: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)

	edge1KP, err := tls.LoadX509KeyPair(edge1CertPath, edge1KeyPath)
	if err != nil {
		t.Fatalf("load edge1 keypair: %v", err)
	}
	adminKP, err := tls.LoadX509KeyPair(adminCertPath, adminKeyPath)
	if err != nil {
		t.Fatalf("load admin keypair: %v", err)
	}

	// 1. Edge client presenting matching node_id cert succeeds.
	okEdge := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("pigeoneer-rv64-edge-01").
		SetUsername("edge_node").
		SetPassword("edge_secret").
		SetTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{edge1KP},
		}).
		SetConnectRetry(false))
	if tok := okEdge.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("expected matching edge mTLS client to connect, got %v", tok.Error())
	}
	okEdge.Disconnect(50)

	// 2. Edge client claiming a different node_id with edge1's cert MUST be rejected.
	spoofEdge := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("pigeoneer-rv64-edge-02").
		SetUsername("edge_node").
		SetPassword("edge_secret").
		SetTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{edge1KP},
		}).
		SetConnectRetry(false))
	if tok := spoofEdge.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
		spoofEdge.Disconnect(50)
		t.Fatal("expected edge client impersonating rv64-edge-02 with rv64-edge-01 cert to be rejected")
	}

	// 3. Client without any client cert MUST be rejected by TLS handshake.
	noCertEdge := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("pigeoneer-rv64-edge-01").
		SetUsername("edge_node").
		SetPassword("edge_secret").
		SetTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    caPool,
		}).
		SetConnectRetry(false))
	if tok := noCertEdge.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
		noCertEdge.Disconnect(50)
		t.Fatal("expected TLS client without client cert to be rejected")
	}

	// 4. Admin client with valid CA-signed cert connects.
	okAdmin := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("coop-admin-01").
		SetUsername("coop_admin").
		SetPassword("admin_secret").
		SetTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{adminKP},
		}).
		SetConnectRetry(false))
	if tok := okAdmin.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("expected admin mTLS client to connect, got %v", tok.Error())
	}
	okAdmin.Disconnect(50)

	// 5. Cert-only edge mTLS mode (EdgeUsername == "", RequireClientCert == true):
	//    matching cert connects and publishes to own topic, while spoofed node_id and sibling writes are rejected.
	embCertOnly, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:        "127.0.0.1:0",
		Username:          "coop_admin",
		Password:          "admin_secret",
		TLSCertFile:       srvCertPath,
		TLSKeyFile:        srvKeyPath,
		TLSClientCAFile:   caPath,
		RequireClientCert: true,
		TopicRoot:         "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded cert-only mTLS failed: %v", err)
	}
	defer embCertOnly.Close()

	// Admin watcher verifies own-topic publish arrives while sibling write is dropped by ACL.
	watcherCh := make(chan string, 4)
	watcher := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(embCertOnly.LocalClientURL()).
		SetClientID("coop-certonly-watcher").
		SetUsername("coop_admin").
		SetPassword("admin_secret").
		SetTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{adminKP},
		}).
		SetConnectRetry(false))
	if tok := watcher.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("cert-only admin watcher connect failed: %v", tok.Error())
	}
	defer watcher.Disconnect(50)
	if tok := watcher.Subscribe("pigeons/registry/+/status", 1, func(_ mqtt.Client, m mqtt.Message) {
		watcherCh <- m.Topic()
	}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("cert-only admin watcher subscribe failed: %v", tok.Error())
	}

	certOnlyEdge := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(embCertOnly.LocalClientURL()).
		SetClientID("pigeoneer-rv64-edge-01").
		SetTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{edge1KP},
		}).
		SetConnectRetry(false))
	if tok := certOnlyEdge.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("expected cert-only edge mTLS client to connect without EdgeUsername, got %v", tok.Error())
	}
	defer certOnlyEdge.Disconnect(50)

	if tok := certOnlyEdge.Publish("pigeons/registry/rv64-edge-01/status", 1, false, []byte("ONLINE")); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("expected cert-only edge publish to own status topic to succeed, got %v", tok.Error())
	}
	select {
	case gotTopic := <-watcherCh:
		if gotTopic != "pigeons/registry/rv64-edge-01/status" {
			t.Fatalf("unexpected topic received: %q", gotTopic)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cert-only edge publish on own topic")
	}

	// Sibling write MUST be rejected by OnACLCheck.
	_ = certOnlyEdge.Publish("pigeons/registry/rv64-edge-02/status", 1, false, []byte("ONLINE")).WaitTimeout(300 * time.Millisecond)
	select {
	case siblingTopic := <-watcherCh:
		t.Fatalf("expected sibling topic write in cert-only mode to be denied, got %q", siblingTopic)
	case <-time.After(100 * time.Millisecond):
	}

	// Spoofed client_id in cert-only mode MUST be rejected at connect.
	certOnlySpoof := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(embCertOnly.LocalClientURL()).
		SetClientID("pigeoneer-rv64-edge-02").
		SetTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      caPool,
			Certificates: []tls.Certificate{edge1KP},
		}).
		SetConnectRetry(false))
	if tok := certOnlySpoof.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
		certOnlySpoof.Disconnect(50)
		t.Fatal("expected cert-only edge client with mismatched client_id to be rejected")
	}
}

func TestPerNodeAuthHookRejectsRetainedCTDAndNonCanonicalClientID(t *testing.T) {
	hook := &perNodeAuthHook{
		adminUser:  "coop_admin",
		adminPass:  "admin_secret",
		edgeUser:   "edge_node",
		edgePass:   "edge_secret",
		edgePrefix: "pigeoneer-",
		topicRoot:  "pigeons",
	}

	// OnPublish must reject Retain=true on <root>/ctd/#.
	retainedCTD := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true, Qos: 1},
		TopicName:   "pigeons/ctd/observer/edge-01/host-os",
		Payload:     []byte(`{"node_id":"edge-01"}`),
	}
	if _, err := hook.OnPublish(&mochi.Client{ID: "pigeoneer-edge-01"}, retainedCTD); !errors.Is(err, packets.ErrRejectPacket) {
		t.Fatalf("expected packets.ErrRejectPacket for retained CTD publish, got %v", err)
	}

	nonRetainedCTD := retainedCTD
	nonRetainedCTD.FixedHeader.Retain = false
	if _, err := hook.OnPublish(&mochi.Client{ID: "pigeoneer-edge-01"}, nonRetainedCTD); err != nil {
		t.Fatalf("expected non-retained CTD publish to be allowed, got %v", err)
	}

	retainedStatus := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true, Qos: 1},
		TopicName:   "pigeons/registry/edge-01/status",
		Payload:     []byte("ONLINE"),
	}
	if _, err := hook.OnPublish(&mochi.Client{ID: "pigeoneer-edge-01"}, retainedStatus); err != nil {
		t.Fatalf("expected retained registry status publish to be allowed, got %v", err)
	}

	// Stealth / non-canonical ClientIDs must be rejected on connect and ACL check.
	for _, badID := range []string{
		"",
		"edge-01",
		"pigeoneer-",
		" pigeoneer-edge-01",
		"pigeoneer-edge-01 ",
		"\tpigeoneer-edge-01\n",
		"pigeoneer-edge 01",
	} {
		cl := &mochi.Client{
			ID:         badID,
			Properties: mochi.ClientProperties{Username: []byte("edge_node")},
		}
		if hook.OnConnectAuthenticate(cl, packets.Packet{
			Connect: packets.ConnectParams{
				Username: []byte("edge_node"),
				Password: []byte("edge_secret"),
			},
		}) {
			t.Fatalf("expected non-canonical/whitespace edge client_id %q to fail auth", badID)
		}
		if hook.OnACLCheck(cl, "pigeons/registry/edge-01/status", true) {
			t.Fatalf("expected non-canonical/whitespace edge client_id %q to fail ACL", badID)
		}
	}

	// Prefix stripping is strictly single-prefix so two distinct ClientIDs cannot alias the same nodeID.
	prefixedNode := &mochi.Client{
		ID:         "pigeoneer-pigeoneer-01",
		Properties: mochi.ClientProperties{Username: []byte("edge_node")},
	}
	if !hook.OnACLCheck(prefixedNode, "pigeons/registry/pigeoneer-01/status", true) {
		t.Fatal("expected pigeoneer-pigeoneer-01 to map strictly to node_id pigeoneer-01")
	}
	singlePrefixed := &mochi.Client{
		ID:         "pigeoneer-01",
		Properties: mochi.ClientProperties{Username: []byte("edge_node")},
	}
	if hook.OnACLCheck(singlePrefixed, "pigeons/registry/pigeoneer-01/status", true) {
		t.Fatal("expected pigeoneer-01 NOT to alias node_id pigeoneer-01")
	}

	// Retained LWT on <root>/ctd/... must be rejected on connect.
	if hook.OnConnectAuthenticate(&mochi.Client{ID: "pigeoneer-edge-01"}, packets.Packet{
		Connect: packets.ConnectParams{
			Username:   []byte("edge_node"),
			Password:   []byte("edge_secret"),
			WillFlag:   true,
			WillRetain: true,
			WillTopic:  "pigeons/ctd/observer/edge-01/host-os",
		},
	}) {
		t.Fatal("expected retained WillTopic on ctd/# to fail auth")
	}
}

func TestEmbeddedBrokerRetainCTDRejectionBoundsAndLocalClientURL(t *testing.T) {
	emb, err := StartEmbedded(EmbeddedOptions{
		ListenAddr:   "127.0.0.1:0",
		Username:     "coop_admin",
		Password:     "admin_secret",
		EdgeUsername: "edge_node",
		EdgePassword: "edge_secret",
		TopicRoot:    "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	caps := emb.server.Options.Capabilities
	if caps.MaximumSessionExpiryInterval != 600 ||
		caps.MaximumInflight != 1024 ||
		caps.MaximumClientWritesPending != 2048 ||
		caps.MaximumClients != 512 ||
		caps.MaximumPacketSize != 1<<20 {
		t.Fatalf("unexpected Mochi capabilities bounds: %+v", caps)
	}

	// Reject stealth ClientIDs live over MQTT.
	for _, badID := range []string{"edge-01", " pigeoneer-edge-01 ", "pigeoneer-"} {
		cli := mqtt.NewClient(mqtt.NewClientOptions().
			AddBroker(emb.LocalClientURL()).
			SetClientID(badID).
			SetUsername("edge_node").
			SetPassword("edge_secret").
			SetConnectRetry(false))
		if tok := cli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
			cli.Disconnect(50)
			t.Fatalf("expected live embedded broker to reject stealth client_id %q", badID)
		}
	}

	edgeCli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("pigeoneer-edge-01").
		SetUsername("edge_node").
		SetPassword("edge_secret").
		SetConnectRetry(false))
	if tok := edgeCli.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("canonical edge connect failed: %v", tok.Error())
	}
	defer edgeCli.Disconnect(50)

	// Publish retained message on ctd/# (QoS 0 so client doesn't block waiting for PUBACK) -> must be rejected/not retained.
	_ = edgeCli.Publish("pigeons/ctd/observer/edge-01/host-os", 0, true, []byte(`{"node_id":"edge-01"}`)).WaitTimeout(500 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if retained := emb.server.Topics.Messages("pigeons/ctd/#"); len(retained) != 0 {
		t.Fatalf("expected 0 retained messages on pigeons/ctd/#, got %d", len(retained))
	}

	// Also verify anonymous mode enforces ctd/# retain guard.
	embAnon, err := StartEmbedded(EmbeddedOptions{ListenAddr: "127.0.0.1:0", TopicRoot: "pigeons"})
	if err != nil {
		t.Fatalf("StartEmbedded anon failed: %v", err)
	}
	defer embAnon.Close()

	anonCli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(embAnon.LocalClientURL()).
		SetClientID("anon-retain-tester").
		SetConnectRetry(false))
	if tok := anonCli.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("anon connect failed: %v", tok.Error())
	}
	defer anonCli.Disconnect(50)

	_ = anonCli.Publish("pigeons/ctd/observer/edge-01/host-os", 0, true, []byte(`{"node_id":"edge-01"}`)).WaitTimeout(500 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if retained := embAnon.server.Topics.Messages("pigeons/ctd/#"); len(retained) != 0 {
		t.Fatalf("expected 0 retained messages on pigeons/ctd/# in anon mode, got %d", len(retained))
	}

	// LocalClientURL rewrite vs preservation rules.
	cases := []struct {
		boundAddr string
		tlsActive bool
		want      string
	}{
		{boundAddr: "", tlsActive: false, want: "tcp://127.0.0.1:1883"},
		{boundAddr: ":1883", tlsActive: false, want: "tcp://127.0.0.1:1883"},
		{boundAddr: "0.0.0.0:1883", tlsActive: false, want: "tcp://127.0.0.1:1883"},
		{boundAddr: "[::]:1883", tlsActive: false, want: "tcp://[::1]:1883"},
		{boundAddr: "[::]:8883", tlsActive: true, want: "tls://[::1]:8883"},
		{boundAddr: "127.0.0.1:1884", tlsActive: false, want: "tcp://127.0.0.1:1884"},
		{boundAddr: "192.0.2.10:1883", tlsActive: false, want: "tcp://192.0.2.10:1883"},
		{boundAddr: "[2001:db8::1]:8883", tlsActive: true, want: "tls://[2001:db8::1]:8883"},
		{boundAddr: "[::1]:1885", tlsActive: false, want: "tcp://[::1]:1885"},
	}
	for _, tc := range cases {
		b := &EmbeddedBroker{boundAddr: tc.boundAddr, tlsActive: tc.tlsActive}
		if got := b.LocalClientURL(); got != tc.want {
			t.Fatalf("LocalClientURL(%q, tls=%v) = %q, want %q", tc.boundAddr, tc.tlsActive, got, tc.want)
		}
	}

	if embV6, err := StartEmbedded(EmbeddedOptions{ListenAddr: "[::1]:0"}); err == nil {
		defer embV6.Close()
		gotURL := embV6.LocalClientURL()
		if !strings.HasPrefix(gotURL, "tcp://[::1]:") {
			t.Fatalf("expected IPv6 loopback LocalClientURL to preserve ::1, got %q", gotURL)
		}
	}
}
