package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
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
