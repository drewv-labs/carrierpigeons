package coop

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/brand"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"github.com/drewv-labs/carrierpigeons/pkg/observer"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type mockBaseDB struct {
	mu           sync.Mutex
	ensureCalls  int
	meshIdentity core.MeshIdentity
	events       []*core.CTDPayload
	tagsByID     map[string][]string
	statusByID   map[string]string
}

func newMockBaseDB() *mockBaseDB {
	return &mockBaseDB{
		tagsByID:   make(map[string][]string),
		statusByID: make(map[string]string),
	}
}

func (m *mockBaseDB) EnsureSchema(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureCalls++
	return nil
}

func (m *mockBaseDB) ProvisionRoles(_ context.Context, _, _ string) error { return nil }

func (m *mockBaseDB) UpsertMeshIdentity(_ context.Context, id core.MeshIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.meshIdentity = id
	return nil
}

func (m *mockBaseDB) GetMeshIdentity(_ context.Context) (*core.MeshIdentity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.meshIdentity
	return &id, nil
}

func (m *mockBaseDB) InsertEvent(_ context.Context, ev *core.CTDPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ev)
	return nil
}

func (m *mockBaseDB) BatchInsertEvents(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, payloads...)
	return int64(len(payloads)), nil
}

func (m *mockBaseDB) UpsertArtifact(_ context.Context, _ core.Artifact) error { return nil }
func (m *mockBaseDB) InsertTrace(_ context.Context, _ core.TraceEdge) error   { return nil }

func (m *mockBaseDB) UpsertNodeHello(_ context.Context, hello core.NodeHello) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tags := core.NormalizeTags(hello.BootTags)
	m.tagsByID[hello.NodeID] = tags
	m.statusByID[hello.NodeID] = "ONLINE"
	return tags, nil
}

func (m *mockBaseDB) UpdateNodePresence(_ context.Context, nodeID, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusByID[nodeID] = status
	return nil
}

func (m *mockBaseDB) UpdateNodePresenceTransition(_ context.Context, nodeID, status string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev := m.statusByID[nodeID]
	m.statusByID[nodeID] = status
	return prev, prev != status, nil
}

func (m *mockBaseDB) ResolveNodes(_ context.Context, sel core.RunnerSelector) ([]string, error) {
	return sel.Nodes, nil
}

func (m *mockBaseDB) AddNodeTags(_ context.Context, nodeID string, tags []string) ([]string, error) {
	return tags, nil
}

func (m *mockBaseDB) RemoveNodeTags(_ context.Context, _ string, _ []string) ([]string, error) {
	return nil, nil
}

func (m *mockBaseDB) GetClusterStatus(_ context.Context, _ time.Duration) ([]db.NodeSummary, error) {
	return nil, nil
}

func (m *mockBaseDB) GetLineageDAG(_ context.Context, _ string, _ int) (*db.DAG, error) {
	return &db.DAG{}, nil
}

func (m *mockBaseDB) GetLedgerActivity(_ context.Context, _ int) ([]db.LedgerActivity, error) {
	return nil, nil
}

func (m *mockBaseDB) QueryReadOnlyMaps(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
	return nil, nil
}

func (m *mockBaseDB) ExplainReadOnly(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (m *mockBaseDB) EnsureMathSchema(_ context.Context) error { return nil }
func (m *mockBaseDB) SavePMVDraft(_ context.Context, rec db.PMVRecord) (*db.PMVRecord, error) {
	return &rec, nil
}
func (m *mockBaseDB) UpdatePMVState(_ context.Context, _ db.PMVRecord) error { return nil }
func (m *mockBaseDB) GetPMV(_ context.Context, _ string) (*db.PMVRecord, error) {
	return nil, nil
}
func (m *mockBaseDB) ListPMVs(_ context.Context) ([]db.PMVRecord, error)       { return nil, nil }
func (m *mockBaseDB) MaterializePMV(_ context.Context, _, _ string) error      { return nil }
func (m *mockBaseDB) DropPMV(_ context.Context, _ string) error                { return nil }
func (m *mockBaseDB) FetchForUnload(_ context.Context, _ db.UnloadFilter) ([]core.CTDPayload, error) {
	return nil, nil
}
func (m *mockBaseDB) RegisterView(_ context.Context, _, _ string) error { return nil }
func (m *mockBaseDB) ExecRaw(_ context.Context, _ string, _ ...any) error {
	return nil
}
func (m *mockBaseDB) QueryMaps(_ context.Context, _ string, _ ...any) ([]map[string]any, error) {
	return nil, nil
}
func (m *mockBaseDB) Close() {}

func writeSelfSignedCert(t *testing.T, dir, prefix string, dnsNames []string, ipAddrs []net.IP) (certPath, keyPath string, pemBytes []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}
	now := time.Now().Add(-1 * time.Minute)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(42),
		Subject:               pkix.Name{CommonName: "coop-test"},
		NotBefore:             now,
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddrs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate failed: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey failed: %v", err)
	}
	pemBytes = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certPath = filepath.Join(dir, prefix+".crt")
	keyPath = filepath.Join(dir, prefix+".key")
	if err := os.WriteFile(certPath, pemBytes, 0o600); err != nil {
		t.Fatalf("write cert failed: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key failed: %v", err)
	}
	return certPath, keyPath, pemBytes
}

func TestServerEmbeddedStartStop(t *testing.T) {
	store := newMockBaseDB()
	srv := New(config.CoopConfig{
		MeshName:    "unit-mesh",
		NodeID:      "coop-unit-01",
		SelfObserve: false,
		MQTT: config.CoopMQTTConfig{
			Mode:   "embedded",
			Listen: "127.0.0.1:0",
		},
		Ingest: config.IngestConfig{
			QueueSize:     32,
			BatchSize:     4,
			FlushInterval: "20ms",
		},
	}, Options{Store: store})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start failed: %v", err)
	}
	defer srv.Stop()

	if srv.Store() != store {
		t.Fatal("expected Server.Store() to return injected store")
	}
	if srv.MeshName() != "unit-mesh" {
		t.Fatalf("unexpected MeshName: %q", srv.MeshName())
	}
	if srv.BrokerURL() == "" {
		t.Fatal("expected non-empty BrokerURL after Start")
	}

	store.mu.Lock()
	ensureCalls := store.ensureCalls
	meshID := store.meshIdentity
	store.mu.Unlock()
	if ensureCalls != 1 {
		t.Fatalf("expected EnsureSchema called once, got %d", ensureCalls)
	}
	if meshID.MeshName != "unit-mesh" || meshID.CoopNodeID != "coop-unit-01" {
		t.Fatalf("unexpected registered MeshIdentity: %+v", meshID)
	}
}

func TestServerEmbeddedLoopbackTLSWithoutLocalhostSAN(t *testing.T) {
	cases := []struct {
		name       string
		dnsNames   []string
		ipAddrs    []net.IP
		serverName string
	}{
		{
			name:       "hostname-only SAN",
			dnsNames:   []string{"coop.example.internal"},
			serverName: "coop.example.internal",
		},
		{
			name:       "LAN IP-only SAN",
			ipAddrs:    []net.IP{net.ParseIP("192.0.2.10")},
			serverName: "192.0.2.10",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			certPath, keyPath, pemBytes := writeSelfSignedCert(t, tmp, "coop", tc.dnsNames, tc.ipAddrs)

			store := newMockBaseDB()
			srv := New(config.CoopConfig{
				MeshName:      "tls-loopback-mesh",
				NodeID:        "coop-tls-01",
				SelfObserve:   true,
				PollInterval:  "50ms",
				SpoolCapacity: 64,
				MQTT: config.CoopMQTTConfig{
					Mode:        "embedded",
					Listen:      "127.0.0.1:0",
					Username:    "coop_admin",
					Password:    "admin_secret",
					TLSCertFile: certPath,
					TLSKeyFile:  keyPath,
				},
				Ingest: config.IngestConfig{
					QueueSize:     64,
					BatchSize:     1,
					FlushInterval: "20ms",
				},
				Observers: config.ObserversConfig{
					OS: true,
				},
			}, Options{Store: store})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := srv.Start(ctx); err != nil {
				t.Fatalf("Server.Start with %s cert failed: %v", tc.name, err)
			}
			defer srv.Stop()

			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pemBytes) {
				t.Fatal("failed to parse test cert PEM")
			}
			cli := mqtt.NewClient(mqtt.NewClientOptions().
				AddBroker(srv.BrokerURL()).
				SetClientID("tls-loopback-verifier").
				SetUsername("coop_admin").
				SetPassword("admin_secret").
				SetTLSConfig(&tls.Config{
					MinVersion: tls.VersionTLS12,
					RootCAs:    pool,
					ServerName: tc.serverName,
				}).
				SetConnectRetry(false))
			if tok := cli.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
				t.Fatalf("verifier TLS client connect failed: %v", tok.Error())
			}
			defer cli.Disconnect(50)

			meshCh := make(chan core.MeshIdentity, 1)
			if tok := cli.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
				var id core.MeshIdentity
				if json.Unmarshal(m.Payload(), &id) == nil && id.MeshName != "" {
					select {
					case meshCh <- id:
					default:
					}
				}
			}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe mesh topic failed: %v", tok.Error())
			}

			select {
			case id := <-meshCh:
				if id.MeshName != "tls-loopback-mesh" || id.CoopNodeID != "coop-tls-01" {
					t.Fatalf("unexpected MNS MeshIdentity over loopback TLS: %+v", id)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for Coop loopback subscriber MNS announcement over TLS")
			}

			// Verify implicit self-observer also published telemetry over loopback TLS into the store.
			deadline := time.Now().Add(4 * time.Second)
			for time.Now().Before(deadline) {
				store.mu.Lock()
				n := len(store.events)
				store.mu.Unlock()
				if n > 0 {
					break
				}
				time.Sleep(25 * time.Millisecond)
			}
			store.mu.Lock()
			eventCount := len(store.events)
			store.mu.Unlock()
			if eventCount == 0 {
				t.Fatal("expected self-observer telemetry events to ingest over loopback TLS")
			}
		})
	}
}

type blockingLedger struct {
	entered atomic.Bool
	signal  chan struct{}
}

func (b *blockingLedger) InsertEvent(_ context.Context, _ *core.CTDPayload) error { return nil }
func (b *blockingLedger) BatchInsertEvents(_ context.Context, payloads []*core.CTDPayload) (int64, error) {
	return int64(len(payloads)), nil
}
func (b *blockingLedger) UpsertNodeHello(ctx context.Context, _ core.NodeHello) ([]string, error) {
	if !b.entered.Swap(true) {
		close(b.signal)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (b *blockingLedger) UpdateNodePresence(_ context.Context, _, _ string) error { return nil }
func (b *blockingLedger) ResolveNodes(_ context.Context, _ core.RunnerSelector) ([]string, error) {
	return nil, nil
}
func (b *blockingLedger) UpsertArtifact(_ context.Context, _ core.Artifact) error { return nil }
func (b *blockingLedger) InsertTrace(_ context.Context, _ core.TraceEdge) error   { return nil }

func TestSubscriberDisconnectCompletesUnderSixSecondsWhenLedgerBlocks(t *testing.T) {
	emb, err := broker.StartEmbedded(broker.EmbeddedOptions{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartEmbedded failed: %v", err)
	}
	defer emb.Close()

	ledger := &blockingLedger{signal: make(chan struct{})}
	sub, err := broker.NewSubscriberWithOptions(broker.SubscriberOptions{
		BrokerURL:   emb.LocalClientURL(),
		MeshName:    "coop-stalled-db",
		CoopNodeID:  "coop-01",
		NtfyEnabled: false,
		Ingest: ingest.Options{
			QueueSize:     16,
			BatchSize:     4,
			FlushInterval: 20 * time.Millisecond,
		},
	}, ledger)
	if err != nil {
		t.Fatalf("NewSubscriberWithOptions failed: %v", err)
	}

	cli := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(emb.LocalClientURL()).
		SetClientID("coop-db-blocker"))
	if tok := cli.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		sub.Disconnect()
		t.Fatalf("client connect failed: %v", tok.Error())
	}
	defer cli.Disconnect(50)

	rawHello, _ := json.Marshal(core.NodeHello{
		NodeID: "edge-blocked-01",
		OS:     "linux",
		Arch:   "riscv64",
	})
	if tok := cli.Publish(core.HelloTopic("edge-blocked-01"), 1, false, rawHello); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		sub.Disconnect()
		t.Fatalf("publish hello failed: %v", tok.Error())
	}

	select {
	case <-ledger.signal:
	case <-time.After(2 * time.Second):
		sub.Disconnect()
		t.Fatal("timed out waiting for subscriber to enter blocked Ledger call")
	}

	start := time.Now()
	sub.Disconnect()
	elapsed := time.Since(start)
	if elapsed >= 6*time.Second {
		t.Fatalf("expected Subscriber.Disconnect() to complete in <6s when Ledger blocks, took %s", elapsed)
	}
}


func writeCertSignedBy(t *testing.T, dir, prefix string, tmpl *x509.Certificate, parent *x509.Certificate, signer *ecdsa.PrivateKey) (certPath, keyPath string, parsed *x509.Certificate, priv *ecdsa.PrivateKey) {
	t.Helper()
	var err error
	priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
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
	parsed, err = x509.ParseCertificate(der)
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

type pulseObserver struct {
	name     string
	interval time.Duration
	started  chan struct{}
	stopped  chan struct{}
	ctxDone  atomic.Bool
	once     sync.Once
}

func (o *pulseObserver) Name() string { return o.name }

func (o *pulseObserver) Observe(ctx context.Context, nodeID string, pub core.TelemetryPublisher) {
	o.once.Do(func() { close(o.started) })
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	defer close(o.stopped)

	_ = pub.Publish(core.NewCTDPayload(nodeID, o.name, "active").AddMetric("pulse", 1))
	for {
		select {
		case <-ctx.Done():
			o.ctxDone.Store(true)
			return
		case <-ticker.C:
			_ = pub.Publish(core.NewCTDPayload(nodeID, o.name, "active").AddMetric("pulse", 1))
		}
	}
}

type flakyBootStore struct {
	*mockBaseDB
	failSchemaAttempts int
	failUpsertAttempts int
	schemaAttempts     int
	upsertAttempts     int
	onUpsert           func(attempt int)
}

func (f *flakyBootStore) EnsureSchema(ctx context.Context) error {
	f.mu.Lock()
	f.schemaAttempts++
	attempt := f.schemaAttempts
	shouldFail := attempt <= f.failSchemaAttempts
	f.mu.Unlock()
	if shouldFail {
		return errors.New("injected schema failure")
	}
	return f.mockBaseDB.EnsureSchema(ctx)
}

func (f *flakyBootStore) UpsertMeshIdentity(ctx context.Context, id core.MeshIdentity) error {
	f.mu.Lock()
	f.upsertAttempts++
	attempt := f.upsertAttempts
	shouldFail := attempt <= f.failUpsertAttempts
	cb := f.onUpsert
	f.mu.Unlock()
	if cb != nil {
		cb(attempt)
	}
	if shouldFail {
		return errors.New("injected MNS upsert failure")
	}
	return f.mockBaseDB.UpsertMeshIdentity(ctx, id)
}

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitForNodeEvents(t *testing.T, store *mockBaseDB, nodeID string, minCount int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		count := 0
		for _, ev := range store.events {
			if ev != nil && ev.NodeID == nodeID {
				count++
			}
		}
		store.mu.Unlock()
		if count >= minCount {
			return count
		}
		time.Sleep(20 * time.Millisecond)
	}
	store.mu.Lock()
	count := 0
	for _, ev := range store.events {
		if ev != nil && ev.NodeID == nodeID {
			count++
		}
	}
	store.mu.Unlock()
	return count
}

func TestServerEmbeddedLoopbackTLSNoSANAndVerifiedSelfNode(t *testing.T) {
	tmp := t.TempDir()
	certPath, keyPath, _ := writeSelfSignedCert(t, tmp, "nosan", nil, nil)

	obs := &pulseObserver{
		name:     "nosan-pulse",
		interval: 25 * time.Millisecond,
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	store := newMockBaseDB()
	srv := New(config.CoopConfig{
		MeshName:      "nosan-loopback-mesh",
		NodeID:        "coop-nosan-01",
		SelfObserve:   true,
		PollInterval:  "25ms",
		SpoolCapacity: 64,
		MQTT: config.CoopMQTTConfig{
			Mode:                  "embedded",
			Listen:                "127.0.0.1:0",
			Username:              "coop_admin",
			Password:              "admin_secret",
			EdgeUsername:          "edge_scoped",
			EdgePassword:          "edge_secret",
			TLSCertFile:           certPath,
			TLSKeyFile:            keyPath,
			TLSInsecureSkipVerify: true, // embedded selfNode must ignore skip-verify and verify via loopback bridge
		},
		Ingest: config.IngestConfig{
			QueueSize:     64,
			BatchSize:     1,
			FlushInterval: "20ms",
		},
	}, Options{
		Store:          store,
		ExtraObservers: []observer.Observer{obs},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start with SAN-less cert failed: %v", err)
	}

	if srv.loopbackBridge == nil {
		srv.Stop()
		t.Fatal("expected loopbackTLSBridge to be active when server cert lacks 127.0.0.1 SAN")
	}
	bridgeDir := srv.loopbackBridge.tmpDir

	if got := waitForNodeEvents(t, store, "coop-nosan-01", 1, 4*time.Second); got == 0 {
		srv.Stop()
		t.Fatal("expected selfNode telemetry over pinned/bridge TLS when leaf cert has no SANs")
	}

	srv.Stop()
	if _, err := os.Stat(bridgeDir); !os.IsNotExist(err) {
		t.Fatalf("expected loopback bridge temp dir %s to be removed on Stop, stat err=%v", bridgeDir, err)
	}
}

func TestServerEmbeddedMutualTLSWiringAndCertOnlyEdgeAuth(t *testing.T) {
	now := time.Now().Add(-1 * time.Minute)
	exp := now.Add(24 * time.Hour)

	for _, tc := range []struct {
		name         string
		serverDNS    []string
		serverIPs    []net.IP
		edgeUsername string
		edgePassword string
		expectBridge bool
	}{
		{
			name:         "mTLS_with_loopback_SAN_and_edge_creds",
			serverIPs:    []net.IP{net.ParseIP("127.0.0.1")},
			edgeUsername: "edge_scoped",
			edgePassword: "edge_secret",
			expectBridge: false,
		},
		{
			name:         "mTLS_cert_only_edge_without_loopback_SAN",
			serverDNS:    []string{"coop.example.internal"},
			edgeUsername: "",
			edgePassword: "",
			expectBridge: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			caPath, _, caCert, caKey := writeCertSignedBy(t, tmp, "ca", &x509.Certificate{
				SerialNumber:          big.NewInt(1),
				Subject:               pkix.Name{CommonName: "test-mesh-ca"},
				NotBefore:             now,
				NotAfter:              exp,
				KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
				BasicConstraintsValid: true,
				IsCA:                  true,
			}, nil, nil)

			srvCertPath, srvKeyPath, _, _ := writeCertSignedBy(t, tmp, "server", &x509.Certificate{
				SerialNumber: big.NewInt(2),
				Subject:      pkix.Name{CommonName: "coop-server"},
				NotBefore:    now,
				NotAfter:     exp,
				KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				DNSNames:     tc.serverDNS,
				IPAddresses:  tc.serverIPs,
			}, caCert, caKey)

			nodeID := "coop-mtls-01"
			selfClientCertPath, selfClientKeyPath, _, _ := writeCertSignedBy(t, tmp, "self-client", &x509.Certificate{
				SerialNumber: big.NewInt(3),
				Subject:      pkix.Name{CommonName: nodeID},
				NotBefore:    now,
				NotAfter:     exp,
				KeyUsage:     x509.KeyUsageDigitalSignature,
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				DNSNames:     []string{nodeID},
			}, caCert, caKey)

			adminClientCertPath, adminClientKeyPath, _, _ := writeCertSignedBy(t, tmp, "admin-client", &x509.Certificate{
				SerialNumber: big.NewInt(4),
				Subject:      pkix.Name{CommonName: "coop-admin-watcher"},
				NotBefore:    now,
				NotAfter:     exp,
				KeyUsage:     x509.KeyUsageDigitalSignature,
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			}, caCert, caKey)

			// Missing client cert/key when RequireClientCert=true must fail closed before starting listeners.
			badSrv := New(config.CoopConfig{
				MeshName:    "mtls-bad-mesh",
				NodeID:      nodeID,
				SelfObserve: false,
				MQTT: config.CoopMQTTConfig{
					Mode:              "embedded",
					Listen:            "127.0.0.1:0",
					Username:          "coop_admin",
					Password:          "admin_secret",
					TLSCertFile:       srvCertPath,
					TLSKeyFile:        srvKeyPath,
					TLSClientCAFile:   caPath,
					RequireClientCert: true,
				},
			}, Options{Store: newMockBaseDB()})
			if err := badSrv.Start(context.Background()); err == nil {
				badSrv.Stop()
				t.Fatal("expected Server.Start to fail fast when RequireClientCert=true without TLSClientCertFile/TLSClientKeyFile")
			}

			obs := &pulseObserver{
				name:     "mtls-pulse",
				interval: 25 * time.Millisecond,
				started:  make(chan struct{}),
				stopped:  make(chan struct{}),
			}
			store := newMockBaseDB()
			srv := New(config.CoopConfig{
				MeshName:      "mtls-wired-mesh",
				NodeID:        nodeID,
				SelfObserve:   true,
				PollInterval:  "25ms",
				SpoolCapacity: 64,
				MQTT: config.CoopMQTTConfig{
					Mode:              "embedded",
					Listen:            "127.0.0.1:0",
					Username:          "coop_admin",
					Password:          "admin_secret",
					EdgeUsername:      tc.edgeUsername,
					EdgePassword:      tc.edgePassword,
					TLSCertFile:       srvCertPath,
					TLSKeyFile:        srvKeyPath,
					TLSCAFile:         caPath,
					TLSClientCAFile:   caPath,
					TLSClientCertFile: selfClientCertPath,
					TLSClientKeyFile:  selfClientKeyPath,
					RequireClientCert: true,
				},
				Ingest: config.IngestConfig{
					QueueSize:     64,
					BatchSize:     1,
					FlushInterval: "20ms",
				},
			}, Options{
				Store:          store,
				ExtraObservers: []observer.Observer{obs},
			})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := srv.Start(ctx); err != nil {
				t.Fatalf("Server.Start mTLS failed: %v", err)
			}
			defer srv.Stop()

			if (srv.loopbackBridge != nil) != tc.expectBridge {
				t.Fatalf("loopbackBridge active=%v, want %v", srv.loopbackBridge != nil, tc.expectBridge)
			}

			// Both subscriber and selfNode must authenticate cleanly over mTLS.
			if got := waitForNodeEvents(t, store, nodeID, 1, 4*time.Second); got == 0 {
				t.Fatal("expected selfNode telemetry to ingest into store under mTLS")
			}

			caPEM, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatalf("read CA file: %v", err)
			}
			caPool := x509.NewCertPool()
			caPool.AppendCertsFromPEM(caPEM)
			serverName := "127.0.0.1"
			if len(tc.serverDNS) > 0 {
				serverName = tc.serverDNS[0]
			}

			// Verify subscriber published retained MNS identity using admin client cert.
			adminKP, err := tls.LoadX509KeyPair(adminClientCertPath, adminClientKeyPath)
			if err != nil {
				t.Fatalf("load admin keypair: %v", err)
			}
			adminWatcher := mqtt.NewClient(mqtt.NewClientOptions().
				AddBroker(srv.BrokerURL()).
				SetClientID("mtls-admin-watcher").
				SetUsername("coop_admin").
				SetPassword("admin_secret").
				SetTLSConfig(&tls.Config{
					MinVersion:   tls.VersionTLS12,
					RootCAs:      caPool,
					ServerName:   serverName,
					Certificates: []tls.Certificate{adminKP},
				}).
				SetConnectRetry(false))
			if tok := adminWatcher.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
				t.Fatalf("mTLS admin watcher connect failed: %v", tok.Error())
			}
			defer adminWatcher.Disconnect(50)

			meshCh := make(chan core.MeshIdentity, 1)
			if tok := adminWatcher.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
				var id core.MeshIdentity
				if json.Unmarshal(m.Payload(), &id) == nil && id.MeshName != "" {
					select {
					case meshCh <- id:
					default:
					}
				}
			}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe mesh topic failed: %v", tok.Error())
			}
			select {
			case id := <-meshCh:
				if id.MeshName != "mtls-wired-mesh" || id.CoopNodeID != nodeID {
					t.Fatalf("unexpected MNS MeshIdentity over mTLS: %+v", id)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for MNS identity over mTLS")
			}

			// Unauthenticated client without client cert must be rejected at TLS handshake.
			noCertCli := mqtt.NewClient(mqtt.NewClientOptions().
				AddBroker(srv.BrokerURL()).
				SetClientID("pigeoneer-" + nodeID).
				SetUsername(tc.edgeUsername).
				SetPassword(tc.edgePassword).
				SetTLSConfig(&tls.Config{
					MinVersion: tls.VersionTLS12,
					RootCAs:    caPool,
					ServerName: serverName,
				}).
				SetConnectRetry(false))
			if tok := noCertCli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
				noCertCli.Disconnect(50)
				t.Fatal("expected mTLS embedded broker to reject client without client cert")
			}

			// Client presenting nodeID's cert MUST NOT impersonate a different node_id.
			selfKP, err := tls.LoadX509KeyPair(selfClientCertPath, selfClientKeyPath)
			if err != nil {
				t.Fatalf("load self keypair: %v", err)
			}
			spoofCli := mqtt.NewClient(mqtt.NewClientOptions().
				AddBroker(srv.BrokerURL()).
				SetClientID("pigeoneer-spoofed-victim").
				SetUsername(tc.edgeUsername).
				SetPassword(tc.edgePassword).
				SetTLSConfig(&tls.Config{
					MinVersion:   tls.VersionTLS12,
					RootCAs:      caPool,
					ServerName:   serverName,
					Certificates: []tls.Certificate{selfKP},
				}).
				SetConnectRetry(false))
			if tok := spoofCli.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() == nil {
				spoofCli.Disconnect(50)
				t.Fatal("expected mTLS embedded broker to reject spoofed node_id for client cert")
			}
		})
	}
}

func TestServerSelfNodeScopedEdgeCredentialsAndPrefix(t *testing.T) {
	obs := &pulseObserver{
		name:     "scoped-pulse",
		interval: 25 * time.Millisecond,
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	store := newMockBaseDB()
	customBrand := brand.Spec{
		ProductName: "ScoutMesh",
		CoopName:    "ScoutHub",
		EdgeName:    "Scout",
		CLIName:     "scout",
		TopicRoot:   "scout",
		EnvPrefix:   "SCOUT",
		MathSchema:  "scoutmath",
	}

	srv := New(config.CoopConfig{
		MeshName:      "scoped-edge-mesh",
		NodeID:        "hub-scoped-01",
		SelfObserve:   true,
		PollInterval:  "25ms",
		SpoolCapacity: 64,
		MQTT: config.CoopMQTTConfig{
			Mode:         "embedded",
			Listen:       "127.0.0.1:0",
			Username:     "hub_admin",
			Password:     "hub_admin_secret",
			EdgeUsername: "edge_scoped",
			EdgePassword: "edge_scoped_secret",
		},
		Ingest: config.IngestConfig{
			QueueSize:     64,
			BatchSize:     1,
			FlushInterval: "20ms",
		},
	}, Options{
		Store:          store,
		Brand:          customBrand,
		ExtraObservers: []observer.Observer{obs},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start failed: %v", err)
	}
	defer srv.Stop()

	if got := waitForNodeEvents(t, store, "hub-scoped-01", 1, 4*time.Second); got == 0 {
		t.Fatal("expected selfNode telemetry with custom EdgeName prefix and EdgeUsername to ingest")
	}

	// Attempt to publish a cross-node spoofed CTD event via selfNode:
	// perNodeAuthHook MUST reject it because selfNode runs with scoped edge credentials, not admin credentials.
	if err := srv.selfNode.Publish(core.NewCTDPayload("victim-node", "spoof", "active").AddMetric("evil", 1)); err != nil {
		t.Fatalf("selfNode.Publish returned unexpected local spool error: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	if victimEvents := waitForNodeEvents(t, store, "victim-node", 1, 200*time.Millisecond); victimEvents != 0 {
		t.Fatalf("expected scoped selfNode cross-node publish to be blocked by per-node ACL, got %d events", victimEvents)
	}
}

func TestServerRunCtxOutlivesBootContextCancellation(t *testing.T) {
	obs := &pulseObserver{
		name:     "runctx-pulse",
		interval: 25 * time.Millisecond,
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	store := newMockBaseDB()
	srv := New(config.CoopConfig{
		MeshName:      "runctx-mesh",
		NodeID:        "coop-runctx-01",
		SelfObserve:   true,
		PollInterval:  "25ms",
		SpoolCapacity: 64,
		MQTT: config.CoopMQTTConfig{
			Mode:         "embedded",
			Listen:       "127.0.0.1:0",
			Username:     "coop_admin",
			Password:     "admin_secret",
			EdgeUsername: "edge_scoped",
			EdgePassword: "edge_secret",
		},
		Ingest: config.IngestConfig{
			QueueSize:     64,
			BatchSize:     1,
			FlushInterval: "20ms",
		},
	}, Options{
		Store:          store,
		ExtraObservers: []observer.Observer{obs},
	})

	bootCtx, bootCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := srv.Start(bootCtx); err != nil {
		bootCancel()
		t.Fatalf("Server.Start failed: %v", err)
	}

	select {
	case <-obs.started:
	case <-time.After(2 * time.Second):
		bootCancel()
		srv.Stop()
		t.Fatal("timed out waiting for selfNode observer to start")
	}

	// Immediately cancel the caller's boot context; long-lived selfNode must remain alive under Server's runCtx.
	bootCancel()

	store.mu.Lock()
	baselineCount := len(store.events)
	store.mu.Unlock()

	if got := waitForNodeEvents(t, store, "coop-runctx-01", baselineCount+3, 3*time.Second); got < baselineCount+3 {
		srv.Stop()
		t.Fatalf("expected selfNode to continue emitting telemetry after boot ctx cancellation (before=%d, after=%d)", baselineCount, got)
	}
	if obs.ctxDone.Load() {
		srv.Stop()
		t.Fatal("expected selfNode observer context NOT to cancel when boot ctx was cancelled")
	}

	srv.Stop()
	select {
	case <-obs.stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for selfNode observer to stop on Server.Stop()")
	}
	if !obs.ctxDone.Load() {
		t.Fatal("expected selfNode observer context to cancel when Server.Stop() was called")
	}
}

func TestServerPartialBootCleanupAndStopSemantics(t *testing.T) {
	t.Run("failed_start_does_not_burn_stopOnce_on_retry", func(t *testing.T) {
		store := &flakyBootStore{
			mockBaseDB:         newMockBaseDB(),
			failUpsertAttempts: 1,
		}
		obs := &pulseObserver{
			name:     "retry-pulse",
			interval: 25 * time.Millisecond,
			started:  make(chan struct{}),
			stopped:  make(chan struct{}),
		}
		srv := New(config.CoopConfig{
			MeshName:      "retry-boot-mesh",
			NodeID:        "coop-retry-01",
			SelfObserve:   true,
			PollInterval:  "25ms",
			SpoolCapacity: 64,
			MQTT: config.CoopMQTTConfig{
				Mode:   "embedded",
				Listen: "127.0.0.1:0",
			},
			Ingest: config.IngestConfig{
				QueueSize:     32,
				BatchSize:     1,
				FlushInterval: "20ms",
			},
		}, Options{
			Store:          store,
			ExtraObservers: []observer.Observer{obs},
		})

		if err := srv.Start(context.Background()); err == nil {
			srv.Stop()
			t.Fatal("expected first Start to fail on UpsertMeshIdentity error")
		}
		if srv.Store() != nil || srv.BrokerURL() != "" {
			t.Fatal("expected partial-boot cleanup to reset server state after failed Start")
		}

		// Retry Start on the same Server; stopOnce must still be intact for final Stop().
		if err := srv.Start(context.Background()); err != nil {
			t.Fatalf("expected retry Start after partial-boot failure to succeed, got %v", err)
		}
		brokerURL := srv.BrokerURL()
		if brokerURL == "" {
			srv.Stop()
			t.Fatal("expected non-empty BrokerURL on retry Start")
		}
		boundAddr := strings.TrimPrefix(brokerURL, "tcp://")

		select {
		case <-obs.started:
		case <-time.After(2 * time.Second):
			srv.Stop()
			t.Fatal("timed out waiting for selfNode observer on retry Start")
		}

		// Final Stop() must tear down the broker listener and selfNode cleanly.
		srv.Stop()
		// Subsequent Stop() must remain idempotent.
		srv.Stop()

		select {
		case <-obs.stopped:
		case <-time.After(3 * time.Second):
			t.Fatal("expected Server.Stop() to stop selfNode after prior failed Start")
		}
		if conn, dialErr := net.DialTimeout("tcp", boundAddr, 250*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			t.Fatalf("expected embedded broker listener %s to be closed after Stop()", boundAddr)
		}
	})

	t.Run("stop_after_failed_start_is_safe", func(t *testing.T) {
		store := &flakyBootStore{
			mockBaseDB:         newMockBaseDB(),
			failSchemaAttempts: 1,
		}
		srv := New(config.CoopConfig{
			MeshName:    "stop-after-fail-mesh",
			NodeID:      "coop-fail-01",
			SelfObserve: false,
			MQTT: config.CoopMQTTConfig{
				Mode:   "embedded",
				Listen: "127.0.0.1:0",
			},
		}, Options{Store: store})

		if err := srv.Start(context.Background()); err == nil {
			srv.Stop()
			t.Fatal("expected Start to fail on schema error")
		}
		srv.Stop()
		srv.Stop()
		if err := srv.Start(context.Background()); err == nil {
			srv.Stop()
			t.Fatal("expected Start after Stop() to be rejected")
		}
	})

	t.Run("mid_boot_ctx_cancel_cleans_up_resources", func(t *testing.T) {
		bootCtx, bootCancel := context.WithCancel(context.Background())
		store := &flakyBootStore{
			mockBaseDB: newMockBaseDB(),
			onUpsert: func(_ int) {
				bootCancel()
			},
		}
		srv := New(config.CoopConfig{
			MeshName:    "midboot-cancel-mesh",
			NodeID:      "coop-cancel-01",
			SelfObserve: true,
			MQTT: config.CoopMQTTConfig{
				Mode:   "embedded",
				Listen: "127.0.0.1:0",
			},
		}, Options{Store: store})

		if err := srv.Start(bootCtx); !errors.Is(err, context.Canceled) {
			srv.Stop()
			t.Fatalf("expected Start to fail with context.Canceled, got %v", err)
		}
		if srv.BrokerURL() != "" || srv.embedded != nil || srv.subscriber != nil || srv.selfNode != nil {
			t.Fatal("expected mid-boot cancellation to clean up all partial resources")
		}
		srv.Stop()
	})
}

func TestServerConcurrentStartStopRace(t *testing.T) {
	for iter := range 4 {
		store := newMockBaseDB()
		srv := New(config.CoopConfig{
			MeshName:      "concurrent-race-mesh",
			NodeID:        "coop-race-01",
			SelfObserve:   true,
			PollInterval:  "25ms",
			SpoolCapacity: 32,
			MQTT: config.CoopMQTTConfig{
				Mode:   "embedded",
				Listen: "127.0.0.1:0",
			},
			Ingest: config.IngestConfig{
				QueueSize:     32,
				BatchSize:     4,
				FlushInterval: "20ms",
			},
		}, Options{Store: store})

		var startOK atomic.Int32
		var wg sync.WaitGroup

		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := srv.Start(ctx); err == nil {
					startOK.Add(1)
				}
			}()
		}

		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 5 {
					_ = srv.Store()
					_ = srv.BrokerURL()
					_ = srv.MeshName()
					time.Sleep(time.Millisecond)
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if iter%2 == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			srv.Stop()
		}()

		wg.Wait()
		srv.Stop()

		if got := startOK.Load(); got > 1 {
			t.Fatalf("iter %d: expected at most 1 Start() to succeed, got %d", iter, got)
		}
		if srv.BrokerURL() != "" || srv.embedded != nil || srv.subscriber != nil || srv.selfNode != nil {
			t.Fatalf("iter %d: expected Server state cleared after Stop()", iter)
		}
	}
}

func TestServerExternalModeRedactsBrokerURLAndRejectsInvalidMode(t *testing.T) {
	// Invalid mqtt.mode must fail closed.
	invalidSrv := New(config.CoopConfig{
		MeshName: "invalid-mode-mesh",
		NodeID:   "coop-invalid-mode",
		MQTT: config.CoopMQTTConfig{
			Mode: "mosquitto",
		},
	}, Options{Store: newMockBaseDB()})
	if err := invalidSrv.Start(context.Background()); err == nil {
		invalidSrv.Stop()
		t.Fatal("expected Server.Start to reject invalid mqtt.mode")
	}

	tmp := t.TempDir()
	now := time.Now().Add(-1 * time.Minute)
	exp := now.Add(24 * time.Hour)
	caPath, _, caCert, caKey := writeCertSignedBy(t, tmp, "ca", &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ext-ca"},
		NotBefore:             now,
		NotAfter:              exp,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}, nil, nil)
	srvCertPath, srvKeyPath, _, _ := writeCertSignedBy(t, tmp, "ext-srv", &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "ext-broker"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, caCert, caKey)
	clientCertPath, clientKeyPath, _, _ := writeCertSignedBy(t, tmp, "ext-client", &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "coop-ext-01"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"coop-ext-01"},
	}, caCert, caKey)

	extBroker, err := broker.StartEmbedded(broker.EmbeddedOptions{
		ListenAddr:        "127.0.0.1:0",
		Username:          "coop_admin",
		Password:          "admin_secret",
		EdgeUsername:      "edge_scoped",
		EdgePassword:      "edge_secret",
		EdgeClientPrefix:  "pigeoneer",
		TLSCertFile:       srvCertPath,
		TLSKeyFile:        srvKeyPath,
		TLSClientCAFile:   caPath,
		RequireClientCert: true,
		TopicRoot:         "pigeons",
	})
	if err != nil {
		t.Fatalf("StartEmbedded external fixture failed: %v", err)
	}
	defer extBroker.Close()

	rawUserInfoURL := strings.Replace(extBroker.LocalClientURL(), "tls://", "tls://url_user:super_secret_mqtt_pw@", 1) + "?token=secret_query_tok"

	var logBuf syncLogBuffer
	origWriter := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(origWriter)

	// Subtest 1: Broker URL containing userinfo password and query token must be redacted even when dial/auth fails.
	userInfoSrv := New(config.CoopConfig{
		MeshName:    "ext-userinfo-redact",
		NodeID:      "coop-ext-01",
		SelfObserve: false,
		MQTT: config.CoopMQTTConfig{
			Mode:              "external",
			URL:               rawUserInfoURL,
			Username:          "coop_admin",
			Password:          "admin_secret",
			TLSCAFile:         caPath,
			TLSClientCertFile: clientCertPath,
			TLSClientKeyFile:  clientKeyPath,
		},
	}, Options{Store: newMockBaseDB()})
	if err := userInfoSrv.Start(context.Background()); err == nil {
		userInfoSrv.Stop()
		t.Fatal("expected external broker to reject mismatched URL userinfo credentials")
	}

	// Subtest 2: Live external mTLS broker with query token connects cleanly (subscriber + scoped selfNode) and redacts URL.
	rawBrokerURL := extBroker.LocalClientURL() + "?token=secret_query_tok"
	store := newMockBaseDB()
	srv := New(config.CoopConfig{
		MeshName:      "ext-redacted-mesh",
		NodeID:        "coop-ext-01",
		SelfObserve:   true,
		PollInterval:  "25ms",
		SpoolCapacity: 64,
		MQTT: config.CoopMQTTConfig{
			Mode:              "external",
			URL:               rawBrokerURL,
			Username:          "coop_admin",
			Password:          "admin_secret",
			EdgeUsername:      "edge_scoped",
			EdgePassword:      "edge_secret",
			TLSCAFile:         caPath,
			TLSClientCertFile: clientCertPath,
			TLSClientKeyFile:  clientKeyPath,
		},
		Ingest: config.IngestConfig{
			QueueSize:     64,
			BatchSize:     1,
			FlushInterval: "20ms",
		},
		Observers: config.ObserversConfig{
			OS: true,
		},
	}, Options{Store: store})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start external mode failed: %v", err)
	}
	defer srv.Stop()

	if got := waitForNodeEvents(t, store, "coop-ext-01", 1, 4*time.Second); got == 0 {
		t.Fatal("expected external mode selfNode telemetry to ingest over verified mTLS")
	}

	logs := logBuf.String()
	for _, raw := range []string{rawUserInfoURL, rawBrokerURL} {
		wantRedacted := config.RedactURL(raw)
		if !strings.Contains(logs, wantRedacted) {
			t.Fatalf("expected startup log to contain redacted URL %q, got:\n%s", wantRedacted, logs)
		}
	}
	if strings.Contains(logs, "super_secret_mqtt_pw") || strings.Contains(logs, "secret_query_tok") {
		t.Fatalf("startup log leaked raw broker credentials/token:\n%s", logs)
	}
}

func TestServerLoopbackBridgeRejectsUnauthorizedLocalClients(t *testing.T) {
	tmp := t.TempDir()
	now := time.Now().Add(-1 * time.Minute)
	exp := now.Add(24 * time.Hour)

	caPath, _, caCert, caKey := writeCertSignedBy(t, tmp, "ca", &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bridge-test-ca"},
		NotBefore:             now,
		NotAfter:              exp,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}, nil, nil)

	srvCertPath, srvKeyPath, _, _ := writeCertSignedBy(t, tmp, "server", &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "coop-server"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"coop.example.internal"},
	}, caCert, caKey)

	nodeID := "coop-bridge-01"
	selfClientCertPath, selfClientKeyPath, _, _ := writeCertSignedBy(t, tmp, "self-client", &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{nodeID},
	}, caCert, caKey)

	otherClientCertPath, otherClientKeyPath, _, _ := writeCertSignedBy(t, tmp, "other-client", &x509.Certificate{
		SerialNumber: big.NewInt(4),
		Subject:      pkix.Name{CommonName: "edge-other-02"},
		NotBefore:    now,
		NotAfter:     exp,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"edge-other-02"},
	}, caCert, caKey)

	store := newMockBaseDB()
	srv := New(config.CoopConfig{
		MeshName:      "bridge-sec-mesh",
		NodeID:        nodeID,
		SelfObserve:   true,
		PollInterval:  "25ms",
		SpoolCapacity: 32,
		MQTT: config.CoopMQTTConfig{
			Mode:              "embedded",
			Listen:            "127.0.0.1:0",
			Username:          "coop_admin",
			Password:          "admin_secret",
			EdgeUsername:      "edge_scoped",
			EdgePassword:      "edge_secret",
			TLSCertFile:       srvCertPath,
			TLSKeyFile:        srvKeyPath,
			TLSCAFile:         caPath,
			TLSClientCAFile:   caPath,
			TLSClientCertFile: selfClientCertPath,
			TLSClientKeyFile:  selfClientKeyPath,
			RequireClientCert: true,
		},
		Ingest: config.IngestConfig{
			QueueSize:     32,
			BatchSize:     1,
			FlushInterval: "20ms",
		},
		Observers: config.ObserversConfig{
			OS: true,
		},
	}, Options{Store: store})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start failed: %v", err)
	}
	defer srv.Stop()

	if srv.loopbackBridge == nil {
		t.Fatal("expected loopbackTLSBridge to be active")
	}
	if got := waitForNodeEvents(t, store, nodeID, 1, 4*time.Second); got == 0 {
		t.Fatal("expected selfNode telemetry via pinned-mTLS loopbackBridge")
	}

	bridgeCAPEM, err := os.ReadFile(srv.loopbackBridge.CAFile())
	if err != nil {
		t.Fatalf("read bridge CAFile: %v", err)
	}
	bridgePool := x509.NewCertPool()
	bridgePool.AppendCertsFromPEM(bridgeCAPEM)

	otherKP, err := tls.LoadX509KeyPair(otherClientCertPath, otherClientKeyPath)
	if err != nil {
		t.Fatalf("load other-client keypair: %v", err)
	}
	selfKP, err := tls.LoadX509KeyPair(selfClientCertPath, selfClientKeyPath)
	if err != nil {
		t.Fatalf("load self-client keypair: %v", err)
	}

	bridgeAddr := strings.TrimPrefix(srv.loopbackBridge.BrokerURL(), "tls://")
	for _, tc := range []struct {
		name  string
		certs []tls.Certificate
	}{
		{name: "no_client_cert", certs: nil},
		{name: "sibling_CA_signed_client_cert", certs: []tls.Certificate{otherKP}},
		{name: "configured_self_client_cert_not_ephemeral_bridge_cert", certs: []tls.Certificate{selfKP}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialer := &tls.Dialer{
				NetDialer: &net.Dialer{Timeout: 2 * time.Second},
				Config: &tls.Config{
					MinVersion:   tls.VersionTLS12,
					RootCAs:      bridgePool,
					Certificates: tc.certs,
				},
			}
			conn, dialErr := dialer.Dial("tcp", bridgeAddr)
			if dialErr == nil {
				// TLS 1.3 delivers client-cert alert on first Read/Write.
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 1)
				_, readErr := conn.Read(buf)
				_ = conn.Close()
				if readErr == nil {
					t.Fatalf("expected loopbackTLSBridge to reject unauthorized client (%s)", tc.name)
				}
			}
		})
	}
}

func TestServerEmbeddedCASignedLeafWithoutTLSCAFile(t *testing.T) {
	now := time.Now().Add(-1 * time.Minute)
	exp := now.Add(24 * time.Hour)

	for _, tc := range []struct {
		name              string
		requireClientCert bool
	}{
		{name: "ca_signed_leaf_without_TLSCAFile", requireClientCert: false},
		{name: "ca_signed_leaf_without_TLSCAFile_and_mTLS", requireClientCert: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			caPath, _, caCert, caKey := writeCertSignedBy(t, tmp, "ca", &x509.Certificate{
				SerialNumber:          big.NewInt(1),
				Subject:               pkix.Name{CommonName: "private-mesh-ca"},
				NotBefore:             now,
				NotAfter:              exp,
				KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
				BasicConstraintsValid: true,
				IsCA:                  true,
			}, nil, nil)

			// Leaf cert has Issuer != Subject, IsCA == false, and valid 127.0.0.1 SAN.
			srvCertPath, srvKeyPath, _, _ := writeCertSignedBy(t, tmp, "server-leaf", &x509.Certificate{
				SerialNumber:          big.NewInt(2),
				Subject:               pkix.Name{CommonName: "coop-leaf-server"},
				NotBefore:             now,
				NotAfter:              exp,
				KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
				ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				BasicConstraintsValid: true,
				IsCA:                  false,
				IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
			}, caCert, caKey)

			nodeID := "coop-ca-leaf-01"
			var clientCertPath, clientKeyPath, clientCAPath string
			if tc.requireClientCert {
				clientCAPath = caPath
				clientCertPath, clientKeyPath, _, _ = writeCertSignedBy(t, tmp, "client-leaf", &x509.Certificate{
					SerialNumber:          big.NewInt(3),
					Subject:               pkix.Name{CommonName: nodeID},
					NotBefore:             now,
					NotAfter:              exp,
					KeyUsage:              x509.KeyUsageDigitalSignature,
					ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
					BasicConstraintsValid: true,
					DNSNames:              []string{nodeID},
				}, caCert, caKey)
			}

			obs := &pulseObserver{
				name:     "ca-leaf-pulse",
				interval: 25 * time.Millisecond,
				started:  make(chan struct{}),
				stopped:  make(chan struct{}),
			}
			store := newMockBaseDB()
			srv := New(config.CoopConfig{
				MeshName:      "ca-leaf-mesh",
				NodeID:        nodeID,
				SelfObserve:   true,
				PollInterval:  "25ms",
				SpoolCapacity: 64,
				MQTT: config.CoopMQTTConfig{
					Mode:              "embedded",
					Listen:            "127.0.0.1:0",
					Username:          "coop_admin",
					Password:          "admin_secret",
					EdgeUsername:      "edge_scoped",
					EdgePassword:      "edge_secret",
					TLSCertFile:       srvCertPath,
					TLSKeyFile:        srvKeyPath,
					TLSCAFile:         "", // Explicitly omitted: Issuer != Subject leaf cannot act as RootCA
					TLSClientCAFile:   clientCAPath,
					TLSClientCertFile: clientCertPath,
					TLSClientKeyFile:  clientKeyPath,
					RequireClientCert: tc.requireClientCert,
				},
				Ingest: config.IngestConfig{
					QueueSize:     64,
					BatchSize:     1,
					FlushInterval: "20ms",
				},
			}, Options{
				Store:          store,
				ExtraObservers: []observer.Observer{obs},
			})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := srv.Start(ctx); err != nil {
				t.Fatalf("Server.Start with CA-signed leaf and empty TLSCAFile failed: %v", err)
			}
			defer srv.Stop()

			if srv.loopbackBridge == nil {
				t.Fatal("expected loopbackTLSBridge to be active for CA-signed leaf without TLSCAFile")
			}
			if got := waitForNodeEvents(t, store, nodeID, 1, 4*time.Second); got == 0 {
				t.Fatal("expected selfNode telemetry to ingest when TLSCertFile is CA-signed and TLSCAFile is omitted")
			}

			// Verify external client trusting caPath also sees subscriber's MNS announcement.
			caPEM, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatalf("read caPath: %v", err)
			}
			caPool := x509.NewCertPool()
			caPool.AppendCertsFromPEM(caPEM)
			watcherTLS := &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    caPool,
			}
			if tc.requireClientCert {
				kp, kpErr := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
				if kpErr != nil {
					t.Fatalf("load client keypair: %v", kpErr)
				}
				watcherTLS.Certificates = []tls.Certificate{kp}
			}
			watcher := mqtt.NewClient(mqtt.NewClientOptions().
				AddBroker(srv.BrokerURL()).
				SetClientID("ca-leaf-watcher").
				SetUsername("coop_admin").
				SetPassword("admin_secret").
				SetTLSConfig(watcherTLS).
				SetConnectRetry(false))
			if tok := watcher.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
				t.Fatalf("watcher connect failed: %v", tok.Error())
			}
			defer watcher.Disconnect(50)

			meshCh := make(chan core.MeshIdentity, 1)
			if tok := watcher.Subscribe(core.MeshTopic(), 1, func(_ mqtt.Client, m mqtt.Message) {
				var id core.MeshIdentity
				if json.Unmarshal(m.Payload(), &id) == nil && id.MeshName != "" {
					select {
					case meshCh <- id:
					default:
					}
				}
			}); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe mesh failed: %v", tok.Error())
			}
			select {
			case id := <-meshCh:
				if id.MeshName != "ca-leaf-mesh" || id.CoopNodeID != nodeID {
					t.Fatalf("unexpected MNS MeshIdentity: %+v", id)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for MNS identity with CA-signed leaf without TLSCAFile")
			}
		})
	}
}
