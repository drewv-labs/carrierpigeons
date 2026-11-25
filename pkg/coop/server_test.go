package coop

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/broker"
	"github.com/drewv-labs/carrierpigeons/internal/pigeoncoop/ingest"
	"github.com/drewv-labs/carrierpigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrierpigeons/pkg/config"
	"github.com/drewv-labs/carrierpigeons/pkg/core"
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
