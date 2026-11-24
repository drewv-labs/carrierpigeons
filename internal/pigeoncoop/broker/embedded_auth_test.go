package broker

import (
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
