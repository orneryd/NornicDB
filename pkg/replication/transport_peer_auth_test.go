package replication

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const peerAuthTestSecret = "replication-secret-of-forty-characters!!"

// peerAuthHeartbeat connects to addr as a peer holding secret (nil: none)
// and sends one heartbeat.
func peerAuthHeartbeat(t *testing.T, addr string, secret []byte) error {
	t.Helper()
	cfg := DefaultClusterTransportConfig()
	cfg.NodeID = "primary-1"
	cfg.AuthSecret = secret
	tr := NewClusterTransport(cfg)
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := tr.Connect(ctx, addr)
	if err != nil {
		return err
	}
	_, err = conn.SendHeartbeat(ctx, &HeartbeatRequest{NodeID: "primary-1", Role: "primary", Timestamp: time.Now().UnixNano()})
	return err
}

// The connection handler runs only once a verified message has arrived on
// the connection, never on a bare connect or an unsigned message.
func TestClusterTransportListen_HandlerRunsAfterVerifiedMessage(t *testing.T) {
	cfg := DefaultClusterTransportConfig()
	cfg.NodeID = "standby-1"
	cfg.AuthSecret = []byte(peerAuthTestSecret)
	transport := NewClusterTransport(cfg)
	defer transport.Close()
	transport.RegisterHandler(ClusterMsgHeartbeat, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: msg.Payload}, nil
	})

	addr := tlsShutdownTestAddr(t)
	connected := make(chan struct{}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = transport.Listen(ctx, addr, func(PeerConnection) { connected <- struct{}{} }) }()
	waitTCPListening(t, addr)

	require.Error(t, peerAuthHeartbeat(t, addr, nil))
	require.Error(t, peerAuthHeartbeat(t, addr, []byte(strings.Repeat("w", 40))))
	select {
	case <-connected:
		t.Fatal("the connection handler ran for a peer that never sent a verified message")
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, peerAuthHeartbeat(t, addr, []byte(peerAuthTestSecret)))
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection handler did not run after a verified message")
	}
}

// An unauthenticated connect to an HA standby neither refreshes the
// primary's health nor replaces the primary connection.
func TestHAStandby_UnauthenticatedConnectLeavesPrimaryState(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "standby-1"
	cfg.BindAddr = tlsShutdownTestAddr(t)
	cfg.HAStandby.Role = "standby"
	cfg.HAStandby.PeerAddr = "127.0.0.1:1"
	cfg.HAStandby.AutoFailover = false
	cfg.ReplicationSecret = peerAuthTestSecret
	r, err := NewHAStandbyReplicator(cfg, NewMockStorage())
	require.NoError(t, err)
	transport, err := NewDefaultTransportFromConfig(cfg)
	require.NoError(t, err)
	r.SetTransport(transport)
	RegisterClusterHandlers(transport.(*ClusterTransport), r)
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Shutdown() })
	waitTCPListening(t, cfg.BindAddr)

	state := func() (PeerConnection, bool, time.Time) {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.primaryConn, r.primaryHealthy.Load(), r.lastPrimaryHB
	}

	// Before any authenticated message, connects change nothing.
	c, err := net.Dial("tcp", cfg.BindAddr)
	require.NoError(t, err)
	require.Error(t, peerAuthHeartbeat(t, cfg.BindAddr, nil))
	time.Sleep(300 * time.Millisecond)
	conn, healthy, lastHB := state()
	require.Nil(t, conn)
	require.False(t, healthy)
	require.True(t, lastHB.IsZero())
	_ = c.Close()

	// The real primary is recorded once its signed heartbeat arrives.
	require.NoError(t, peerAuthHeartbeat(t, cfg.BindAddr, []byte(peerAuthTestSecret)))
	require.Eventually(t, func() bool { conn, _, _ := state(); return conn != nil }, 5*time.Second, 20*time.Millisecond)
	liveConn, _, liveHB := state()

	// Later unauthenticated connects keep that connection and heartbeat time.
	time.Sleep(50 * time.Millisecond)
	c, err = net.Dial("tcp", cfg.BindAddr)
	require.NoError(t, err)
	defer c.Close()
	require.Error(t, peerAuthHeartbeat(t, cfg.BindAddr, nil))
	time.Sleep(300 * time.Millisecond)
	conn, _, lastHB = state()
	require.Same(t, liveConn, conn, "an unauthenticated connect replaced the primary connection")
	require.Equal(t, liveHB, lastHB, "an unauthenticated connect refreshed the primary heartbeat")
}
