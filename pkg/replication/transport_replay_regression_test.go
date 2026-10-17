package replication

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func replayRegressionTransport() *ClusterTransport {
	cfg := DefaultClusterTransportConfig()
	cfg.NodeID = "node-a"
	cfg.AuthSecret = []byte("replication-secret-of-forty-characters!!")
	return NewClusterTransport(cfg)
}

// A captured, validly signed frame sent again, on the same connection or a
// new one, is refused and never reaches the handler.
func TestClusterTransport_ReplayedFrameIsRejected(t *testing.T) {
	receiver := replayRegressionTransport()
	defer receiver.Close()
	var handled atomic.Int32
	receiver.RegisterHandler(ClusterMsgHeartbeat, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
		handled.Add(1)
		return nil, nil
	})
	addr := tlsShutdownTestAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = receiver.Listen(ctx, addr, nil) }()
	waitTCPListening(t, addr)

	sender := replayRegressionTransport()
	defer sender.Close()
	captured := &ClusterMessage{Type: ClusterMsgHeartbeat, Payload: []byte("payload")}
	sender.createConnection("test", nil).signMessage(captured)

	send := func(conn net.Conn, msg *ClusterMessage) {
		w := bufio.NewWriter(conn)
		require.NoError(t, writeClusterMessage(w, msg))
		require.NoError(t, w.Flush())
	}
	first, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer first.Close()
	send(first, captured)
	require.Eventually(t, func() bool { return handled.Load() == 1 }, 5*time.Second, 10*time.Millisecond)

	// Same frame on the same connection: refused, and the connection closed.
	send(first, captured)
	_ = first.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = first.Read(make([]byte, 1))
	requireClosedByPeer(t, err, "the receiver should close a connection that replays a frame")

	// Same frame on a new connection: refused too.
	second, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer second.Close()
	send(second, captured)
	_ = second.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = second.Read(make([]byte, 1))
	requireClosedByPeer(t, err, "the receiver should close a new connection that replays a frame")
	require.Equal(t, int32(1), handled.Load(), "a replayed frame reached the handler")
}

// requireClosedByPeer checks that a read ended because the peer closed the
// connection, not because the read deadline passed.
func requireClosedByPeer(t *testing.T, err error, msg string) {
	t.Helper()
	require.Error(t, err, msg)
	var netErr net.Error
	if errors.As(err, &netErr) {
		require.False(t, netErr.Timeout(), msg)
	}
}
