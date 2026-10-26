package replication

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var _ Transport = (*ClusterTransport)(nil)
var _ PeerConnection = (*ClusterConnection)(nil)

func TestClusterLifecycleZeroValues(t *testing.T) {
	var connection ClusterConnection
	require.NoError(t, connection.Close())
	require.NoError(t, connection.Close())
	var transport ClusterTransport
	require.Panics(t, func() { _ = transport.Close() }, "the uninitialized transport has no shutdown channel, as before")
}

func TestClusterLifecycleRepeatedCloseWorkerJoin(t *testing.T) {
	for _, role := range []string{"transport", "connection"} {
		t.Run(role, func(t *testing.T) {
			transport := NewClusterTransport(nil)
			left, right := net.Pipe()
			observed := &closeRendezvousConn{Conn: left, closed: make(chan struct{}), closes: make(chan struct{}, 2)}
			connection := transport.createConnection("peer", observed)
			var closer io.Closer = connection
			workers := &connection.wg
			retired := (<-chan struct{})(observed.closes)
			if role == "transport" {
				closer, workers, retired = transport, &transport.wg, transport.closeCh
			}
			workers.Add(1)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(workers.Done) }
			first, second := make(chan error, 1), make(chan error, 1)
			firstJoined, secondJoined := make(chan struct{}), make(chan struct{})
			secondStarted := false
			t.Cleanup(func() {
				release()
				_ = right.Close()
				_ = left.Close()
				select {
				case <-firstJoined:
				case <-time.After(2 * time.Second):
					t.Error("first Close did not join")
				}
				if secondStarted {
					select {
					case <-secondJoined:
					case <-time.After(2 * time.Second):
						t.Error("repeated Close did not join")
					}
				}
			})
			go func() { defer close(firstJoined); first <- closer.Close() }()
			select {
			case <-retired:
			case <-time.After(2 * time.Second):
				t.Fatal("first Close did not retire resources")
			}
			select {
			case err := <-first:
				t.Fatalf("first Close missed admitted worker: %v", err)
			default:
			}
			secondStarted = true
			go func() { defer close(secondJoined); second <- closer.Close() }()
			if role == "transport" {
				select {
				case err := <-second:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("transport repeat did not return immediately")
				}
			} else {
				select {
				case <-observed.closes:
				case <-time.After(2 * time.Second):
					t.Fatal("connection repeat did not close socket")
				}
				select {
				case err := <-second:
					t.Fatalf("connection repeat missed reader join: %v", err)
				default:
				}
			}
			release()
			select {
			case err := <-first:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("first Close did not finish")
			}
			if role == "connection" {
				select {
				case err := <-second:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("connection repeat did not finish")
				}
			}
		})
	}
}
