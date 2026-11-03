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
	require.NoError(t, transport.Close(), "the zero-value owner is ready; Close joins an empty lifetime")
	require.NoError(t, transport.Close())
}

func TestClusterLifecycleRepeatedCloseWorkerJoin(t *testing.T) {
	for _, role := range []string{"transport", "connection"} {
		t.Run(role, func(t *testing.T) {
			transport := NewClusterTransport(nil)
			left, right := net.Pipe()
			observed := &closeRendezvousConn{Conn: left, closed: make(chan struct{}), closes: make(chan struct{}, 2)}
			connection := transport.createConnection("peer", observed)
			var closer io.Closer = connection
			owner := &connection.work
			retired := (<-chan struct{})(observed.closes)
			if role == "transport" {
				closer, owner, retired = transport, &transport.work, transport.work.Context().Done()
			}
			// An admitted worker that Close must join.
			_, workerRelease, _ := owner.Acquire(owner.Context())
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(workerRelease) }
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
			// A repeated Close joins the first one's actual completion: it must not
			// report success while the admitted worker still runs, in either role.
			select {
			case err := <-second:
				t.Fatalf("%s repeat returned before the admitted worker finished: %v", role, err)
			case <-time.After(50 * time.Millisecond):
			}
			release()
			select {
			case err := <-first:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("first Close did not finish")
			}
			select {
			case err := <-second:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatalf("%s repeat did not finish", role)
			}
		})
	}
}
