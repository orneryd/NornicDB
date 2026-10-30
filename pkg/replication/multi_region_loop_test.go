package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// walPositionErrorStorage fails every WAL position lookup.
type walPositionErrorStorage struct{ *MockStorage }

func (walPositionErrorStorage) GetWALPosition() (uint64, error) {
	return 0, errors.New("forced WAL position failure")
}

func newLoopMultiRegionReplicator(t *testing.T, leader bool) (*MultiRegionReplicator, *MockStorage, *MockPeerConn) {
	t.Helper()
	local, _ := newCoverageRaftReplicator(t)
	local.started.Store(true)
	if leader {
		local.state = StateLeader
	}

	cfg := DefaultConfig()
	cfg.Mode = ModeMultiRegion
	cfg.NodeID = "region-node-1"
	cfg.MultiRegion.RegionID = "us-east"

	store := NewMockStorage()
	remote := &MockPeerConn{connected: true}
	r := &MultiRegionReplicator{
		config:      cfg,
		storage:     store,
		localRaft:   local,
		stopCh:      make(chan struct{}),
		remoteConns: map[string]PeerConnection{"eu-west": remote},
	}
	r.started.Store(true)
	return r, store, remote
}

// While the local node leads, each tick of the cross-region loop streams the
// new WAL entries to the remote regions; shutdown stops the loop.
func TestMultiRegionReplicator_CrossRegionLoopStreamsWhileLeader(t *testing.T) {
	t.Parallel()

	r, store, remote := newLoopMultiRegionReplicator(t, true)
	store.mu.Lock()
	store.walPosition = 2
	store.mu.Unlock()

	r.wg.Add(1)
	go r.runCrossRegionReplication(context.Background())
	require.Eventually(t, func() bool { return remote.GetWALBatchCalls() >= 1 }, 5*time.Second, 10*time.Millisecond)
	close(r.stopCh)
	r.wg.Wait()
}

// The cross-region loop stops when its context ends.
func TestMultiRegionReplicator_CrossRegionLoopStopsOnContext(t *testing.T) {
	t.Parallel()

	r, _, remote := newLoopMultiRegionReplicator(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.wg.Add(1)
	r.runCrossRegionReplication(ctx)
	require.Zero(t, remote.GetWALBatchCalls())
}

// Nothing is streamed when the WAL has not advanced or its position can't be
// read.
func TestMultiRegionReplicator_StreamWALSkipsWithoutNewEntries(t *testing.T) {
	t.Parallel()

	r, store, remote := newLoopMultiRegionReplicator(t, true)
	store.mu.Lock()
	store.walPosition = 3
	store.mu.Unlock()
	r.walPosition = 3
	r.streamWALToRemoteRegions(context.Background())
	require.Zero(t, remote.GetWALBatchCalls())

	r.storage = walPositionErrorStorage{store}
	r.walPosition = 0
	r.streamWALToRemoteRegions(context.Background())
	require.Zero(t, remote.GetWALBatchCalls())
}
