package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Position is asynchronous until the adapter's public FIFO barrier completes.
// This wrapper models its slow-writer schedule without changing storage apply.
type managedDelayedPosition struct {
	*StorageAdapter
	flushed bool
}

func (s *managedDelayedPosition) GetWALPosition() (uint64, error) {
	if !s.flushed {
		return 0, nil
	}
	return s.StorageAdapter.GetWALPosition()
}

func (s *managedDelayedPosition) FlushWAL() error {
	if err := s.StorageAdapter.FlushWAL(); err != nil {
		return err
	}
	s.flushed = true
	return nil
}

func TestManagedHAQuorumWaitUsesPublishedCommandPosition(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	local := &managedDelayedPosition{StorageAdapter: adapter}
	cfg := DefaultConfig()
	r := &HAStandbyReplicator{config: cfg, storage: local, walStreamer: NewWALStreamer(local, 100)}
	r.started.Store(true)
	r.isPrimary.Store(true)
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, r, 30*time.Millisecond), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase("managed_ack"))
	view, err := manager.GetStorage("managed_ack")
	require.NoError(t, err)
	local.flushed = false
	cfg.HAStandby.SyncMode = SyncQuorum
	_, err = view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}})
	require.ErrorIs(t, err, context.DeadlineExceeded, "an unflushed zero position cannot stand in for the committed command's ACK")
	require.True(t, storage.MutationApplied(err))
	_, err = view.GetNode("n")
	require.NoError(t, err)
	local.flushed = false
	cfg.HAStandby.SyncMode = SyncAsync
	_, err = view.CreateNode(&storage.Node{ID: "async", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.False(t, local.flushed, "async mode retains asynchronous journal latency")
}
