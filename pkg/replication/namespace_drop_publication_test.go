package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pause exactly after the physical owner's deletion lease has been released,
// before the adapter can publish the DROP in the replication journal.
type solR3AfterDelete struct {
	storage.Engine
	deleted chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (e *solR3AfterDelete) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	n, edges, err := storage.DeletePrefixContext(ctx, e.Engine, prefix)
	if err == nil {
		close(e.deleted)
		<-e.resume
	}
	return n, edges, err
}

func TestSolR3DropJournalPublicationOrder(t *testing.T) {
	primary, standby := storage.NewMemoryEngine(), storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, primary.Close()); require.NoError(t, standby.Close()) })
	for _, base := range []storage.Engine{primary, standby} {
		_, err := base.CreateNode(&storage.Node{ID: "a:old"})
		require.NoError(t, err)
	}
	gate := &solR3AfterDelete{Engine: primary, deleted: make(chan struct{}), resume: make(chan struct{})}
	unblock := func() { gate.once.Do(func() { close(gate.resume) }) }
	defer unblock()
	adapter, err := NewStorageAdapterWithWAL(gate, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	cfg := DefaultConfig()
	cfg.HAStandby.SyncMode = SyncAsync
	r := &HAStandbyReplicator{config: cfg, storage: adapter}
	r.started.Store(true)
	r.isPrimary.Store(true)
	engine := NewReplicatedEngine(gate, r, time.Second)
	done := make(chan error, 1)
	go func() { _, _, err := engine.DeleteByPrefixContext(context.Background(), "a:"); done <- err }()
	select {
	case <-gate.deleted:
	case <-time.After(time.Second):
		t.Fatal("DROP did not finish local deletion")
	}
	_, err = engine.CreateNode(&storage.Node{ID: "a:new"})
	require.NoError(t, err)
	require.NoError(t, adapter.FlushWAL())
	unblock()
	require.NoError(t, <-done)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 100)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	t.Logf("journal order: %v then %v", entries[0].Command.Type, entries[1].Command.Type)
	follower, err := NewStorageAdapterWithWAL(standby, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, follower.Close()) })
	_, err = NewWALApplier(follower).ApplyBatch(entries)
	require.NoError(t, err)
	_, primaryErr := primary.GetNode("a:new")
	_, standbyErr := standby.GetNode("a:new")
	t.Logf("new node: primary=%v standby=%v", primaryErr, standbyErr)
	require.Equal(t, primaryErr, standbyErr, "successful HA DROP plus concurrent successful write must replay to the same state")
}

type solR3CancelAfterApply struct {
	*StorageAdapter
	cancel context.CancelFunc
}

func (s *solR3CancelAfterApply) ApplyCommandContext(ctx context.Context, cmd *Command) error {
	if err := s.StorageAdapter.ApplyCommandContext(ctx, cmd); err != nil {
		return err
	}
	if cmd.Type != CmdDeleteByPrefix {
		return nil
	}
	if err := s.FlushWAL(); err != nil {
		return err
	}
	s.cancel()
	return nil
}

func TestSolR3DropHACancelAfterLocalCommit(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local := &solR3CancelAfterApply{StorageAdapter: adapter, cancel: cancel}
	cfg := DefaultConfig()
	cfg.HAStandby.SyncMode = SyncAsync
	r := &HAStandbyReplicator{config: cfg, storage: local, walStreamer: NewWALStreamer(adapter, 100)}
	r.started.Store(true)
	r.isPrimary.Store(true)
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, r, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("a"))
	_, err = base.CreateNode(&storage.Node{ID: "a:old"})
	require.NoError(t, err)
	r.config.HAStandby.SyncMode = SyncQuorum
	err = manager.DropDatabaseContext(ctx, "a")
	require.ErrorIs(t, err, context.Canceled)
	_, readErr := base.GetNode("a:old")
	entries, journalErr := adapter.GetWALEntries(0, 100)
	require.NoError(t, journalErr)
	t.Logf("DROP result=%v metadata-exists=%v seed=%v journal-entries=%d", err, manager.Exists("a"), readErr, len(entries))
	require.False(t, manager.Exists("a"), "irreversible DROP must finish manager retirement even if the ACK wait is cancelled")
}
