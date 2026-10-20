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

func TestNamespaceDropJournalFailureLeavesDatabaseIntact(t *testing.T) {
	t.Run("queued", func(t *testing.T) { testNamespaceDropJournalFailure(t, false) })
	t.Run("full-queue", func(t *testing.T) { testNamespaceDropJournalFailure(t, true) })
}

func testNamespaceDropJournalFailure(t *testing.T, fullQueue bool) {
	b := storage.NewMemoryEngine()
	defer b.Close()
	a, err := NewStorageAdapterWithWAL(b, t.TempDir())
	require.NoError(t, err)
	defer a.Close()
	r := NewStandaloneReplicator(DefaultConfig(), a)
	require.NoError(t, r.Start(context.Background()))
	m, err := multidb.NewDatabaseManager(NewReplicatedEngine(b, r, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, m.CreateDatabase("journal_a"))
	_, err = b.CreateNode(&storage.Node{ID: "journal_a:seed"})
	require.NoError(t, err)
	old, err := b.BeginTransaction()
	require.NoError(t, err)
	defer old.Rollback()
	require.NoError(t, old.SetNamespace("journal_a"))
	require.NoError(t, a.FlushWAL())
	if fullQueue {
		a.stopWALWriterForTest()
		a.walQueue = make(chan *walWriteRequest)
	}
	a.walMu.Lock()
	err = a.wal.Close()
	a.walMu.Unlock()
	require.NoError(t, err)
	err = m.DropDatabase("journal_a")
	require.ErrorIs(t, err, storage.ErrWALClosed)
	if outcome, ok := err.(interface{ MutationApplied() bool }); ok {
		require.False(t, outcome.MutationApplied(), "a failed journal append never commits DROP")
	}
	require.True(t, m.Exists("journal_a"))
	_, err = b.GetNode("journal_a:seed")
	require.NoError(t, err)
	require.NoError(t, old.Commit(), "failed preparation must not retire transactions")
}

type namespaceJournalGate struct {
	storage.Engine
	entered, deleted chan struct{}
}

func (g *namespaceJournalGate) GetInnerEngine() storage.Engine { return g.Engine }
func (g *namespaceJournalGate) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	close(g.entered)
	n, e, err := storage.DeletePrefixContext(ctx, g.Engine, prefix)
	close(g.deleted)
	return n, e, err
}

func TestNamespaceDropHAWaitsForItsJournalPosition(t *testing.T) {
	b := storage.NewMemoryEngine()
	defer b.Close()
	_, err := b.CreateNode(&storage.Node{ID: "ack_a:seed"})
	require.NoError(t, err)
	g := &namespaceJournalGate{Engine: b, entered: make(chan struct{}), deleted: make(chan struct{})}
	a, err := NewStorageAdapterWithWAL(g, t.TempDir())
	require.NoError(t, err)
	defer a.Close()
	cfg := DefaultConfig()
	cfg.HAStandby.SyncMode = SyncQuorum
	r := &HAStandbyReplicator{config: cfg, storage: a, walStreamer: NewWALStreamer(a, 100)}
	r.started.Store(true)
	r.isPrimary.Store(true)
	e := NewReplicatedEngine(g, r, 2*time.Second)
	ctx := context.Background()
	a.walMu.Lock()
	var once sync.Once
	unblock := func() { once.Do(a.walMu.Unlock) }
	defer unblock()
	done := make(chan error, 1)
	go func() { _, _, err := e.DeleteByPrefixContext(ctx, "ack_a:"); done <- err }()
	select {
	case <-g.entered:
	case <-time.After(time.Second):
		t.Fatal("DROP did not reach storage")
	}
	select {
	case err := <-done:
		t.Fatalf("DROP acknowledged before its journal could append: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	_, err = b.GetNode("ack_a:seed")
	require.NoError(t, err, "journal admission precedes irreversible deletion")
	unblock()
	select {
	case <-g.deleted:
	case <-time.After(time.Second):
		t.Fatal("DROP failed to finish local deletion")
	}
	position, err := a.GetWALPosition()
	require.NoError(t, err)
	require.Greater(t, position, uint64(0))
	select {
	case err := <-done:
		t.Fatalf("DROP acknowledged before standby position %d: %v", position, err)
	case <-time.After(50 * time.Millisecond):
	}
	r.walStreamer.AcknowledgePosition(position)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("standby ACK did not release DROP")
	}
}
