package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Tests for commits larger than one Badger batch (#703). The low-memory
// profile has an 8 MB memtable, so one Badger batch holds about 13,000
// entries and a few thousand nodes already need several batches.

func openLargeCommitTestEngine(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true})
	require.NoError(t, err)
	return engine
}

func largeCommitNodeID(i int) NodeID { return NodeID(fmt.Sprintf("test:big-%d", i)) }

// stageLargeCreate stages n labelled nodes, chained by relationships, in tx.
func stageLargeCreate(t *testing.T, tx *BadgerTransaction, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := tx.CreateNode(&Node{ID: largeCommitNodeID(i), Labels: []string{"Big"}, Properties: map[string]any{"i": int64(i), "pad": "0123456789abcdef"}})
		require.NoError(t, err)
		if i > 0 {
			require.NoError(t, tx.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:big-e-%d", i)), StartNode: largeCommitNodeID(i - 1), EndNode: largeCommitNodeID(i), Type: "NEXT"}))
		}
	}
}

func countBigNodes(t *testing.T, engine *BadgerEngine) int {
	t.Helper()
	nodes, err := engine.GetNodesByLabel("Big")
	require.NoError(t, err)
	return len(nodes)
}

func requireNoLargeCommitIntent(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(largeCommitIntentKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

// countBatches counts the batches of large commits while the returned
// restore func is not called.
func countBatches(t *testing.T) *atomic.Int64 {
	t.Helper()
	var batches atomic.Int64
	t.Cleanup(setLargeCommitBatchHook(func(int) error { batches.Add(1); return nil }))
	return &batches
}

func TestLargeCommit_CommitsBeyondOneBatchAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	engine := openLargeCommitTestEngine(t, dir)
	batches := countBatches(t)

	const n = 6000
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, n)
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the commit must have needed several Badger batches")

	require.Equal(t, n, countBigNodes(t, engine))
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(n), count)
	edges, err := engine.GetOutgoingEdges(largeCommitNodeID(n / 2))
	require.NoError(t, err)
	require.Len(t, edges, 1)
	requireNoLargeCommitIntent(t, engine)

	require.NoError(t, engine.Close())
	engine = openLargeCommitTestEngine(t, dir)
	defer engine.Close()
	require.Equal(t, n, countBigNodes(t, engine))
	node, err := engine.GetNode(largeCommitNodeID(n - 1))
	require.NoError(t, err)
	require.Equal(t, int64(n-1), node.Properties["i"])
	requireNoLargeCommitIntent(t, engine)
}

func TestLargeCommit_DeleteBeyondOneBatch(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()

	const n = 6000
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, n)
	// One node with many relationships: its deletion is one operation.
	_, err = tx.CreateNode(&Node{ID: "test:hub", Labels: []string{"Hub"}})
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, tx.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:hub-e-%d", i)), StartNode: "test:hub", EndNode: largeCommitNodeID(i), Type: "HAS"}))
	}
	require.NoError(t, tx.Commit())

	batches := countBatches(t)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode("test:hub"))
	for i := 0; i < n; i++ {
		require.NoError(t, tx.DeleteNode(largeCommitNodeID(i)))
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1))

	require.Zero(t, countBigNodes(t, engine))
	_, err = engine.GetNode("test:hub")
	require.ErrorIs(t, err, ErrNotFound)
	nodes, err := engine.NodeCount()
	require.NoError(t, err)
	require.Zero(t, nodes)
	edges, err := engine.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, edges)
}

// Readers see none of a large commit until all of it is written, and they
// never wait for it.
func TestLargeCommit_NoPartialVisibilityAndReadersDoNotWait(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()

	const n = 6000
	var midCommitReads atomic.Int64
	restore := setLargeCommitBatchHook(func(int) error {
		// A reader on another goroutine completes while the commit holds
		// its batches back, and sees none of them.
		seen := make(chan int, 1)
		go func() { seen <- countBigNodes(t, engine) }()
		select {
		case got := <-seen:
			require.Zero(t, got, "a reader saw part of an unfinished commit")
		case <-time.After(10 * time.Second):
			t.Error("a reader waited for an unfinished large commit")
		}
		midCommitReads.Add(1)
		return nil
	})
	defer restore()

	// A reader polling throughout sees either nothing or everything.
	stop := make(chan struct{})
	var partial atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if got := countBigNodes(t, engine); got != 0 && got != n {
				partial.Store(int64(got))
			}
		}
	}()

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, n)
	require.NoError(t, tx.Commit())
	close(stop)
	wg.Wait()

	require.Greater(t, midCommitReads.Load(), int64(0))
	require.Zero(t, partial.Load(), "a reader saw a partial commit")
	require.Equal(t, n, countBigNodes(t, engine))
}

// Ordinary writers wait for a large commit to publish instead of landing
// between its batches, and continue afterwards.
func TestLargeCommit_OrdinaryWritersWaitForPublication(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()

	var once sync.Once
	writerDone := make(chan error, 1)
	restore := setLargeCommitBatchHook(func(int) error {
		once.Do(func() {
			go func() {
				_, err := engine.CreateNode(&Node{ID: "test:small", Labels: []string{"Small"}})
				writerDone <- err
			}()
			select {
			case err := <-writerDone:
				t.Errorf("an ordinary write landed inside a large commit: %v", err)
			case <-time.After(300 * time.Millisecond):
			}
		})
		return nil
	})
	defer restore()

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, 6000)
	require.NoError(t, tx.Commit())
	select {
	case err := <-writerDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the ordinary write did not complete after the large commit published")
	}
	_, err = engine.GetNode("test:small")
	require.NoError(t, err)
}

// A large commit that fails after writing some batches leaves no trace, and
// the engine keeps working.
func TestLargeCommit_FailureRollsBackWrittenBatches(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)

	injected := errors.New("injected batch failure")
	restore := setLargeCommitBatchHook(func(batch int) error {
		if batch == 2 {
			return injected
		}
		return nil
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(2)}}))
	stageLargeCreate(t, tx, 6000)
	require.ErrorIs(t, tx.Commit(), injected)
	restore()

	require.Zero(t, countBigNodes(t, engine))
	_, err = engine.GetNode(largeCommitNodeID(0))
	require.ErrorIs(t, err, ErrNotFound)
	base, err := engine.GetNode("test:base")
	require.NoError(t, err)
	require.Equal(t, int64(1), base.Properties["v"])
	requireNoLargeCommitIntent(t, engine)

	// Both ordinary and large commits still work.
	_, err = engine.CreateNode(&Node{ID: "test:after", Labels: []string{"After"}})
	require.NoError(t, err)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, 6000)
	require.NoError(t, tx.Commit())
	require.Equal(t, 6000, countBigNodes(t, engine))
}

// A crash in the middle of a large commit is rolled back when the store is
// opened again.
func TestLargeCommit_CrashMidCommitRollsBackOnOpen(t *testing.T) {
	dir := t.TempDir()
	engine := openLargeCommitTestEngine(t, dir)
	_, err := engine.CreateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)

	crashed := errors.New("simulated crash")
	restore := setLargeCommitBatchHook(func(batch int) error {
		if batch == 2 {
			// The process dies: Badger stops with the written batches
			// durable and the intent record still present.
			require.NoError(t, engine.db.DB.Close())
			return crashed
		}
		return nil
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(2)}}))
	stageLargeCreate(t, tx, 6000)
	require.Error(t, tx.Commit())
	restore()
	_ = engine.Close()

	engine = openLargeCommitTestEngine(t, dir)
	defer engine.Close()
	require.Zero(t, countBigNodes(t, engine))
	base, err := engine.GetNode("test:base")
	require.NoError(t, err)
	require.Equal(t, int64(1), base.Properties["v"])
	requireNoLargeCommitIntent(t, engine)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, 6000)
	require.NoError(t, tx.Commit())
	require.Equal(t, 6000, countBigNodes(t, engine))
}

// A transaction that read an entity a concurrent commit then changed fails
// as a conflict, and none of its large commit is written.
func TestLargeCommit_ConflictCoversTheWholeTransaction(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	_, err := engine.CreateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, 6000)
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(2)}}))

	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, peer.UpdateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(3)}}))
	require.NoError(t, peer.Commit())

	err = tx.Commit()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)
	require.Zero(t, countBigNodes(t, engine))
	base, err := engine.GetNode("test:base")
	require.NoError(t, err)
	require.Equal(t, int64(3), base.Properties["v"])
}

// Deletes inside a transaction archive superseded bodies while a snapshot
// reader is open; a large delete keeps the reader's snapshot intact.
func TestLargeCommit_DeleteWithOpenSnapshotReader(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	const n = 6000
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, n)
	require.NoError(t, tx.Commit())

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer reader.Rollback()
	before, err := reader.GetNodesByLabel("Big")
	require.NoError(t, err)
	require.Len(t, before, n)

	batches := countBatches(t)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, tx.DeleteNode(largeCommitNodeID(i)))
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1))

	still, err := reader.GetNode(largeCommitNodeID(n / 2))
	require.NoError(t, err, "the open snapshot must still see the deleted node")
	require.Equal(t, int64(n/2), still.Properties["i"])
	require.Zero(t, countBigNodes(t, engine))
}

// DROP DATABASE flushes its write batch every 50,000 keys and keeps writing;
// it used to fail with "This transaction has been discarded" and leave the
// database half-dropped (#819).
func TestDeleteByPrefix_FlushesPeriodicallyAndCompletes(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	const n = 15000
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		_, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("drop:n-%d", i)), Labels: []string{"V"}, Properties: map[string]any{"id": int64(i)}})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	nodesDeleted, _, err := engine.DeleteByPrefix("drop:")
	require.NoError(t, err)
	require.Equal(t, int64(n), nodesDeleted)
	nodes, err := engine.GetNodesByLabel("V")
	require.NoError(t, err)
	require.Empty(t, nodes)
}

func TestManagedBadger_RecoversInterruptedLargeCommit(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	m, err := openManagedBadger(opts)
	require.NoError(t, err)
	require.NoError(t, m.Update(func(txn *badger.Txn) error {
		require.NoError(t, txn.Set([]byte("k1"), []byte("a")))
		return txn.Set([]byte("k2"), []byte("b"))
	}))

	lc, err := m.beginLargeCommit()
	require.NoError(t, err)
	batch := lc.openBatch()
	require.NoError(t, batch.Set([]byte("k1"), []byte("x")))
	require.NoError(t, batch.Delete([]byte("k2")))
	require.NoError(t, batch.Set([]byte("k3"), []byte("c")))
	require.NoError(t, lc.commitBatch(batch))
	// A rollback a crash interrupted: one key already restored.
	restoreTs, err := m.oracle.assign()
	require.NoError(t, err)
	partial := m.DB.NewTransactionAt(lc.intentTs, true)
	require.NoError(t, partial.Set([]byte("k1"), []byte("a")))
	require.NoError(t, partial.CommitAt(restoreTs, nil))
	require.NoError(t, m.DB.Close())

	m, err = openManagedBadger(opts)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "k1", "a")
		requireValue(t, txn, "k2", "b")
		_, err := txn.Get([]byte("k3"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(largeCommitIntentKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	require.NoError(t, m.Update(func(txn *badger.Txn) error { return txn.Set([]byte("k4"), []byte("d")) }))
}

func TestManagedBadger_FirstBatchConflictWritesNothing(t *testing.T) {
	m, err := openManagedBadger(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.Update(func(txn *badger.Txn) error { return txn.Set([]byte("k"), []byte("a")) }))

	txn, readTs := m.beginTxn(true)
	defer m.endRead(readTs)
	_, err = txn.Get([]byte("k"))
	require.NoError(t, err)
	require.NoError(t, txn.Set([]byte("other"), []byte("y")))
	require.NoError(t, m.Update(func(txn *badger.Txn) error { return txn.Set([]byte("k"), []byte("b")) }))

	lc, err := m.beginLargeCommit()
	require.NoError(t, err)
	require.ErrorIs(t, lc.commitBatch(txn), badger.ErrConflict)
	require.NoError(t, lc.abort())
	require.NoError(t, m.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "k", "b")
		_, err := txn.Get([]byte("other"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

// A store written by Badger's own timestamp oracle (earlier releases) opens
// in managed mode with all its data, and stays readable by the unmanaged
// mode afterwards.
func TestManagedBadger_OpensUnmanagedStoreBothWays(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	raw, err := badger.Open(opts)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, raw.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("old-%d", i)), []byte("v"))
		}))
	}
	require.NoError(t, raw.Close())

	m, err := openManagedBadger(opts)
	require.NoError(t, err)
	require.NoError(t, m.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "old-9", "v")
		return nil
	}))
	require.NoError(t, m.Update(func(txn *badger.Txn) error { return txn.Set([]byte("old-9"), []byte("new")) }))
	require.NoError(t, m.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "old-9", "new")
		return nil
	}))
	require.NoError(t, m.Close())

	raw, err = badger.Open(opts)
	require.NoError(t, err)
	defer raw.Close()
	require.NoError(t, raw.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "old-9", "new")
		requireValue(t, txn, "old-0", "v")
		return nil
	}))
}

func TestCommitOracle_DiscardStaysBelowOpenReads(t *testing.T) {
	o := newCommitOracle(10)
	readTs := o.beginRead()
	require.Equal(t, uint64(10), readTs)
	for i := 0; i < 3; i++ {
		ts, err := o.assign()
		require.NoError(t, err)
		o.finish(ts)
	}
	require.Equal(t, uint64(13), o.published.Load())
	discard, ok := o.advanceDiscard()
	require.True(t, ok)
	require.Equal(t, uint64(10), discard, "an open read pins the discard timestamp")
	o.endRead(readTs)
	discard, ok = o.advanceDiscard()
	require.True(t, ok)
	require.Equal(t, uint64(13), discard)
}

func TestCommitOracle_PublishesOnlyContiguousFinishes(t *testing.T) {
	o := newCommitOracle(0)
	first, err := o.assign()
	require.NoError(t, err)
	second, err := o.assign()
	require.NoError(t, err)
	o.finish(second)
	require.Zero(t, o.published.Load(), "a later commit is not visible before an earlier one finishes")
	waited := make(chan struct{})
	go func() {
		require.NoError(t, o.waitPublished(second))
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("waitPublished returned before the earlier commit finished")
	case <-time.After(50 * time.Millisecond):
	}
	o.finish(first)
	<-waited
	require.Equal(t, second, o.published.Load())
}

func requireValue(t *testing.T, txn *badger.Txn, key, want string) {
	t.Helper()
	item, err := txn.Get([]byte(key))
	require.NoError(t, err, key)
	got, err := item.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, want, string(got), key)
}

// A statement may introduce more property names than one Badger batch
// holds. Their tokens are written in as many batches as they need: ahead
// of a large commit's batches, or as ordinary commits before a one-batch
// commit, and they survive a reopen.
func TestLargeCommit_ManyNewPropertyNames(t *testing.T) {
	for _, shape := range []struct {
		name           string
		nodes, perNode int
	}{
		{name: "large commit", nodes: 40000, perNode: 1},
		{name: "one-batch commit", nodes: 40, perNode: 1000},
	} {
		t.Run(shape.name, func(t *testing.T) {
			dir := t.TempDir()
			engine := openLargeCommitTestEngine(t, dir)
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			for i := 0; i < shape.nodes; i++ {
				props := make(map[string]any, shape.perNode)
				for j := 0; j < shape.perNode; j++ {
					props[fmt.Sprintf("key_%d_%d", i, j)] = int64(j)
				}
				_, err := tx.CreateNode(&Node{ID: largeCommitNodeID(i), Labels: []string{"Big"}, Properties: props})
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			require.NoError(t, engine.Close())

			engine = openLargeCommitTestEngine(t, dir)
			defer engine.Close()
			require.Equal(t, shape.nodes, countBigNodes(t, engine))
			last := shape.nodes - 1
			node, err := engine.GetNode(largeCommitNodeID(last))
			require.NoError(t, err)
			require.Len(t, node.Properties, shape.perNode)
			require.Equal(t, int64(shape.perNode-1), node.Properties[fmt.Sprintf("key_%d_%d", last, shape.perNode-1)])
		})
	}
}
