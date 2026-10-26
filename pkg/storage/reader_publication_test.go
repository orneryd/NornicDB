// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// oracleCapture is what snapshotReadRevision captures from the oracle, or
// false when it refuses to capture.
func oracleCapture(o *commitOracle) (SnapshotReadRevision, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.readerPairCurrentLocked() {
		return SnapshotReadRevision{}, false
	}
	return SnapshotReadRevision{oracle: o, timestamp: o.published.Load(), readerLatest: o.readerLatest, readerPrevious: o.readerPrevious}, true
}

func mustCapture(t *testing.T, o *commitOracle) SnapshotReadRevision {
	t.Helper()
	rev, ok := oracleCapture(o)
	require.True(t, ok, "the reader pair must be current")
	return rev
}

func requireNoCapture(t *testing.T, o *commitOracle, why string) {
	t.Helper()
	_, ok := oracleCapture(o)
	require.False(t, ok, why)
}

func mustAssign(t *testing.T, o *commitOracle, p *readerPublication) uint64 {
	t.Helper()
	ts, err := o.assign(p)
	require.NoError(t, err)
	return ts
}

// A write's commit, batches and follow-up commits are one reader publication,
// counted once, at its highest timestamp, when it is closed and all of them
// finished. A standalone commit is a publication by itself.
func TestReaderPublicationOracleCountsOneStepPerWrite(t *testing.T) {
	o := newCommitOracle(10)
	start := mustCapture(t, o)
	w := new(readerPublication)
	var last uint64
	for i := 0; i < 4; i++ {
		last = mustAssign(t, o, w)
		o.finish(last)
		requireNoCapture(t, o, "a finished timestamp of an open publication is visible but not counted")
	}
	o.closePublication(w)
	one := mustCapture(t, o)
	require.Equal(t, last, one.readerLatest)
	require.True(t, one.SingleReaderCommitAfter(start), "four timestamps of one write are one publication")

	o.closePublication(new(readerPublication))
	require.Equal(t, one, mustCapture(t, o), "a write that took no timestamp publishes nothing")

	o.finish(mustAssign(t, o, standalonePublication))
	two := mustCapture(t, o)
	require.True(t, two.SingleReaderCommitAfter(one), "a standalone commit is one publication")
	o.finish(mustAssign(t, o, standalonePublication))
	require.False(t, mustCapture(t, o).SingleReaderCommitAfter(one), "two standalone commits are two publications")

	require.Panics(t, func() { _, _ = o.assign(w) }, "a closed publication takes no timestamp")
	require.Panics(t, func() { _, _ = o.assign(nil) }, "every timestamp needs a publication")
}

// Concurrent writers: a publication held open across its follow-up commits
// while another writer's publication closes.
func TestReaderPublicationOracleHeldOpenAcrossAnotherClose(t *testing.T) {
	o := newCommitOracle(10)
	start := mustCapture(t, o)
	a, b := new(readerPublication), new(readerPublication)
	a1 := mustAssign(t, o, a)
	b1 := mustAssign(t, o, b)
	o.finish(a1)
	o.finish(b1)
	o.closePublication(b)
	requireNoCapture(t, o, "A's commit is visible while A, still open, is not counted")
	a2 := mustAssign(t, o, a) // A's follow-up commit
	o.finish(a2)
	requireNoCapture(t, o, "A's follow-up is visible while A is not counted")
	o.closePublication(a)
	end := mustCapture(t, o)
	require.Equal(t, a2, end.readerLatest)
	require.Equal(t, b1, end.readerPrevious)
	require.False(t, end.SingleReaderCommitAfter(start), "two writers' publications are two steps")
	require.Empty(t, o.open)

	// The same writers when B's commit lands while A is open but before
	// any of A's timestamps is visible: B is capturable on its own.
	o = newCommitOracle(10)
	start = mustCapture(t, o)
	b = new(readerPublication)
	b1 = mustAssign(t, o, b)
	o.finish(b1)
	o.closePublication(b)
	a = new(readerPublication)
	a1 = mustAssign(t, o, a)
	afterB := mustCapture(t, o)
	require.True(t, afterB.SingleReaderCommitAfter(start), "B alone is one step")
	o.finish(a1)
	a2 = mustAssign(t, o, a)
	o.finish(a2)
	requireNoCapture(t, o, "A is visible and open")
	o.closePublication(a)
	afterA := mustCapture(t, o)
	require.True(t, afterA.SingleReaderCommitAfter(afterB), "A with its follow-up is one step after B")
	require.False(t, afterA.SingleReaderCommitAfter(start))
}

// Concurrent writers closing in the reverse order of their first
// timestamps: the later-closed, lower publication only moves readerPrevious,
// so no view captured in between is taken to be one step behind.
func TestReaderPublicationOracleReverseCloseOrder(t *testing.T) {
	o := newCommitOracle(10)
	start := mustCapture(t, o)
	a, b := new(readerPublication), new(readerPublication)
	a1 := mustAssign(t, o, a)
	b1 := mustAssign(t, o, b)
	a2 := mustAssign(t, o, a)
	o.finish(a1)
	o.finish(a2)
	o.closePublication(a)
	requireNoCapture(t, o, "A is counted at a2 while b1, below it, is not published")
	o.finish(b1)
	requireNoCapture(t, o, "B is visible and open")
	o.closePublication(b)
	end := mustCapture(t, o)
	require.Equal(t, a2, end.readerLatest)
	require.Equal(t, b1, end.readerPrevious)
	require.False(t, end.SingleReaderCommitAfter(start), "two publications are two steps in either close order")
	require.False(t, end.SameReaderView(start))

	// Reverse order where both are visible at once: B (higher) closes first.
	o = newCommitOracle(10)
	start = mustCapture(t, o)
	a, b = new(readerPublication), new(readerPublication)
	a1 = mustAssign(t, o, a)
	b1 = mustAssign(t, o, b)
	o.finish(a1)
	o.finish(b1)
	o.closePublication(b)
	requireNoCapture(t, o, "A is visible and open")
	o.closePublication(a)
	end = mustCapture(t, o)
	require.Equal(t, b1, end.readerLatest)
	require.Equal(t, a1, end.readerPrevious)
	require.False(t, end.SingleReaderCommitAfter(start))
	require.Empty(t, o.open)
}

func captureRevision(t *testing.T, engine Engine) SnapshotReadRevision {
	t.Helper()
	snapshot, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), SnapshotAllNodes), engine)
	require.NoError(t, err)
	defer snapshot.Close()
	rev, ok := snapshot.(QueryReadSnapshot).CapturedRevision()
	require.True(t, ok)
	return rev
}

func commitInNamespace(t *testing.T, e *BadgerEngine, apply func(tx *BadgerTransaction) error) {
	t.Helper()
	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("db"))
	require.NoError(t, apply(tx))
	require.NoError(t, tx.Commit())
}

// One transactional update is one reader publication: the namespace's
// sequence high-water mark and the ID counters are persisted in it.
func TestReaderPublicationKeepsOneStepPerCommit(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	base := engine.BadgerEngine
	view := NewNamespacedEngine(base, "db")
	commitInNamespace(t, base, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "db:n1", Labels: []string{"L"}, Properties: map[string]any{"v": 1}})
		return err
	})
	before := captureRevision(t, view)
	commitInNamespace(t, base, func(tx *BadgerTransaction) error {
		node, err := tx.GetNode("db:n1")
		if err != nil {
			return err
		}
		node.Properties = map[string]any{"v": 2}
		return tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits})
	})
	after := captureRevision(t, view)
	require.True(t, after.SingleReaderCommitAfter(before), "an update and its follow-up commits must be one reader publication")
	state, err := base.namespaceMVCC("db")
	require.NoError(t, err)
	persisted, err := base.loadPersistedNamespaceSequence("db")
	require.NoError(t, err)
	require.Equal(t, state.seq.Load(), persisted, "the update must persist the namespace's sequence high-water mark")
}

// A transaction larger than one Badger batch is one reader publication: its
// intent, every batch and its follow-up commits are one step.
func TestReaderPublicationLargeTransactionIsOneStep(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	batches := countBatches(t)
	const n = 6000
	before := captureRevision(t, engine)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, n)
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the create must have needed several Badger batches")
	created := captureRevision(t, engine)
	require.True(t, created.SingleReaderCommitAfter(before), "a multi-batch create must be one reader publication")

	batches.Store(0)
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		node, err := tx.GetNode(largeCommitNodeID(i))
		require.NoError(t, err)
		node.Properties["pad"] = "fedcba9876543210-updated"
		require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the update must have needed several Badger batches")
	updated := captureRevision(t, engine)
	require.True(t, updated.SingleReaderCommitAfter(created), "a multi-batch update of existing nodes must be one reader publication")
}

// The public full-text test's shape: the in-memory engine keeps bodies inline,
// so one transaction growing 2,000 of them to about 6 KB needs several
// batches, and it is one reader publication.
func TestReaderPublicationLargeInlineUpdateIsOneStep(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	const n = 2000
	text := strings.Repeat("routine correspondence meeting attachment ", 48)
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:inline-%d", i)), Labels: []string{"Source"}, Properties: map[string]any{"text": text, "title": "Correspondence"}}
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))
	batches := countBatches(t)
	before := captureRevision(t, engine.BadgerEngine)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		node, err := tx.GetNode(NodeID(fmt.Sprintf("test:inline-%d", i)))
		require.NoError(t, err)
		node.Properties["title"] = text + " " + text + " revised"
		require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, batches.Load(), int64(1), "the update must have needed several Badger batches")
	require.True(t, captureRevision(t, engine.BadgerEngine).SingleReaderCommitAfter(before), "a multi-batch update of inline bodies must be one reader publication")
}

// Concurrent writers, transactional and engine-level, leave every
// publication closed and counted, and a reader never captures a pair that
// runs ahead of what it can see or goes back.
func TestReaderPublicationConcurrentWriters(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	base := engine.BadgerEngine
	view := NewNamespacedEngine(base, "db")
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		var last SnapshotReadRevision
		for {
			select {
			case <-stop:
				return
			default:
			}
			rev, ok := base.snapshotReadRevision(SnapshotAllNodes)
			if ok {
				if rev.readerLatest > rev.timestamp {
					t.Errorf("reader pair %d runs ahead of the published timestamp %d", rev.readerLatest, rev.timestamp)
				}
				if rev.readerLatest < last.readerLatest {
					t.Errorf("reader pair went back from %d to %d", last.readerLatest, rev.readerLatest)
				}
				last = rev
			}
			runtime.Gosched()
		}
	}()
	const writers, commits = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < commits; i++ {
				var err error
				if w%2 == 0 {
					_, err = view.CreateNode(&Node{ID: NodeID(fmt.Sprintf("e%d-%d", w, i)), Labels: []string{"L"}})
				} else {
					var tx *BadgerTransaction
					if tx, err = base.BeginTransaction(); err == nil {
						if err = tx.SetNamespace("db"); err == nil {
							_, err = tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("db:t%d-%d", w, i)), Labels: []string{"L"}})
						}
						if err == nil {
							err = tx.Commit()
						} else {
							_ = tx.Rollback()
						}
					}
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	<-readerDone
	o := base.db.oracle
	o.mu.Lock()
	open := len(o.open)
	o.mu.Unlock()
	require.Zero(t, open, "every write's publication must be closed and counted")
	before := captureRevision(t, view)
	_, err := view.CreateNode(&Node{ID: "last", Labels: []string{"L"}})
	require.NoError(t, err)
	require.True(t, captureRevision(t, view).SingleReaderCommitAfter(before), "one more write is one more step")
}

// Crash and recovery: the sequence high-water mark a write persists in its
// publication keeps the namespace's floor after the newest entity is deleted
// and the store reopens, so no version is minted twice.
func TestReaderPublicationSequenceFloorAfterDeleteAndReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	for _, id := range []NodeID{"db:n1", "db:n2"} {
		commitInNamespace(t, engine, func(tx *BadgerTransaction) error {
			_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"L"}})
			return err
		})
	}
	commitInNamespace(t, engine, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode("db:n2", CascadeEdges) })
	state, err := engine.namespaceMVCC("db")
	require.NoError(t, err)
	issued := state.seq.Load()
	require.Positive(t, issued)
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer reopened.Close()
	persisted, err := reopened.loadPersistedNamespaceSequence("db")
	require.NoError(t, err)
	require.GreaterOrEqual(t, persisted, issued, "the persisted high-water mark must cover every sequence issued before the reopen")
	require.NoError(t, reopened.EnsureNamespaceMVCC("db"))
	tx, err := reopened.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("db"))
	require.GreaterOrEqual(t, tx.readTS.CommitSequence, issued, "a reopened namespace must not restart below a sequence it already issued")
	require.NoError(t, tx.Rollback())
}

// A namespaced delete is one publication: the inner delete removes the node's
// pending-embedding marker in its own commit, with no follow-up commit.
func TestNamespacedDeleteRetiresPendingMarkerInItsCommit(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	base := engine.BadgerEngine
	view := NewNamespacedEngine(base, "db")
	_, err := view.CreateNode(&Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel"}})
	require.NoError(t, err)
	base.AddToPendingEmbeddings("db:n1")
	pending := func() bool {
		t.Helper()
		found := false
		require.NoError(t, base.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(pendingEmbedKey("db:n1"))
			found = err == nil
			return nil
		}))
		return found
	}
	require.True(t, pending(), "the fixture node must carry a pending marker")
	before := captureRevision(t, view)
	require.NoError(t, view.DeleteNode("n1"))
	require.False(t, pending(), "the delete must remove the node's pending marker")
	require.True(t, captureRevision(t, view).SingleReaderCommitAfter(before), "a namespaced delete must be one reader-relevant publication")
}
