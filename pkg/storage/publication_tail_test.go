// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package storage

import (
	"context"
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestPublicationTailKeyFamilies(t *testing.T) {
	for _, key := range [][]byte{mvccSequenceKey(), mvccNamespaceSequenceKey("db"), idCounterNodeKey, idCounterEdgeKey,
		edgeTypeLabelCountKey(prefixMVCCMetaEdgeTypeStartLabelCount, "db", "L", "T")} {
		require.True(t, publicationTailKey(key), "%x", key)
	}
	for _, key := range [][]byte{nil, nodeKey("db:n1"), edgeKey("db:e1"), pendingEmbedKey("db:n1"), {prefixLabelIndex, 'x'},
		{prefixEmbedding, 'x'}, {prefixSchema, 'x'}, {prefixPropKeyForward, 'x'}, {prefixIDDictNodeForward, 'x'},
		{prefixMVCCNodeHead, 'x'}, {prefixTemporalHead, 'x'}, {prefixChangeEvent, 'x'}} {
		require.False(t, publicationTailKey(key), "%x", key)
	}
}

// A publication tail is not another reader-relevant publication; an ordinary
// commit is; a row key fails a tail instead of being committed.
func TestPublicationTailCommitOracle(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	db := engine.BadgerEngine.db
	latest := func() uint64 {
		db.oracle.mu.Lock()
		defer db.oracle.mu.Unlock()
		return db.oracle.readerLatest
	}
	before := latest()
	require.NoError(t, db.updatePublicationTail(func(w metadataKV) error {
		return w.Set(mvccNamespaceSequenceKey("db"), []byte{0, 0, 0, 0, 0, 0, 0, 7})
	}))
	require.Equal(t, before, latest(), "a publication tail was counted as a reader-relevant publication")
	require.NoError(t, db.updateKV(func(w metadataKV) error {
		return w.Set(mvccNamespaceSequenceKey("db"), []byte{0, 0, 0, 0, 0, 0, 0, 8})
	}))
	require.Greater(t, latest(), before, "an ordinary commit must stay reader-relevant")
	after := latest()
	err := db.updatePublicationTail(func(w metadataKV) error { return w.Set(nodeKey("db:n1"), []byte("x")) })
	require.True(t, errors.Is(err, ErrInvalidData), "a row key must fail a publication tail: %v", err)
	require.NoError(t, engine.BadgerEngine.withPublicationTailUpdate(func(w metadataKV) error {
		return w.Set(mvccNamespaceSequenceKey("db"), []byte{0, 0, 0, 0, 0, 0, 0, 9})
	}))
	require.Equal(t, after, latest(), "an engine-writer publication tail was counted as a reader-relevant publication")
	err = engine.BadgerEngine.withPublicationTailUpdate(func(w metadataKV) error { return w.Set(edgeKey("db:e1"), []byte("x")) })
	require.True(t, errors.Is(err, ErrInvalidData), "a row key must fail an engine-writer publication tail: %v", err)
}

// One transactional update is one reader-relevant publication: its tail
// persists the namespace's sequence high-water mark without being counted.
func TestPublicationTailKeepsOneReaderPublicationPerCommit(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	base := engine.BadgerEngine
	view := NewNamespacedEngine(base, "db")
	commit := func(apply func(tx *BadgerTransaction) error) {
		t.Helper()
		tx, err := base.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("db"))
		require.NoError(t, apply(tx))
		require.NoError(t, tx.Commit())
	}
	revision := func() SnapshotReadRevision {
		t.Helper()
		snapshot, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), SnapshotAllNodes), view)
		require.NoError(t, err)
		defer snapshot.Close()
		rev, ok := snapshot.(QueryReadSnapshot).CapturedRevision()
		require.True(t, ok)
		return rev
	}
	commit(func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "db:n1", Labels: []string{"L"}, Properties: map[string]any{"v": 1}})
		return err
	})
	before := revision()
	commit(func(tx *BadgerTransaction) error {
		node, err := tx.GetNode("db:n1")
		if err != nil {
			return err
		}
		node.Properties = map[string]any{"v": 2}
		return tx.UpdateGraphNode(node, ClientNodeEdits)
	})
	after := revision()
	require.True(t, after.SingleReaderCommitAfter(before), "an update and its tail must be one reader-relevant publication")
	state, err := base.namespaceMVCC("db")
	require.NoError(t, err)
	persisted, err := base.loadPersistedNamespaceSequence("db")
	require.NoError(t, err)
	require.Equal(t, state.seq.Load(), persisted, "the tail must persist the namespace's sequence high-water mark")
}

// Crash and recovery: the sequence high-water mark a publication tail
// persists keeps the namespace's floor after the newest entity is deleted
// and the store reopens, so no version is minted twice.
func TestPublicationTailSequenceFloorAfterDeleteAndReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	commit := func(e *BadgerEngine, apply func(tx *BadgerTransaction) error) {
		t.Helper()
		tx, err := e.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("db"))
		require.NoError(t, apply(tx))
		require.NoError(t, tx.Commit())
	}
	for _, id := range []NodeID{"db:n1", "db:n2"} {
		commit(engine, func(tx *BadgerTransaction) error {
			_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"L"}})
			return err
		})
	}
	commit(engine, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode("db:n2", CascadeEdges) })
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
	revision := func() SnapshotReadRevision {
		t.Helper()
		snapshot, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), SnapshotAllNodes), view)
		require.NoError(t, err)
		defer snapshot.Close()
		rev, ok := snapshot.(QueryReadSnapshot).CapturedRevision()
		require.True(t, ok)
		return rev
	}
	before := revision()
	require.NoError(t, view.DeleteNode("n1"))
	require.False(t, pending(), "the delete must remove the node's pending marker")
	require.True(t, revision().SingleReaderCommitAfter(before), "a namespaced delete must be one reader-relevant publication")
}
