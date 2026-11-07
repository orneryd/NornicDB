package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// persistedCountDeltaKeys counts the delta keys of one kind the database still
// holds.
func persistedCountDeltaKeys(t *testing.T, engine *BadgerEngine, kind byte) int {
	t.Helper()
	var keys int
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		prefix := countDeltaAllPrefix(kind)
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			keys++
		}
		return nil
	}))
	return keys
}

// hasDerivedCountBase reports whether one persisted base count key exists.
func hasDerivedCountBase(t *testing.T, engine *BadgerEngine, key []byte) bool {
	t.Helper()
	var found bool
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		found = err == nil
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}))
	return found
}

// TestDiscardCountDeltasWalksBacklogInChunksAndRecountsFromBodies pins the
// recovery completion owner's discard step on a backlog far larger than one
// chunk: every delta of the discarded kind goes, the sibling family stays, no
// base count is folded from the superseded deltas, a canceled caller leaves the
// backlog for the next attempt, and the authoritative recount that follows the
// discard answers from the committed bodies rather than from the delta set.
func TestDiscardCountDeltasWalksBacklogInChunksAndRecountsFromBodies(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	const (
		ns      = "ns1"
		label   = "DiscardBig"
		typeNew = "DiscardBigNew"
	)
	start, end := NodeID(ns+":d1"), NodeID(ns+":d2")
	for _, id := range []NodeID{start, end} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{label}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: ns + ":dk1", Type: typeNew, StartNode: start, EndNode: end}))

	// A replayed semantic record beside a transported one leaves exactly this
	// shape: deltas whose sibling deltas never reached this destination, so the
	// set says more than the bodies do (and one delta alone would underflow).
	backlog := countDeltaFoldChunk*2 + 137 // spans three discard chunks
	for _, appendDeltas := range []func(*badger.Txn) error{
		func(txn *badger.Txn) error {
			for i := 0; i < backlog; i++ {
				if err := engine.adjustLabelCountInTxn(txn, ns, label, 1); err != nil {
					return err
				}
			}
			return engine.adjustLabelCountInTxn(txn, ns, label, -1)
		},
		func(txn *badger.Txn) error {
			for i := 0; i < backlog; i++ {
				if err := engine.adjustEdgeTypeCountInTxn(txn, ns, typeNew, 1); err != nil {
					return err
				}
			}
			return engine.adjustEdgeTypeCountInTxn(txn, ns, typeNew, -1)
		},
	} {
		require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, appendDeltas))
	}
	require.Positive(t, persistedCountDeltaKeys(t, engine, countDeltaKindLabel))
	require.Positive(t, persistedCountDeltaKeys(t, engine, countDeltaKindEdgeType))

	// A canceled caller stops the walk where it scans, before deleting anything.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, engine.discardCountDeltas(canceled, countDeltaKindLabel), context.Canceled)
	require.Greater(t, persistedCountDeltaKeys(t, engine, countDeltaKindLabel), countDeltaFoldChunk,
		"a canceled walk leaves its whole backlog for the next attempt")

	require.NoError(t, engine.discardCountDeltas(context.Background(), countDeltaKindLabel))
	require.Zero(t, persistedCountDeltaKeys(t, engine, countDeltaKindLabel),
		"the discard must consume every delta across all of its chunks")
	require.Positive(t, persistedCountDeltaKeys(t, engine, countDeltaKindEdgeType),
		"a discard owns one delta family")

	require.NoError(t, engine.discardCountDeltas(context.Background(), countDeltaKindEdgeType))
	require.Zero(t, persistedCountDeltaKeys(t, engine, countDeltaKindEdgeType))

	// Discarding supersedes the delta set: it must not fold it into the base.
	require.False(t, hasDerivedCountBase(t, engine, labelCountKey(ns, label)),
		"a discard supersedes deltas without folding them into a base count")
	require.False(t, hasDerivedCountBase(t, engine, edgeTypeCountKey(ns, typeNew)))

	// The recount that owns the recovery boundary answers from the bodies.
	require.NoError(t, engine.ensureLabelCounts(t.Context()))
	require.NoError(t, engine.ensureEdgeTypeCounts(t.Context()))
	count, err := engine.NodeCountByLabelInNamespace(t.Context(), ns, label)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	typeCount, err := engine.EdgeCountByTypeInNamespace(t.Context(), ns, typeNew)
	require.NoError(t, err)
	require.Equal(t, int64(1), typeCount)
}
