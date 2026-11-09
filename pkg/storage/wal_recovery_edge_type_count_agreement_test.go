package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recoveryCountDestination is one destination mode recovery must agree on: an
// ordinary Badger engine and a write-behind engine differ in when a committed
// body reaches Badger, not in what the recovered counts must say.
type recoveryCountDestination struct {
	name string
	open func(*testing.T) *BadgerEngine
}

func recoveryCountDestinations() []recoveryCountDestination {
	return []recoveryCountDestination{
		{name: "ordinary-destination", open: createTestBadgerEngine},
		{name: "write-behind-destination", open: func(t *testing.T) *BadgerEngine {
			return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
		}},
	}
}

// seedEdgeTypeDestination creates the pre-commit relationship body a
// destination that missed the buffered commit still holds.
func seedEdgeTypeDestination(t *testing.T, destination *BadgerEngine, edgeType string) {
	t.Helper()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := destination.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, destination.CreateEdge(t.Context(), &Edge{
		ID:        "test:e1",
		Type:      edgeType,
		StartNode: "test:a",
		EndNode:   "test:b",
	}))
}

// requireEdgeTypeCounts asserts the counted cardinality of both relationship
// types the relabel moved between.
func requireEdgeTypeCounts(t *testing.T, destination *BadgerEngine, namespace string, want map[string]int64) {
	t.Helper()
	for edgeType, count := range want {
		got, err := destination.EdgeCountByTypeInNamespace(t.Context(), namespace, edgeType)
		require.NoError(t, err)
		require.Equal(t, count, got, "edge type %q must follow the recovered bodies", edgeType)
	}
}

// TestReplayedBufferedCommitReconcilesEdgeTypeCountsWithRestoredBody records the
// single-entry recovery contract for the edge-type family: replaying one
// transported commit into a store that still holds the pre-commit relationship
// body must leave the body and its derived type counts agreeing, exactly as the
// batched recovery paths do.
func TestReplayedBufferedCommitReconcilesEdgeTypeCountsWithRestoredBody(t *testing.T) {
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
	seedEdgeTypeDestination(t, source, "OldType")

	tx, err := source.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	edge, err := tx.GetEdge(t.Context(), "test:e1")
	require.NoError(t, err)
	edge.Type = "NewType"
	require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge}))
	require.NoError(t, tx.Commit())

	entries, err := ReadWALEntriesFromDir(t.Context(), source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)

	for _, destinationKind := range recoveryCountDestinations() {
		t.Run(destinationKind.name, func(t *testing.T) {
			destination := destinationKind.open(t)
			seedEdgeTypeDestination(t, destination, "OldType")
			// The pre-commit body is committed before the transported record
			// replays over it, so the replay is the only writer left.
			require.NoError(t, destination.FlushWriteBehind(t.Context()))

			replayed := 0
			for _, entry := range entries {
				if entry.Operation != OpBufferedCommit {
					continue
				}
				replayed++
				require.NoError(t, ReplayWALEntry(t.Context(), destination, entry))
			}
			require.Equal(t, 1, replayed)

			got, err := destination.GetEdge(t.Context(), "test:e1")
			require.NoError(t, err)
			require.Equal(t, "NewType", got.Type)
			requireEdgeTypeCounts(t, destination, "test", map[string]int64{"OldType": 0, "NewType": 1})

			// The replayed counts must support the next ordinary write and close,
			// not only the read that follows replay: a stale NewType delta would
			// underflow here.
			require.NoError(t, destination.DeleteEdge(t.Context(), "test:e1"))
			requireEdgeTypeCounts(t, destination, "test", map[string]int64{"OldType": 0, "NewType": 0})
			require.NoError(t, destination.Close())
		})
	}
}

// TestRecoveredEdgeTypeRelabelThenDeletedEdgeLeavesNoStaleTypeCount records
// snapshot + WAL recovery of a relationship type change followed by an
// acknowledged relationship delete. The transported relabel carries no
// edge-type delta, so recovered counts must follow the replayed bodies instead
// of deltas derived from the source store's own counts.
func TestRecoveredEdgeTypeRelabelThenDeletedEdgeLeavesNoStaleTypeCount(t *testing.T) {
	const namespace = "test"
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
	seedEdgeTypeDestination(t, source, "OldType")
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	require.NoError(t, SaveSnapshot(&Snapshot{
		Timestamp: time.Now(),
		Version:   "1.0",
		Sequence:  source.bufferedCommitWAL.sequence.Load(),
		Nodes:     []*Node{{ID: "test:a"}, {ID: "test:b"}},
		Edges: []*Edge{{
			ID:        "test:e1",
			Type:      "OldType",
			StartNode: "test:a",
			EndNode:   "test:b",
		}},
	}, snapshotPath))

	tx, err := source.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace(namespace))
	edge, err := tx.GetEdge(t.Context(), "test:e1")
	require.NoError(t, err)
	edge.Type = "NewType"
	require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.NoError(t, source.FlushWriteBehind(t.Context()))

	walEngine := NewWALEngine(source, source.bufferedCommitWAL)
	require.NoError(t, walEngine.DeleteEdge(t.Context(), "test:e1"))
	require.NoError(t, source.bufferedCommitWAL.Sync())

	walDir := source.bufferedCommitWAL.Config().Dir
	entries, err := ReadWALEntriesFromDir(t.Context(), walDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	for _, destinationKind := range recoveryCountDestinations() {
		t.Run(destinationKind.name, func(t *testing.T) {
			destination := destinationKind.open(t)
			result, _, err := RecoverIntoEngine(t.Context(), destination, walDir, snapshotPath)
			require.NoError(t, err)
			require.Equal(t, 2, result.Applied)
			require.Zero(t, result.Failed)

			_, err = destination.GetEdge(t.Context(), "test:e1")
			require.ErrorIs(t, err, ErrNotFound)
			requireEdgeTypeCounts(t, destination, namespace, map[string]int64{"OldType": 0, "NewType": 0})
			require.NoError(t, destination.Close())
		})
	}
}
