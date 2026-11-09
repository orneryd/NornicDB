package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReplayedBufferedCommitReconcilesLabelCountsWithRestoredBody records the
// single-entry recovery contract: replaying one transported commit into a store
// that still holds the pre-commit body must leave the derived label counts
// agreeing with the replayed body, exactly as the batched recovery paths do.
func TestReplayedBufferedCommitReconcilesLabelCountsWithRestoredBody(t *testing.T) {
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
	_, err := source.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Old"}})
	require.NoError(t, err)
	tx, err := source.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	node.Labels = []string{"New"}
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())

	entries, err := ReadWALEntriesFromDir(t.Context(), source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)
	destination := createTestBadgerEngine(t)
	_, err = destination.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Old"}})
	require.NoError(t, err)
	replayed := false
	for _, entry := range entries {
		if entry.Operation != OpBufferedCommit {
			continue
		}
		replayed = true
		require.NoError(t, ReplayWALEntry(t.Context(), destination, entry))
	}
	require.True(t, replayed)

	got, err := destination.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"New"}, got.Labels)
	old, err := destination.NodeCountByLabelInNamespace(t.Context(), "test", "Old")
	require.NoError(t, err)
	fresh, err := destination.NodeCountByLabelInNamespace(t.Context(), "test", "New")
	require.NoError(t, err)
	require.Zero(t, old)
	require.Equal(t, int64(1), fresh)

	// The replayed counts must support the next ordinary write and close, not
	// only the read that follows replay: a stale Old delta would underflow here.
	require.NoError(t, destination.DeleteNode(t.Context(), "test:a"))
	for _, label := range []string{"Old", "New"} {
		count, err := destination.NodeCountByLabelInNamespace(t.Context(), "test", label)
		require.NoError(t, err)
		require.Zero(t, count, "label=%s must follow the deleted body", label)
	}
	require.NoError(t, destination.Close())
}

// TestRecoveredRelabelThenDeletedNodeLeavesNoStaleLabelCount records
// snapshot + WAL recovery of a relabel followed by an acknowledged delete. The
// transported relabel carries no label delta, so recovered counts must follow
// the replayed bodies instead of deltas derived from the source store's own
// counts. The startup recovery path opens its destination through the same
// RecoverIntoEngine boundary with write-behind either off or on.
func TestRecoveredRelabelThenDeletedNodeLeavesNoStaleLabelCount(t *testing.T) {
	const namespace = "test"
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
	_, err := source.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Old"}})
	require.NoError(t, err)
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	require.NoError(t, SaveSnapshot(&Snapshot{
		Timestamp: time.Now(),
		Version:   "1.0",
		Sequence:  source.bufferedCommitWAL.sequence.Load(),
		Nodes:     []*Node{{ID: "test:a", Labels: []string{"Old"}}},
	}, snapshotPath))

	tx, err := source.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace(namespace))
	node, err := tx.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	node.Labels = []string{"New"}
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.NoError(t, source.FlushWriteBehind(t.Context()))

	walEngine := NewWALEngine(source, source.bufferedCommitWAL)
	require.NoError(t, walEngine.DeleteNode(t.Context(), "test:a"))
	require.NoError(t, source.bufferedCommitWAL.Sync())

	walDir := source.bufferedCommitWAL.Config().Dir
	entries, err := ReadWALEntriesFromDir(t.Context(), walDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	destinations := []struct {
		name string
		open func(*testing.T) *BadgerEngine
	}{
		{name: "ordinary-destination", open: createTestBadgerEngine},
		{name: "write-behind-destination", open: func(t *testing.T) *BadgerEngine {
			return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
		}},
	}
	for _, destinationKind := range destinations {
		t.Run(destinationKind.name, func(t *testing.T) {
			destination := destinationKind.open(t)
			result, _, err := RecoverIntoEngine(t.Context(), destination, walDir, snapshotPath)
			require.NoError(t, err)
			require.Equal(t, 2, result.Applied)
			require.Zero(t, result.Failed)

			_, err = destination.GetNode(t.Context(), "test:a")
			require.ErrorIs(t, err, ErrNotFound)
			for _, label := range []string{"Old", "New"} {
				count, err := destination.NodeCountByLabelInNamespace(t.Context(), namespace, label)
				require.NoError(t, err)
				require.Zero(t, count, "label=%s must not survive the recovered delete", label)
			}
			require.NoError(t, destination.Close())
		})
	}
}
