// SPDX-License-Identifier: MIT
package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Pause the existing commit stages after MVCC reservation but before Badger
// publication. A reader must not admit a peer's still-uncommitted version into
// its snapshot merely because the namespace sequence already includes it.
func TestTransactionSnapshotExcludesReservedUnpublishedDelete(t *testing.T) {
	engine := createTestBadgerEngine(t)
	source, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)
	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	require.NoError(t, peer.DeleteEdge(edgeID))
	require.NoError(t, peer.validateSnapshotIsolationConflicts())
	version, err := engine.allocateMVCCVersion(peer.badgerTx, peer.namespace, time.Now())
	require.NoError(t, err)
	peer.CommitVersion = version
	require.NoError(t, engine.materializeMVCCCommitInTxn(peer.badgerTx, version, peer.operations))
	require.NoError(t, peer.flushBufferedWrites())

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	require.NoError(t, reader.SetNamespace(peer.namespace))
	candidates, err := reader.GetOutgoingEdges(source)
	require.NoError(t, err)
	require.Len(t, candidates, 1, "unpublished deletion is not yet visible")
	require.Equal(t, edgeID, candidates[0].ID)
	t.Logf("reserved=%s reader=%s enumerated=%s", version, reader.readTS, edgeID)

	require.NoError(t, peer.badgerTx.Commit())
	peer.mu.Lock()
	peer.closeLocked(TxStatusCommitted, false, nil)
	peer.mu.Unlock()
	_, latestErr := engine.GetEdge(edgeID)
	require.ErrorIs(t, latestErr, ErrNotFound, "peer deletion really committed")
	snapshotEdge, snapshotErr := reader.GetEdge(edgeID)
	deleteErr := reader.DeleteEdge(candidates[0].ID)
	require.NoError(t, snapshotErr, "snapshot edge vanished after a reserved peer version published")
	require.Equal(t, edgeID, snapshotEdge.ID)
	require.NoError(t, deleteErr, "DELETE of an enumerated snapshot edge must not fail not-found")
}
