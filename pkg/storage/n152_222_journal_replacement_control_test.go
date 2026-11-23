package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepeatedJournalUpdatesPreserveStagedVectorReplacement(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:journal-staged-replacement"
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, repeatJournalChunks)
	wal := wrapped.GetWAL()
	before := wal.Sequence()
	ctx := t.Context()
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	for _, edit := range []int64{1, 2, 3} {
		node, err := tx.GetNode(ctx, id)
		require.NoError(t, err)
		node.Properties["flag"] = edit
		if edit == 2 {
			node.ChunkEmbeddings[0][0] = 42
		}
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	defer prepared.Abort()
	receipt, err := wrapped.SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(ctx))
	journal := propertyUpdateJournalUpdate(t, wal, before)
	require.Equal(t, 1, journal.References, "a staged replacement is not the committed predecessor")
	recovered := openLargeCommitTestEngine(t, t.TempDir())
	entries, err := ReadWALEntriesFromDir(ctx, wal.Config().Dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, ReplayWALEntry(ctx, recovered, entry))
	}
	node, err := recovered.GetNode(ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 3, node.Properties["flag"])
	require.Len(t, node.ChunkEmbeddings, repeatJournalChunks)
	require.Equal(t, float32(42), node.ChunkEmbeddings[0][0])
	require.Equal(t, float32(1)+float32(propertyUpdateVectorDims)/7, node.ChunkEmbeddings[1][0])
}
