package storage

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseoutNamespacedCanceledOperations(t *testing.T) {
	store := NewNamespacedEngine(newTestEngine(t), "closeout")
	live := t.Context()
	for _, id := range []NodeID{"a", "b"} {
		_, err := store.CreateNode(live, &Node{ID: id, Labels: []string{"Seed"}})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(live)
	cancel()
	_, err := store.GetNode(ctx, "a")
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.AllNodes(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.GetNodesByLabel(ctx, "Seed")
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.CreateNode(ctx, &Node{ID: "c"})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, store.DeleteNode(ctx, "a"), context.Canceled)
	edge := &Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "TEST"}
	require.ErrorIs(t, store.CreateEdge(ctx, edge), context.Canceled)
	require.ErrorIs(t, store.BulkCreateNodes(ctx, []*Node{{ID: "d"}}), context.Canceled)
	require.ErrorIs(t, store.BulkDeleteNodes(ctx, []NodeID{"b"}), context.Canceled)

	nodes, err := store.AllNodes(live)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	nodes, err = store.GetNodesByLabel(live, "Seed")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, id := range []NodeID{"a", "b"} {
		_, err = store.GetNode(live, id)
		require.NoError(t, err)
	}
	_, err = store.GetEdge(live, "ab")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, store.CreateEdge(live, edge))
	_, err = store.GetEdge(live, "ab")
	require.NoError(t, err)
}

func TestCloseoutStandardJournalRejectsOversizedAppendForms(t *testing.T) {
	for _, form := range []string{"single", "batch", "batch_second", "buffered"} {
		t.Run(form, func(t *testing.T) {
			wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "immediate", RecordPolicy: WALRecordsStandard})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"control": "before"}))
			large := walEntityCommitData{Payload: make([]byte, walMaxEntrySize)}
			expected := 2
			switch form {
			case "single":
				err = wal.Append(OpEntityCommit, large)
			case "batch", "batch_second":
				batch := wal.NewBatch()
				if form == "batch_second" {
					require.NoError(t, batch.append(OpCheckpoint, map[string]any{"control": "batch prefix"}))
					expected++
				}
				require.NoError(t, batch.append(OpEntityCommit, large))
				_, _, err = batch.CommitWithSeq()
			case "buffered":
				data := make([]byte, 17<<20)
				_, err = rand.New(rand.NewSource(1801)).Read(data)
				require.NoError(t, err)
				commit := bufferedCommit{txID: "large", logicalTxID: "large", namespace: "test", writes: map[string][]byte{"k": data}}
				err = wal.AppendBufferedCommit(&commit, newTestEngine(t).propKeyDict)
			}
			require.ErrorContains(t, err, "exceeds maximum WAL frame size")
			require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"control": "after"}))
			entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
			require.NoError(t, err)
			require.Len(t, entries, expected)
			for _, entry := range entries {
				require.Equal(t, OpCheckpoint, entry.Operation)
			}
		})
	}
}
