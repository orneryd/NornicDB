package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const repeatJournalChunks = 128

func prepareRepeatedJournalUpdate(t testing.TB, engine *BadgerEngine, id NodeID, updates int) *PreparedEntityCommit {
	t.Helper()
	ctx := t.Context()
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for i := 0; i < updates; i++ {
		node, err := tx.GetNode(ctx, id)
		require.NoError(t, err)
		node.Properties["flag"] = int64(i + 1)
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	return prepared
}

func TestRepeatedPropertyUpdatesKeepJournalChunkPayloadBounded(t *testing.T) {
	for _, updates := range []int{1, 4} {
		t.Run(fmt.Sprint(updates), func(t *testing.T) {
			engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
			const id NodeID = "test:journal-repeated-update"
			seedPropertyUpdateVectorNode(t, engine, wrapped, id, repeatJournalChunks)
			wal := wrapped.GetWAL()
			before := wal.Sequence()
			prepared := prepareRepeatedJournalUpdate(t, engine, id, updates)
			defer prepared.Abort()
			receipt, err := wrapped.SubmitEntityCommit(t.Context(), prepared, nil)
			require.NoError(t, err)
			require.NoError(t, receipt.Wait(t.Context()))
			record := propertyUpdateJournalUpdate(t, wal, before)
			t.Logf("updates=%d chunks=%d record_bytes=%d semantic_bytes=%d vector_references=%d", updates, repeatJournalChunks, record.Bytes, record.Payload, record.References)
			recovered := openLargeCommitTestEngine(t, t.TempDir())
			entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
			require.NoError(t, err)
			for _, entry := range entries {
				require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
			}
			node, err := recovered.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.EqualValues(t, updates, node.Properties["flag"])
			requirePropertyUpdateVectors(t, node, repeatJournalChunks)
			require.LessOrEqual(t, record.Payload, updates*propertyUpdateJournalByteBound,
				"the semantic update record must not carry the unchanged managed vectors once per staged edit")
		})
	}
}

func BenchmarkRepeatedPropertyUpdateJournalSubmit(b *testing.B) {
	for _, updates := range []int{1, 4} {
		b.Run(fmt.Sprintf("updates=%d", updates), func(b *testing.B) {
			engine, wrapped := newPropertyUpdateVectorEngine(b, WALRecordsLarge)
			const id NodeID = "test:journal-repeated-bench"
			seedPropertyUpdateVectorNode(b, engine, wrapped, id, repeatJournalChunks)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				prepared := prepareRepeatedJournalUpdate(b, engine, id, updates)
				b.StartTimer()
				receipt, err := wrapped.SubmitEntityCommit(b.Context(), prepared, nil)
				require.NoError(b, err)
				require.NoError(b, receipt.Wait(b.Context()))
			}
		})
	}
}
