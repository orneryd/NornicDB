package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTransactionRetainedSidecarSurvivesVersionChange(t *testing.T) {
	for _, layout := range []string{"chunks", "empty", "failed"} {
		for _, writes := range []int{1, 3} {
			t.Run(layout+time.Duration(writes).String(), func(t *testing.T) {
				dir := t.TempDir()
				b, err := NewBadgerEngine(dir)
				require.NoError(t, err)
				b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
				at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				n := sidecarTestNode(t, b, "stamp", map[string]any{"text": "source"}, at)
				chunks := [][]float32{{.25, .5}, {.75, 1}}
				meta := map[string]any{"has_embedding": true, "chunk_count": 2}
				if layout != "chunks" {
					chunks = nil
					meta = map[string]any{"chunk_count": 0, "marker": "keep"}
				}
				if layout == "failed" {
					meta["embedding_failed"] = true
					meta["embedding_error"] = "permanent"
				}
				require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, n.ID, chunks, meta, time.Time{})))
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				for i := 1; i <= writes; i++ {
					v, e := tx.GetNode(n.ID)
					require.NoError(t, e)
					v.UpdatedAt = at.Add(time.Duration(i) * time.Second)
					v.Properties["other"] = int64(i)
					require.NoError(t, tx.UpdateNode(v))
				}
				require.NoError(t, tx.Commit())
				require.NoError(t, b.Close())
				b, err = NewBadgerEngine(dir)
				require.NoError(t, err)
				defer b.Close()
				got, err := b.GetNode(n.ID)
				require.NoError(t, err)
				require.Equal(t, chunks, got.ChunkEmbeddings)
				require.Equal(t, meta, got.EmbedMeta)
				require.True(t, at.Add(time.Duration(writes)*time.Second).Equal(got.UpdatedAt))
			})
		}
	}
}

func TestTransactionRetainedSidecarConflictsWithWorker(t *testing.T) {
	b := newSidecarTestBadger(t)
	defer b.Close()
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n := sidecarTestNode(t, b, "conflict", map[string]any{"text": "source"}, at)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, n.ID, [][]float32{{1, 2}}, map[string]any{"chunk_count": 1}, time.Time{})))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	v, err := tx.GetNode(n.ID)
	require.NoError(t, err)
	v.UpdatedAt = at.Add(time.Second)
	v.Properties["other"] = true
	require.NoError(t, tx.UpdateNode(v))
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, n.ID, [][]float32{{3, 4}}, map[string]any{"chunk_count": 1}, time.Time{})))
	require.ErrorIs(t, tx.Commit(), ErrConflict)
	got, err := b.GetNode(n.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{3, 4}}, got.ChunkEmbeddings)
	require.True(t, at.Equal(got.UpdatedAt))
	require.NotContains(t, got.Properties, "other")
}

func TestTransactionRetainedSidecarRejectsUnavailableMetadata(t *testing.T) {
	for _, failure := range []string{"writer", "snapshot", "staged"} {
		t.Run(failure, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			defer b.Close()
			b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
			at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			n := sidecarTestNode(t, b, "errors", map[string]any{"text": "source"}, at)
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, n.ID, [][]float32{{1, 2}}, map[string]any{"chunk_count": 1}, time.Time{})))
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			v, err := tx.GetNode(n.ID)
			require.NoError(t, err)
			switch failure {
			case "writer":
				tx.badgerTx.Discard()
			case "snapshot":
				tx.snapshotTx.Discard()
			case "staged":
				tx.bufferSet(embeddingMetaKey(n.ID), []byte{0xc1})
			}
			v.UpdatedAt = at.Add(time.Second)
			v.Properties["other"] = true
			require.Error(t, tx.UpdateNode(v))
			require.NoError(t, tx.Rollback())
			got, err := b.GetNode(n.ID)
			require.NoError(t, err)
			require.Equal(t, [][]float32{{1, 2}}, got.ChunkEmbeddings)
			require.True(t, at.Equal(got.UpdatedAt))
			require.NotContains(t, got.Properties, "other")
		})
	}
}
