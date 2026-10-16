package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A transaction's query label stream gives whole nodes without separately
// stored vectors (worker sidecar and large-body chunks; inline vectors are
// body data), on the snapshot and the write-behind stream. Its caches never
// answer a later vector read in the same transaction without the vectors.
func TestTransactionQueryLabelStreamLeavesStoredVectors(t *testing.T) {
	for _, mode := range []string{"snapshot", "write_behind"} {
		t.Run(mode, func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: mode == "write_behind", WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			sidecar := &Node{ID: "test:sidecar", Labels: []string{"Doc"}, Properties: map[string]any{"k": "s"}}
			_, err = b.CreateNode(sidecar)
			require.NoError(t, err)
			stored, err := b.GetNode(sidecar.ID)
			require.NoError(t, err)
			stored.ChunkEmbeddings = [][]float32{{1, 2}}
			stored.EmbedMeta = map[string]any{"chunk_count": 1}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(stored))
			legacy := separatelyStoredNode("test:legacy")
			_, err = b.CreateNode(legacy)
			require.NoError(t, err)
			inline := &Node{ID: "test:inline", Labels: []string{"Doc"}, Properties: map[string]any{"k": "i"}, ChunkEmbeddings: [][]float32{{3, 4}}}
			_, err = b.CreateNode(inline)
			require.NoError(t, err)
			require.NoError(t, b.FlushWriteBehind())
			want := map[NodeID][][]float32{sidecar.ID: stored.ChunkEmbeddings, legacy.ID: legacy.ChunkEmbeddings, inline.ID: inline.ChunkEmbeddings}
			// Cold reads, as after a restart: the stream reads stored bodies.
			b.invalidateCachesAfterRestore()

			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			rows := map[NodeID]*Node{}
			require.NoError(t, tx.StreamQueryNodesByLabel("Doc", func(n *Node) error { rows[n.ID] = n; return nil }))
			require.Len(t, rows, 3)
			require.Empty(t, rows[sidecar.ID].ChunkEmbeddings)
			require.True(t, rows[sidecar.ID].HasChunkEmbeddings(), "metadata answers has_embedding")
			require.Empty(t, rows[legacy.ID].ChunkEmbeddings)
			require.True(t, rows[legacy.ID].HasChunkEmbeddings())
			require.Equal(t, inline.ChunkEmbeddings, rows[inline.ID].ChunkEmbeddings, "inline vectors are body data")

			for id, chunks := range want {
				got, err := tx.GetNode(id)
				require.NoError(t, err)
				require.Equal(t, chunks, got.ChunkEmbeddings, "vector read of %s after the query stream", id)
			}
			full := map[NodeID]*Node{}
			require.NoError(t, tx.StreamNodesByLabelProjected("Doc", nil, func(n *Node) error { full[n.ID] = n; return nil }))
			for id, chunks := range want {
				require.Equal(t, chunks, full[id].ChunkEmbeddings, "full stream of %s after the query stream", id)
			}
		})
	}
}

// A node the stream finds in memory (node cache) keeps its in-memory vectors,
// shared rather than read, and its row still answers has_embedding.
func TestTransactionQueryLabelStreamSharesCachedNodes(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	legacy := separatelyStoredNode("test:legacy")
	_, err = b.CreateNode(legacy)
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind())
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	var row *Node
	require.NoError(t, tx.StreamQueryNodesByLabel("Doc", func(n *Node) error { row = n; return nil }))
	require.NotNil(t, row)
	require.True(t, row.HasChunkEmbeddings())
	got, err := tx.GetNode(legacy.ID)
	require.NoError(t, err)
	require.Equal(t, legacy.ChunkEmbeddings, got.ChunkEmbeddings)
}
