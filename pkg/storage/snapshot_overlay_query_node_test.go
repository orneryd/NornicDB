package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Query-node reads must share the captured view without hydrating stored
// vectors, including when transaction reads delegate through the engine.
func TestSnapshotOverlayQueryNodeVectorModes(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			id := NodeID("test:query")
			tc.create(t, b, id)
			corruptFirstEmbeddingChunk(t, b, id)
			for _, phase := range []string{"idle", "pending", "retired"} {
				t.Run(phase, func(t *testing.T) {
					if phase == "pending" {
						bufferedCreate(t, b, &Node{ID: "test:pending"})
						require.Positive(t, b.writeBehind.PendingOps())
					} else if phase == "retired" {
						require.NoError(t, b.FlushWriteBehind())
						require.Zero(t, b.writeBehind.PendingOps())
					}
					b.invalidateCachesAfterRestore()
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					defer tx.Rollback()
					for _, read := range []func(NodeID) (*Node, error){b.GetQueryNode, tx.GetQueryNode} {
						for i := 0; i < 2; i++ {
							node, err := read(id)
							require.NoError(t, err)
							require.Empty(t, node.ChunkEmbeddings)
							require.False(t, node.RuntimeMetadata().EmbeddingsStoredSeparately)
							require.True(t, node.HasChunkEmbeddings())
							require.Equal(t, tc.chunks, chunkCountFromMeta(node.EmbedMeta))
							require.Equal(t, "t", node.Properties["title"])
						}
					}
					_, err = b.GetNode(id)
					require.ErrorContains(t, err, "embedding chunk 0")
					_, err = tx.GetNode(id)
					require.ErrorContains(t, err, "embedding chunk 0")
				})
			}
		})
	}
}
