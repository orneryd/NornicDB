package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionQueryNodeScansKeepMetadataAndFullReads(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		for _, bounded := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/bounded=%v", tc.name, bounded), func(t *testing.T) {
				b := newSidecarTestBadger(t)
				id := NodeID("test:lazy")
				tc.create(t, b, id)
				corruptFirstEmbeddingChunk(t, b, id)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				for read := 0; read < 2; read++ {
					rows := 0
					err = tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
						Prefix: "test:", Label: "Doc", QueryNodes: true, ApplyDecayFilter: true,
					}, func(node *Node) error {
						require.Equal(t, id, node.ID)
						require.True(t, node.HasChunkEmbeddings())
						require.Equal(t, tc.chunks, chunkCountFromMeta(node.EmbedMeta))
						require.Empty(t, node.ChunkEmbeddings)
						require.False(t, node.RuntimeMetadata().EmbeddingsStoredSeparately)
						rows++
						if bounded {
							return ErrIterationStopped
						}
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, 1, rows)
					_, err = tx.GetNode(id)
					require.ErrorContains(t, err, "embedding chunk 0")
				}
			})
		}
	}
}
