package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkPreparedVectorPropertyUpdate measures native preparation, including
// staging and capture, for a property update that retains managed vectors.
// Each iteration discards its prepared frame: the fixture stays bounded.
func BenchmarkPreparedVectorPropertyUpdate(b *testing.B) {
	for _, chunks := range []int{0, 256} {
		b.Run(fmt.Sprintf("chunks=%d", chunks), func(b *testing.B) {
			engine, err := NewBadgerEngine(b.TempDir())
			require.NoError(b, err)

			engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
			b.Cleanup(func() { require.NoError(b, engine.Close()) })
			vectors := make([][]float32, chunks)
			for i := range vectors {
				vectors[i] = make([]float32, 1024)
				vectors[i][0] = float32(i + 1)
			}
			_, err = engine.CreateNode(b.Context(), &Node{ID: "test:images", Properties: map[string]any{"text": "same", "flag": false}, ChunkEmbeddings: vectors})
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				err = tx.UpdateGraphNode(b.Context(), NodeUpdateRequest{After: &Node{ID: "test:images", Properties: map[string]any{"text": "same", "flag": true}}, Policy: ClientNodeEdits})
				if err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				prepared, err := tx.PrepareEntityCommit(b.Context(), EntityCommitRequest{LogicalID: fmt.Sprintf("image-%d", i)})
				if err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				prepared.Abort()
			}
		})
	}
}
