package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReview1548CypherVectorUpdateDelete(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "autocommit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			base.SetEmbeddingTextPolicy(storage.EmbeddingTextPolicy{Exclude: []string{"review_count"}, IncludeLabels: true})
			e := storage.NewNamespacedEngine(base, "review")
			n := &storage.Node{ID: "vec", Labels: []string{"ReviewGuard"}, Properties: map[string]any{"key": "x"}, ChunkEmbeddings: [][]float32{{1, 0}, {0, 1}}}
			_, err := e.CreateNode(n)
			require.NoError(t, err)
			exec := NewStorageExecutorWithQueryCachePolicy(e, 0, 0)
			run := func(q string) {
				_, err := exec.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
			}
			if explicit {
				run("BEGIN")
			}
			run("MATCH (n:ReviewGuard {key:'x'}) SET n.review_count=1")
			if explicit {
				run("COMMIT")
			}
			got, err := e.GetNode(n.ID)
			require.NoError(t, err)
			require.Equal(t, n.ChunkEmbeddings, got.ChunkEmbeddings)
			if explicit {
				run("BEGIN")
			}
			run("MATCH (n:ReviewGuard {key:'x'}) DELETE n")
			if explicit {
				run("COMMIT")
			}
			_, err = e.GetNode(n.ID)
			require.ErrorIs(t, err, storage.ErrNotFound)
		})
	}
}
