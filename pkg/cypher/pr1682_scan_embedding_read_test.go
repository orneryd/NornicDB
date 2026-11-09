package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPR1682EqualityScanWithReadProcedureKeepsEmbeddingState(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "sidecar_scan")
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "embedded", Labels: []string{"Sidecar"}, Properties: map[string]interface{}{"id": "embedded", "content": "body"}})
	require.NoError(t, err)
	node, err := base.GetNode(t.Context(), "sidecar_scan:embedded")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 0}}
	node.EmbedMeta = map[string]interface{}{"has_embedding": true, "chunk_count": 1}
	require.NoError(t, base.UpdateNodeEmbeddingSidecar(t.Context(), node))
	exec := NewStorageExecutor(store)
	for _, query := range []string{
		"MATCH (n {id: 'embedded'}) RETURN n.has_embedding AS embedded",
		"MATCH (n {id: 'embedded'}) CALL db.labels() YIELD label RETURN n.has_embedding AS embedded",
		"MATCH (n {id: 'embedded'}) CALL db.labels() YIELD label RETURN n.has_embedding AS embedded, n",
	} {
		result, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, true, result.Rows[0][0], query)
	}
}
