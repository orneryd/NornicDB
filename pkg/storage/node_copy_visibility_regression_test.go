package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNamespacedNodesPreserveSuppressionFlag(t *testing.T) {
	for _, update := range []bool{false, true} {
		name := "create"
		if update {
			name = "update"
		}
		t.Run(name, func(t *testing.T) {
			eng := newTestEngine(t)
			eng.SetRevealAll(true)
			ns := NewNamespacedEngine(eng, "test")
			node := &Node{ID: "n", VisibilitySuppressed: !update}
			_, err := ns.CreateNode(node)
			require.NoError(t, err)
			if update {
				node.VisibilitySuppressed = true
				node.Properties = map[string]any{"edited": true}
				require.NoError(t, ns.UpdateNode(node))
			}
			got, err := eng.GetNode("test:n")
			require.NoError(t, err)
			require.True(t, got.VisibilitySuppressed, "namespace node copies retain visibility metadata")
			require.Equal(t, NodeID("n"), node.ID)
		})
	}
}

func TestNodeCopyPreservesEntityFields(t *testing.T) {
	now := time.Now().UTC()
	node := &Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(7)}, CreatedAt: now, UpdatedAt: now.Add(time.Hour),
		NamedEmbeddings: map[string][]float32{"title": {1, 2}}, ChunkEmbeddings: [][]float32{{3, 4}}, EmbedMeta: map[string]any{"chunk_count": int64(1)},
		EmbeddingsStoredSeparately: true, VisibilitySuppressed: true}
	require.Equal(t, node, copyNode(node), "full copy preserves every entity field")
	got := copyNodeWithoutEmbeddings(node)
	expected := *node
	expected.NamedEmbeddings = nil
	expected.ChunkEmbeddings = nil
	require.Equal(t, &expected, got, "embedding-free copy drops only vectors")
	require.Equal(t, node, copyNodeForCaller(node), "caller copy preserves metadata and shared vectors")
}
