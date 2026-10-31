package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNamespacedEntitiesPreserveSuppressionFlag(t *testing.T) {
	for _, check := range []struct {
		name string
		run  func(*testing.T, *BadgerEngine, *NamespacedEngine, bool)
	}{
		{"TestNamespacedNodesPreserveSuppressionFlag", func(t *testing.T, eng *BadgerEngine, ns *NamespacedEngine, update bool) {
			node := &Node{ID: "n"}
			metadata := node.RuntimeMetadata()
			metadata.VisibilitySuppressed = !update
			node.SetRuntimeMetadata(metadata)
			_, err := ns.CreateNode(node)
			require.NoError(t, err)
			if update {
				metadata := node.RuntimeMetadata()
				metadata.VisibilitySuppressed = true
				node.SetRuntimeMetadata(metadata)
				node.Properties = map[string]any{"edited": true}
				require.NoError(t, ns.UpdateNode(NodeUpdateRequest{After: node}))
			}
			got, err := eng.GetNode("test:n")
			require.NoError(t, err)
			require.True(t, got.RuntimeMetadata().VisibilitySuppressed, "namespace node copies retain visibility metadata")
			require.Equal(t, NodeID("n"), node.ID)
		}},
		{"TestNamespacedEdgesPreserveSuppressionFlag", func(t *testing.T, eng *BadgerEngine, ns *NamespacedEngine, update bool) {
			for _, id := range []NodeID{"a", "b"} {
				_, err := ns.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			edge := &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINKS", VisibilitySuppressed: !update}
			require.NoError(t, ns.CreateEdge(edge))
			if update {
				edge.VisibilitySuppressed = true
				edge.Properties = map[string]any{"edited": true}
				require.NoError(t, ns.UpdateEdge(EdgeUpdateRequest{After: edge}))
			}
			got, err := eng.GetEdge("test:e")
			require.NoError(t, err)
			require.True(t, got.VisibilitySuppressed, "namespace translation must retain stored visibility metadata")
			require.Equal(t, EdgeID("e"), edge.ID, "caller IDs are unchanged")
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			for _, update := range []bool{false, true} {
				name := "create"
				if update {
					name = "update"
				}
				t.Run(name, func(t *testing.T) {
					eng := newTestEngine(t)
					eng.SetRevealAll(true)
					check.run(t, eng, NewNamespacedEngine(eng, "test"), update)
				})
			}
		})
	}
}

func TestNodeCopyPreservesEntityFields(t *testing.T) {
	now := time.Now().UTC()
	node := &Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{EmbeddingsStoredSeparately: true, VisibilitySuppressed: true}},
		ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(7)}, CreatedAt: now, UpdatedAt: now.Add(time.Hour),
		NamedEmbeddings: map[string][]float32{"title": {1, 2}}, ChunkEmbeddings: [][]float32{{3, 4}}, EmbedMeta: map[string]any{"chunk_count": int64(1)}}
	require.Equal(t, node, copyNode(node), "full copy preserves every entity field")
	got := copyNodeWithoutEmbeddings(node)
	expected := *node
	expected.NamedEmbeddings = nil
	expected.ChunkEmbeddings = nil
	require.Equal(t, &expected, got, "embedding-free copy drops only vectors")
	require.Equal(t, node, copyNode(node), "caller copy preserves metadata and shared vectors")
}
