package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProjectedNodeCopyPreservesSelectionAndIsolation(t *testing.T) {
	now := time.Now().UTC()
	for _, test := range []struct {
		name       string
		properties []string
		want       map[string]any
	}{
		{"all", nil, map[string]any{"id": int64(1), "name": "Ada"}},
		{"none", []string{}, map[string]any{}},
		{"selected", []string{"name", "missing", "name"}, map[string]any{"name": "Ada"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := &Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}},
				ID: "db:node", Labels: []string{"Person"}, Properties: map[string]any{"id": int64(1), "name": "Ada"},
				EmbedMeta: map[string]any{"source": "text"}, ChunkEmbeddings: [][]float32{{1}},
				NamedEmbeddings: map[string][]float32{"v": {2}}, CreatedAt: now, UpdatedAt: now}
			got := projectCachedNodeForRead(node, test.properties, true)
			require.Equal(t, test.want, got.Properties)
			require.Equal(t, node.ID, got.ID)
			require.Equal(t, node.CreatedAt, got.CreatedAt)
			require.True(t, got.RuntimeMetadata().VisibilitySuppressed)
			require.Nil(t, got.ChunkEmbeddings)
			require.Nil(t, got.NamedEmbeddings)
			got.Labels[0] = "Changed"
			got.Properties["added"] = true
			got.EmbedMeta["source"] = "changed"
			require.Equal(t, "Person", node.Labels[0])
			require.NotContains(t, node.Properties, "added")
			require.Equal(t, "text", node.EmbedMeta["source"])
		})
	}
}

var benchmarkProjectedNode *Node

func BenchmarkProjectedNodeCopy(b *testing.B) {
	for _, count := range []int{8, 64} {
		b.Run(fmt.Sprintf("properties=%d", count), func(b *testing.B) {
			node := &Node{ID: "db:node", Labels: []string{"Person"}, Properties: make(map[string]any, count)}
			for i := range count {
				node.Properties[fmt.Sprintf("p%d", i)] = i
			}
			selected := []string{"p0"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkProjectedNode = projectCachedNodeForRead(node, selected, true)
			}
		})
	}
}
