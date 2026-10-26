package storage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). Exported committed-append APIs
// own their input; AddCommit has a separate explicit payload-handover contract.
func TestResidualBufferAppendOwnsInput(t *testing.T) {
	w := storage.NewWriteBehindBuffer(time.Hour, 100000, func(*storage.CommitBuffer) error { return nil })
	t.Cleanup(w.Close)
	t.Run("node", func(t *testing.T) {
		node := &storage.Node{
			ID: "test:append-node", Labels: []string{"Owned"},
			Properties:      map[string]any{"nested": map[string]any{"list": []int64{1}}},
			EmbedMeta:       map[string]any{"weights": []float32{2}, "typed": []any{map[string]string{"state": "before"}}},
			ChunkEmbeddings: [][]float32{{3}},
		}
		w.AppendNode(node)
		node.Labels[0] = "Changed"
		node.Properties["nested"].(map[string]any)["list"].([]int64)[0] = 99
		node.Properties["new"] = true
		node.EmbedMeta["weights"].([]float32)[0] = 99
		node.EmbedMeta["typed"].([]any)[0].(map[string]string)["state"] = "changed"
		node.ChunkEmbeddings[0][0] = 99
		buffered, found, deleted := w.GetNode("test:append-node")
		require.True(t, found)
		require.False(t, deleted)
		require.Equal(t, []int64{1}, buffered.Properties["nested"].(map[string]any)["list"])
		require.Equal(t, []string{"Owned"}, buffered.Labels)
		require.NotContains(t, buffered.Properties, "new")
		require.Equal(t, []float32{2}, buffered.EmbedMeta["weights"])
		require.Equal(t, "map[state:before]", fmt.Sprint(buffered.EmbedMeta["typed"].([]any)[0]))
		require.Equal(t, [][]float32{{3}}, buffered.ChunkEmbeddings)

	})
	t.Run("edge", func(t *testing.T) {
		edge := &storage.Edge{
			ID: "test:append-edge", StartNode: "test:append-node", EndNode: "test:append-peer", Type: "OWNED",
			Properties: map[string]any{"nested": map[string]any{"list": []string{"old"}}},
		}
		w.AppendEdge(edge)
		edge.Properties["nested"].(map[string]any)["list"].([]string)[0] = "changed"
		edge.Properties["new"] = true
		bufferedEdge, found, deleted := w.GetEdge("test:append-edge")
		require.True(t, found)
		require.False(t, deleted)
		require.Equal(t, []string{"old"}, bufferedEdge.Properties["nested"].(map[string]any)["list"])
		require.NotContains(t, bufferedEdge.Properties, "new")
	})
}
