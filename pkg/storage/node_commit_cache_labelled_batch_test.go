package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Count persistence belongs to the same uncontended publication tail. It must
// not revoke the data commit's node/edge token merely because labels and types
// made its merged count delta nonempty.
func TestNodeCommitCacheKeepsLabelledMixedBatchWarm(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%t", buffered), func(t *testing.T) {
			b := NewMemoryEngine().BadgerEngine
			if buffered {
				require.NoError(t, b.Close())
				b = newWriteBehindTestEngine(t, time.Hour)
			} else {
				t.Cleanup(func() { _ = b.Close() })
			}
			b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}, IncludeLabels: true})
			nodes := make([]*Node, 16)
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:labelled%d", i)), Labels: []string{"WarmLabel"}, Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{{1, 0}}}
				_, err := b.CreateNode(nodes[i])
				require.NoError(t, err)
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.SetImplicit(buffered))
			for _, n := range nodes {
				n.Properties["v"] = int64(1)
				require.NoError(t, tx.UpdateNode(n))
			}
			edge := &Edge{ID: "test:labelled-edge", StartNode: nodes[0].ID, EndNode: nodes[1].ID, Type: "WARM_TYPE"}
			require.NoError(t, tx.CreateEdge(edge))
			require.NoError(t, tx.Commit())
			if buffered {
				require.NoError(t, b.FlushWriteBehind())
			}
			for _, n := range nodes {
				b.nodeCacheMu.RLock()
				cached, ok := b.cachedNodeLocked(n.ID, false, 0)
				b.nodeCacheMu.RUnlock()
				require.True(t, ok, "first read must remain warm for %s", n.ID)
				require.Equal(t, int64(1), cached.Properties["v"])
			}
			_, warm := b.cacheLoadEdge(edge.ID)
			require.True(t, warm, "same commit's edge callback keeps its durable token")
			for _, n := range nodes {
				got, err := b.GetNode(n.ID)
				require.NoError(t, err)
				require.Equal(t, int64(1), got.Properties["v"])
			}
		})
	}
}
