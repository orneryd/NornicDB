package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNodeCacheKnownWritersKeepUnrelatedNodeWarm(t *testing.T) {
	for _, kind := range []string{"edge-delete", "edge-bulk-delete", "sidecar", "transaction-vectors", "buffered-vectors", "transaction-create-vectors", "buffered-create-vectors"} {
		t.Run(kind, func(t *testing.T) {
			b := NewMemoryEngine().BadgerEngine
			if kind == "buffered-vectors" || kind == "buffered-create-vectors" {
				require.NoError(t, b.Close())
				b = newWriteBehindTestEngine(t, time.Hour)
			} else {
				t.Cleanup(func() { _ = b.Close() })
			}
			b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}})
			target := &Node{ID: "test:review-untouched", Properties: map[string]any{"v": int64(7)}, ChunkEmbeddings: [][]float32{{1, 0}}}
			_, err := b.CreateNode(target)
			require.NoError(t, err)
			vector := make([]float32, 20000)
			vector[0] = 1
			other := &Node{ID: "test:review-other", Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{vector}}
			_, err = b.CreateNode(other)
			require.NoError(t, err)
			edge := &Edge{ID: "test:review-edge", StartNode: target.ID, EndNode: other.ID, Type: "REVIEW"}
			require.NoError(t, b.CreateEdge(edge))
			_, err = b.GetNode(target.ID)
			require.NoError(t, err)
			b.nodeCacheMu.RLock()
			_, warmBefore := b.cachedNodeLocked(target.ID, false, 0)
			fenceBefore := b.nodeCacheInvalidationTs
			b.nodeCacheMu.RUnlock()
			require.True(t, warmBefore)
			switch kind {
			case "edge-delete":
				require.NoError(t, b.DeleteEdge(edge.ID))
			case "edge-bulk-delete":
				require.NoError(t, b.BulkDeleteEdges([]EdgeID{edge.ID}))
			case "sidecar":
				other.ChunkEmbeddings[0][0] = 2
				require.NoError(t, b.UpdateNodeEmbeddingSidecar(other))
			default:
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				buffered := kind == "buffered-vectors" || kind == "buffered-create-vectors"
				require.NoError(t, tx.SetImplicit(buffered))
				if kind == "transaction-create-vectors" || kind == "buffered-create-vectors" {
					created := copyNode(other)
					created.ID = "test:review-created"
					_, err = tx.CreateNode(created)
					require.NoError(t, err)
				} else {
					other.Properties["v"] = int64(1)
					other.ChunkEmbeddings[0][0] = 2
					require.NoError(t, tx.UpdateNode(other))
				}
				require.NoError(t, tx.Commit())
				if buffered {
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			b.nodeCacheMu.RLock()
			_, warmAfter := b.cachedNodeLocked(target.ID, false, 0)
			fenceAfter := b.nodeCacheInvalidationTs
			b.nodeCacheMu.RUnlock()
			missesBefore := b.cacheMisses
			got, err := b.GetNode(target.ID)
			require.NoError(t, err)
			require.Equal(t, int64(7), got.Properties["v"])
			t.Logf("unrelated warm=%t -> %t; full fence=%d -> %d; public GetNode cache misses delta=%d", warmBefore, warmAfter, fenceBefore, fenceAfter, b.cacheMisses-missesBefore)
			require.True(t, warmAfter, "known writer must preserve unrelated node-body warmth")
		})
	}
}
