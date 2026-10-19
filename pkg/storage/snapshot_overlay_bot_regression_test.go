package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotOverlayProjectedCallbacksOwnPayload(t *testing.T) {
	for _, reader := range []string{"stream", "label", "transaction-stream"} {
		t.Run(reader, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			bufferedCreate(t, b, &Node{ID: "test:owned", Labels: []string{"Doc"},
				Properties:      map[string]any{"keep": "ack", "drop": "retained"},
				ChunkEmbeddings: [][]float32{{1, 2}}, EmbedMeta: map[string]any{"chunk_count": 1, "origin": "ack"}})
			calls := 0
			mutate := func(node *Node) error {
				calls++
				require.Equal(t, map[string]any{"keep": "ack"}, node.Properties)
				node.Labels[0] = "callback"
				node.Properties["keep"] = "callback"
				node.EmbedMeta["origin"] = "callback"
				if len(node.ChunkEmbeddings) != 0 {
					node.ChunkEmbeddings[0][0] = 99
				}
				return nil
			}
			opts := StreamNodesOptions{Projection: []string{"keep"}}
			if reader == "label" {
				require.NoError(t, b.StreamNodesByLabelProjectedInScope("test", "Doc", opts.Projection, mutate))
			} else if reader == "transaction-stream" {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.StreamNodesWithOptions(context.Background(), opts, mutate))
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, b.StreamNodesWithOptions(context.Background(), opts, mutate))
			}
			require.Equal(t, 1, calls)
			check := func() {
				node, err := b.GetNode("test:owned")
				require.NoError(t, err)
				require.Equal(t, []string{"Doc"}, node.Labels)
				require.Equal(t, map[string]any{"keep": "ack", "drop": "retained"}, node.Properties)
				require.Equal(t, "ack", node.EmbedMeta["origin"])
				require.Equal(t, [][]float32{{1, 2}}, node.ChunkEmbeddings)
			}
			check()
			require.NoError(t, b.FlushWriteBehind())
			b.invalidateCachesAfterRestore()
			check()
		})
	}
}

func TestSnapshotOverlayQueryCacheWithWriteBehind(t *testing.T) {
	for _, phase := range []string{"idle", "busy", "retired"} {
		t.Run(phase, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			_, err := b.CreateNode(&Node{ID: "test:cached", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			if phase != "idle" {
				bufferedCreate(t, b, &Node{ID: "test:other"})
				if phase == "retired" {
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			b.invalidateCachesAfterRestore()
			_, err = b.GetQueryNode("test:cached")
			require.NoError(t, err)
			b.nodeCacheMu.RLock()
			_, fullCached := b.nodeCache["test:cached"]
			_, queryCached := b.queryNodeCache["test:cached"]
			b.nodeCacheMu.RUnlock()
			require.False(t, fullCached)
			require.True(t, queryCached)
			misses, hits := atomic.LoadInt64(&b.cacheMisses), atomic.LoadInt64(&b.cacheHits)
			node, err := b.GetQueryNode("test:cached")
			require.NoError(t, err)
			require.Equal(t, misses, atomic.LoadInt64(&b.cacheMisses), "a warm query cache must avoid another physical read")
			require.Equal(t, hits+1, atomic.LoadInt64(&b.cacheHits))
			node.Properties["v"] = int64(2)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.UpdateNode(node))
			require.NoError(t, tx.Commit())
			node, err = b.GetQueryNode("test:cached")
			require.NoError(t, err)
			require.Equal(t, int64(2), node.Properties["v"], "pending update shadows the cache")
			require.NoError(t, b.FlushWriteBehind())
			node, err = b.GetQueryNode("test:cached")
			require.NoError(t, err)
			require.Equal(t, int64(2), node.Properties["v"])
			bufferedDeleteNode(t, b, "test:cached")
			_, err = b.GetQueryNode("test:cached")
			require.ErrorIs(t, err, ErrNotFound, "pending tombstone shadows the cache")
		})
	}
}

func TestSnapshotOverlayCascadeInvalidatesAdjacency(t *testing.T) {
	for _, writer := range []string{"engine", "transaction", "buffered"} {
		t.Run(writer, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			for _, id := range []NodeID{"test:hub", "test:peer"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:edge", StartNode: "test:hub", EndNode: "test:peer", Type: "R"}))
			edges, err := b.GetIncomingEdges("test:peer")
			require.NoError(t, err)
			require.Len(t, edges, 1, "warm both the adjacency IDs and edge body")
			if writer == "engine" {
				require.NoError(t, b.DeleteNode("test:hub"))
			} else if writer == "buffered" {
				bufferedDeleteNode(t, b, "test:hub")
			} else {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.DeleteNode("test:hub"))
				require.NoError(t, tx.Commit())
			}
			require.NoError(t, b.FlushWriteBehind())
			_, err = b.GetEdge("test:edge")
			require.ErrorIs(t, err, ErrNotFound, "the record was physically deleted")
			edges, err = b.GetIncomingEdges("test:peer")
			require.NoError(t, err)
			require.Empty(t, edges, "a retired cascade must also disappear from a warm traversal")
		})
	}
}
