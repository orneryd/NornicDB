package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitCacheFinalDeletes(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, kind := range []string{"edge-delete", "cascade", "create-delete"} {
			t.Run(fmt.Sprintf("buffered=%t/%s", buffered, kind), func(t *testing.T) {
				var b *BadgerEngine
				if buffered {
					b = newWriteBehindTestEngine(t, time.Hour)
					b.writeBehind.Close()
				} else {
					b = NewMemoryEngine().BadgerEngine
					t.Cleanup(func() { require.NoError(t, b.Close()) })
				}
				b.SetDecayEnabled(false)
				for _, id := range []NodeID{"test:delete-a", "test:delete-b", "test:delete-middle"} {
					_, err := b.CreateNode(&Node{ID: id, ChunkEmbeddings: [][]float32{{1}}})
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:deleted-image", StartNode: "test:delete-a", EndNode: "test:delete-b", Type: "R", Properties: map[string]any{"v": int64(0)}}
				if kind != "create-delete" {
					require.NoError(t, b.CreateEdge(edge))
				}
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetImplicit(buffered))
				next := copyEdge(edge)
				next.Properties["v"] = int64(1)
				if kind == "create-delete" {
					require.NoError(t, tx.CreateEdge(next))
				} else {
					require.NoError(t, tx.UpdateEdge(next))
				}
				node, err := tx.GetNode("test:delete-middle")
				require.NoError(t, err)
				node.Properties = map[string]any{"v": int64(1)}
				require.NoError(t, tx.UpdateNode(node))
				if kind == "cascade" {
					require.NoError(t, tx.DeleteNode(edge.StartNode))
				} else {
					require.NoError(t, tx.DeleteEdge(edge.ID))
				}
				require.NoError(t, tx.Commit())
				if buffered {
					require.NoError(t, b.FlushWriteBehind())
				}
				_, err = b.GetStoredEdge(edge.ID)
				require.ErrorIs(t, err, ErrNotFound)
				_, cached := b.cacheLoadEdge(edge.ID)
				require.False(t, cached, "a deleted final image must never be published as a live body")
				rows, err := b.GetStoredIncomingEdges(edge.EndNode)
				require.NoError(t, err)
				require.Empty(t, rows)
				rows, err = b.GetIncomingEdges(edge.EndNode)
				require.NoError(t, err)
				require.Empty(t, rows)
				if kind == "cascade" {
					_, err := b.GetStoredNode(edge.StartNode)
					require.ErrorIs(t, err, ErrNotFound)
				}
			})
		}
	}
}
