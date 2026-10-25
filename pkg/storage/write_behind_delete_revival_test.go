package storage

// With no concurrency, an acknowledged buffered write of an entity followed
// by a direct or explicit delete of the same entity must stay deleted: the
// flush must not replay the older buffered write over the delete.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteBehindDeleteAfterBufferedWriteStaysDeleted(t *testing.T) {
	for _, route := range []string{"direct", "explicit"} {
		for _, shape := range []string{"relationship", "node", "dangling-relationship"} {
			t.Run(fmt.Sprintf("%s/%s", route, shape), func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour)
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"v": int64(0)}})
					require.NoError(t, err)
				}
				require.NoError(t, b.FlushWriteBehind())

				deleteNode := func(id NodeID) error {
					if route == "direct" {
						return b.DeleteNode(id)
					}
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace("test"))
					if err := tx.DeleteNode(id); err != nil {
						_ = tx.Rollback()
						return err
					}
					return tx.Commit()
				}
				deleteEdge := func(id EdgeID) error {
					if route == "direct" {
						return b.DeleteEdge(id)
					}
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace("test"))
					if err := tx.DeleteEdge(id); err != nil {
						_ = tx.Rollback()
						return err
					}
					return tx.Commit()
				}

				var goneNodes []NodeID
				var goneEdges []EdgeID
				switch shape {
				case "relationship":
					bufferedCreateEdge(t, b, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"})
					require.NoError(t, deleteEdge("test:e"))
					goneEdges = []EdgeID{"test:e"}
				case "node":
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetImplicit(true))
					require.NoError(t, tx.SetNamespace("test"))
					require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}}))
					require.NoError(t, tx.Commit())
					require.NoError(t, deleteNode("test:a"))
					goneNodes = []NodeID{"test:a"}
				case "dangling-relationship":
					bufferedCreateEdge(t, b, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"})
					require.NoError(t, deleteNode("test:b"))
					goneNodes = []NodeID{"test:b"}
					goneEdges = []EdgeID{"test:e"}
				}

				require.NoError(t, b.FlushWriteBehind())
				for _, id := range goneNodes {
					_, err := b.GetStoredNode(id)
					require.True(t, errors.Is(err, ErrNotFound), "node %s revived by the flush: %v", id, err)
					_, err = b.GetNode(id)
					require.True(t, errors.Is(err, ErrNotFound), "node %s visible after the flush: %v", id, err)
				}
				for _, id := range goneEdges {
					_, err := b.GetStoredEdge(id)
					require.True(t, errors.Is(err, ErrNotFound), "relationship %s revived by the flush: %v", id, err)
					_, err = b.GetEdge(id)
					require.True(t, errors.Is(err, ErrNotFound), "relationship %s visible after the flush: %v", id, err)
				}
				edges, err := b.GetOutgoingEdges("test:a")
				if err == nil {
					require.Empty(t, edges, "test:a keeps a relationship that was deleted")
				}
				nodes, err := b.NodeCount()
				require.NoError(t, err)
				require.EqualValues(t, 2-len(goneNodes), nodes)
				count, err := b.EdgeCount()
				require.NoError(t, err)
				require.Zero(t, count)
			})
		}
	}
}
