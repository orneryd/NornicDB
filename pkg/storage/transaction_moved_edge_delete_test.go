package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionDeleteFormerEndpointKeepsMovedEdge(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, incoming := range []bool{false, true} {
			name := "outgoing/sync"
			if incoming {
				name = "incoming/sync"
			}
			if buffered {
				name += "/buffered"
			}
			t.Run(name, func(t *testing.T) {
				engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
					DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
				}))
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })
				for _, node := range []*Node{
					{ID: "test:a", Labels: []string{"Old"}},
					{ID: "test:b", Labels: []string{"Peer"}},
					{ID: "test:c", Labels: []string{"New"}},
				} {
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
				}
				edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}
				if incoming {
					edge.StartNode, edge.EndNode = edge.EndNode, edge.StartNode
				}
				require.NoError(t, engine.CreateEdge(edge))
				require.NoError(t, engine.FlushWriteBehind())
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(true))
				moved := copyEdge(edge)
				if incoming {
					moved.EndNode = "test:c"
				} else {
					moved.StartNode = "test:c"
				}
				require.NoError(t, tx.UpdateEdge(moved))
				require.NoError(t, tx.DeleteNode("test:a"))
				pending, err := tx.GetEdge(edge.ID)
				require.NoError(t, err, "deleting the former endpoint must retain the pending rewrite")
				require.Equal(t, moved.StartNode, pending.StartNode)
				require.Equal(t, moved.EndNode, pending.EndNode)
				require.NoError(t, tx.Commit())
				require.NoError(t, engine.FlushWriteBehind())
				stored, err := engine.GetStoredEdge(edge.ID)
				require.NoError(t, err)
				require.Equal(t, moved.StartNode, stored.StartNode)
				require.Equal(t, moved.EndNode, stored.EndNode)
				_, err = engine.GetStoredNode("test:a")
				require.ErrorIs(t, err, ErrNotFound)
				outgoing, err := engine.GetStoredOutgoingEdges(moved.StartNode)
				require.NoError(t, err)
				require.Len(t, outgoing, 1)
				incomingEdges, err := engine.GetStoredIncomingEdges(moved.EndNode)
				require.NoError(t, err)
				require.Len(t, incomingEdges, 1)
				count, err := engine.EdgeCountByType("LINK")
				require.NoError(t, err)
				require.EqualValues(t, 1, count)
				count, err = engine.EdgeCount()
				require.NoError(t, err)
				require.EqualValues(t, 1, count)
			})
		}
	}
}
