package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompositeIdentityMutationRouting(t *testing.T) {
	for _, operation := range []string{"update node", "update edge", "delete node", "delete edge", "create edge", "bulk create edge"} {
		t.Run(operation, func(t *testing.T) {
			engines := map[string]Engine{}
			for _, database := range []string{"one", "two"} {
				base, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
				require.NoError(t, err)
				base = runtimeFixtureOwner.Own(t, base)
				engine := NewNamespacedEngine(base, database)
				t.Cleanup(func() { require.NoError(t, engine.Close()) })
				engines[database] = engine
				for _, id := range []NodeID{"same", "target"} {
					_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"value": int64(1)}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(&Edge{ID: "same", StartNode: "same", EndNode: "target", Type: "R", Properties: map[string]interface{}{"value": int64(1)}}))
			}
			composite := NewCompositeEngine(engines, map[string]string{"one": "one", "two": "two"}, map[string]string{"one": "read_write", "two": "read_write"})
			composite.SetCompositeName("comp")
			switch operation {
			case "create edge", "bulk create edge":
				edge := &Edge{ID: "new", Database: "two", StartNode: "same", EndNode: "target", Type: "CREATED"}
				if operation == "create edge" {
					require.NoError(t, composite.CreateEdge(edge))
				} else {
					require.NoError(t, composite.BulkCreateEdges([]*Edge{edge}))
				}
				_, err := engines["one"].GetEdge("new")
				require.ErrorIs(t, err, ErrNotFound)
				created, err := engines["two"].GetEdge("new")
				require.NoError(t, err)
				require.Equal(t, NodeID("same"), created.StartNode)

			case "update node":
				node, err := composite.GetNode("4:two:same")
				require.NoError(t, err)
				node.Properties["value"] = int64(2)
				require.NoError(t, composite.UpdateNode(node))
			case "update edge":
				edge, err := composite.GetEdge("5:two:same")
				require.NoError(t, err)
				edge.Properties["value"] = int64(2)
				require.NoError(t, composite.UpdateEdge(edge))
			case "delete node":
				require.NoError(t, composite.DeleteNode("4:two:same"))
			case "delete edge":
				require.NoError(t, composite.DeleteEdge("5:two:same"))
			}
			node, err := engines["one"].GetNode("same")
			require.NoError(t, err)
			require.Equal(t, int64(1), node.Properties["value"])
			edge, err := engines["one"].GetEdge("same")
			require.NoError(t, err)
			require.Equal(t, int64(1), edge.Properties["value"])
			node, err = engines["two"].GetNode("same")
			if operation == "delete node" {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
				if operation == "update node" {
					require.Equal(t, int64(2), node.Properties["value"])
				}
			}
			edge, err = engines["two"].GetEdge("same")
			if operation == "delete node" || operation == "delete edge" {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
				if operation == "update edge" {
					require.Equal(t, int64(2), edge.Properties["value"])
				}
			}
		})
	}
}
