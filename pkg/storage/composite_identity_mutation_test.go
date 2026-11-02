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
					_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"value": int64(1)}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "same", StartNode: "same", EndNode: "target", Type: "R", Properties: map[string]interface{}{"value": int64(1)}}))
			}
			composite := NewCompositeEngine(engines, map[string]string{"one": "one", "two": "two"}, map[string]string{"one": "read_write", "two": "read_write"})
			composite.SetCompositeName("comp")
			switch operation {
			case "create edge", "bulk create edge":
				edge := &Edge{ID: "new", Database: "two", StartNode: "same", EndNode: "target", Type: "CREATED"}
				if operation == "create edge" {
					require.NoError(t, composite.CreateEdge(t.Context(), edge))
				} else {
					require.NoError(t, composite.BulkCreateEdges(t.Context(), []*Edge{edge}))
				}
				_, err := engines["one"].GetEdge(t.Context(), "new")
				require.ErrorIs(t, err, ErrNotFound)
				created, err := engines["two"].GetEdge(t.Context(), "new")
				require.NoError(t, err)
				require.Equal(t, NodeID("same"), created.StartNode)

			case "update node":
				node, err := composite.GetNode(t.Context(), "4:two:same")
				require.NoError(t, err)
				node.Properties["value"] = int64(2)
				require.NoError(t, composite.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
			case "update edge":
				edge, err := composite.GetEdge(t.Context(), "5:two:same")
				require.NoError(t, err)
				edge.Properties["value"] = int64(2)
				require.NoError(t, composite.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge}))
			case "delete node":
				require.NoError(t, composite.DeleteNode(t.Context(), "4:two:same"))
			case "delete edge":
				require.NoError(t, composite.DeleteEdge(t.Context(), "5:two:same"))
			}
			node, err := engines["one"].GetNode(t.Context(), "same")
			require.NoError(t, err)
			require.Equal(t, int64(1), node.Properties["value"])
			edge, err := engines["one"].GetEdge(t.Context(), "same")
			require.NoError(t, err)
			require.Equal(t, int64(1), edge.Properties["value"])
			node, err = engines["two"].GetNode(t.Context(), "same")
			if operation == "delete node" {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
				if operation == "update node" {
					require.Equal(t, int64(2), node.Properties["value"])
				}
			}
			edge, err = engines["two"].GetEdge(t.Context(), "same")
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
