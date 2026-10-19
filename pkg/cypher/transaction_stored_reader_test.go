package cypher

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionStoredReaderKeepsOverlayAndHiddenBodies(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		name := "sync"
		if buffered {
			name = "buffered"
		}
		t.Run(name, func(t *testing.T) {
			base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
				DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			for _, db := range []string{"a", "b"} {
				ns := storage.NewNamespacedEngine(base, db)
				for _, id := range []storage.NodeID{"x", "y", "z", "removed"} {
					_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"k": string(id)}})
					require.NoError(t, err)
				}
				for _, edge := range []*storage.Edge{
					{ID: "hidden", StartNode: "x", EndNode: "y", Type: "REL", Properties: map[string]any{"k": "dup"}},
					{ID: "deleted", StartNode: "x", EndNode: "z", Type: "REL", Properties: map[string]any{"k": "deleted"}},
				} {
					require.NoError(t, ns.CreateEdge(edge))
				}
			}
			require.NoError(t, base.FlushWriteBehind())
			require.NoError(t, base.SetNodeManuallySuppressed("a:x", true))
			require.NoError(t, base.SetEdgeManuallySuppressed("a:hidden", true))
			tx, err := base.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("a"))
			_, err = tx.CreateNode(&storage.Node{ID: "a:pending", Labels: []string{"N"}, Properties: map[string]any{"k": "x"}})
			require.NoError(t, err)
			require.NoError(t, tx.DeleteNode("a:removed"))
			require.NoError(t, tx.DeleteEdge("a:deleted"))
			require.NoError(t, tx.CreateEdge(&storage.Edge{ID: "a:pending", StartNode: "a:y", EndNode: "a:z", Type: "REL", Properties: map[string]any{"k": "dup"}}))
			wrapper := &transactionStorageWrapper{tx: tx, underlying: storage.NewNamespacedEngine(base, "a"), namespace: "a", separator: ":"}
			_, err = base.GetNode("a:x")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = wrapper.GetEdge("hidden")
			require.ErrorIs(t, err, storage.ErrNotFound)
			node, err := storage.GetStoredNode(wrapper, "pending")
			require.NoError(t, err)
			require.Equal(t, storage.NodeID("pending"), node.ID)
			node, err = storage.GetStoredNode(wrapper, "x")
			require.NoError(t, err)
			require.Equal(t, storage.NodeID("x"), node.ID)
			_, err = storage.GetStoredNode(wrapper, "removed")
			require.ErrorIs(t, err, storage.ErrNotFound)
			for _, id := range []storage.EdgeID{"hidden", "pending"} {
				edge, err := storage.GetStoredEdge(wrapper, id)
				require.NoError(t, err)
				require.Equal(t, id, edge.ID)
			}
			_, err = storage.GetStoredEdge(wrapper, "deleted")
			require.ErrorIs(t, err, storage.ErrNotFound)
			outgoing, err := storage.GetStoredOutgoingEdges(wrapper, "x")
			require.NoError(t, err)
			require.Len(t, outgoing, 1)
			require.Equal(t, storage.EdgeID("hidden"), outgoing[0].ID)
			incoming, err := storage.GetStoredIncomingEdges(wrapper, "z")
			require.NoError(t, err)
			require.Len(t, incoming, 1)
			require.Equal(t, storage.EdgeID("pending"), incoming[0].ID)
			nodes, err := storage.GetStoredNodesByLabel(wrapper, "N")
			require.NoError(t, err)
			ids := make([]string, 0, len(nodes))
			for _, n := range nodes {
				ids = append(ids, string(n.ID))
			}
			sort.Strings(ids)
			require.Equal(t, []string{"pending", "x", "y", "z"}, ids)
			ids = nil
			require.NoError(t, storage.StreamStoredNodesByLabelProjected(wrapper, "N", []string{"k"}, func(n *storage.Node) error {
				ids = append(ids, string(n.ID))
				return nil
			}))
			sort.Strings(ids)
			require.Equal(t, []string{"pending", "x", "y", "z"}, ids)
			ids = nil
			require.NoError(t, wrapper.StreamNodesByLabelProjected("N", []string{"k"}, func(n *storage.Node) error {
				ids = append(ids, string(n.ID))
				return nil
			}))
			sort.Strings(ids)
			require.Equal(t, []string{"pending", "y", "z"}, ids, "stored label cache must not supply a visible read")
			ids = nil
			require.NoError(t, storage.StreamStoredEdgesByType(context.Background(), wrapper, "REL", func(e *storage.Edge) error {
				ids = append(ids, string(e.ID))
				return nil
			}))
			sort.Strings(ids)
			require.Equal(t, []string{"hidden", "pending"}, ids)
			ids = nil
			require.NoError(t, storage.StreamStoredEdgesByType(context.Background(), wrapper, "", func(e *storage.Edge) error {
				ids = append(ids, string(e.ID))
				return nil
			}))
			sort.Strings(ids)
			require.Equal(t, []string{"hidden", "pending"}, ids)
			for _, entity := range []storage.ConstraintEntityType{storage.ConstraintEntityNode, storage.ConstraintEntityRelationship} {
				label := "N"
				if entity == storage.ConstraintEntityRelationship {
					label = "REL"
				}
				err := storage.ValidateConstraintOnCreationForEngine(wrapper, storage.Constraint{
					Name: "u", Type: storage.ConstraintUnique, EntityType: entity, Label: label, Properties: []string{"k"},
				})
				require.Error(t, err, "hidden committed and pending values both occupy their unique keys")
			}
		})
	}
}

func TestTransactionStoredReaderKeepsSnapshot(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	ns := storage.NewNamespacedEngine(base, "a")
	for _, id := range []storage.NodeID{"x", "y"} {
		_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"k": "before"}})
		require.NoError(t, err)
	}
	require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "hidden", StartNode: "x", EndNode: "y", Type: "REL", Properties: map[string]any{"k": "before"}}))
	require.NoError(t, base.SetNodeManuallySuppressed("a:x", true))
	require.NoError(t, base.SetEdgeManuallySuppressed("a:hidden", true))
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("a"))
	wrapper := &transactionStorageWrapper{tx: tx, underlying: ns, namespace: "a", separator: ":"}
	require.NoError(t, tx.CreateEdge(&storage.Edge{ID: "a:pending", StartNode: "a:y", EndNode: "a:y", Type: "REL"}))
	// A peer changes committed bodies and membership after this snapshot.
	require.NoError(t, ns.UpdateNode(&storage.Node{ID: "x", Labels: []string{"N"}, Properties: map[string]any{"k": "after"}}))
	require.NoError(t, ns.UpdateEdge(&storage.Edge{ID: "hidden", StartNode: "x", EndNode: "y", Type: "REL", Properties: map[string]any{"k": "after"}}))
	_, err = ns.CreateNode(&storage.Node{ID: "late", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "late", StartNode: "y", EndNode: "y", Type: "REL"}))
	node, err := storage.GetStoredNode(wrapper, "x")
	require.NoError(t, err)
	require.Equal(t, "before", node.Properties["k"])
	edge, err := storage.GetStoredEdge(wrapper, "hidden")
	require.NoError(t, err)
	require.Equal(t, "before", edge.Properties["k"])
	nodes, err := storage.GetStoredNodesByLabel(wrapper, "N")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, typ := range []string{"REL", ""} {
		var ids []string
		require.NoError(t, storage.StreamStoredEdgesByType(context.Background(), wrapper, typ, func(edge *storage.Edge) error {
			ids = append(ids, string(edge.ID))
			return nil
		}))
		sort.Strings(ids)
		require.Equal(t, []string{"hidden", "pending"}, ids)
	}
}
