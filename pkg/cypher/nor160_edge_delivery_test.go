package cypher

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNOR160TransactionEdgeNamespaceDeliveryIsIndependent(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	ctx := t.Context()
	ns := storage.NewNamespacedEngine(base, "delivery")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := ns.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	require.NoError(t, ns.CreateEdge(ctx, &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "COHELD", Properties: map[string]any{"v": []int64{7}}}))
	tx, err := base.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("delivery"))
	w := &transactionStorageWrapper{tx: tx, underlying: ns, namespace: "delivery", separator: ":"}
	for _, pending := range []bool{false, true} {
		if pending {
			edge, err := tx.GetEdge(ctx, "delivery:e")
			require.NoError(t, err)
			before := storage.CopyEdge(edge)
			edge.Properties["v"] = []int64{8}
			require.NoError(t, tx.UpdateEdge(ctx, storage.EdgeUpdateRequest{Before: before, After: edge}))
		}
		want := int64(7)
		if pending {
			want = 8
		}
		for _, route := range []string{"point", "adjacency", "incoming", "between", "between-list", "stored", "stored-outgoing", "stored-incoming", "type", "all", "matched", "outgoing-header", "incoming-header"} {
			var got *storage.Edge
			switch route {
			case "point":
				got, err = w.GetEdge(ctx, "e")
			case "adjacency":
				var edges []*storage.Edge
				edges, err = w.GetOutgoingEdges(ctx, "a")
				require.NoError(t, err)
				require.Len(t, edges, 1)
				got = edges[0]
			case "incoming", "between-list", "stored-outgoing", "stored-incoming", "type", "all", "matched", "outgoing-header", "incoming-header":
				var edges []*storage.Edge
				switch route {
				case "incoming":
					edges, err = w.GetIncomingEdges(ctx, "b")
				case "between-list":
					edges, err = w.GetEdgesBetween(ctx, "a", "b")
				case "stored-outgoing":
					edges, err = w.GetStoredOutgoingEdges(ctx, "a")
				case "stored-incoming":
					edges, err = w.GetStoredIncomingEdges(ctx, "b")
				case "type":
					edges, err = w.GetEdgesByType(ctx, "COHELD")
				case "all":
					edges, err = w.AllEdges(ctx)
				case "matched":
					edges, err = w.MatchEdgesBetween(ctx, "a", "b", "COHELD", []string{"v"}, func(*storage.Edge) bool { return true })
				case "outgoing-header":
					var answered bool
					edges, answered, err = w.OutgoingEdgeHeaders(ctx, "a")
					require.True(t, answered)
				case "incoming-header":
					var answered bool
					edges, answered, err = w.IncomingEdgeHeaders(ctx, "b")
					require.True(t, answered)
				}
				require.NoError(t, err)
				require.Len(t, edges, 1)
				got = edges[0]
			case "between":
				got, err = w.GetEdgeBetween(ctx, "a", "b", "COHELD")
			case "stored":
				got, err = w.GetStoredEdge(ctx, "e")
			}
			require.NoError(t, err, route)
			require.Equal(t, storage.EdgeID("e"), got.ID)
			require.Equal(t, storage.NodeID("a"), got.StartNode)
			require.Equal(t, storage.NodeID("b"), got.EndNode)
			if got.Properties != nil {
				require.Equal(t, want, got.Properties["v"].([]int64)[0])
				got.Properties["v"].([]int64)[0] = 99
			} else {
				require.Contains(t, route, "header")
				got.Properties = map[string]any{"v": []int64{99}}
			}
			retained, err := tx.GetEdge(ctx, "delivery:e")
			require.NoError(t, err)
			require.Equal(t, storage.EdgeID("delivery:e"), retained.ID)
			require.Equal(t, want, retained.Properties["v"].([]int64)[0], route)
		}
	}
}
