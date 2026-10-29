package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionBulkCreateEdgesPreservesTokenOrder(t *testing.T) {
	for _, minted := range []bool{false, true} {
		for _, distinct := range []int{1, 12} {
			t.Run(fmt.Sprintf("minted=%t/types=%d", minted, distinct), func(t *testing.T) {
				base := newTestMemoryEngine(t)
				ns := storage.NewNamespacedEngine(base, "tenant")
				for _, id := range []storage.NodeID{"from", "to"} {
					_, err := ns.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, base.EnsureNamespaceMVCC("tenant"))
				tx, err := base.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("tenant"))
				w := &transactionStorageWrapper{tx: tx, underlying: ns, namespace: "tenant", separator: ":"}
				create := w.BulkCreateEdges
				if minted {
					create = w.BulkCreateMintedEdges
				}
				require.ErrorIs(t, create([]*storage.Edge{{Type: "invalid-batch"}, nil}), storage.ErrInvalidData)
				require.Empty(t, w.GetSchema().ExportDefinition().RelationshipTypeTokens)
				var edges []*storage.Edge
				var want []string
				for i := 0; i < distinct; i++ {
					kind := fmt.Sprintf("type-%02d", i)
					want = append(want, kind)
					for j := 0; j < 2; j++ {
						edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e-%d-%d", i, j)), StartNode: "from", EndNode: "to", Type: kind})
					}
				}
				require.NoError(t, create(edges))
				require.Equal(t, want, w.GetSchema().ExportDefinition().RelationshipTypeTokens)
				require.NoError(t, tx.Commit())
				for _, edge := range edges {
					got, err := ns.GetEdge(edge.ID)
					require.NoError(t, err)
					require.Equal(t, edge.ID, got.ID)
					require.Equal(t, edge.StartNode, got.StartNode)
					require.Equal(t, edge.EndNode, got.EndNode)
					require.Equal(t, edge.Type, got.Type)
				}
			})
		}
	}
}
