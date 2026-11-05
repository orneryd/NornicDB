package storage

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestNamespacedBulkCreateEdgesPreservesTokenOrder(t *testing.T) {
	for _, distinct := range []int{1, 12} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			base := fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed)
			t.Cleanup(func() { _ = base.Close() })
			ns := NewNamespacedEngine(base, "tenant")
			for _, id := range []NodeID{"from", "to"} {
				_, err := ns.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			schema := ns.GetSchema()
			require.ErrorIs(t, ns.BulkCreateEdges(t.Context(), []*Edge{{Type: "invalid-batch"}, nil}), ErrInvalidData)
			require.Empty(t, schemaDefinitionForTest(t, schema).RelationshipTypeTokens)
			var edges []*Edge
			var want []string
			for i := 0; i < distinct; i++ {
				kind := fmt.Sprintf("type-%02d", i)
				want = append(want, kind)
				for j := 0; j < 2; j++ {
					edges = append(edges, &Edge{ID: EdgeID(fmt.Sprintf("e-%d-%d", i, j)), StartNode: "from", EndNode: "to", Type: kind})
				}
			}
			require.NoError(t, ns.BulkCreateEdges(t.Context(), edges))
			require.Equal(t, want, schemaDefinitionForTest(t, schema).RelationshipTypeTokens)
			for _, edge := range edges {
				got, err := ns.GetEdge(t.Context(), edge.ID)
				require.NoError(t, err)
				require.Equal(t, edge.ID, got.ID)
				require.Equal(t, edge.StartNode, got.StartNode)
				require.Equal(t, edge.EndNode, got.EndNode)
				require.Equal(t, edge.Type, got.Type)
			}
		})
	}
}
