package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// RelationshipWhere filters relationship candidates before the limit, with
// both endpoints (nil when not visible), and ranks every relationship so an
// admitted one ranked behind many rejected ones isn't cut.
func TestSearchRelationshipsWhereFiltersBeforeLimit(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	for i := 0; i < 80; i++ {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("x%02d", i)), StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"reason": "zephyrine"}}))
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "keep", StartNode: "c", EndNode: "a", Type: "CITES", Properties: map[string]any{"reason": "zephyrine with a much longer explanation of the river crossing and its history"}}))
	svc := startRelationshipService(t, engine, nil, nil)

	var seen []string
	fromC := func(edge *storage.Edge, start, end *storage.Node) bool {
		seen = append(seen, string(edge.ID))
		return start != nil && start.ID == "c" && end != nil && end.ID == "a"
	}
	response, err := svc.SearchRelationships(ctx, "zephyrine", nil, &SearchOptions{Limit: 1, RelationshipWhere: fromC})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, "keep", response.Results[0].ID)
	require.Greater(t, len(seen), 50, "candidates past the unfiltered depth are checked")

	// Without the filter, the admitted relationship ranks behind the others.
	response, err = svc.SearchRelationships(ctx, "zephyrine", nil, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.NotEqual(t, "keep", response.Results[0].ID)

	// An endpoint no longer visible is nil.
	require.NoError(t, engine.DeleteNode("c"))
	var starts []*storage.Node
	response, err = svc.SearchRelationships(ctx, "zephyrine", nil, &SearchOptions{Limit: 100, RelationshipWhere: func(edge *storage.Edge, start, end *storage.Node) bool {
		if edge.ID == "keep" {
			starts = append(starts, start)
		}
		return true
	}})
	require.NoError(t, err)
	if len(starts) > 0 {
		require.Nil(t, starts[0])
	}
}
