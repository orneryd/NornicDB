package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/stretchr/testify/require"
)

// Labels, relationship types and property keys GraphQL splices into Cypher
// are names, quoted so they read back literally: a type named A|B is that one
// type, never the alternation of A and B, and a key with a space is one key.
func TestResolverNamesAreLiteral(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	resolver := NewResolver(db, testutil.DatabaseManager(t, db))
	qr := &queryResolver{resolver}

	start, err := resolver.createNodeViaCypher(ctx, []string{"Lit|Person", "Plain"}, map[string]interface{}{"odd key": "x"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Lit|Person", "Plain"}, start.Labels)
	require.Equal(t, "x", start.Properties["odd key"])
	end, err := resolver.createNodeViaCypher(ctx, []string{"Plain"}, nil)
	require.NoError(t, err)
	for _, relType := range []string{"A|B", "A", "B", "A:B"} {
		edge, err := resolver.createEdgeViaCypher(ctx, start.ID, end.ID, relType, map[string]interface{}{"odd key": relType})
		require.NoError(t, err, relType)
		require.Equal(t, relType, edge.Type)
	}

	for _, relType := range []string{"A|B", "A:B", "A"} {
		edges, err := resolver.listEdgesViaCypher(ctx, relType, 10, 0)
		require.NoError(t, err, relType)
		require.Len(t, edges, 1, relType)
		require.Equal(t, relType, edges[0].Type)
	}
	nodes, err := resolver.listNodesViaCypher(ctx, "Lit|Person", 10, 0)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	updated, err := resolver.updateNodeViaCypher(ctx, start.ID, map[string]interface{}{"other key": "y"})
	require.NoError(t, err)
	require.Equal(t, "y", updated.Properties["other key"])

	stats, err := qr.queryStats(ctx)
	require.NoError(t, err)
	types := map[string]int{}
	for _, typeStats := range stats.RelationshipTypes {
		types[typeStats.Type] = typeStats.Count
	}
	require.Equal(t, map[string]int{"A|B": 1, "A": 1, "B": 1, "A:B": 1}, types)
	labels := map[string]int{}
	for _, labelStats := range stats.Labels {
		labels[labelStats.Label] = labelStats.Count
	}
	require.Equal(t, 1, labels["Lit|Person"])
	require.Equal(t, 2, labels["Plain"])
}
