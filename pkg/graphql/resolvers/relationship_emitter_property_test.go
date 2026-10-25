package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/internal/testutil/emittercheck"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/stretchr/testify/require"
)

func TestRelationshipEmitterProperty(t *testing.T) {
	db := testDB(t)
	r := NewResolver(db, testutil.DatabaseManager(t, db))
	qr := &queryResolver{r}
	mr := &mutationResolver{r}
	ctx := context.Background()
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			props, params := buildPropertiesString(map[string]any{name: int64(1)})
			emittercheck.Check(t, createEdgeStatement(name, props), name, params, map[string]any{name: int64(1)})
			a, err := r.createNodeViaCypher(ctx, []string{name}, map[string]any{name: int64(1)})
			require.NoError(t, err)
			require.EqualValues(t, 1, a.Properties[name])
			b, err := r.createNodeViaCypher(ctx, []string{name}, nil)
			require.NoError(t, err)
			updated, err := r.updateNodeViaCypher(ctx, a.ID, map[string]any{name: int64(7)})
			require.NoError(t, err)
			require.EqualValues(t, 7, updated.Properties[name])
			nodes, err := r.listNodesViaCypher(ctx, name, 10, 0)
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			for _, labels := range [][]string{nil, {name}} {
				// This API currently types its value as a JSON map, while
				// stored properties are scalars/lists. Null is a supported
				// no-match input and exercises both real query constructions.
				found, err := qr.querySearchByProperty(ctx, name, nil, labels, nil)
				require.NoError(t, err)
				require.Empty(t, found)
			}
			// The merge predicate and label writer run even when the direct
			// result adapter elects to create instead of updating a match.
			merged, err := mr.mutationMergeNode(ctx, []string{name}, models.JSON(map[string]any{name: int64(7)}), models.JSON(map[string]any{"merged": true}))
			require.NoError(t, err)
			require.NotEmpty(t, merged.ID)
			require.EqualValues(t, 7, merged.Properties[name])
			edge, err := r.createEdgeViaCypher(ctx, a.ID, b.ID, name, map[string]any{name: 1})
			require.NoError(t, err)
			require.Equal(t, name, edge.Type)
			require.EqualValues(t, 1, edge.Properties[name])
			edges, err := r.listEdgesViaCypher(ctx, name, 10, 0)
			require.NoError(t, err)
			require.Len(t, edges, 1)
			require.Equal(t, name, edges[0].Type)
			// Query paths emits each literal type in an alternation, including
			// a name which itself contains a pipe.
			for _, shortest := range []bool{false, true} {
				paths, err := qr.queryPaths(ctx, a.ID, b.ID, 1, 2, []string{name}, shortest)
				require.NoError(t, err)
				require.Len(t, paths, 1)
				require.Equal(t, b.ID, paths[0][1].ID)
			}
		})
	}
	stats, err := qr.queryStats(ctx)
	require.NoError(t, err)
	require.Len(t, stats.RelationshipTypes, len(cyphernames.Names()))
	for _, item := range stats.RelationshipTypes {
		require.Equal(t, 1, item.Count, item.Type)
	}
}
