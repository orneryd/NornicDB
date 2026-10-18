package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGraphQLStatsRequireSelectedDatabaseRead(t *testing.T) {
	db := testDB(t)
	dbManager := testDBManager(t, db)
	query := &queryResolver{NewResolver(db, dbManager)}

	_, err := db.GetStorage().CreateNode(&storage.Node{ID: "default-node", Labels: []string{"DefaultDoc"}})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateDatabase("named"))
	named, err := dbManager.GetStorage("named")
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"named-a", "named-b"} {
		_, err = named.CreateNode(&storage.Node{ID: id, Labels: []string{"NamedDoc"}})
		require.NoError(t, err)
	}
	require.NoError(t, named.CreateEdge(&storage.Edge{ID: "named-edge", StartNode: "named-a", EndNode: "named-b", Type: "LINKS"}))

	tests := []struct {
		name          string
		database      string
		read          bool
		wantNodes     int
		wantRelations int
	}{
		{name: "default allowed", database: "nornic", read: true, wantNodes: 1},
		{name: "default denied", database: "nornic", read: false},
		{name: "named allowed", database: "named", read: true, wantNodes: 2, wantRelations: 1},
		{name: "named denied", database: "named", read: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := auth.WithRequestDatabaseScope(context.Background(), auth.NewRequestDatabaseScope(tc.database, map[string]string{tc.database: tc.database}))
			ctx = withResolvedAccess(ctx, func(database string) auth.ResolvedAccess {
				return auth.ResolvedAccess{Read: database == tc.database && tc.read, Write: true}
			})

			nodes, nodeErr := query.queryNodeCount(ctx, nil)
			relationships, relationshipErr := query.queryRelationshipCount(ctx, nil)
			stats, statsErr := query.queryStats(ctx)
			if !tc.read {
				require.ErrorContains(t, nodeErr, tc.database)
				require.ErrorContains(t, relationshipErr, tc.database)
				require.ErrorContains(t, statsErr, tc.database)
				require.Nil(t, stats)
				return
			}
			require.NoError(t, nodeErr)
			require.NoError(t, relationshipErr)
			require.NoError(t, statsErr)
			require.Equal(t, tc.wantNodes, nodes)
			require.Equal(t, tc.wantRelations, relationships)
			require.Equal(t, tc.wantNodes, stats.NodeCount)
			require.Equal(t, tc.wantRelations, stats.RelationshipCount)
		})
	}

	// Direct use with no authentication context preserves the default database.
	nodes, err := query.queryNodeCount(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, nodes)
}
