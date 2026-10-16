package resolvers

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Search and similarity read the database the request selects, as every
// other GraphQL query does, not always the default database; and they need
// read on it.
func TestSearchAndSimilarReadTheSelectedDatabase(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}
	nr := &nodeResolver{resolver}

	defaultNode := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "Alice builds graph search systems"},
		ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
	}
	defaultID, err := db.GetStorage().CreateNode(defaultNode)
	require.NoError(t, err)
	_, err = db.GetStorage().CreateNode(&storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "Alice maintains graph pipelines"},
		ChunkEmbeddings: [][]float32{{0.9, 0.1, 0.0}},
	})
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(ctx))

	// The default database finds its own nodes, as before.
	limit := 5
	resp, err := qr.querySearch(ctx, "Alice graph", &models.SearchOptions{Limit: &limit})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Results)
	similar, err := qr.querySimilar(ctx, string(defaultID), &limit, nil)
	require.NoError(t, err)
	require.NotEmpty(t, similar)

	// A request whose only database is another one reads that database.
	require.NoError(t, dbManager.CreateDatabase("other"))
	otherCtx := auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope("", map[string]string{"other": "other"}))
	readable := withResolvedAccess(otherCtx, func(string) auth.ResolvedAccess { return auth.ResolvedAccess{Read: true} })
	resp, err = qr.querySearch(readable, "Alice graph", &models.SearchOptions{Limit: &limit})
	require.NoError(t, err)
	for _, result := range resp.Results {
		require.NotEqual(t, string(defaultID), result.Node.ID, "search read the default database")
	}
	_, err = qr.querySimilar(readable, string(defaultID), &limit, nil)
	require.Error(t, err, "similarity found a node of the default database")
	_, err = nr.nodeSimilar(readable, &models.Node{ID: string(defaultID)}, &limit, nil)
	require.Error(t, err, "similarity found a node of the default database")

	// Without read on the selected database, both are refused.
	unreadable := withResolvedAccess(otherCtx, func(string) auth.ResolvedAccess { return auth.ResolvedAccess{Write: true} })
	_, err = qr.querySearch(unreadable, "Alice graph", &models.SearchOptions{Limit: &limit})
	require.ErrorContains(t, err, "other")
	_, err = qr.querySimilar(unreadable, string(defaultID), &limit, nil)
	require.ErrorContains(t, err, "other")
	_, err = nr.nodeSimilar(unreadable, &models.Node{ID: string(defaultID)}, &limit, nil)
	require.ErrorContains(t, err, "other")
}

// Search and similarity refuse a composite database, as the REST routes do:
// its storage reads every constituent, and read on the composite does not
// cover each constituent.
func TestSearchAndSimilarRefuseCompositeDatabase(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}
	nr := &nodeResolver{resolver}

	require.NoError(t, dbManager.CreateDatabase("open_part"))
	require.NoError(t, dbManager.CreateDatabase("closed_part"))
	require.NoError(t, dbManager.CreateCompositeDatabase("portfolio", []multidb.ConstituentRef{
		{Alias: "open", DatabaseName: "open_part", Type: "local", AccessMode: "read_write"},
		{Alias: "closed", DatabaseName: "closed_part", Type: "local", AccessMode: "read_write"},
	}))
	closed, err := dbManager.GetStorage("closed_part")
	require.NoError(t, err)
	closedID, err := closed.CreateNode(&storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "Closed constituent graph notes"},
		ChunkEmbeddings: [][]float32{{1.0, 0.0, 0.0}},
	})
	require.NoError(t, err)

	// Read on the composite only, none on the closed constituent.
	compositeCtx := auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope("portfolio", map[string]string{"portfolio": "portfolio"}))
	readable := withResolvedAccess(compositeCtx, func(database string) auth.ResolvedAccess {
		return auth.ResolvedAccess{Read: database == "portfolio"}
	})
	limit := 5
	_, err = qr.querySearch(readable, "Closed constituent", &models.SearchOptions{Limit: &limit})
	require.ErrorContains(t, err, "composite")
	_, err = qr.querySimilar(readable, string(closedID), &limit, nil)
	require.ErrorContains(t, err, "composite")
	_, err = nr.nodeSimilar(readable, &models.Node{ID: string(closedID)}, &limit, nil)
	require.ErrorContains(t, err, "composite")
}
