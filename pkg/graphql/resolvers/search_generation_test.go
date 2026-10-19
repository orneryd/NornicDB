package resolvers

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGraphQLExecutorFollowsSearchResetAndSharesCursors(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	resolver := NewResolver(db, manager)
	name := manager.DefaultDatabaseName()
	store, err := manager.GetStorage(name)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("graphql-generation-%d", i)),
			Properties: map[string]interface{}{"content": "shared cursor"},
		})
		require.NoError(t, err)
	}
	ctx := cypher.WithAuthenticatedPrincipal(context.Background(), "sub:graphql-generation")
	firstExecutor, err := resolver.getCypherExecutor(ctx, name)
	require.NoError(t, err)
	secondExecutor, err := resolver.getCypherExecutor(ctx, name)
	require.NoError(t, err)
	start := func() string {
		result, err := firstExecutor.Execute(ctx, "CALL db.retrieve({mode: 'id', n: 1})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		qid := result.Rows[0][0].(map[string]interface{})["qid"].(string)
		require.NotEmpty(t, qid)
		return qid
	}
	qid := start()
	pull := map[string]interface{}{"request": map[string]interface{}{"qid": qid, "n": int64(1)}}
	_, err = secondExecutor.Execute(ctx, "CALL db.retrieve($request)", pull)
	require.NoError(t, err, "separate GraphQL requests must share the database cursor owner")
	db.ResetSearchService(name)
	_, err = secondExecutor.Execute(ctx, "CALL db.retrieve($request)", pull)
	require.ErrorIs(t, err, resultstream.ErrGoneQID)
	require.NotEqual(t, qid, start(), "an existing executor must use the replacement service generation")
}
