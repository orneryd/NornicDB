package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// A principal restricted by collection rules (#935) runs GraphQL Cypher
// through its graph view, and search and similarity, which read the index
// and storage directly, refuse it.
func TestGraphQLCollectionRestrictedPrincipal(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	dbManager := testDBManager(t, db)
	resolver := NewResolver(db, dbManager)
	qr := &queryResolver{resolver}

	system, err := dbManager.GetStorage("system")
	require.NoError(t, err)
	require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "agent"))
	for _, statement := range []string{
		"CREATE ACCESS SCOPE collections ON GRAPH nornic MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id",
		"GRANT MATCH {*} ON GRAPH nornic FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent",
	} {
		_, err := cypher.NewStorageExecutor(system).Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	data, err := dbManager.GetStorage("nornic")
	require.NoError(t, err)
	_, err = cypher.NewStorageExecutor(data).Execute(ctx, `CREATE (a:Collection {id: 'A'}), (b:Collection {id: 'B'}),
		(:Document {id: 'd1', content: 'household budget'})-[:MEMBER_OF]->(a),
		(:Document {id: 'd2', content: 'household medical'})-[:MEMBER_OF]->(b)`, nil)
	require.NoError(t, err)

	readable := withResolvedAccess(ctx, func(string) auth.ResolvedAccess { return auth.ResolvedAccess{Read: true} })
	restricted := cypher.WithRequestPrincipalAccess(readable, cypher.PrincipalAccess{
		Visibility: func(database string) (*cypher.GraphVisibility, error) {
			return cypher.ResolveGraphVisibility(ctx, system, []string{"agent"}, database)
		},
	})

	result, err := qr.queryCypher(restricted, models.CypherInput{Statement: "MATCH (n:Document) RETURN n.id AS id"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"d1"}}, result.Rows)

	limit := 5
	_, err = qr.querySearch(restricted, "household", &models.SearchOptions{Limit: &limit})
	var localized *localization.LocalizedError
	require.True(t, errors.As(err, &localized), "want a Forbidden error, got %v", err)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", localized.Code)
}
