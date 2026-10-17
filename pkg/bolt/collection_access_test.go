package bolt

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A Bolt RUN from a login restricted by collection rules reads through its
// graph visibility (the server's resolver, SetGraphVisibilityResolver): it
// sees the documents of the collections it is granted, and a login with no
// collection rule sees them all.
func TestBoltRunReadsThroughCollectionVisibility(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	system := storage.NewNamespacedEngine(inner, "system")
	require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "agent"))
	admin := cypher.NewStorageExecutor(system)
	for _, statement := range []string{
		"CREATE ACCESS SCOPE collections ON GRAPH nornic MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id",
		"GRANT MATCH {*} ON GRAPH nornic FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent",
	} {
		_, err := admin.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	data := cypher.NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	_, err := data.Execute(ctx, `CREATE (a:Collection {id: 'A'}), (b:Collection {id: 'B'}),
		(:Document {id: 'd1'})-[:MEMBER_OF]->(a), (:Document {id: 'd2'})-[:MEMBER_OF]->(b),
		(:Document {id: 'd3'})-[:MEMBER_OF]->(a), (d3b:Document {id: 'd4'})`, nil)
	require.NoError(t, err)

	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	server.SetGraphVisibilityResolver(func(roles []string, database string) (*cypher.GraphVisibility, error) {
		return cypher.ResolveGraphVisibility(ctx, system, roles, database)
	})
	readAs := func(roles []string) []string {
		var ids []string
		result := &BoltAuthResult{Authenticated: true, Username: "u", Roles: roles,
			Permissions: []string{string(auth.PermRead), string(auth.PermWrite)}}
		executed, code := runOnBoltSession(t, server, result, "MATCH (n:Document) RETURN n.id AS id", func(ctx context.Context, query string, params map[string]any) error {
			rows, err := data.Execute(ctx, query, params)
			if err != nil {
				return err
			}
			for _, row := range rows.Rows {
				ids = append(ids, row[0].(string))
			}
			return nil
		})
		require.True(t, executed)
		require.Empty(t, code)
		sort.Strings(ids)
		return ids
	}
	require.Equal(t, []string{"d1", "d3"}, readAs([]string{"agent"}))
	require.Equal(t, []string{"d1", "d2", "d3", "d4"}, readAs([]string{"editor"}))
}

// Under Bolt impersonation the impersonated user's collection rules decide
// what a statement sees, never the signed-in account's: the service account
// sees every document, impersonating a user granted collection A it sees
// A's only, and its next RUN without imp_user sees every document again.
func TestBoltImpersonationReadsThroughTargetCollectionVisibility(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateAllUsers, false)
	ctx := context.Background()

	privilegesBase := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = privilegesBase.Close() })
	privileges := storage.NewNamespacedEngine(privilegesBase, "system")
	require.NoError(t, auth.NewRoleStore(privileges).CreateRole(ctx, "reader"))
	system := cypher.NewStorageExecutor(privileges)
	for _, statement := range []string{
		"CREATE ACCESS SCOPE collections ON GRAPH nornic MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id",
		"GRANT MATCH {*} ON GRAPH nornic FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO reader",
	} {
		_, err := system.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	f.server.SetGraphVisibilityResolver(func(roles []string, database string) (*cypher.GraphVisibility, error) {
		return cypher.ResolveGraphVisibility(ctx, privileges, roles, database)
	})
	_, err := f.executor.Execute(ctx, `CREATE (a:Collection {id: 'A'}), (b:Collection {id: 'B'}),
		(:Document {id: 'd1'})-[:MEMBER_OF]->(a), (:Document {id: 'd2'})-[:MEMBER_OF]->(b)`, nil)
	require.NoError(t, err)

	documents := func(extra map[string]any) []any {
		rows, code := f.run("MATCH (n:Document) RETURN n.id AS id ORDER BY id", extra)
		require.Empty(t, code)
		var ids []any
		for _, row := range rows {
			ids = append(ids, row[0])
		}
		return ids
	}
	require.Equal(t, []any{"d1", "d2"}, documents(nil))
	require.Equal(t, []any{"d1"}, documents(imp("alice936")))
	require.Equal(t, []any{"d1", "d2"}, documents(nil))
}

// stubRoleOwner is a RoleAdministration a session can be handed; its methods
// are never called here.
type stubRoleOwner struct {
	cypher.RoleAdministration
	name string
}

// A session's identity carries the server's role owner
// (SetRoleAdministration), which the Cypher role commands run through; a
// server without one leaves the commands unsupported.
func TestBoltSessionIdentityCarriesRoleOwner(t *testing.T) {
	session := newTestSession(&mockConn{}, nil)
	require.Nil(t, session.signedInIdentity().Roles)

	owner := stubRoleOwner{name: "server roles"}
	session = newTestSession(&mockConn{}, nil)
	session.server.SetRoleAdministration(func() cypher.RoleAdministration { return owner })
	require.Equal(t, owner, session.signedInIdentity().Roles)
}
