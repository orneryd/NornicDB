package server

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// collectionAccessServer is a server whose default database holds
// collections A and B, d1 in A and d2 in B, with an access scope and a login
// "agent" granted collection A; it returns the admin's and the agent's tokens.
func collectionAccessServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	ctx := context.Background()
	server, users := setupTestServer(t)
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleStore = auth.NewRoleStore(system)
	server.allowlistStore = auth.NewAllowlistStore(system)
	server.privilegesStore = auth.NewPrivilegesStore(system)
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
	database := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.roleStore.CreateRole(ctx, "agent"))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "agent", []string{database}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "agent", database, true, true))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "agent", []string{"read", "write"}))
	_, err = users.CreateUser("agent1", "password123", []auth.Role{"agent"})
	require.NoError(t, err)
	admin := getAuthToken(t, users, "admin")

	for _, statement := range []string{
		"CREATE ACCESS SCOPE collections ON GRAPH " + database + " MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id DERIVE (x:Collection) FROM COLLECTION x.id",
		"GRANT MATCH {*} ON GRAPH " + database + " FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent",
	} {
		require.Empty(t, txErrorCode(t, server, "system", admin, statement), statement)
	}
	engine := getDefaultStorage(t, server)
	for _, node := range []*storage.Node{
		{ID: "A", Labels: []string{"Collection"}, Properties: map[string]interface{}{"id": "A"}},
		{ID: "B", Labels: []string{"Collection"}, Properties: map[string]interface{}{"id": "B"}},
		{ID: "d1", Labels: []string{"Document"}, Properties: map[string]interface{}{"id": "d1", "content": "household budget"}},
		{ID: "d2", Labels: []string{"Document"}, Properties: map[string]interface{}{"id": "d2", "content": "household medical"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "m1", StartNode: "d1", EndNode: "A", Type: "MEMBER_OF"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "m2", StartNode: "d2", EndNode: "B", Type: "MEMBER_OF"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "x", StartNode: "d1", EndNode: "d2", Type: "CITES"}))
	return server, admin, getAuthToken(t, users, "agent1")
}

func cypherIDs(t *testing.T, server *Server, token, statement string) []string {
	t.Helper()
	body, raw := systemProcedureResponse(t, server, token, server.dbManager.DefaultDatabaseName(), statement, false)
	require.Empty(t, body.Errors, raw)
	var ids []string
	for _, row := range body.Results[0].Data {
		ids = append(ids, row.Row[0].(string))
	}
	sort.Strings(ids)
	return ids
}

func graphNodeIDs(payload graphPayload) []string {
	var ids []string
	for _, node := range payload.Nodes {
		ids = append(ids, node.ID)
	}
	sort.Strings(ids)
	return ids
}

// A login restricted to collection A sees A's documents and nothing else on
// every route a restricted agent uses: HTTP Cypher, and the graph history routes
// (judged by current membership); an unrestricted login sees everything.
func TestCollectionAccessRoutes(t *testing.T) {
	server, admin, agent := collectionAccessServer(t)

	require.Equal(t, []string{"d1"}, cypherIDs(t, server, agent, "MATCH (n:Document) RETURN n.id"))
	require.Equal(t, []string{"A"}, cypherIDs(t, server, agent, "MATCH (n:Collection) RETURN n.id"))
	require.Empty(t, cypherIDs(t, server, agent, "MATCH (:Document)-[:CITES]->(n) RETURN n.id"))
	require.Equal(t, []string{"d1", "d2"}, cypherIDs(t, server, admin, "MATCH (n:Document) RETURN n.id"))

	engine := getDefaultStorage(t, server)
	asOfVersion := edgeHeadVersionOrNow(t, engine, "x")
	asOf := asOfVersion.CommitTimestamp.UTC().Format(time.RFC3339Nano)
	history := func(operation, token string) graphPayload {
		resp := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, operation), map[string]interface{}{
			"node_ids": []string{"d1", "d2", "A", "B"},
			"as_of":    asOf,
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
		return decodeGraphPayload(t, resp.Body)
	}
	edgeIDs := func(payload graphPayload) []string {
		var ids []string
		for _, edge := range payload.Edges {
			ids = append(ids, edge.ID)
		}
		sort.Strings(ids)
		return ids
	}

	// The snapshot: the admin sees all of it, the agent A and d1.
	require.Equal(t, []string{"A", "B", "d1", "d2"}, graphNodeIDs(history("temporal", admin)))
	snapshot := history("temporal", agent)
	require.Equal(t, []string{"A", "d1"}, graphNodeIDs(snapshot))
	require.NotContains(t, edgeIDs(snapshot), "x")

	// The diff lists what changed since then: both documents, and x removed;
	// the agent sees d1's change only.
	advanceMVCCClockPast(t, engine, asOfVersion.CommitTimestamp)
	for _, node := range []*storage.Node{
		{ID: "d1", Labels: []string{"Document"}, Properties: map[string]interface{}{"id": "d1", "content": "household budget 2026"}},
		{ID: "d2", Labels: []string{"Document"}, Properties: map[string]interface{}{"id": "d2", "content": "household medical 2026"}},
	} {
		require.NoError(t, engine.UpdateNode(node))
	}
	require.NoError(t, engine.DeleteEdge("x"))
	full := history("diff", admin)
	require.Equal(t, []string{"d1", "d2"}, graphNodeIDs(full))
	require.Contains(t, edgeIDs(full), "x")
	diff := history("diff", agent)
	require.Equal(t, []string{"d1"}, graphNodeIDs(diff))
	require.NotContains(t, edgeIDs(diff), "x")

	resp := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"d1"},
		"depth":    2,
	}, "Bearer "+agent)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	require.Equal(t, []string{"A", "d1"}, graphNodeIDs(decodeGraphPayload(t, resp.Body)))

	// The routes that search or read storage directly refuse the agent.
	for _, path := range []string{"/nornicdb/search", "/nornicdb/similar"} {
		resp := makeRequest(t, server, http.MethodPost, path, map[string]interface{}{"query": "household", "node_id": "d1"}, "Bearer "+agent)
		require.Equal(t, http.StatusForbidden, resp.Code, path)
	}
	// So do the direct operations (admitHTTPOperation): they read and change
	// storage around the view, a GDPR export of the agent's own data too.
	resp = makeRequest(t, server, http.MethodPost, "/gdpr/export", map[string]interface{}{"user_id": "agent1"}, "Bearer "+agent)
	require.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
	for _, path := range []string{"/status", "/nornicdb/embed/stats"} {
		resp := makeRequest(t, server, http.MethodGet, path, nil, "Bearer "+agent)
		require.Equal(t, http.StatusForbidden, resp.Code, path)
	}
	resp = makeRequest(t, server, http.MethodGet, "/status", nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
}

// Heimdall serves a restricted principal through its view: Query reads only
// what it can see (admission carries its visibility), and Discover and
// Stats, which read storage and search directly, refuse it.
func TestCollectionAccessHeimdall(t *testing.T) {
	server, _, _ := collectionAccessServer(t)
	database := server.dbManager.DefaultDatabaseName()
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	access := server.principalAccess(&auth.JWTClaims{Sub: "agent1", Username: "agent1", Roles: []string{"agent"}})
	ctx := cypher.WithRequestPrincipalAccess(
		heimdallPrincipal([]string{database}, heimdallPermissions(map[auth.Permission]bool{"access": true, auth.PermRead: true})),
		access)

	rows, err := router.Query(ctx, database, "MATCH (n:Document) RETURN n.id AS id", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "d1", rows[0]["id"])

	_, err = router.Discover(ctx, database, "household", nil, 5, 0)
	requireHeimdallForbidden(t, err)
	_, err = router.Stats(ctx, database)
	requireHeimdallForbidden(t, err)
}

// The role owner reads a user's roles from the authenticator; a user that
// doesn't exist is auth.ErrUserNotFound.
func TestRoleAdministrationUserRoles(t *testing.T) {
	server, _, _ := collectionAccessServer(t)
	roles := server.RoleAdministration()
	held, err := roles.UserRoles("agent1")
	require.NoError(t, err)
	require.Equal(t, []string{"agent"}, held)
	_, err = roles.UserRoles("nobody")
	require.ErrorIs(t, err, auth.ErrUserNotFound)
}

// SHOW CURRENT USER YIELD collectionLimited lists only databases the login
// may use and where its view is restricted: a database outside its allowlist
// is never named, and one it may use without collection rules isn't limited.
func TestCollectionLimitedListsOnlyAccessibleRestrictedDatabases(t *testing.T) {
	server, _, agent := collectionAccessServer(t)
	ctx := context.Background()
	database := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase("open"))
	require.NoError(t, server.dbManager.CreateDatabase("other"))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "agent", []string{database, "open"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "agent", "open", true, true))

	body, raw := systemProcedureResponse(t, server, agent, database, "SHOW CURRENT USER YIELD collectionLimited RETURN collectionLimited", false)
	require.Empty(t, body.Errors, raw)
	require.Len(t, body.Results[0].Data, 1, raw)
	require.Equal(t, []interface{}{database}, body.Results[0].Data[0].Row[0], raw)
	require.NotContains(t, raw, "other")
}
