package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fineGrainedGraphPrincipal builds the admitted principal a graph request runs
// with: one compiled policy for the role, bound to the database identities the
// server resolves, exactly as admission binds them for an authenticated
// request. The graph handlers read through the view such a principal carries.
func fineGrainedGraphPrincipal(t *testing.T, server *Server, role string, entitlements ...auth.FineGrainedEntitlement) *auth.FineGrainedRequest {
	t.Helper()
	definitions := make([]auth.FineGrainedEntitlement, 0, len(entitlements))
	ids := make([]string, 0, len(entitlements))
	for index, entitlement := range entitlements {
		entitlement.ID = fmt.Sprintf("grant-%d", index)
		definitions = append(definitions, entitlement)
		ids = append(ids, entitlement.ID)
	}
	policy, err := auth.CompileFineGrainedPolicy(fineGrainedCatalogActions(), auth.FineGrainedCatalog{
		Definitions: definitions,
		Assignments: map[string][]string{role: ids},
	})
	require.NoError(t, err)
	databaseID := func(name string) string {
		resolved, resolveErr := server.dbManager.ResolveDatabase(name)
		if resolveErr != nil {
			return ""
		}
		identity, _ := server.dbManager.DatabaseIdentity(resolved)
		return identity
	}
	return &auth.FineGrainedRequest{
		Access:         policy.ForRoles([]string{role}),
		DatabaseID:     databaseID,
		HomeDatabaseID: databaseID(server.dbManager.DefaultDatabaseName()),
	}
}

func graphDatabaseGrant(action, databaseID string) auth.FineGrainedEntitlement {
	return auth.FineGrainedEntitlement{
		Action: action,
		Scope:  auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: databaseID},
	}
}

func graphElementGrant(action string, kind auth.FineGrainedResourceKind, databaseID string, types, properties []string) auth.FineGrainedEntitlement {
	return auth.FineGrainedEntitlement{
		Action: action,
		Scope:  auth.FineGrainedScope{Kind: kind, DatabaseID: databaseID, Types: types, Properties: properties},
	}
}

// fineGrainedGraphRequest is a graph request carrying the admitted principal
// the HTTP middleware binds to the request context after admission.
func fineGrainedGraphRequest(database string, principal *auth.FineGrainedRequest, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/nornicdb/graph/"+database+"/graph", strings.NewReader(body))
	request.SetPathValue("database", database)
	claims := &auth.JWTClaims{Username: "graph_reader", Roles: []string{"graph_reader"}}
	ctx := context.WithValue(request.Context(), contextKeyClaims, claims)
	return request.WithContext(auth.WithFineGrainedRequest(ctx, principal))
}

// seedFineGrainedGraph stores two traversable articles joined by a granted
// relationship type and a secret reachable only through relationships the
// grants do not include.
func seedFineGrainedGraph(t *testing.T, engine storage.Engine) {
	t.Helper()
	for _, node := range []*storage.Node{
		{ID: "fg-article", Labels: []string{"Article"}, Properties: map[string]interface{}{"title": "Example", "draft": "hidden draft"}},
		{ID: "fg-linked", Labels: []string{"Article"}, Properties: map[string]interface{}{"title": "Linked"}},
		{ID: "fg-secret", Labels: []string{"Secret"}, Properties: map[string]interface{}{"title": "Sensitive title"}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	edges := []*storage.Edge{
		{ID: "fg-link", StartNode: "fg-article", EndNode: "fg-linked", Type: "LINK", Properties: map[string]interface{}{"weight": 2}},
		{ID: "fg-linked-secret", StartNode: "fg-linked", EndNode: "fg-secret", Type: "LINK"},
		{ID: "fg-article-secret", StartNode: "fg-article", EndNode: "fg-secret", Type: "CLASSIFIED"},
	}
	for _, edge := range edges {
		require.NoError(t, engine.CreateEdge(t.Context(), edge))
	}
}

// graphArticleGrants is the policy a graph reader holds on one database: it may
// traverse Article nodes and LINK relationships, read only the granted
// properties, and may not see any other label or relationship type.
func graphArticleGrants(databaseID string) []auth.FineGrainedEntitlement {
	return []auth.FineGrainedEntitlement{
		graphDatabaseGrant("database.access", databaseID),
		graphElementGrant("graph.node.traverse", auth.FineGrainedNode, databaseID, []string{"Article"}, nil),
		graphElementGrant("graph.node.property.read", auth.FineGrainedNode, databaseID, []string{"Article"}, []string{"title"}),
		graphElementGrant("graph.relationship.traverse", auth.FineGrainedRelationship, databaseID, []string{"LINK"}, nil),
		graphElementGrant("graph.relationship.property.read", auth.FineGrainedRelationship, databaseID, []string{"LINK"}, []string{"weight"}),
	}
}

func TestGraphNeighborhoodAndExpandReadThroughFineGrainedView(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	database := server.dbManager.DefaultDatabaseName()
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, databaseID)
	seedFineGrainedGraph(t, engine)
	principal := fineGrainedGraphPrincipal(t, server, "graph_reader", graphArticleGrants(databaseID)...)

	for name, handle := range map[string]func(http.ResponseWriter, *http.Request){
		"neighborhood": server.handleGraphNeighborhood,
		"expand":       server.handleGraphExpand,
	} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handle(response, fineGrainedGraphRequest(database, principal, `{"node_ids":["fg-article"],"depth":2}`))
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())

			payload := decodeGraphPayload(t, response.Body)
			nodes := make(map[string]graphNodePayload, len(payload.Nodes))
			for _, node := range payload.Nodes {
				nodes[node.ID] = node
			}
			require.Len(t, nodes, 2)
			require.Contains(t, nodes, "fg-article")
			require.Contains(t, nodes, "fg-linked")
			require.NotContains(t, nodes, "fg-secret", "a node the principal may not traverse is invisible")
			require.Equal(t, map[string]interface{}{"title": "Example"}, nodes["fg-article"].Properties, "only the granted property is read")
			require.Len(t, payload.Edges, 1)
			require.Equal(t, "fg-link", payload.Edges[0].ID)
			require.Equal(t, map[string]interface{}{"weight": float64(2)}, payload.Edges[0].Properties)
			require.NotContains(t, response.Body.String(), "Sensitive title")
			require.NotContains(t, response.Body.String(), "hidden draft")
			require.Equal(t, "node", payload.Meta.GeneratedFrom)
		})
	}
}

func TestGraphPathReadsThroughFineGrainedView(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	database := server.dbManager.DefaultDatabaseName()
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, databaseID)
	seedFineGrainedGraph(t, engine)
	principal := fineGrainedGraphPrincipal(t, server, "graph_reader", graphArticleGrants(databaseID)...)

	visible := httptest.NewRecorder()
	server.handleGraphPath(visible, fineGrainedGraphRequest(database, principal, `{"source_node_id":"fg-article","target_node_id":"fg-linked"}`))
	require.Equal(t, http.StatusOK, visible.Code, visible.Body.String())
	payload := decodeGraphPayload(t, visible.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.Equal(t, "query", payload.Meta.GeneratedFrom)

	// The relationship to the secret is granted by type, so only the hidden
	// node itself stops the walk: the path endpoint reports it as not found.
	hidden := httptest.NewRecorder()
	server.handleGraphPath(hidden, fineGrainedGraphRequest(database, principal, `{"source_node_id":"fg-article","target_node_id":"fg-secret"}`))
	require.Equal(t, http.StatusNotFound, hidden.Code, hidden.Body.String())
	require.NotContains(t, hidden.Body.String(), "Sensitive title")
}

func TestGraphNeighborhoodFineGrainedRequiresDatabaseAccess(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	database := server.dbManager.DefaultDatabaseName()
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, databaseID)
	seedFineGrainedGraph(t, engine)
	principal := fineGrainedGraphPrincipal(t, server, "graph_reader",
		graphElementGrant("graph.node.traverse", auth.FineGrainedNode, databaseID, []string{"Article"}, nil),
	)

	response := httptest.NewRecorder()
	server.handleGraphNeighborhood(response, fineGrainedGraphRequest(database, principal, `{"node_ids":["fg-article"]}`))
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "Neo.ClientError.Security.Forbidden")
	require.NotContains(t, response.Body.String(), "Example")
}

func TestGraphNeighborhoodFineGrainedResolvesConstituentToCanonicalDatabase(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	database := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "fg_graph_composite", []multidb.ConstituentRef{{
		Alias:        "local",
		DatabaseName: database,
		Type:         "local",
		AccessMode:   "read_write",
	}}))
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	compositeID, _ := server.dbManager.DatabaseIdentity("fg_graph_composite")
	require.NotEmpty(t, databaseID)
	require.NotEmpty(t, compositeID)
	seedFineGrainedGraph(t, engine)
	grants := make([]auth.FineGrainedEntitlement, 0, 6)
	grants = append(grants, graphDatabaseGrant("database.access", compositeID))
	grants = append(grants, graphArticleGrants(databaseID)...)
	principal := fineGrainedGraphPrincipal(t, server, "graph_reader", grants...)

	response := httptest.NewRecorder()
	server.handleGraphNeighborhood(response, fineGrainedGraphRequest("fg_graph_composite.local", principal, `{"node_ids":["fg-article"],"depth":1}`))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	payload := decodeGraphPayload(t, response.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, "fg_graph_composite.local", payload.Meta.Database)
}

func TestGraphTemporalAndDiffFineGrainedUseAuthorizedHistory(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	database := server.dbManager.DefaultDatabaseName()
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, databaseID)
	seedFineGrainedGraph(t, engine)

	grants := graphArticleGrants(databaseID)
	historical := make([]auth.FineGrainedEntitlement, 0, len(grants)+1)
	historical = append(historical, grants...)
	historical = append(historical, graphDatabaseGrant("database.history.read", databaseID))
	withHistory := fineGrainedGraphPrincipal(t, server, "graph_reader", historical...)
	withoutHistory := fineGrainedGraphPrincipal(t, server, "graph_reader", grants...)
	body := `{"node_ids":["fg-article"],"as_of":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`

	temporal := httptest.NewRecorder()
	server.handleGraphTemporal(temporal, fineGrainedGraphRequest(database, withHistory, body))
	require.Equal(t, http.StatusOK, temporal.Code, temporal.Body.String())
	require.Contains(t, temporal.Body.String(), "Example")
	require.NotContains(t, temporal.Body.String(), "draft")

	diff := httptest.NewRecorder()
	server.handleGraphDiff(diff, fineGrainedGraphRequest(database, withHistory, body))
	require.Equal(t, http.StatusOK, diff.Code, diff.Body.String())
	require.NotContains(t, diff.Body.String(), "draft")

	denied := httptest.NewRecorder()
	server.handleGraphTemporal(denied, fineGrainedGraphRequest(database, withoutHistory, body))
	require.Equal(t, http.StatusForbidden, denied.Code, denied.Body.String())
	require.Contains(t, denied.Body.String(), "Neo.ClientError.Security.Forbidden")
}

func TestGraphNeighborhoodFineGrainedRemoteConstituentFailsClosed(t *testing.T) {
	server, _ := setupTestServer(t)
	database := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "fg_graph_remote", []multidb.ConstituentRef{{
		Alias:        "r",
		DatabaseName: database,
		Type:         "remote",
		AccessMode:   "read_write",
		URI:          "http://remote.example",
	}}))
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	compositeID, _ := server.dbManager.DatabaseIdentity("fg_graph_remote")
	require.NotEmpty(t, databaseID)
	require.NotEmpty(t, compositeID)
	principal := fineGrainedGraphPrincipal(t, server, "graph_reader",
		graphDatabaseGrant("database.access", compositeID),
		graphDatabaseGrant("database.access", databaseID),
	)

	response := httptest.NewRecorder()
	server.handleGraphNeighborhood(response, fineGrainedGraphRequest("fg_graph_remote.r", principal, `{"node_ids":["fg-article"]}`))
	require.Equal(t, http.StatusNotImplemented, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "remote graph reads")
	require.Contains(t, response.Body.String(), "fg_graph_remote.r")
}

// Without an admitted fine-grained principal the graph handlers keep reading
// the database engine: the same request returns the hidden label, the ungranted
// relationship type and every property.
func TestGraphNeighborhoodWithoutFineGrainedRequestReadsDatabaseEngine(t *testing.T) {
	server, authenticator := setupTestServer(t)
	engine := getDefaultStorage(t, server)
	token := getAuthToken(t, authenticator, "admin")
	seedFineGrainedGraph(t, engine)

	response := makeRequest(t, server, http.MethodPost, defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"fg-article"},
		"depth":    1,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	payload := decodeGraphPayload(t, response.Body)
	require.Equal(t, 3, payload.Meta.NodeCount)
	require.Equal(t, 2, payload.Meta.EdgeCount)
	require.Contains(t, response.Body.String(), "Sensitive title")
	require.Contains(t, response.Body.String(), "hidden draft")
}
