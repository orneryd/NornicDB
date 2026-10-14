package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedEmbeddedNode stores one node with an embedding in the named database.
func seedEmbeddedNode(t *testing.T, server *Server, database, id string) storage.Engine {
	t.Helper()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:              storage.NodeID(id),
		Labels:          []string{"Doc"},
		Properties:      map[string]interface{}{"content": id},
		ChunkEmbeddings: [][]float32{{1, 2, 3}},
	})
	require.NoError(t, err)
	return engine
}

// as signs the request in with the given roles, as the auth middleware would.
func as(req *http.Request, roles ...string) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), contextKeyClaims, &auth.JWTClaims{
		Sub: "u-" + strings.Join(roles, "-"), Username: "u-" + strings.Join(roles, "-"), Roles: roles,
	}))
}

func hasEmbedding(t *testing.T, engine storage.Engine, id string) bool {
	t.Helper()
	node, err := engine.GetNode(storage.NodeID(id))
	require.NoError(t, err)
	return len(node.ChunkEmbeddings) > 0
}

func TestEmbedClearTargetsTheNamedDatabases(t *testing.T) {
	server, _ := setupTestServer(t)
	defaultDB := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase("tenant_b"))
	require.NoError(t, server.dbManager.CreateDatabase("tenant_c"))
	defaultEngine := seedEmbeddedNode(t, server, defaultDB, "doc-default")
	tenantB := seedEmbeddedNode(t, server, "tenant_b", "doc-b")
	tenantC := seedEmbeddedNode(t, server, "tenant_c", "doc-c")

	t.Run("a JSON list clears only those databases", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear", strings.NewReader(`{"databases":["tenant_b"]}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, as(req, "admin"))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var body struct {
			Cleared   int            `json:"cleared"`
			Databases map[string]int `json:"databases"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, 1, body.Cleared)
		assert.Equal(t, map[string]int{"tenant_b": 1}, body.Databases)
		assert.False(t, hasEmbedding(t, tenantB, "doc-b"))
		assert.True(t, hasEmbedding(t, defaultEngine, "doc-default"), "the default database is untouched")
		assert.True(t, hasEmbedding(t, tenantC, "doc-c"), "unnamed databases are untouched")
	})

	t.Run("no database named means the default database only", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/nornicdb/embed/clear", nil)
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, as(req, "admin"))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), `"`+defaultDB+`":1`)
		assert.False(t, hasEmbedding(t, defaultEngine, "doc-default"))
		assert.True(t, hasEmbedding(t, tenantC, "doc-c"))
	})

	t.Run("query parameters accept a comma list and repeats", func(t *testing.T) {
		seedEmbeddedNode(t, server, "tenant_b", "doc-b2")
		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear?database=tenant_b,tenant_c&database=tenant_b", nil)
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, as(req, "admin"))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), `"tenant_b":1`)
		assert.Contains(t, rec.Body.String(), `"tenant_c":1`)
		assert.False(t, hasEmbedding(t, tenantB, "doc-b2"))
		assert.False(t, hasEmbedding(t, tenantC, "doc-c"))
	})

	t.Run("an unknown or system database clears nothing", func(t *testing.T) {
		seedEmbeddedNode(t, server, "tenant_b", "doc-b3")
		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear", strings.NewReader(`{"databases":["tenant_b","missing_db"]}`))
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, as(req, "admin"))
		assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
		assert.True(t, hasEmbedding(t, tenantB, "doc-b3"), "a bad name stops the request before anything is cleared")

		req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear?database=system", nil)
		rec = httptest.NewRecorder()
		server.handleEmbedClear(rec, as(req, "admin"))
		assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

		req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear", strings.NewReader(`{"databases":`))
		rec = httptest.NewRecorder()
		server.handleEmbedClear(rec, as(req, "admin"))
		assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	})
}

func TestEmbedClearNeedsWriteAccessToEveryDatabase(t *testing.T) {
	server, _ := setupTestServer(t)
	engine := seedEmbeddedNode(t, server, server.dbManager.DefaultDatabaseName(), "doc-default")

	for _, roles := range [][]string{nil, {"viewer"}} {
		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear", nil)
		if roles != nil {
			req = as(req, roles...)
		}
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code, "roles %v: %s", roles, rec.Body.String())
	}
	assert.True(t, hasEmbedding(t, engine, "doc-default"))
}

func TestEmbedRegenerateReportsItsDatabases(t *testing.T) {
	server, _ := setupTestServer(t)
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})
	require.NoError(t, server.dbManager.CreateDatabase("tenant_b"))

	req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", strings.NewReader(`{"databases":["tenant_b"]}`))
	rec := httptest.NewRecorder()
	server.handleEmbedTrigger(rec, as(req, "admin"))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var body struct {
		Databases []string `json:"databases"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, []string{"tenant_b"}, body.Databases)

	req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedTrigger(rec, as(req, "admin"))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, []string{server.dbManager.DefaultDatabaseName()}, body.Databases)

	req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true&database=missing_db", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedTrigger(rec, as(req, "admin"))
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}

// Regenerating clears embeddings, so it needs admin like /nornicdb/embed/clear; waking the
// worker without regenerate keeps the route's write permission.
func TestEmbedRegenerateNeedsAdmin(t *testing.T) {
	server, _ := setupTestServer(t)
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})
	engine := seedEmbeddedNode(t, server, server.dbManager.DefaultDatabaseName(), "doc-default")

	for _, roles := range [][]string{{"editor"}, {"viewer"}} {
		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil)
		rec := httptest.NewRecorder()
		server.handleEmbedTrigger(rec, as(req, roles...))
		assert.Equal(t, http.StatusForbidden, rec.Code, "roles %v: %s", roles, rec.Body.String())
	}
	assert.True(t, hasEmbedding(t, engine, "doc-default"), "a refused regenerate clears nothing")

	req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil)
	rec := httptest.NewRecorder()
	server.handleEmbedTrigger(rec, as(req, "editor"))
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestEmbedDestructiveRoutesNeedAdmin(t *testing.T) {
	server, authenticator := setupTestServer(t)
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})
	engine := seedEmbeddedNode(t, server, server.dbManager.DefaultDatabaseName(), "doc-default")
	_, err := authenticator.CreateUser("editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)

	for _, user := range []string{"reader", "editor"} {
		token := getAuthToken(t, authenticator, user)
		for _, endpoint := range []struct{ method, path string }{
			{http.MethodPost, "/nornicdb/embed/clear"},
			{http.MethodDelete, "/nornicdb/embed/clear"},
			{http.MethodPost, "/nornicdb/embed/trigger?regenerate=true"},
		} {
			t.Run(user+" "+endpoint.method+" "+endpoint.path, func(t *testing.T) {
				rec := makeRequest(t, server, endpoint.method, endpoint.path, nil, "Bearer "+token)
				require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
				require.True(t, hasEmbedding(t, engine, "doc-default"), "denied requests must preserve embeddings")
			})
		}
	}

	// Waking the worker does not clear embeddings and still permits editors.
	token := getAuthToken(t, authenticator, "editor")
	rec := makeRequest(t, server, http.MethodPost, "/nornicdb/embed/trigger", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}
