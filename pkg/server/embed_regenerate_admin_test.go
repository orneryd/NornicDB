package server

import (
	"context"
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
