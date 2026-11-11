package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEmbedRegenerateRejectsUnknownDatabase(t *testing.T) {
	s, _ := setupTestServer(t)
	s.db.SetEmbedder(&countingEmbedder{dims: 1024})
	engine := seedEmbeddedNode(t, s, s.dbManager.DefaultDatabaseName(), "scope-sentinel")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true&database=missing_database", nil)
	s.handleEmbedTrigger(rec, as(req, "admin"))
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.True(t, hasEmbedding(t, engine, "scope-sentinel"))
}

func TestEmbedRegenerateDatabaseScopePermissionAndResponse(t *testing.T) {
	s, _ := setupTestServer(t)
	s.db.SetEmbedder(&countingEmbedder{dims: 1024})
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "scope_target"}))
	engine := seedEmbeddedNode(t, s, s.dbManager.DefaultDatabaseName(), "other-database-sentinel")
	for _, role := range []string{"viewer", "editor"} {
		rec := httptest.NewRecorder()
		s.handleEmbedTrigger(rec, as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true&database=scope_target", nil), role))
		require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	}
	rec := httptest.NewRecorder()
	s.handleEmbedTrigger(rec, as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true&database=scope_target", nil), "admin"))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "scope_target", response["database"])
	require.True(t, hasEmbedding(t, engine, "other-database-sentinel"))
}
