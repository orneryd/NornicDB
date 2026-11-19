package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

func TestClearDatabaseEmbeddingsThroughManagerStorage(t *testing.T) {
	s, _ := setupTestServer(t)
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "scope_target"}))
	engine := seedEmbeddedNode(t, s, "scope_target", "target-sentinel")
	count, err := s.db.ClearDatabaseEmbeddings("scope_target", engine)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.False(t, hasEmbedding(t, engine, "target-sentinel"))
}

func TestEmbedRegenerateDatabaseScopePermissionAndResponse(t *testing.T) {
	s, _ := setupTestServer(t)
	s.db.SetEmbedder(&countingEmbedder{dims: 1024})
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "scope_target"}))
	engine := seedEmbeddedNode(t, s, s.dbManager.DefaultDatabaseName(), "other-database-sentinel")
	target := seedEmbeddedNode(t, s, "scope_target", "target-sentinel")
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
	require.Eventually(t, func() bool {
		return !hasEmbedding(t, target, "target-sentinel")
	}, 5*time.Second, 20*time.Millisecond, "the accepted regeneration must clear the target's old vector")
	require.True(t, hasEmbedding(t, engine, "other-database-sentinel"))
	other, err := engine.GetNode(t.Context(), "other-database-sentinel")
	require.NoError(t, err)
	require.Len(t, other.ChunkEmbeddings[0], 3, "another database must retain its original vector")
}
