package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeadlessRouterDisablesBrowserSurfaceAndPreservesCoreAPIs(t *testing.T) {
	server, authenticator := setupTestServer(t)
	server.config.Headless = true
	adminToken := getAuthToken(t, authenticator, "admin")
	router := server.buildRouter()

	for _, path := range []string{
		"/assets/app.js",
		"/favicon.ico",
		"/nornicdb.svg",
		"/login",
		"/security",
		"/security/knowledge-policies",
		"/help",
		"/auth/config",
		"/graphql/playground",
	} {
		t.Run("browser route "+path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("Accept", "text/html")
			req.Header.Set("Authorization", "Bearer "+adminToken)
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, req)

			require.Equal(t, http.StatusNotFound, recorder.Code)
			require.NotContains(t, recorder.Header().Get("Content-Type"), "text/html")
		})
	}

	t.Run("Neo4j discovery remains available at root", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, req)

		require.Equal(t, http.StatusOK, recorder.Code)
		require.Contains(t, recorder.Header().Get("Content-Type"), "application/json")
	})

	t.Run("health remains available", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, req)

		require.Equal(t, http.StatusOK, recorder.Code)
	})

	t.Run("GraphQL API remains registered", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{__typename}"}`))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, req)

		require.NotEqual(t, http.StatusNotFound, recorder.Code)
	})

	t.Run("admin API remains registered", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/stats", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, req)

		require.NotEqual(t, http.StatusNotFound, recorder.Code)
	})
}
