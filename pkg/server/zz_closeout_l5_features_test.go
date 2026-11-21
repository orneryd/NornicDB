package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseoutL5HTTPFeatures(t *testing.T) {
	server, authenticator := setupTestServer(t)
	router := server.buildRouter()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	request := func(method, path, accept, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", token)
		req.Header.Set("Accept", accept)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	t.Run("P-N152-115", func(t *testing.T) {
		const path = "/db/nornic/tx/commit"
		const body = `{"statements":[{"statement":"RETURN date('2026-11-21') AS d, duration('PT1.000000001S') AS elapsed"}]}`
		plain := request(http.MethodPost, path, "application/json", body)
		require.Equal(t, http.StatusOK, plain.Code, plain.Body.String())
		var data map[string]any
		require.NoError(t, json.Unmarshal(plain.Body.Bytes(), &data))
		require.Empty(t, data["errors"], plain.Body.String())
		results := data["results"].([]any)
		require.Len(t, results, 1)
		row := results[0].(map[string]any)["data"].([]any)[0].(map[string]any)["row"].([]any)
		require.Equal(t, "2026-11-21", row[0])
		typed := request(http.MethodPost, path, "application/vnd.neo4j.jolt+json", body)
		t.Logf("L5 P-N152-115 plain=%s typed-status=%d typed-content-type=%s typed=%s",
			plain.Body.String(), typed.Code, typed.Header().Get("Content-Type"), typed.Body.String())
		if typed.Code == http.StatusOK {
			var typedData map[string]any
			require.NoError(t, json.Unmarshal(typed.Body.Bytes(), &typedData))
			require.Equal(t, data["results"], typedData["results"])
			require.Equal(t, data["errors"], typedData["errors"])
			require.NotContains(t, typed.Header().Get("Content-Type"), "jolt")
		} else {
			require.Equal(t, http.StatusNotAcceptable, typed.Code)
		}
	})
	t.Run("H-67", func(t *testing.T) {
		stats := request(http.MethodGet, "/nornicdb/embed/stats", "application/json", "")
		require.Equal(t, http.StatusOK, stats.Code, stats.Body.String())
		for _, action := range []string{"pause", "resume"} {
			response := request(http.MethodPost, "/nornicdb/embed/"+action, "application/json", "{}")
			t.Logf("L5 H-67 %s status=%d", action, response.Code)
			require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
		}
	})
}
