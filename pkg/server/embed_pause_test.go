package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// The /nornicdb/embed/pause, resume and status routes and the
// db.embedding.* procedures over HTTP (#1021): administrators only, the
// state in effect as {paused, pending}, idempotent, POST to change and GET
// to read.
func TestEmbedPauseRoutes(t *testing.T) {
	server, authenticator := setupTestServer(t)
	admin := "Bearer " + getAuthToken(t, authenticator, "admin")
	reader := "Bearer " + getAuthToken(t, authenticator, "reader")

	state := func(method, path, authHeader string) map[string]any {
		t.Helper()
		rec := makeRequest(t, server, method, path, nil, authHeader)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Len(t, body, 2, rec.Body.String())
		require.Contains(t, body, "pending")
		return body
	}
	cypher := func(statement, authHeader string) (row []any, code string) {
		t.Helper()
		rec := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, authHeader)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var body struct {
			Results []struct {
				Data []struct {
					Row []any `json:"row"`
				} `json:"data"`
			} `json:"results"`
			Errors []struct {
				Code string `json:"code"`
			} `json:"errors"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		if len(body.Errors) > 0 {
			return nil, body.Errors[0].Code
		}
		require.Len(t, body.Results, 1)
		require.Len(t, body.Results[0].Data, 1)
		return body.Results[0].Data[0].Row, ""
	}

	for _, route := range []struct{ method, path string }{
		{"POST", "/nornicdb/embed/pause"},
		{"POST", "/nornicdb/embed/resume"},
		{"GET", "/nornicdb/embed/status"},
	} {
		require.Equal(t, http.StatusUnauthorized, makeRequest(t, server, route.method, route.path, nil, "").Code, route.path)
		require.Equal(t, http.StatusForbidden, makeRequest(t, server, route.method, route.path, nil, reader).Code, route.path)
	}
	require.Equal(t, http.StatusMethodNotAllowed, makeRequest(t, server, "GET", "/nornicdb/embed/pause", nil, admin).Code)
	require.Equal(t, http.StatusMethodNotAllowed, makeRequest(t, server, "GET", "/nornicdb/embed/resume", nil, admin).Code)
	require.Equal(t, http.StatusMethodNotAllowed, makeRequest(t, server, "POST", "/nornicdb/embed/status", nil, admin).Code)
	require.False(t, server.db.EmbeddingStatus().Paused, "refused calls change nothing")

	require.Equal(t, false, state("GET", "/nornicdb/embed/status", admin)["paused"])
	require.Equal(t, true, state("POST", "/nornicdb/embed/pause", admin)["paused"])
	require.Equal(t, true, state("POST", "/nornicdb/embed/pause", admin)["paused"], "idempotent")
	require.Equal(t, true, state("GET", "/nornicdb/embed/status", admin)["paused"])
	require.True(t, server.db.EmbeddingStatus().Paused)

	// The same state through the procedures, on the HTTP Cypher endpoint.
	row, code := cypher("CALL db.embedding.status() YIELD paused, pending RETURN paused, pending", admin)
	require.Empty(t, code)
	require.Equal(t, true, row[0])
	_, code = cypher("CALL db.embedding.resume()", reader)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.True(t, server.db.EmbeddingStatus().Paused, "a reader cannot resume")
	row, code = cypher("CALL db.embedding.resume()", admin)
	require.Empty(t, code)
	require.Equal(t, false, row[0])
	require.Equal(t, false, state("GET", "/nornicdb/embed/status", admin)["paused"])
	require.Equal(t, false, state("POST", "/nornicdb/embed/resume", admin)["paused"], "idempotent")

	// /nornicdb/embed/stats reports the pause with the worker's figures.
	row, code = cypher("CALL db.embedding.pause()", admin)
	require.Empty(t, code)
	require.Equal(t, true, row[0])
	if stats := server.db.EmbedQueueStats(); stats != nil {
		require.True(t, stats.Paused)
	}
}
