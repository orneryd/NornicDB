package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchContinuationPageSizeHTTP(t *testing.T) {
	server, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"mode": "id", "n": 501,
	}, token)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "page_size_exceeded", body["request_status"])
	require.EqualValues(t, 500, body["max_page_size"])
}
