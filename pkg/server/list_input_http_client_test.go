package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// listInputHTTPClient gives every independent representation its own transaction.
// Neo4j ends an explicit HTTP transaction on a statement error, so sharing one
// between sibling subtests would hide their results behind TransactionNotFound.
type listInputHTTPClient struct {
	peer      *httptest.Server
	base      string
	token     string
	useNumber bool
}

func newListInputHTTPClient(t *testing.T, srv *Server, token string, useNumber bool) *listInputHTTPClient {
	t.Helper()
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	return &listInputHTTPClient{
		peer: peer, base: "/db/" + srv.dbManager.DefaultDatabaseName() + "/tx",
		token: token, useNumber: useNumber,
	}
}

func (client *listInputHTTPClient) post(t *testing.T, path, query string, params map[string]interface{}) TransactionResponse {
	t.Helper()
	statements := []map[string]interface{}{}
	if query != "" {
		statements = append(statements, map[string]interface{}{"statement": query, "parameters": params})
	}
	body, err := json.Marshal(map[string]interface{}{"statements": statements})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, client.peer.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+client.token)
	response, err := client.peer.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	status := http.StatusOK
	if path == client.base {
		status = http.StatusCreated
	}
	require.Equal(t, status, response.StatusCode, query)
	var payload TransactionResponse
	decoder := json.NewDecoder(response.Body)
	if client.useNumber {
		decoder.UseNumber()
	}
	require.NoError(t, decoder.Decode(&payload))
	return payload
}

func (client *listInputHTTPClient) run(t *testing.T, explicit bool, query string, params map[string]interface{}) TransactionResponse {
	t.Helper()
	path := client.base + "/commit"
	if explicit {
		opened := client.post(t, client.base, "", nil)
		require.Empty(t, opened.Errors)
		commit, err := url.Parse(opened.Commit)
		require.NoError(t, err)
		path = strings.TrimSuffix(commit.Path, "/commit")
		defer client.rollback(t, path)
	}
	payload := client.post(t, path, query, params)
	if explicit && len(payload.Errors) == 0 {
		committed := client.post(t, path+"/commit", "", nil)
		require.Empty(t, committed.Errors)
	}
	return payload
}

func (client *listInputHTTPClient) rollback(t *testing.T, path string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, client.peer.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+client.token)
	response, err := client.peer.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Contains(t, []int{http.StatusOK, http.StatusNotFound}, response.StatusCode)
}
