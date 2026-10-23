package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInnerAggregateSubqueryHTTP(t *testing.T) {
	raw, err := os.ReadFile("../cypher/testdata/inner_aggregate_subquery_oracle.json")
	require.NoError(t, err)
	var cases []struct {
		ID    string
		Query string
		Rows  json.RawMessage
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.Len(t, cases, 16)
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	srv, users := setupTestServer(t)
	token := getAuthToken(t, users, "admin")
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	client := peer.Client()
	client.Timeout = 10 * time.Second
	database := srv.dbManager.DefaultDatabaseName()
	request := func(t *testing.T, method, path, query string) (TransactionResponse, int) {
		t.Helper()
		statements := []map[string]interface{}{}
		if query != "" {
			statements = append(statements, map[string]interface{}{"statement": query})
		}
		body, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest(method, peer.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var payload TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
		return payload, response.StatusCode
	}
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s", normalize, explicit, tc.ID), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					path := "/db/" + database + "/tx/commit"
					if explicit {
						opened, status := request(t, http.MethodPost, "/db/"+database+"/tx", "")
						require.Equal(t, http.StatusCreated, status)
						require.Empty(t, opened.Errors)
						commit, err := url.Parse(opened.Commit)
						require.NoError(t, err)
						path = strings.TrimSuffix(commit.Path, "/commit")
						t.Cleanup(func() { _, _ = request(t, http.MethodDelete, path, "") })
					}
					payload, status := request(t, http.MethodPost, path, tc.Query)
					rows := make([][]interface{}, 0)
					if len(payload.Results) > 0 {
						for _, row := range payload.Results[0].Data {
							rows = append(rows, row.Row)
						}
					}
					record, err := json.Marshal(map[string]interface{}{
						"id": tc.ID, "query": tc.Query, "route": "http",
						"normalize": normalize, "explicit": explicit,
						"rows": rows, "errors": payload.Errors, "status": status,
					})
					require.NoError(t, err)
					t.Log("INNER_AGG_RESULT " + string(record))
					assert.Equal(t, http.StatusOK, status)
					if assert.Empty(t, payload.Errors) {
						got, err := json.Marshal(rows)
						require.NoError(t, err)
						assert.JSONEq(t, string(tc.Rows), string(got))
					}
				})
			}
		}
	}
}
