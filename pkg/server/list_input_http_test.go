package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputHTTPRepresentations(t *testing.T) {
	srv, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	base := "/db/" + srv.dbManager.DefaultDatabaseName() + "/tx"
	store, err := srv.dbManager.GetStorage(srv.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	for _, input := range listinputs.TransportInputs() {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"TypedWireList"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
		require.NoError(t, err)
	}
	request := func(t *testing.T, path, query string, params map[string]interface{}) TransactionResponse {
		t.Helper()
		statements := []map[string]interface{}{}
		if query != "" {
			statements = append(statements, map[string]interface{}{"statement": query, "parameters": params})
		}
		body, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, peer.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := peer.Client().Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var payload TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
		require.Empty(t, payload.Errors, query)
		return payload
	}
	for _, explicit := range []bool{false, true} {
		for _, input := range listinputs.TransportInputs() {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, input.Name), func(t *testing.T) {
				path := base + "/commit"
				if explicit {
					opened := request(t, base, "", nil)
					commit, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					path = strings.TrimSuffix(commit.Path, "/commit")
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
					t.Helper()
					payload := request(t, path, query, params)
					require.Len(t, payload.Results, 1, query)
					require.Len(t, payload.Results[0].Data, 1, query)
					return payload.Results[0].Data[0].Row[0]
				}
				if input.CypherWritableProperty() {
					read(t, "CREATE (n:WireListInput {xs:$xs}) RETURN n.xs AS v", params)
				}
				for _, template := range listinputs.Queries() {
					t.Run(template, func(t *testing.T) {
						want := read(t, listinputs.Query(template, input.Literal), nil)
						for _, form := range []struct{ name, prefix, value string }{
							{"parameter", "", "$xs"}, {"with", "WITH $xs AS xs ", "xs"},
							{"written-property", "MATCH (n:WireListInput) ", "n.xs"},
							{"typed-property", "MATCH (n:TypedWireList {name:$name}) ", "n.xs"},
						} {
							if form.name == "written-property" && !input.CypherWritableProperty() {
								continue
							}
							t.Run(form.name, func(t *testing.T) {
								require.Equal(t, want, read(t, form.prefix+listinputs.Query(template, form.value), params))
							})
						}
					})
				}
				request(t, path, "MATCH (n:WireListInput) DETACH DELETE n", nil)
				if explicit {
					request(t, path+"/commit", "", nil)
				}
			})
		}
	}
}
