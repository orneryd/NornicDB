package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A coordinator editor may use an alias but cannot override the administrator's
// read-only restriction with the stronger credentials stored for its remote.
func TestReadOnlyRemoteAliasRejectsEditorWrites(t *testing.T) {
	for _, noAuth := range []bool{false, true} {
		name := "authenticated"
		if noAuth {
			name = "no-auth"
		}
		t.Run(name, func(t *testing.T) { checkRemoteAliasAccess(t, noAuth) })
	}
}

func checkRemoteAliasAccess(t *testing.T, noAuth bool) {
	remote, _ := setupTestServer(t)
	remoteWire := httptest.NewServer(remote.buildRouter())
	t.Cleanup(remoteWire.Close)
	coordinator, users := setupTestServer(t)
	_, err := users.CreateUser("alias-editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, "alias-editor")
	if noAuth {
		coordinator.auth = nil
	}
	refs := []multidb.ConstituentRef{}
	for _, mode := range []string{"read", "read_write"} {
		refs = append(refs, multidb.ConstituentRef{Alias: mode, DatabaseName: "nornic", Type: "remote", AccessMode: mode, URI: remoteWire.URL, AuthMode: "user_password", User: "admin", Password: "password123"})
	}
	require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), "restricted", refs))
	store, err := remote.dbManager.GetDefaultStorage()
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "readonly-remote-marker", Labels: []string{"AliasReadMarker"}})
	require.NoError(t, err)
	// Establish successful remote authentication and coordinator read permission.
	read := makeRequest(t, coordinator, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "USE restricted.read MATCH (n:AliasReadMarker) RETURN count(n) AS n"}}}, token)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(read.Body.Bytes(), &response))
	require.Empty(t, response.Errors, read.Body.String())
	require.Len(t, response.Results, 1)
	require.EqualValues(t, 1, response.Results[0].Data[0].Row[0])
	// The same local principal and remote identity can write through a write alias.
	write := makeRequest(t, coordinator, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "USE restricted.read_write CREATE (:AliasWriteControl)"}}}, token)
	require.NoError(t, json.Unmarshal(write.Body.Bytes(), &response))
	require.Empty(t, response.Errors, write.Body.String())
	controls, err := store.GetNodesByLabel("AliasWriteControl")
	require.NoError(t, err)
	require.Len(t, controls, 1)

	// A known pure-read procedure remains usable through the same remote alias.
	labels := makeRequest(t, coordinator, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "USE restricted.read CALL db.labels() YIELD label RETURN label"}}}, token)
	require.NoError(t, json.Unmarshal(labels.Body.Bytes(), &response))
	require.Empty(t, response.Errors, labels.Body.String())
	require.Len(t, response.Results, 1)
	require.NotEmpty(t, response.Results[0].Data)

	for _, route := range []string{"use", "dynamic", "direct"} {
		for _, scenario := range []struct{ name, body string }{
			{"write", "CREATE (:AliasDeniedWrite)"},
			{"opaque-procedure", "CALL security_test.unknownWrite()"},
			{"dynamic-procedure", "CALL apoc.cypher.run('CREATE (:AliasDeniedWrite)', {}) YIELD value RETURN value"},
		} {
			for _, explicit := range []bool{false, true} {
				name := route + "/" + scenario.name
				if explicit {
					name += "/explicit"
				} else {
					name += "/implicit"
				}
				t.Run(name, func(t *testing.T) {
					database, statement := "nornic", "USE restricted.read "+scenario.body
					if route == "dynamic" {
						database = "restricted"
						statement = "USE graph.byName($selectedGraph) " + scenario.body
					}
					if route == "direct" {
						database = "restricted.read"
						statement = scenario.body
					}
					path := "/db/" + database + "/tx/commit"
					if explicit {
						opened := makeRequest(t, coordinator, http.MethodPost, "/db/"+database+"/tx", map[string]any{"statements": []any{}}, token)
						var tx TransactionResponse
						require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &tx))
						if route == "direct" {
							// Direct remote transaction startup is unsupported, independently
							// of statement admission. Assert its existing refusal explicitly.
							require.Len(t, tx.Errors, 1, opened.Body.String())
							require.Contains(t, tx.Errors[0].Message, "engine does not support transactions")
							nodes, err := store.GetNodesByLabel("AliasDeniedWrite")
							require.NoError(t, err)
							require.Empty(t, nodes)
							return
						}
						require.Empty(t, tx.Errors, opened.Body.String())
						commit, err := url.Parse(tx.Commit)
						require.NoError(t, err)
						path = commit.Path
					}
					result := makeRequest(t, coordinator, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": map[string]any{"selectedGraph": "restricted.read"}}}}, token)
					var denied TransactionResponse
					require.NoError(t, json.Unmarshal(result.Body.Bytes(), &denied))
					if len(denied.Errors) == 0 {
						t.Error("read-only alias accepted a remote write")
					} else {
						require.Contains(t, strings.ToLower(denied.Errors[0].Message), "read-only")
					}
					nodes, err := store.GetNodesByLabel("AliasDeniedWrite")
					require.NoError(t, err)
					require.Empty(t, nodes, "the actual authenticated remote must remain unchanged")
				})
			}
		}
	}
}
