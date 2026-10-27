package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestIdentityRemoteAliasPermissionIsolation(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNIC_CYPHER_PARSER", parser)
			remote, _ := setupTestServer(t)
			wire := httptest.NewServer(remote.buildRouter())
			t.Cleanup(wire.Close)
			coordinator, users := setupTestServer(t)
			_, err := users.CreateUser("unit-alias-editor", "password123", []auth.Role{auth.RoleEditor})
			require.NoError(t, err)
			token := "Bearer " + getAuthToken(t, users, "unit-alias-editor")
			high, low, literal := "Route_\xed\xa0\x80", "Route_\xed\xb0\x80", `Route_\ud800`
			refs := []multidb.ConstituentRef{}
			for _, tc := range []struct{ alias, mode string }{{high, "read"}, {low, "read_write"}, {literal, "read"}} {
				refs = append(refs, multidb.ConstituentRef{Alias: tc.alias, DatabaseName: "nornic", Type: "remote", AccessMode: tc.mode, URI: wire.URL, AuthMode: "user_password", User: "admin", Password: "password123"})
			}
			require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), "unitrestricted", refs))
			router := coordinator.buildRouter()
			request := func(t *testing.T, database, statement string, params map[string]any) TransactionResponse {
				t.Helper()
				body, err := jsontext.Marshal(map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}})
				require.NoError(t, err)
				req := httptest.NewRequest(http.MethodPost, "/db/"+url.PathEscape(database)+"/tx/commit", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", token)
				output := httptest.NewRecorder()
				router.ServeHTTP(output, req)
				require.Equal(t, http.StatusOK, output.Code, output.Body.String())
				var response TransactionResponse
				require.NoError(t, jsontext.Unmarshal(output.Body.Bytes(), &response))
				return response
			}
			allowed := request(t, "nornic", "USE unitrestricted."+cyphertext.PrintIdentifier(low, nil, cyphertext.RawSource)+" CREATE (:UnitAliasAllowed)", nil)
			require.Empty(t, allowed.Errors)
			for _, alias := range []string{high, literal} {
				for _, route := range []string{"use", "dynamic", "direct"} {
					t.Run(route+"/"+cyphertext.EmitRawSource(alias), func(t *testing.T) {
						database, statement := "nornic", "USE unitrestricted."+cyphertext.PrintIdentifier(alias, nil, cyphertext.RawSource)+" CREATE (:UnitAliasDenied)"
						var params map[string]any
						if route == "dynamic" {
							database = "unitrestricted"
							statement = "USE graph.byName($selectedGraph) CREATE (:UnitAliasDenied)"
							params = map[string]any{"selectedGraph": "unitrestricted." + alias}
						}
						if route == "direct" {
							database = "unitrestricted." + alias
							statement = "CREATE (:UnitAliasDenied)"
						}
						response := request(t, database, statement, params)
						require.Len(t, response.Errors, 1)
						require.Contains(t, strings.ToLower(response.Errors[0].Message), "read-only")
					})
				}
			}
			store, err := remote.dbManager.GetDefaultStorage()
			require.NoError(t, err)
			allowedNodes, err := store.GetNodesByLabel("UnitAliasAllowed")
			require.NoError(t, err)
			require.Len(t, allowedNodes, 1)
			deniedNodes, err := store.GetNodesByLabel("UnitAliasDenied")
			require.NoError(t, err)
			require.Empty(t, deniedNodes, "the actual authenticated remote must reject all read-only alias writes")
		})
	}
}
