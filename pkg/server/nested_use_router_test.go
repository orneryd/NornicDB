package server

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// A statement a procedure runs with USE of a user database, sent to the
// system database, writes nothing in either database: in an explicit
// transaction and in auto-commit, the nested statement goes through the one
// database router, so it can neither write the system database nor leave
// the caller's transaction.
func TestNestedUseFromSystemDatabaseWritesNothing(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := map[bool]string{false: "auto-commit", true: "explicit transaction"}[explicit]
		t.Run(name, func(t *testing.T) {
			server, users := setupTestServer(t)
			_, err := users.CreateUser("writer", "password123", []auth.Role{auth.RoleEditor})
			require.NoError(t, err)
			writer := getAuthToken(t, users, "writer")
			system, err := server.dbManager.GetStorage("system")
			require.NoError(t, err)
			before, err := system.GetNodesByLabel("_User")
			require.NoError(t, err)

			statement := map[string]any{
				"statement":  "CALL apoc.cypher.run('USE nornic CREATE (u:_User {username: $name}) RETURN u.username AS name', {name: $name})",
				"parameters": map[string]any{"name": "nested_account"},
			}
			var body TransactionResponse
			if explicit {
				response := makeRequest(t, server, "POST", "/db/system/tx", map[string]any{"statements": []map[string]any{statement}}, "Bearer "+writer)
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				if location := response.Header().Get("Location"); location != "" {
					parsed, err := url.Parse(location)
					require.NoError(t, err)
					makeRequest(t, server, "POST", parsed.Path+"/commit", map[string]any{"statements": []map[string]any{}}, "Bearer "+writer)
				}
			} else {
				response := makeRequest(t, server, "POST", "/db/system/tx/commit", map[string]any{"statements": []map[string]any{statement}}, "Bearer "+writer)
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			}
			require.NotEmpty(t, body.Errors, "the nested statement is refused")

			after, err := system.GetNodesByLabel("_User")
			require.NoError(t, err)
			require.Len(t, after, len(before), "the system database is unchanged")
			nornic, err := server.dbManager.GetStorage("nornic")
			require.NoError(t, err)
			written, err := nornic.GetNodesByLabel("_User")
			require.NoError(t, err)
			require.Empty(t, written, "the user database is unchanged")
		})
	}
}
