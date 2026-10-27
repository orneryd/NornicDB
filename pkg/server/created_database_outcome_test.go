package server

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPCreatedCompositeAccessIdentity(t *testing.T) {
	for name, statement := range map[string]string{
		"name_gap": "CREATE COMPOSITE DATABASE created_outcome\u00a0ALIAS r FOR DATABASE nornic",
		"head_gap": "CREATE\u00a0COMPOSITE DATABASE created_outcome ALIAS r FOR DATABASE nornic",
		"comments": "CREATE/*head*/COMPOSITE DATABASE created_outcome/*name*/ALIAS r FOR DATABASE nornic",
		"quoted":   "CREATE COMPOSITE DATABASE `created_outcome` ALIAS r FOR DATABASE nornic",
	} {
		t.Run(name, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			dbName := server.dbManager.DefaultDatabaseName()
			require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "wrong_target"}))
			require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "admin", []string{dbName}))
			require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "admin", dbName, true, true))
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			request := func(database, query string) TransactionResponse {
				recorder := makeRequest(t, server, http.MethodPost, "/db/"+database+"/tx/commit", map[string]any{
					"statements": []map[string]any{{"statement": query}},
				}, token)
				require.Equal(t, http.StatusOK, recorder.Code)
				var response TransactionResponse
				require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
				return response
			}
			require.Empty(t, request(dbName, statement).Errors)
			require.True(t, server.dbManager.IsCompositeDatabase("created_outcome"))
			require.True(t, server.GetDatabaseAccessModeForRoles([]string{"admin"}).CanSeeDatabase("created_outcome"), "creation must grant the administration outcome identity")
			require.Empty(t, request("created_outcome", "RETURN 1 AS ok").Errors)
			require.NotEmpty(t, request("wrong_target", "RETURN 1 AS ok").Errors, "unrelated target must remain inaccessible")
			// A generic result row is not a creation outcome.
			require.Empty(t, request(dbName, "RETURN 'wrong_target' AS name").Errors)
			require.False(t, server.GetDatabaseAccessModeForRoles([]string{"admin"}).CanSeeDatabase("wrong_target"))
			require.Empty(t, request(dbName, "CREATE DATABASE wrong_target IF NOT EXISTS").Errors)
			require.False(t, server.GetDatabaseAccessModeForRoles([]string{"admin"}).CanSeeDatabase("wrong_target"), "a no-op is not a newly created database")
		})
	}
}
