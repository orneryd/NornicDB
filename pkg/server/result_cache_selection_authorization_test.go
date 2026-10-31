package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// TestResultCacheDoesNotAnswerForUnauthorizedSelection is a regression test: a
// statement selecting a database gets the same refusal from a principal without
// access to it, whether or not a principal with access has already run that text.
func TestResultCacheDoesNotAnswerForUnauthorizedSelection(t *testing.T) {
	server, authenticator := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, true, false))
	reader := getAuthToken(t, authenticator, "reader")
	admin := getAuthToken(t, authenticator, "admin")

	post := func(bearer, database, statement string) string {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+database+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+bearer)
		var shape map[string]any
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&shape))
		delete(shape, "lastBookmarks")
		encoded, err := json.Marshal(shape)
		require.NoError(t, err)
		return string(encoded)
	}
	require.Contains(t, post(admin, "private", "CREATE (:Secret {v: 'private-row'})"), `"errors":[]`)

	const statement = "UNWIND [1] AS u CALL { USE private MATCH (n:Secret) RETURN n.v AS v } RETURN v"
	cold := post(reader, dbName, statement)
	t.Log("reader, cold: ", cold)
	require.NotContains(t, cold, "private-row")

	privileged := post(admin, dbName, statement)
	t.Log("admin: ", privileged)
	require.Contains(t, privileged, "private-row", "the fixture must return the row to a principal with access")

	warmed := post(reader, dbName, statement)
	t.Log("reader, after admin: ", warmed)
	require.NotContains(t, warmed, "private-row", "a cached result answered for a database the principal may not access")
	require.Equal(t, cold, warmed)
}
