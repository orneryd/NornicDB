package server

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestShowUsersOverHTTP: SHOW USERS lists the auth store's users and SHOW
// CURRENT USER the signed-in one, with Neo4j's columns (#718).
func TestShowUsersOverHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	defer stopTestServer(t, server)
	token := getAuthToken(t, authenticator, "admin")

	run := func(statement string) map[string]interface{} {
		t.Helper()
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": statement}},
		}, "Bearer "+token)
		require.Equal(t, 200, resp.Code, resp.Body.String())
		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &body))
		require.Empty(t, body["errors"], resp.Body.String())
		return body["results"].([]interface{})[0].(map[string]interface{})
	}

	users := run("SHOW USERS YIELD user RETURN user ORDER BY user")
	var names []interface{}
	for _, row := range users["data"].([]interface{}) {
		names = append(names, row.(map[string]interface{})["row"].([]interface{})[0])
	}
	require.Equal(t, []interface{}{"admin", "reader"}, names)

	current := run("SHOW CURRENT USER")
	require.Equal(t, []interface{}{"user", "roles", "passwordChangeRequired", "suspended", "home"}, current["columns"])
	require.Equal(t, "admin", current["data"].([]interface{})[0].(map[string]interface{})["row"].([]interface{})[0])
}
