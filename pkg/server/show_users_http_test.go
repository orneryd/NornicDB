package server

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestListConnectionsOverHTTPUsesInstanceInventory(t *testing.T) {
	server, authenticator := setupTestServer(t)
	defer stopTestServer(t, server)
	server.SetConnectionLister(func() []cypher.ConnectionListing {
		return []cypher.ConnectionListing{{ConnectionID: "bolt-17", ConnectTime: "2026-10-02T00:00:00Z", Connector: "bolt", Username: "reader", UserAgent: "live-driver", ServerAddress: "127.0.0.1:7687", ClientAddress: "127.0.0.1:12345"}}
	})
	token := getAuthToken(t, authenticator, "admin")
	response := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{{"statement": "CALL dbms.listConnections()"}},
	}, "Bearer "+token)
	require.Equal(t, 200, response.Code, response.Body.String())
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Empty(t, body["errors"])
	result := body["results"].([]interface{})[0].(map[string]interface{})
	require.Equal(t, []interface{}{"connectionId", "connectTime", "connector", "username", "userAgent", "serverAddress", "clientAddress"}, result["columns"])
	data := result["data"].([]interface{})
	require.Len(t, data, 1)
	require.Equal(t, []interface{}{"bolt-17", "2026-10-02T00:00:00Z", "bolt", "reader", "live-driver", "127.0.0.1:7687", "127.0.0.1:12345"}, data[0].(map[string]interface{})["row"])
}

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
