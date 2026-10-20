package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/stretchr/testify/require"
)

// Calls go through the authenticated product router and real database executors.
// Neither the MCP handler nor the scoped executor is replaced by a test double.
func TestMCPNativeCypherTransport(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("tenant_a"))
	require.NoError(t, server.dbManager.CreateDatabase("private"))
	require.NoError(t, server.dbManager.CreateAlias("primary", "tenant_a"))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{"tenant_a"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", "tenant_a", true, false))
	host := httptest.NewServer(server.buildRouter())
	t.Cleanup(host.Close)
	admin := getAuthToken(t, authenticator, "admin")
	reader := getAuthToken(t, authenticator, "reader")

	call := func(token, path string, args map[string]interface{}) mcp.CallToolResponse {
		t.Helper()
		payload := map[string]interface{}{"name": "cypher", "arguments": args}
		if path == "/mcp" {
			payload = map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": payload}
		}
		body, err := json.Marshal(payload)
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, host.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := host.Client().Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		var result mcp.CallToolResponse
		if path == "/mcp" {
			var rpc struct {
				Result mcp.CallToolResponse `json:"result"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&rpc))
			result = rpc.Result
		} else {
			require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		}
		require.NotEmpty(t, result.Content)
		return result
	}
	success := func(result mcp.CallToolResponse) map[string]interface{} {
		t.Helper()
		require.False(t, result.IsError, result.Content)
		var value map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(result.Content[0].Text), &value))
		return value
	}

	t.Run("nested bound values and selected database", func(t *testing.T) {
		payload := map[string]interface{}{"nested": map[string]interface{}{"list": []interface{}{1, true, nil, "' MATCH (n) DELETE n //"}}}
		result := success(call(admin, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "CREATE (n:Item {payload: $payload}) RETURN n.payload AS payload", "parameters": map[string]interface{}{"payload": payload},
		}))
		require.Equal(t, payload, result["rows"].([]interface{})[0].([]interface{})[0])
		result = success(call(reader, "/mcp/tools/call", map[string]interface{}{
			"database": " PRIMARY ", "query": "MATCH (n:Item) RETURN n.payload AS payload",
		}))
		require.Len(t, result["rows"], 1)
	})

	t.Run("denied database and native USE", func(t *testing.T) {
		for _, args := range []map[string]interface{}{
			{"database": "private", "query": "RETURN 1"},
			{"database": "tenant_a", "query": "USE private RETURN 1"},
		} {
			result := call(reader, "/mcp", args)
			require.True(t, result.IsError, result.Content)
		}
	})

	t.Run("URL database pin", func(t *testing.T) {
		result := success(call(reader, "/mcp/tenant_a/tools/call", map[string]interface{}{
			"database": "private", "query": "MATCH (n:Item) RETURN n.payload",
		}))
		require.Len(t, result["rows"], 1)
	})

	t.Run("native write and nested procedure modes", func(t *testing.T) {
		for _, query := range []string{
			"// read-looking comment\nCREATE (:Forbidden)",
			"CALL apoc.cypher.run('CREATE (:Forbidden) RETURN 1 AS x', {})",
		} {
			result := call(reader, "/mcp/tools/call", map[string]interface{}{"database": "tenant_a", "query": query})
			require.True(t, result.IsError, result.Content)
			require.Contains(t, strings.ToLower(result.Content[0].Text), "write")
		}
		result := success(call(reader, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "MATCH (n:Forbidden) RETURN count(n) AS count",
		}))
		require.Equal(t, float64(0), result["rows"].([]interface{})[0].([]interface{})[0])
	})

	t.Run("native APOC bound procedure", func(t *testing.T) {
		result := success(call(reader, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "CALL apoc.cypher.run($statement, $params) YIELD value RETURN value.answer AS answer",
			"parameters": map[string]interface{}{"statement": "RETURN $answer AS answer", "params": map[string]interface{}{"answer": "bound"}},
		}))
		require.Equal(t, "bound", result["rows"].([]interface{})[0].([]interface{})[0])
	})
}
