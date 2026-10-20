package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
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

	call := func(t *testing.T, token, path string, args map[string]interface{}) mcp.CallToolResponse {
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
	success := func(t *testing.T, result mcp.CallToolResponse) map[string]interface{} {
		t.Helper()
		require.False(t, result.IsError, result.Content)
		var value map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(result.Content[0].Text), &value))
		require.Equal(t, value, result.StructuredContent)
		return value
	}

	t.Run("nested bound values and selected database", func(t *testing.T) {
		payload := map[string]interface{}{"nested": map[string]interface{}{"list": []interface{}{float64(1), true, nil, "' MATCH (n) DELETE n //"}}}
		result := success(t, call(t, admin, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "CREATE (n:Item {payload: $payload}) RETURN n.payload AS payload", "parameters": map[string]interface{}{"payload": payload},
		}))
		require.Equal(t, payload, result["rows"].([]interface{})[0].([]interface{})[0])
		result = success(t, call(t, reader, "/mcp/tools/call", map[string]interface{}{
			"database": " PRIMARY ", "query": "MATCH (n:Item) RETURN n.payload AS payload",
		}))
		require.Len(t, result["rows"], 1)
	})

	t.Run("denied database and native USE", func(t *testing.T) {
		for _, args := range []map[string]interface{}{
			{"database": "private", "query": "RETURN 1"},
			{"database": "tenant_a", "query": "USE private RETURN 1"},
		} {
			result := call(t, reader, "/mcp", args)
			require.True(t, result.IsError, result.Content)
		}
	})

	t.Run("URL database pin", func(t *testing.T) {
		result := success(t, call(t, reader, "/mcp/tenant_a/tools/call", map[string]interface{}{
			"database": "private", "query": "MATCH (n:Item) RETURN n.payload",
		}))
		require.Len(t, result["rows"], 1)
	})

	t.Run("native write and nested procedure modes", func(t *testing.T) {
		for _, query := range []string{
			"// read-looking comment\nCREATE (:Forbidden)",
			"CALL apoc.cypher.run('CREATE (:Forbidden) RETURN 1 AS x', {})",
		} {
			result := call(t, reader, "/mcp/tools/call", map[string]interface{}{"database": "tenant_a", "query": query})
			require.True(t, result.IsError, result.Content)
			require.Contains(t, strings.ToLower(result.Content[0].Text), "write")
		}
		result := success(t, call(t, reader, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "MATCH (n:Forbidden) RETURN count(n) AS count",
		}))
		require.Equal(t, float64(0), result["rows"].([]interface{})[0].([]interface{})[0])
	})

	t.Run("native APOC bound procedure", func(t *testing.T) {
		result := success(t, call(t, reader, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "CALL apoc.cypher.run($statement, $params) YIELD value RETURN value.answer AS answer",
			"parameters": map[string]interface{}{"statement": "RETURN $answer AS answer", "params": map[string]interface{}{"answer": "bound"}},
		}))
		require.Equal(t, "bound", result["rows"].([]interface{})[0].([]interface{})[0])
	})

	t.Run("integer precision and numeric types", func(t *testing.T) {
		for _, path := range []string{"/mcp", "/mcp/tools/call"} {
			result := success(t, call(t, reader, path, map[string]interface{}{
				"database": "tenant_a", "query": "RETURN $big = 9007199254740993 AS exact, $integer / 2 AS half",
				"parameters": map[string]interface{}{"big": int64(9007199254740993), "integer": 3},
			}))
			require.Equal(t, []interface{}{true, float64(1)}, result["rows"].([]interface{})[0])
		}
	})

	t.Run("native GDS stream and procedure mode", func(t *testing.T) {
		query := "CALL gds.graph.project($name, 'Item', '*')"
		args := map[string]interface{}{"database": "tenant_a", "query": query, "parameters": map[string]interface{}{"name": "mcp-native"}}
		require.True(t, call(t, reader, "/mcp", args).IsError)
		success(t, call(t, admin, "/mcp", args))
		result := success(t, call(t, reader, "/mcp", map[string]interface{}{
			"database": "tenant_a", "query": "CALL gds.louvain.stream($name) YIELD nodeId, communityId RETURN nodeId, communityId",
			"parameters": map[string]interface{}{"name": "mcp-native"},
		}))
		require.Len(t, result["rows"], 1)
	})

	t.Run("native retrieval eligibility evidence continuation and errors", func(t *testing.T) {
		store, err := server.dbManager.GetStorage("tenant_a")
		require.NoError(t, err)
		for _, node := range []*storage.Node{
			{ID: "excluded", Labels: []string{"Document"}, Properties: map[string]interface{}{"eligible": false, "content": "needle highest"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
			{ID: "best", Labels: []string{"Document"}, Properties: map[string]interface{}{"eligible": true, "content": "needle eligible best"}, ChunkEmbeddings: [][]float32{{1, 0.1, 0}}},
			{ID: "next", Labels: []string{"Document"}, Properties: map[string]interface{}{"eligible": true, "content": "needle eligible next"}, ChunkEmbeddings: [][]float32{{1, 0.5, 0}}},
		} {
			_, err := store.CreateNode(node)
			require.NoError(t, err)
		}
		executor, err := server.getExecutorForDatabase("tenant_a")
		require.NoError(t, err)
		service := search.NewServiceWithDimensions(store, 3)
		t.Cleanup(func() { service.Close() })
		require.NoError(t, service.BuildIndexes(context.Background()))
		executor.SetSearchService(service)
		request := map[string]interface{}{"query": "needle", "embedding": []float64{1, 0, 0}, "labels": []string{"Document"}, "where": "node.eligible = $allowed", "limit": 1, "min_similarity": 0.0}
		args := map[string]interface{}{"database": "tenant_a", "query": "CALL db.retrieve($request)", "parameters": map[string]interface{}{"request": request, "allowed": true}}
		result := success(t, call(t, admin, "/mcp", args))
		require.Len(t, result["rows"], 1, "ineligible highest-scoring candidate must not crowd out eligible rows")
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.Contains(t, string(encoded), "best")
		require.NotContains(t, string(encoded), "excluded")
		for _, column := range []string{"score", "search_method", "matches", "fallback_reason"} {
			require.Contains(t, result["columns"], column)
		}

		request["n"] = 1
		first := success(t, call(t, admin, "/mcp", args))
		page := first["rows"].([]interface{})[0].([]interface{})[0].(map[string]interface{})
		require.True(t, page["has_more"].(bool))
		qid := page["qid"].(string)
		pull := map[string]interface{}{"database": "tenant_a", "query": "CALL db.retrieve($request)", "parameters": map[string]interface{}{"request": map[string]interface{}{"qid": qid, "n": 1}}}
		require.True(t, call(t, reader, "/mcp", pull).IsError, "another principal cannot pull the cursor")
		second := success(t, call(t, admin, "/mcp", pull))
		page = second["rows"].([]interface{})[0].([]interface{})[0].(map[string]interface{})
		items := page["results"].([]interface{})
		require.Len(t, items, 1)
		node := items[0].(map[string]interface{})["node"].(map[string]interface{})
		require.Equal(t, true, node["properties"].(map[string]interface{})["eligible"], "continuation retains where and statement parameters")
		for _, query := range []string{
			"CALL db.retrieve({query: 'needle', failClosed: true})",
			"CALL db.retrieve({query: 'needle', where: 42})",
			"CALL db.retrieveRelationships({query: 'needle', n: 1})",
		} {
			failure := call(t, admin, "/mcp", map[string]interface{}{"database": "tenant_a", "query": query})
			require.True(t, failure.IsError, failure.Content)
			require.Nil(t, failure.StructuredContent)
		}
	})

	t.Run("bounded rows bytes native result limits and errors", func(t *testing.T) {
		result := success(t, call(t, admin, "/mcp", map[string]interface{}{"database": "tenant_a", "query": "UNWIND range(1, 4) AS x RETURN x", "max_rows": 2}))
		require.Len(t, result["rows"], 2)
		marker := result["truncation"].(map[string]interface{})
		require.True(t, marker["truncated"].(bool))
		require.Contains(t, marker["reasons"], "max_rows")
		result = success(t, call(t, admin, "/mcp", map[string]interface{}{"database": "tenant_a", "query": "RETURN $large AS large", "parameters": map[string]interface{}{"large": strings.Repeat("x", 1<<20)}}))
		require.Empty(t, result["rows"])
		require.Contains(t, result["truncation"].(map[string]interface{})["reasons"], "max_bytes")
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.LessOrEqual(t, len(encoded), 1<<20)

		limits := multidb.DefaultLimits()
		limits.Query.MaxResults = 1
		require.NoError(t, server.dbManager.SetDatabaseLimits("tenant_a", limits))
		defer func() { require.NoError(t, server.dbManager.SetDatabaseLimits("tenant_a", multidb.DefaultLimits())) }()
		result = success(t, call(t, admin, "/mcp", map[string]interface{}{"database": "tenant_a", "query": "UNWIND range(1, 3) AS bounded RETURN bounded"}))
		require.Len(t, result["rows"], 1)
		require.Contains(t, result["truncation"].(map[string]interface{})["reasons"], "database_max_results")
		for _, args := range []map[string]interface{}{
			{"query": "RETURN $missing"}, {"query": "RETURN 1", "parameters": []string{"bad"}},
			{"query": "RETURN 1", "max_rows": 0}, {"query": "RETURN 1", "max_rows": 1.5},
		} {
			args["database"] = "tenant_a"
			require.True(t, call(t, admin, "/mcp", args).IsError)
		}
	})

	t.Run("database execution timeout stays an error", func(t *testing.T) {
		limits := multidb.DefaultLimits()
		limits.Query.MaxQueryTime = 20 * time.Millisecond
		require.NoError(t, server.dbManager.SetDatabaseLimits("tenant_a", limits))
		defer func() { require.NoError(t, server.dbManager.SetDatabaseLimits("tenant_a", multidb.DefaultLimits())) }()
		failure := call(t, admin, "/mcp", map[string]interface{}{"database": "tenant_a", "query": "UNWIND range(1, 100000000) AS x WITH x WHERE x < 0 RETURN count(x)"})
		require.True(t, failure.IsError, failure.Content)
	})

	t.Run("failClosed provider failure remains visible", func(t *testing.T) {
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "embedding backend unavailable", http.StatusServiceUnavailable)
		}))
		defer provider.Close()
		server.mcpServer.SetEmbedder(embed.NewOpenAI(&embed.Config{APIURL: provider.URL, APIPath: "/embeddings", Model: "test", Dimensions: 3, Timeout: 5 * time.Second}))
		defer server.mcpServer.SetEmbedder(nil)
		failure := call(t, admin, "/mcp", map[string]interface{}{"database": "tenant_a", "query": "CALL db.retrieve({query: 'needle', failClosed: true})"})
		require.True(t, failure.IsError, failure.Content)
		require.Contains(t, failure.Content[0].Text, "embedding backend unavailable")
	})

	t.Run("request cancellation reaches native embedding HTTP request", func(t *testing.T) {
		started := make(chan struct{})
		stopped := make(chan struct{})
		provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			close(started)
			select {
			case <-r.Context().Done():
				close(stopped)
			case <-time.After(5 * time.Second):
			}
		}))
		defer provider.Close()
		server.mcpServer.SetEmbedder(embed.NewOpenAI(&embed.Config{APIURL: provider.URL, APIPath: "/embeddings", Model: "test", Dimensions: 3, Timeout: 10 * time.Second}))
		defer server.mcpServer.SetEmbedder(nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"cypher","arguments":{"database":"tenant_a","query":"CALL db.retrieve({query: 'needle', failClosed: true})"}}}`
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, host.URL+"/mcp", strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+admin)
		req.Header.Set("Content-Type", "application/json")
		done := make(chan error, 1)
		go func() {
			response, err := host.Client().Do(req)
			if response != nil {
				response.Body.Close()
			}
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("native embedding did not start")
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("MCP request did not cancel")
		}
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation did not reach the native embedding provider")
		}
	})
}
