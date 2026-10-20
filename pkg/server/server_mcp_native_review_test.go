package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func nativeReviewCall(t *testing.T, host *httptest.Server, path string, args map[string]interface{}, authenticate func(*http.Request)) ([]byte, mcp.CallToolResponse) {
	t.Helper()
	var payload interface{} = map[string]interface{}{"name": "cypher", "arguments": args}
	if path == "/mcp" {
		payload = map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": payload}
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, host.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if authenticate != nil {
		authenticate(req)
	}
	response, err := host.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(raw))
	var result mcp.CallToolResponse
	if path == "/mcp" {
		var rpc struct {
			Result mcp.CallToolResponse `json:"result"`
		}
		require.NoError(t, json.Unmarshal(raw, &rpc))
		result = rpc.Result
	} else {
		require.NoError(t, json.Unmarshal(raw, &result))
	}
	require.NotEmpty(t, result.Content)
	return raw, result
}

func TestMCPNativeReviewStandaloneDeniesAnonymous(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	enabled := mcp.DefaultServerConfig()
	enabled.NativeCypherEnabled = true
	for _, config := range []*mcp.ServerConfig{nil, enabled} {
		s := mcp.NewServer(db, config)
		mux := http.NewServeMux()
		s.RegisterRoutes(mux)
		for _, handler := range []http.Handler{s, mux} {
			host := httptest.NewServer(handler)
			for _, path := range []string{"/mcp", "/mcp/tools/call"} {
				_, result := nativeReviewCall(t, host, path, map[string]interface{}{"query": "CREATE (:AnonymousNative)"}, nil)
				require.True(t, result.IsError, result.Content)
			}
			host.Close()
		}
	}
	result, err := db.GetCypherExecutor().Execute(context.Background(), "MATCH (n:AnonymousNative) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestMCPNativeReviewExecutionAndWireBounds(t *testing.T) {
	s, authenticator := setupTestServer(t)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")
	authenticate := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, result := nativeReviewCall(t, host, path, map[string]interface{}{"query": "UNWIND range(1, 200000) AS x RETURN x", "max_rows": 1}, authenticate)
		runtime.ReadMemStats(&after)
		require.False(t, result.IsError, result.Content)
		var value struct {
			Rows       [][]interface{} `json:"rows"`
			Truncation struct {
				Reasons []string `json:"reasons"`
			} `json:"truncation"`
		}
		require.NoError(t, json.Unmarshal([]byte(result.Content[0].Text), &value))
		require.Len(t, value.Rows, 1)
		require.Contains(t, value.Truncation.Reasons, "max_rows")
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(16<<20), "row bound must stop production, not materialize 200000 rows")
		t.Logf("%s TotalAlloc delta=%d", path, after.TotalAlloc-before.TotalAlloc)
		for _, payload := range []string{strings.Repeat("x", 600000), strings.Repeat("\"\\<", 200000)} {
			raw, response := nativeReviewCall(t, host, path, map[string]interface{}{"query": "RETURN $payload AS payload", "parameters": map[string]interface{}{"payload": payload}}, authenticate)
			require.False(t, response.IsError, response.Content)
			require.LessOrEqual(t, len(raw), 1<<20, "bound includes text, structuredContent, escaping and envelope")
			require.NoError(t, json.Unmarshal([]byte(response.Content[0].Text), &value))
			require.Contains(t, value.Truncation.Reasons, "max_bytes")
		}
		// Blocking/native list-producing operations use the existing transaction
		// memory owner rather than materializing an unbounded result for the sink.
		for _, query := range []string{"RETURN range(1, 100000000)", "UNWIND range(1, 200000) AS x RETURN x ORDER BY x"} {
			_, response := nativeReviewCall(t, host, path, map[string]interface{}{"query": query, "max_rows": 1}, authenticate)
			require.True(t, response.IsError, response.Content)
			require.Contains(t, response.Content[0].Text, "db.memory.transaction.max threshold reached")
		}
	}
}

func TestMCPNativeReviewBoundsPreserveWrites(t *testing.T) {
	s, authenticator := setupTestServer(t)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")
	authenticate := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		for _, query := range []string{
			"UNWIND range(1, 40) AS x CREATE (:BoundWrite {x:x}) RETURN x",
			"UNWIND range(1, 40) AS x CALL apoc.cypher.run($statement, {x:x}) YIELD value RETURN value",
		} {
			_, result := nativeReviewCall(t, host, path, map[string]interface{}{
				"query": query, "max_rows": 1, "parameters": map[string]interface{}{"statement": "CREATE (:BoundWrite {x:$x}) RETURN $x AS x"},
			}, authenticate)
			require.False(t, result.IsError, result.Content)
			var value struct {
				Rows       [][]interface{} `json:"rows"`
				Truncation struct {
					Reasons []string `json:"reasons"`
				} `json:"truncation"`
			}
			require.NoError(t, json.Unmarshal([]byte(result.Content[0].Text), &value))
			require.Len(t, value.Rows, 1)
			require.Contains(t, value.Truncation.Reasons, "max_rows")
		}
	}
	executor, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)
	result, err := executor.Execute(context.Background(), "MATCH (n:BoundWrite) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(160)}}, result.Rows)
}

func TestMCPNativeReviewTemporalAndBytes(t *testing.T) {
	s, authenticator := setupTestServer(t)
	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "CREATE (:NativeBytes {value:$bytes, day:date('2024-01-02')})", map[string]interface{}{"bytes": []byte{0, 127, 128, 255}})
	require.NoError(t, err)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		_, result := nativeReviewCall(t, host, path, map[string]interface{}{
			"query":      "MATCH (n:NativeBytes) RETURN date('2024-01-02'), duration('P1DT2S'), n.value, {nested: [date('2024-01-02'), n.value]}, $big, point({x:1, y:2}), n",
			"parameters": map[string]interface{}{"big": int64(9007199254740993)},
		}, func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) })
		require.False(t, result.IsError, result.Content)
		var value struct {
			Rows [][]json.RawMessage `json:"rows"`
		}
		require.NoError(t, json.Unmarshal([]byte(result.Content[0].Text), &value))
		require.Equal(t, `"2024-01-02"`, string(value.Rows[0][0]))
		require.Equal(t, `"P1DT2S"`, string(value.Rows[0][1]))
		require.Equal(t, `[0,127,-128,-1]`, string(value.Rows[0][2]))
		require.Equal(t, `{"nested":["2024-01-02",[0,127,-128,-1]]}`, string(value.Rows[0][3]))
		require.Equal(t, "9007199254740993", string(value.Rows[0][4]))
		require.JSONEq(t, `{"type":"Point","coordinates":[1.0,2.0],"crs":{"srid":7203,"name":"cartesian","type":"link","properties":{"href":"https://spatialreference.org/ref/sr-org/7203/ogcwkt/","type":"ogcwkt"}}}`, string(value.Rows[0][5]))
		var node struct {
			ID         string                     `json:"id"`
			Properties map[string]json.RawMessage `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(value.Rows[0][6], &node))
		require.NotEmpty(t, node.ID)
		require.Equal(t, `"2024-01-02"`, string(node.Properties["day"]))
		require.Equal(t, `[0,127,-128,-1]`, string(node.Properties["value"]))
		_, vector := nativeReviewCall(t, host, path, map[string]interface{}{"query": "CYPHER 25 RETURN vector([1, 2], 2, INTEGER)"}, func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) })
		require.False(t, vector.IsError, vector.Content)
		require.NoError(t, json.Unmarshal([]byte(vector.Content[0].Text), &value))
		require.JSONEq(t, `{"typeName":"Int64Vector","incomparableType":true,"sequenceValue":false}`, string(value.Rows[0][0]))
	}
}

func TestMCPNativeReviewCredentialForwarding(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	var mu sync.Mutex
	var seen string
	require.NoError(t, s.dbManager.CreateDatabase("native_remote"))
	router := s.buildRouter()
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/db/native_remote/") {
			mu.Lock()
			seen = r.Header.Get("Authorization")
			mu.Unlock()
		}
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(host.Close)
	require.NoError(t, s.dbManager.CreateCompositeDatabase("native_cmp", []multidb.ConstituentRef{{Alias: "remote", DatabaseName: "native_remote", Type: "remote", AccessMode: "read", URI: host.URL, AuthMode: "oidc_forwarding"}}))
	credentials := map[string]func(*http.Request){
		"basic":         func(r *http.Request) { r.SetBasicAuth("admin", "password123") },
		"bearer":        func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) },
		"api_key":       func(r *http.Request) { r.Header.Set("X-API-Key", token) },
		"cookie":        func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "nornicdb_token", Value: token}) },
		"legacy_cookie": func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "token", Value: token}) },
		"query_token":   func(r *http.Request) { q := r.URL.Query(); q.Set("token", token); r.URL.RawQuery = q.Encode() },
		"query_api_key": func(r *http.Request) { q := r.URL.Query(); q.Set("api_key", token); r.URL.RawQuery = q.Encode() },
	}
	for name, authenticate := range credentials {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/mcp", "/mcp/tools/call", "/db/native_cmp/tx/commit"} {
				mu.Lock()
				seen = ""
				mu.Unlock()
				if strings.HasPrefix(path, "/db/") {
					request, err := http.NewRequest(http.MethodPost, host.URL+path, strings.NewReader(`{"statements":[{"statement":"USE native_cmp.remote RETURN 1 AS value"}]}`))
					require.NoError(t, err)
					request.Header.Set("Content-Type", "application/json")
					authenticate(request)
					response, err := host.Client().Do(request)
					require.NoError(t, err)
					raw, err := io.ReadAll(response.Body)
					require.NoError(t, response.Body.Close())
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, response.StatusCode, string(raw))
					var result TransactionResponse
					require.NoError(t, json.Unmarshal(raw, &result))
					require.Empty(t, result.Errors)
				} else {
					_, result := nativeReviewCall(t, host, path, map[string]interface{}{"database": "native_cmp", "query": "USE native_cmp.remote RETURN 1 AS value"}, authenticate)
					require.False(t, result.IsError, result.Content)
				}
				mu.Lock()
				actual := seen
				mu.Unlock()
				expected := "Bearer " + token
				if name == "basic" {
					expected = "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:password123"))
				}
				require.Equal(t, expected, actual)
			}
		})
	}
}
