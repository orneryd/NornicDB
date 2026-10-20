package server

import (
	"bytes"
	"context"
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
	for _, config := range []*mcp.ServerConfig{nil, {NativeCypherEnabled: true}} {
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
	}
}

func TestMCPNativeReviewTemporalAndBytes(t *testing.T) {
	s, authenticator := setupTestServer(t)
	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "CREATE (:NativeBytes {value:$bytes})", map[string]interface{}{"bytes": []byte{0, 127, 128, 255}})
	require.NoError(t, err)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		_, result := nativeReviewCall(t, host, path, map[string]interface{}{
			"query":      "MATCH (n:NativeBytes) RETURN date('2024-01-02'), duration('P1DT2S'), n.value, {nested: [date('2024-01-02'), n.value]}, $big",
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
	}
}

func TestMCPNativeReviewCredentialForwarding(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	var mu sync.Mutex
	var seen string
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.Header.Get("Authorization")
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "credential missing", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[{"columns":["value"],"data":[{"row":[1],"meta":[null]}]}],"errors":[]}`)
	}))
	t.Cleanup(remote.Close)
	require.NoError(t, s.dbManager.CreateCompositeDatabase("native_cmp", []multidb.ConstituentRef{{Alias: "remote", DatabaseName: "native_remote", Type: "remote", AccessMode: "read", URI: remote.URL, AuthMode: "oidc_forwarding"}}))
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	credentials := map[string]func(*http.Request){
		"bearer":        func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) },
		"api_key":       func(r *http.Request) { r.Header.Set("X-API-Key", token) },
		"cookie":        func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "nornicdb_token", Value: token}) },
		"legacy_cookie": func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "token", Value: token}) },
		"query_token":   func(r *http.Request) { q := r.URL.Query(); q.Set("token", token); r.URL.RawQuery = q.Encode() },
		"query_api_key": func(r *http.Request) { q := r.URL.Query(); q.Set("api_key", token); r.URL.RawQuery = q.Encode() },
	}
	for name, authenticate := range credentials {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/mcp", "/mcp/tools/call"} {
				_, result := nativeReviewCall(t, host, path, map[string]interface{}{"database": "native_cmp", "query": "USE native_cmp.remote RETURN 1 AS value"}, authenticate)
				require.False(t, result.IsError, result.Content)
				mu.Lock()
				actual := seen
				mu.Unlock()
				require.Equal(t, "Bearer "+token, actual)
			}
		})
	}
}
