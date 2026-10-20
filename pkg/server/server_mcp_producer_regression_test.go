package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestReview1601R2BoundedSinkSurvivesNativeRerouting(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "leading_USE", query: "USE bounded_use UNWIND range(1, 200000) AS x RETURN x"},
		{name: "quoted_identifier_rewrite", query: "UNWIND range(1, 200000) AS `x` RETURN `x`"},
		{name: "profile", query: "PROFILE UNWIND range(1, 200000) AS x RETURN x"},
		{name: "APOC_result_stream", query: "CALL apoc.cypher.run('UNWIND range(1, 200000) AS x RETURN x', {}) YIELD value RETURN value.x AS x"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			require.NoError(t, s.dbManager.CreateDatabase("bounded_use"))
			host := httptest.NewServer(s.buildRouter())
			t.Cleanup(host.Close)
			token := getAuthToken(t, authenticator, "admin")

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, response := nativeReviewCall(t, host, "/mcp", map[string]interface{}{
				"query": test.query, "max_rows": 1,
			}, func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+token) })
			runtime.ReadMemStats(&after)
			t.Logf("query=%q TotalAlloc delta=%d response=%s", test.query, after.TotalAlloc-before.TotalAlloc, response.Content[0].Text)

			require.False(t, response.IsError, "a max_rows response bound must not become a 16 MiB materialization failure")
			var result struct {
				Rows       [][]interface{} `json:"rows"`
				Truncation struct {
					Reasons []string `json:"reasons"`
				} `json:"truncation"`
			}
			require.NoError(t, json.Unmarshal([]byte(response.Content[0].Text), &result))
			require.Len(t, result.Rows, 1)
			require.Contains(t, result.Truncation.Reasons, "max_rows")
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(16<<20), "bounded production must not retain the full result")
		})
	}
}

type review1601CountingWriter struct {
	http.ResponseWriter
	remoteBytes *atomic.Int64
}

func (w review1601CountingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.remoteBytes.Add(int64(n))
	return n, err
}

func TestReview1601R2RemoteConstituentKeepsMCPBound(t *testing.T) {
	s, authenticator := setupTestServer(t)
	require.NoError(t, s.dbManager.CreateDatabase("review1601_remote"))
	router := s.buildRouter()
	var remoteBytes atomic.Int64
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/db/review1601_remote/") {
			w = review1601CountingWriter{ResponseWriter: w, remoteBytes: &remoteBytes}
		}
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(host.Close)
	require.NoError(t, s.dbManager.CreateCompositeDatabase("review1601_cmp", []multidb.ConstituentRef{{
		Alias: "remote", DatabaseName: "review1601_remote", Type: "remote", AccessMode: "read", URI: host.URL, AuthMode: "oidc_forwarding",
	}}))
	token := getAuthToken(t, authenticator, "admin")

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, response := nativeReviewCall(t, host, "/mcp", map[string]interface{}{
		"database": "review1601_cmp",
		"query":    "USE review1601_cmp.remote UNWIND range(1, 200000) AS x RETURN x",
		"max_rows": 1,
	}, func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+token) })
	runtime.ReadMemStats(&after)
	t.Logf("remote transaction bytes=%d TotalAlloc delta=%d response=%s", remoteBytes.Load(), after.TotalAlloc-before.TotalAlloc, response.Content[0].Text)

	require.False(t, response.IsError)
	var result struct {
		Rows       [][]interface{} `json:"rows"`
		Truncation struct {
			Reasons []string `json:"reasons"`
		} `json:"truncation"`
	}
	require.NoError(t, json.Unmarshal([]byte(response.Content[0].Text), &result))
	require.Len(t, result.Rows, 1)
	require.Contains(t, result.Truncation.Reasons, "max_rows")
	require.Less(t, remoteBytes.Load(), int64(1<<20), "the remote result must be streamed rather than fully materialized before the MCP sink")
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(16<<20), "the local Fabric/remote path must retain only the bounded MCP result")
}

func TestReview1601R2AdmissionRejectsUntrustedIdentity(t *testing.T) {
	s, authenticator := setupTestServer(t)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	const payload = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"cypher","arguments":{"query":"CREATE (:ForgedAdmission)"}}}`

	request := func(configure func(*http.Request)) (int, []byte) {
		req, err := http.NewRequest(http.MethodPost, host.URL+"/mcp", strings.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if configure != nil {
			configure(req)
		}
		response, err := host.Client().Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var raw json.RawMessage
		require.NoError(t, json.NewDecoder(response.Body).Decode(&raw))
		return response.StatusCode, raw
	}

	t.Run("forged_headers_without_authentication", func(t *testing.T) {
		status, _ := request(func(req *http.Request) {
			req.Header.Set("X-Authenticated-Principal", "sub:admin")
			req.Header.Set("X-NornicDB-Roles", "admin")
			req.Header.Set("X-Forwarded-User", "admin")
		})
		require.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("expired_token", func(t *testing.T) {
		user, err := authenticator.GetUser("admin")
		require.NoError(t, err)
		token, err := authenticator.GenerateAPIToken(user, "review-expired", time.Second)
		require.NoError(t, err)
		time.Sleep(2100 * time.Millisecond)
		status, _ := request(func(req *http.Request) { req.Header.Set("X-API-Key", token) })
		require.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("disabled_and_deleted_principals", func(t *testing.T) {
		for _, action := range []struct {
			name string
			do   func(string) error
		}{
			{name: "disabled", do: authenticator.DisableUser},
			{name: "deleted", do: authenticator.DeleteUser},
		} {
			t.Run(action.name, func(t *testing.T) {
				username := "review_" + action.name
				_, err := authenticator.CreateUser(username, "password123", []auth.Role{auth.RoleAdmin})
				require.NoError(t, err)
				token := getAuthToken(t, authenticator, username)
				require.NoError(t, action.do(username))
				status, _ := request(func(req *http.Request) { req.Header.Set("Authorization", "Bearer "+token) })
				require.Equal(t, http.StatusUnauthorized, status)
			})
		}
	})
}

func TestReview1601R2BlockingAggregationReleasesMemory(t *testing.T) {
	s, authenticator := setupTestServer(t)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")

	_, response := nativeReviewCall(t, host, "/mcp", map[string]interface{}{
		"query": "UNWIND range(1, 2000000) AS x RETURN collect(x)", "max_rows": 1,
	}, func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+token) })
	require.True(t, response.IsError, response.Content)
	require.Contains(t, response.Content[0].Text, "db.memory.transaction.max threshold reached")
	global, database := cypher.TransactionMemoryPoolUsage("nornic")
	require.Zero(t, global)
	require.Zero(t, database)
}
