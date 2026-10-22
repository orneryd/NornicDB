package server

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestHTTPTransactionMemoryPoolReloadPreservesRunningStatement(t *testing.T) {
	server, client, token := startMemoryHTTPServer(t)
	cypher.ConfigureTransactionMemory(64<<20, cypher.TransactionMemoryLimits{DatabaseTotalMax: 64 << 20})
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once, release sync.Once
	previous := cypher.PluginFunctionLookup
	cypher.PluginFunctionLookup = func(name string) (any, bool) {
		if name == "test.poolReloadBarrier" {
			return func() int64 { once.Do(func() { close(entered); <-proceed }); return 1 }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	done := make(chan error, 1)
	t.Cleanup(func() { release.Do(func() { close(proceed) }); cypher.PluginFunctionLookup = previous })
	go func() {
		_, body, _, err := client.do("/db/nornic/tx/commit", "UNWIND range(1, 200000) AS x WITH x WHERE test.poolReloadBarrier() = 1 RETURN size(collect(x)) AS n")
		if err == nil && len(body.Errors) > 0 {
			err = &poolReloadHTTPError{body.Errors[0].Message}
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("statement ended before its barrier: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("statement did not reach its barrier")
	}
	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{"db.memory.transaction.total.max": "1m"}}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	release.Do(func() { close(proceed) })
	require.NoError(t, <-done, "a hot reload must not revoke the running statement's pool ceiling")
	status, body, _ := client.post("/db/nornic/tx/commit", httpHeavyCollect)
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body.Errors, 1)
	require.True(t, strings.Contains(body.Errors[0].Message, "db.memory.transaction.total.max"), "%+v", body)
}

type poolReloadHTTPError struct{ message string }

func (e *poolReloadHTTPError) Error() string { return e.message }
