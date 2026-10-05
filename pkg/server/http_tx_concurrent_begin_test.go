package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Concurrent HTTP clients each get their own explicit transaction: every
// BEGIN returns a different commit URL and every client runs and commits its
// own transaction (#915).
func TestHTTPConcurrentBeginsGetDistinctTransactions(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	const readers, cycles = 8, 24

	var mu sync.Mutex
	commits := map[string]bool{}
	var failures []string
	fail := func(format string, args ...any) {
		mu.Lock()
		failures = append(failures, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	post := func(path string, statements ...map[string]any) (int, TransactionResponse) {
		if statements == nil {
			statements = []map[string]any{}
		}
		response := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": statements}, token)
		var result TransactionResponse
		_ = json.Unmarshal(response.Body.Bytes(), &result)
		return response.Code, result
	}

	var wg sync.WaitGroup
	for reader := 0; reader < readers; reader++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for cycle := 0; cycle < cycles; cycle++ {
				code, begun := post("/db/nornic/tx")
				if code != http.StatusCreated || len(begun.Errors) > 0 || begun.Commit == "" {
					fail("begin: %d %v", code, begun.Errors)
					continue
				}
				mu.Lock()
				duplicate := commits[begun.Commit]
				commits[begun.Commit] = true
				mu.Unlock()
				if duplicate {
					fail("duplicate commit URL %s", begun.Commit)
				}
				value := reader*cycles + cycle
				code, ran := post(strings.TrimSuffix(begun.Commit, "/commit"), map[string]any{"statement": "RETURN $value AS value", "parameters": map[string]any{"value": value}})
				if code != http.StatusOK || len(ran.Errors) > 0 || len(ran.Results) != 1 || len(ran.Results[0].Data) != 1 {
					fail("run: %d %v", code, ran.Errors)
					continue
				}
				if got := fmt.Sprint(ran.Results[0].Data[0].Row[0]); got != fmt.Sprint(value) {
					fail("run returned %s, want %d", got, value)
				}
				if code, committed := post(begun.Commit); code != http.StatusOK || len(committed.Errors) > 0 {
					fail("commit: %d %v", code, committed.Errors)
				}
			}
		}(reader)
	}
	wg.Wait()
	require.Empty(t, failures)
	require.Len(t, commits, readers*cycles)
}
