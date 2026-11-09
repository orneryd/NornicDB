package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MERGE statements that write the node they match, racing over HTTP
// (NOR-117). In Neo4j 5.26 every statement succeeds and one node exists: a
// later writer waits for the earlier one. Before, the later commit failed
// with a UNIQUE violation.

var httpMergeWriteShapes = []string{
	"MERGE (n:Doc {id: $k}) ON CREATE SET n.c = 1 ON MATCH SET n.m = $w",
	"MERGE (n:Doc {id: $k}) SET n.w = $w",
	"MERGE (n:Doc {id: $k})",
}

// postMerge runs one statement on endpoint and returns its errors.
func postMerge(t *testing.T, server *Server, token, endpoint, query string, params map[string]any) []any {
	response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}}, token)
	var result map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		return []any{err.Error()}
	}
	errs, _ := result["errors"].([]any)
	return errs
}

// openHTTPTransaction opens an explicit transaction and returns its commit
// URL, or the response body when it didn't open.
func openHTTPTransaction(t *testing.T, server *Server, token string) (string, []any) {
	opened := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []any{}}, token)
	var tx TransactionResponse
	if err := json.Unmarshal(opened.Body.Bytes(), &tx); err != nil || tx.Commit == "" {
		return "", []any{opened.Body.String()}
	}
	return tx.Commit, nil
}

// runHTTPMerge runs query auto-commit or in an explicit transaction.
func runHTTPMerge(t *testing.T, server *Server, token string, explicit bool, query string, params map[string]any) []any {
	if !explicit {
		return postMerge(t, server, token, "/db/nornic/tx/commit", query, params)
	}
	commit, errs := openHTTPTransaction(t, server, token)
	if errs != nil {
		return errs
	}
	return postMerge(t, server, token, commit, query, params)
}

func countHTTPDocs(t *testing.T, server *Server, key string) any {
	result, err := server.db.ExecuteCypher(t.Context(), "MATCH (n:Doc {id: $k}) RETURN count(n)", map[string]any{"k": key})
	require.NoError(t, err)
	return result.Rows[0][0]
}

// TestHTTPMergeWritersOfOneNodeSerialize: the second statement starts while
// the first one's open transaction holds the node; the key wait, not
// timing, orders them.
func TestHTTPMergeWritersOfOneNodeSerialize(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	_, err := server.db.ExecuteCypher(t.Context(), "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	waits := make(chan struct{}, 64)
	t.Cleanup(storage.SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	}))
	for _, query := range httpMergeWriteShapes[:2] {
		for _, explicit := range []bool{false, true} {
			key := fmt.Sprintf("%s/%v", query, explicit)
			_, err := server.db.ExecuteCypher(t.Context(), "CREATE (:Doc {id: $k})", map[string]any{"k": key})
			require.NoError(t, err)
			for len(waits) > 0 {
				<-waits
			}
			commit, errs := openHTTPTransaction(t, server, token)
			require.Empty(t, errs)
			require.Empty(t, postMerge(t, server, token, strings.TrimSuffix(commit, "/commit"), query, map[string]any{"k": key, "w": 1}))
			done := make(chan []any, 1)
			go func() { done <- runHTTPMerge(t, server, token, explicit, query, map[string]any{"k": key, "w": 2}) }()
			var secondErrs []any
			waited := false
			select {
			case <-waits:
				waited = true
			case secondErrs = <-done:
			}
			committed := makeRequest(t, server, http.MethodPost, commit, map[string]any{"statements": []any{}}, token)
			var commitResult map[string]any
			require.NoError(t, json.Unmarshal(committed.Body.Bytes(), &commitResult))
			require.Empty(t, commitResult["errors"], key)
			if waited {
				secondErrs = <-done
			}
			require.Empty(t, secondErrs, key)
			require.True(t, waited, key)
			require.Equal(t, int64(1), countHTTPDocs(t, server, key), key)
		}
	}
}

func TestHTTPConcurrentMergeWritersConverge(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	_, err := server.db.ExecuteCypher(t.Context(), "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	var failures []string
	var mu sync.Mutex
	for s, query := range httpMergeWriteShapes {
		for _, explicit := range []bool{false, true} {
			for round := 0; round < 8; round++ {
				key := fmt.Sprintf("%d-%v-%d", s, explicit, round)
				var start, done sync.WaitGroup
				start.Add(1)
				for w := 0; w < 6; w++ {
					done.Add(1)
					go func() {
						defer done.Done()
						start.Wait()
						if errs := runHTTPMerge(t, server, token, explicit, query, map[string]any{"k": key, "w": w}); len(errs) > 0 {
							mu.Lock()
							failures = append(failures, fmt.Sprintf("%s explicit=%v: %v", query, explicit, errs))
							mu.Unlock()
						}
					}()
				}
				start.Done()
				done.Wait()
				if count := countHTTPDocs(t, server, key); count != int64(1) {
					failures = append(failures, fmt.Sprintf("%s explicit=%v: %v nodes", query, explicit, count))
				}
			}
		}
	}
	require.Empty(t, failures)
}
