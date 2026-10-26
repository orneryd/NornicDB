// Author: Claude, NOR-180 committed-outcome retry contract, 30 October 2026.
// Uses the outcome matrix's fixtures, so it runs on release as a comparison.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

// A committed transaction whose post-commit action fails transiently is
// reported over HTTP with a status a retrying client does not retry (the
// pinned driver's own decision), implicit and explicit, with the logical WAL
// open or closed after the native commit. The primary is kept.
func TestHTTPWALTerminalCommittedOutcomeIsNotRetriable(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%v/closed=%v", explicit, closed), func(t *testing.T) {
				wal, err := storage.NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })
				engine := &httpMatrixEngine{MemoryEngine: storage.NewMemoryEngine(), wal: wal}
				t.Cleanup(func() { _ = engine.MemoryEngine.Close() })
				if closed {
					engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
						err := tx.CommitContext(ctx)
						if err == nil {
							_ = wal.Close()
						}
						return err
					}
				}
				store := storage.NewNamespacedEngine(engine, "nornic")
				_, err = store.CreateNode(&storage.Node{ID: "committed-retry", Labels: []string{"WalOutcome"}})
				require.NoError(t, err)
				faulty := &httpMatrixSuppression{Engine: store, cause: storage.ErrStorageRestoring}
				s, auth := setupTestServer(t)
				s.txSessions = txsession.NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) { return cypher.NewStorageExecutor(faulty), nil })
				s.executorsMu.Lock()
				s.executors["nornic"] = cypher.NewStorageExecutor(faulty)
				s.executorsMu.Unlock()
				token := "Bearer " + getAuthToken(t, auth, "admin")
				query := "MATCH (n:WalOutcome) CALL kp.suppress(n)"
				var errs []any
				if explicit {
					commit, opened := openHTTPTransaction(t, s, token)
					require.Empty(t, opened)
					require.Empty(t, postMerge(t, s, token, strings.TrimSuffix(commit, "/commit"), query, nil))
					response := makeRequest(t, s, http.MethodPost, commit, map[string]any{"statements": []any{}}, token)
					var committed map[string]any
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &committed))
					errs, _ = committed["errors"].([]any)
				} else {
					errs = postMerge(t, s, token, "/db/nornic/tx/commit", query, nil)
				}
				require.NotEmpty(t, errs, "the post-commit action failed")
				first, _ := errs[0].(map[string]any)
				code, _ := first["code"].(string)
				require.False(t, (&db.Neo4jError{Code: code}).IsRetriable(), "a retrying client must not retry committed work: %v", errs)
				require.Contains(t, fmt.Sprint(errs), storage.ErrStorageRestoring.Error(), "the reply keeps the primary")
				if closed {
					require.Contains(t, fmt.Sprint(errs), "committed, but failed to write WAL tx commit")
				}
			})
		}
	}
}
