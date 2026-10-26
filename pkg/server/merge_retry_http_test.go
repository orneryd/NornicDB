// Author: Claude, NOR-180 one MERGE retry classification owner, 2026-10-30.
// Release comparison: before, only Bolt classified an explicit transaction's
// commit-time MERGE race as retryable; HTTP reported the constraint violation.
// The transaction now classifies it itself, so every route reports the same.
// Self-contained (uses the outcome matrix's engine) so it runs on release.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

func TestHTTPMergeCommitRaceStatusIsTheTransactions(t *testing.T) {
	for _, cell := range []struct {
		name     string
		explicit bool
		query    string
		want     string
	}{
		{"explicit MERGE race is retryable", true, "MERGE (n:Doc {id: 'k'})", "Neo.TransientError.Transaction.Outdated"},
		{"implicit MERGE race is retryable", false, "MERGE (n:Doc {id: 'k'})", "Neo.TransientError.Transaction.Outdated"},
		{"explicit CREATE duplicate is a constraint violation", true, "CREATE (n:Doc {id: 'k'})", "Neo.ClientError.Schema.ConstraintValidationFailed"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			wal, err := storage.NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			engine := &httpMatrixEngine{MemoryEngine: storage.NewMemoryEngine(), wal: wal}
			t.Cleanup(func() { _ = engine.MemoryEngine.Close() })
			store := storage.NewNamespacedEngine(engine, "nornic")
			// The commit loses the race: another writer committed the key.
			engine.commit = func(_ context.Context, tx *storage.BadgerTransaction) error {
				_ = tx.Rollback()
				return fmt.Errorf("constraint violation: %w", &storage.ConstraintViolationError{
					Type: storage.ConstraintUnique, Label: "Doc", Properties: []string{"id"}, Message: "Node with id=k already exists",
				})
			}
			s, auth := setupTestServer(t)
			s.txSessions = txsession.NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) { return cypher.NewStorageExecutor(store), nil })
			s.executorsMu.Lock()
			s.executors["nornic"] = cypher.NewStorageExecutor(store)
			s.executorsMu.Unlock()
			token := "Bearer " + getAuthToken(t, auth, "admin")
			var errs []any
			if cell.explicit {
				commit, opened := openHTTPTransaction(t, s, token)
				require.Empty(t, opened)
				require.Empty(t, postMerge(t, s, token, strings.TrimSuffix(commit, "/commit"), cell.query, nil))
				response := makeRequest(t, s, http.MethodPost, commit, map[string]any{"statements": []any{}}, token)
				var committed map[string]any
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &committed))
				errs, _ = committed["errors"].([]any)
			} else {
				errs = postMerge(t, s, token, "/db/nornic/tx/commit", cell.query, nil)
			}
			require.NotEmpty(t, errs, "the commit must fail")
			first, _ := errs[0].(map[string]any)
			require.Equal(t, cell.want, first["code"], "status of a commit that lost a race: %v", errs)
		})
	}
}
