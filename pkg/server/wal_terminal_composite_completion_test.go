// Author: Codex, NOR-180 explicit composite HTTP completion, 2026-11-01.
package server

import (
	"encoding/json"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPWALTerminalCompositeExplicitCompletion(t *testing.T) {
	for _, mode := range []string{"healthy", "wal", "action", "both", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			f := walterminal.NewCompositeTerminal(t)
			s, auth := setupTestServer(t)
			s.txSessions = txsession.NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) { return f.Executor, nil })
			host := httptest.NewServer(s.buildRouter())
			t.Cleanup(host.Close)
			token := getAuthToken(t, auth, "admin")
			request := func(method, path, body string) TransactionResponse {
				req, err := http.NewRequest(method, path, strings.NewReader(body))
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")
				resp, err := host.Client().Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				require.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated, "status %d", resp.StatusCode)
				var out TransactionResponse
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				return out
			}
			encode := func(query string) string {
				b, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: query}}})
				require.NoError(t, err)
				return string(b)
			}
			opened := request(http.MethodPost, host.URL+"/db/nornic/tx", encode("USE cmp.local CREATE (:WalWire)"))
			require.Empty(t, opened.Errors)
			require.NotEmpty(t, opened.Commit)
			txURL := strings.TrimSuffix(opened.Commit, "/commit")
			if mode == "action" || mode == "both" {
				f.ActionError = storage.ErrStorageRestoring
				out := request(http.MethodPost, txURL, encode("USE cmp.local MATCH (n:WalWire) CALL kp.suppress(n)"))
				require.Empty(t, out.Errors)
			}
			f.CloseOnCommit = mode == "wal" || mode == "both"
			expected := int64(1)
			var out TransactionResponse
			if mode == "rollback" {
				expected = 0
				require.NoError(t, f.WAL.Close())
				out = request(http.MethodDelete, txURL, "")
			} else {
				out = request(http.MethodPost, opened.Commit, `{"statements":[]}`)
			}
			require.Equal(t, expected, f.Count(t))
			if mode == "healthy" {
				require.Empty(t, out.Errors)
				return
			}
			require.NotEmpty(t, out.Errors, "completion must not become an HTTP success envelope")
			code := out.Errors[0].Code
			require.False(t, (&db.Neo4jError{Code: code}).IsRetriable())
			if mode != "rollback" {
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
			}
			var messages []string
			for _, e := range out.Errors {
				messages = append(messages, e.Message)
			}
			message := strings.Join(messages, "\n")
			if mode == "wal" || mode == "both" || mode == "rollback" {
				require.Contains(t, message, storage.ErrWALClosed.Error())
			}
			if mode == "action" || mode == "both" {
				require.Contains(t, message, storage.ErrStorageRestoring.Error())
			}
		})
	}
}
