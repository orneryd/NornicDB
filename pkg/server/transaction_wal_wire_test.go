// Author: Codex, NOR-180 implementation lane, 2026-10-27.
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

func walHTTPFixture(t *testing.T) (*Server, *walterminal.Engine, func(string, string, string) (int, TransactionResponse)) {
	t.Helper()
	s, engine, request := walHTTPServer(t)
	return s, engine, func(method, path, body string) (int, TransactionResponse) {
		t.Helper()
		return request(t, method, path, body)
	}
}

// walHTTPServer builds the server, its outer store and its login once; each
// request is made by, and fails, the test that sends it (Claude, 30 Oct 2026).
func walHTTPServer(t *testing.T) (*Server, *walterminal.Engine, func(testing.TB, string, string, string) (int, TransactionResponse)) {
	t.Helper()
	s, auth := setupTestServer(t)
	engine := walterminal.New(t)
	newExec := func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic")), nil
	}
	s.txSessions = txsession.NewManager(time.Minute, newExec)
	exec, err := newExec("nornic")
	require.NoError(t, err)
	s.executorsMu.Lock()
	s.executors["nornic"] = exec
	s.executorsMu.Unlock()
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, auth, "admin")
	request := func(t testing.TB, method, path, body string) (int, TransactionResponse) {
		t.Helper()
		if !strings.HasPrefix(path, "http") {
			path = host.URL + path
		}
		req, err := http.NewRequest(method, path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := host.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var result TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		return resp.StatusCode, result
	}
	return s, engine, request
}
func walHTTPStatements(query string) string {
	b, _ := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: query}}})
	return string(b)
}
func walHTTPMessages(response TransactionResponse) string {
	messages := make([]string, len(response.Errors))
	for i, e := range response.Errors {
		messages[i] = e.Message
	}
	return strings.Join(messages, "\n")
}
func requireHTTPWALDiagnostic(t *testing.T, response TransactionResponse, closed bool) {
	t.Helper()
	message := walHTTPMessages(response)
	if closed {
		require.Contains(t, message, storage.ErrWALClosed.Error())
		require.Regexp(t, `failed to write WAL tx abort for [^\s:]+:`, message)
	} else {
		require.NotContains(t, message, "WAL tx abort")
	}
}

func TestHTTPWALTerminalCleanupResponses(t *testing.T) {
	for _, mode := range []string{"rollback", "statement", "execute_malformed", "commit_malformed", "execute_shape", "commit_shape", "open_statement", "oneshot_statement"} {
		for _, closed := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/open", true: "/closed"}[closed], func(t *testing.T) {
				_, engine, request := walHTTPFixture(t)
				if mode == "open_statement" || mode == "oneshot_statement" {
					path := "/db/nornic/tx"
					if mode == "oneshot_statement" {
						path += "/commit"
					}
					_, response := request(http.MethodPost, path, walHTTPStatements(walterminal.FailingProcedure(t, engine, closed)))
					require.NotEmpty(t, response.Errors)
					require.Contains(t, response.Errors[0].Message, "NOR-180 primary statement failure")
					requireHTTPWALDiagnostic(t, response, closed)
					require.Zero(t, walterminal.Count(t, engine))
					return
				}
				status, opened := request(http.MethodPost, "/db/nornic/tx", walHTTPStatements("CREATE (:WalWire)"))
				require.Equal(t, http.StatusCreated, status)
				require.Empty(t, opened.Errors)
				path := strings.TrimSuffix(opened.Commit, "/commit")
				require.NotEmpty(t, path)
				if closed {
					require.NoError(t, engine.WAL.Close())
				}
				method, body := http.MethodPost, walHTTPStatements("THIS IS INVALID")
				switch mode {
				case "rollback":
					method, body = http.MethodDelete, ""
				case "execute_malformed", "commit_malformed":
					body = "{"
				case "execute_shape", "commit_shape":
					body = `{"statements":"invalid"}`
				}
				if strings.HasPrefix(mode, "commit_") {
					path += "/commit"
				}
				status, response := request(method, path, body)
				// Transaction decoding classifies both malformed JSON and a wrong
				// statements shape as InvalidFormat with the existing HTTP 200.
				require.Equal(t, http.StatusOK, status)
				if mode == "rollback" && !closed {
					require.Empty(t, response.Errors)
				} else {
					require.NotEmpty(t, response.Errors)
				}
				if strings.Contains(mode, "malformed") || strings.Contains(mode, "shape") {
					require.Equal(t, "Neo.ClientError.Request.InvalidFormat", response.Errors[0].Code)
				}
				requireHTTPWALDiagnostic(t, response, closed)
				require.Zero(t, walterminal.Count(t, engine))
				status, _ = request(http.MethodDelete, strings.TrimSuffix(opened.Commit, "/commit"), "")
				require.Equal(t, http.StatusNotFound, status)
			})
		}
	}
}

func TestHTTPWALTerminalPrimaryStatusMessage(t *testing.T) {
	for _, kind := range []string{"timeout", "memory"} {
		for _, closed := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/open", true: "/closed"}[closed], func(t *testing.T) {
				_, engine, request := walHTTPFixture(t)
				primary := walterminal.Primary(kind)
				walterminal.RejectCommit(engine, primary, closed)
				status, response := request(http.MethodPost, "/db/nornic/tx/commit", walHTTPStatements("CREATE (:WalWire)"))
				require.Equal(t, http.StatusOK, status)
				require.Len(t, response.Errors, 1)
				code, message := nornicerrors.Neo4jStatus(primary)
				require.Equal(t, code, response.Errors[0].Code)
				require.Contains(t, response.Errors[0].Message, message)
				requireHTTPWALDiagnostic(t, response, closed)
				require.Zero(t, walterminal.Count(t, engine))
			})
		}
	}
}
