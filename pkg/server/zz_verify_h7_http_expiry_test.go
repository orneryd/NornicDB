package server

// H7 verification helper (verify-h7 lane, scratch branch only; test-side, not product code).
// L-H7-008: does an HTTP explicit transaction whose MVCC snapshot the storage cancels
// (graceful expiry, Neo.TransientError.Transaction.Outdated) stay listed by SHOW TRANSACTIONS
// on the RUNNING server after the HTTP endpoint already answers TransactionNotFound for it?
// Same steps as TestExplicitTransactionLifecycleExpiration_EndsTransactionAndAuditsOnce
// (server_extra_test.go:736) up to the 404, then SHOW TRANSACTIONS over HTTP on the same server.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const h7ShowIdle = "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN transactionId"

func h7ListIdle(t *testing.T, server *Server, token string) []string {
	t.Helper()
	rec := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": h7ShowIdle}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp TransactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Empty(t, resp.Errors, rec.Body.String())
	ids := []string{}
	for _, result := range resp.Results {
		for _, row := range result.Data {
			if len(row.Row) > 0 {
				ids = append(ids, fmt.Sprint(row.Row[0]))
			}
		}
	}
	return ids
}

func h7NewIDs(before, after []string) []string {
	seen := map[string]bool{}
	for _, id := range before {
		seen[id] = true
	}
	added := []string{}
	for _, id := range after {
		if !seen[id] {
			added = append(added, id)
		}
	}
	return added
}

func h7OpenTx(t *testing.T, server *Server, token string) string {
	t.Helper()
	openRec := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]any{"statements": []any{}}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openRec.Code, openRec.Body.String())
	var openResp TransactionResponse
	require.NoError(t, json.Unmarshal(openRec.Body.Bytes(), &openResp))
	require.NotEmpty(t, openResp.Commit)
	return strings.TrimSuffix(openResp.Commit, "/commit")
}

// Control: an HTTP explicit transaction rolled back by the client is not listed afterwards.
func TestH7HTTPRolledBackTransactionNotListed(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	before := h7ListIdle(t, server, token)
	txPath := h7OpenTx(t, server, token)
	execRec := makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Doc {name:'rolled-back'})"}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execRec.Code, execRec.Body.String())
	during := h7NewIDs(before, h7ListIdle(t, server, token))
	t.Logf("H7_HTTP control: idle transactions while open: %v", during)
	require.Len(t, during, 1, "the open transaction is listed while it is open")
	delRec := makeRequest(t, server, http.MethodDelete, txPath, nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, delRec.Code, delRec.Body.String())
	after := h7NewIDs(before, h7ListIdle(t, server, token))
	t.Logf("H7_HTTP control: idle transactions after ROLLBACK: %v", after)
	require.Empty(t, after, "a rolled-back HTTP transaction is no longer listed")
}

// Disagreement: the expired transaction is gone for HTTP (404) but still listed.
func TestH7HTTPExpiredTransactionStillListed(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	storageEngine, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	storageEngine = unwrapInnerStorage(storageEngine)
	setter, ok := storageEngine.(interface {
		SetLifecycleController(storage.MVCCLifecycleController)
	})
	require.True(t, ok)
	controller := &expiringLifecycleController{}
	setter.SetLifecycleController(controller)
	defer setter.SetLifecycleController(nil)

	before := h7ListIdle(t, server, token)
	txPath := h7OpenTx(t, server, token)
	controller.gracefulExpire = true
	execRec := makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Doc {name:'expired'})"}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execRec.Code)
	var execResp TransactionResponse
	require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))
	require.Len(t, execResp.Errors, 1)
	require.Equal(t, "Neo.TransientError.Transaction.Outdated", execResp.Errors[0].Code)
	controller.gracefulExpire = false

	execRec = makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "RETURN 1"}},
	}, "Bearer "+token)
	t.Logf("H7_HTTP expired: next request on the transaction: HTTP %d %s", execRec.Code, strings.TrimSpace(execRec.Body.String()))
	require.Equal(t, http.StatusNotFound, execRec.Code)

	rollbackRec := makeRequest(t, server, http.MethodDelete, txPath, nil, "Bearer "+token)
	t.Logf("H7_HTTP expired: client ROLLBACK: HTTP %d %s", rollbackRec.Code, strings.TrimSpace(rollbackRec.Body.String()))

	after := h7NewIDs(before, h7ListIdle(t, server, token))
	t.Logf("H7_HTTP expired: idle transactions on the running server after the 404 and the client ROLLBACK: %v", after)
	if len(after) == 1 {
		termRec := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", after[0])}},
		}, "Bearer "+token)
		t.Logf("H7_HTTP expired: TERMINATE TRANSACTIONS %s: HTTP %d %s", after[0], termRec.Code, strings.TrimSpace(termRec.Body.String()))
		t.Logf("H7_HTTP expired: idle transactions after TERMINATE: %v", h7NewIDs(before, h7ListIdle(t, server, token)))
	}
	require.Empty(t, after, "SHOW TRANSACTIONS still lists the transaction the HTTP endpoint no longer knows")
}
