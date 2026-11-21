package server

// H7 verification helper (verify-h7 lane, scratch branch only; test-side, not product code).
// L-H7-008 follow-up (DECISIONS 23): is the ghost SHOW TRANSACTIONS entry reached by ordinary
// use of HTTP explicit transactions, without an MVCC snapshot cancellation? Each test opens an
// HTTP explicit transaction, ends it the ordinary way, waits for the server's own transaction
// timeout (30 s, server.go:1212) without touching it, then checks on the same running server:
// is it still listed by SHOW TRANSACTIONS, what does HTTP answer for it, and does TERMINATE
// TRANSACTIONS remove it. Uses h7ListIdle/h7NewIDs/h7OpenTx (zz_verify_h7_http_expiry_test.go).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const h7TxTimeoutWait = 36 * time.Second // server transaction timeout is 30 s

func h7GhostCheck(t *testing.T, server *Server, token, txPath string, before []string, label string) {
	t.Helper()
	listed := h7NewIDs(before, h7ListIdle(t, server, token))
	t.Logf("H7_ORD %s: idle transactions after the timeout, before touching the transaction: %v", label, listed)
	rec := makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "RETURN 1"}},
	}, "Bearer "+token)
	t.Logf("H7_ORD %s: request on the transaction: HTTP %d %s", label, rec.Code, strings.TrimSpace(rec.Body.String()))
	after := h7NewIDs(before, h7ListIdle(t, server, token))
	t.Logf("H7_ORD %s: idle transactions after that request: %v", label, after)
	for _, id := range after {
		term := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", id)}},
		}, "Bearer "+token)
		t.Logf("H7_ORD %s: TERMINATE TRANSACTIONS %s: HTTP %d %s", label, id, term.Code, strings.TrimSpace(term.Body.String()))
	}
	if len(after) > 0 {
		t.Logf("H7_ORD %s: idle transactions after TERMINATE: %v", label, h7NewIDs(before, h7ListIdle(t, server, token)))
	}
	require.Equal(t, http.StatusNotFound, rec.Code, "the server ended the transaction at its timeout")
	require.Empty(t, after, "SHOW TRANSACTIONS lists a transaction HTTP no longer knows")
}

// Ordinary 1: the client opens a transaction, writes, and never comes back.
func TestH7HTTPOrdinaryTimeoutExpiry(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	before := h7ListIdle(t, server, token)
	txPath := h7OpenTx(t, server, token)
	rec := makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Doc {name:'left-to-expire'})"}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	t.Logf("H7_ORD timeout: idle transactions while open: %v", h7NewIDs(before, h7ListIdle(t, server, token)))
	time.Sleep(h7TxTimeoutWait)
	h7GhostCheck(t, server, token, txPath, before, "timeout")
}

// Ordinary 2: the client goes away while its statement runs (request context cancelled),
// and never comes back.
func TestH7HTTPOrdinaryAbandonedMidStatement(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	before := h7ListIdle(t, server, token)
	txPath := h7OpenTx(t, server, token)
	body, err := json.Marshal(map[string]any{
		"statements": []map[string]any{{"statement": "UNWIND range(1, 200000000) AS x RETURN sum(x * x) AS s"}},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", txPath, bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	started := time.Now()
	go func() { server.buildRouter().ServeHTTP(recorder, req); close(done) }()
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the cancelled request did not return within 30 s")
	}
	t.Logf("H7_ORD abandon: cancelled request returned after %s: HTTP %d %s", time.Since(started).Round(time.Millisecond), recorder.Code, strings.TrimSpace(recorder.Body.String()))
	t.Logf("H7_ORD abandon: idle transactions right after the cancelled request: %v", h7NewIDs(before, h7ListIdle(t, server, token)))
	time.Sleep(h7TxTimeoutWait)
	h7GhostCheck(t, server, token, txPath, before, "abandon")
}

// Ordinary 3: a statement fails inside the transaction (Neo4j rolls the transaction back).
func TestH7HTTPOrdinaryStatementError(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	before := h7ListIdle(t, server, token)
	txPath := h7OpenTx(t, server, token)
	rec := makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Doc {name:'x'}) RETRUN n"}},
	}, "Bearer "+token)
	t.Logf("H7_ORD error: failing statement: HTTP %d %s", rec.Code, strings.TrimSpace(rec.Body.String()))
	t.Logf("H7_ORD error: idle transactions right after the failing statement: %v", h7NewIDs(before, h7ListIdle(t, server, token)))
	time.Sleep(h7TxTimeoutWait)
	h7GhostCheck(t, server, token, txPath, before, "error")
}
