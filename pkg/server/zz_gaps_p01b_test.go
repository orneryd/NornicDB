package server

// Report gap P01, second part (test-side only): does an explicit HTTP
// transaction whose statement failed with a terminal MVCC error take the
// txsession path that ends a session without ROLLBACK, and does its
// registry entry remain with and without the server stop?
//
// pkg/txsession/manager.go: a statement error classified terminal
// (isTerminalLifecycleError :876-886) is remembered as session.terminalErr
// and reported to the manager's terminal-error observer (:661-668); the
// ending paths then skip ROLLBACK when terminalErr is set: delete (:333),
// expiry (:569), COMMIT (:708-713), ROLLBACK (:790-794), and the close the
// server's stop runs (beginClose :442-454).
//
// Per case, on a fresh stop-R2 server: open POST /db/nornic/tx, run
// CALL gapp01b.fail() (the procedure returns the case's error), run RETURN 1
// in the same transaction, then end it (none / COMMIT / DELETE / server Stop).
// Logged: whether the terminal-error observer fired (the terminal branch),
// what the second statement returned, whether the session's executor still
// holds an active transaction (no ROLLBACK ran), whether the manager still
// has the session, and the change in SHOW TRANSACTIONS idle rows
// (process-wide registry) after the end and after the final server stop.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

func gapP01bIdle(t *testing.T) int {
	store := storage.NewMemoryEngine()
	defer func() { _ = store.Close() }()
	res, err := cypher.NewStorageExecutor(store).Execute(context.Background(), "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN transactionId", nil)
	if err != nil {
		t.Logf("GAPP01B probe error: %v", err)
		return -1
	}
	return len(res.Rows)
}

func gapP01bErrs(resp TransactionResponse) string {
	if len(resp.Errors) == 0 {
		return "none"
	}
	e := resp.Errors[len(resp.Errors)-1]
	msg := e.Message
	if len(msg) > 90 {
		msg = msg[:90]
	}
	return e.Code + ": " + msg
}

func gapP01bCase(t *testing.T, kind string, procErr error, end string) {
	s, _ := stopR2Server(t)
	var mu sync.Mutex
	var observed []string
	s.txSessions.SetTerminalErrorObserver(func(session *txsession.Session, err error) {
		mu.Lock()
		observed = append(observed, err.Error())
		mu.Unlock()
	})
	cypher.ClearUserProcedures()
	t.Cleanup(cypher.ClearUserProcedures)
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "gapp01b.fail", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
		if procErr == nil {
			return &cypher.ExecuteResult{Columns: []string{"ok"}, Rows: [][]interface{}{{true}}}, nil
		}
		return nil, procErr
	}))
	token := "Bearer " + getAuthToken(t, s.auth, "admin")
	base := gapP01bIdle(t)
	status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx")
	require.Equal(t, http.StatusCreated, status)
	path := strings.TrimSuffix(opened.Commit, "/commit")
	if i := strings.Index(path, "/db/"); i >= 0 {
		path = path[i:]
	}
	id := path[strings.LastIndex(path, "/")+1:]
	session, ok := s.txSessions.GetForOwner(id, lifecycleTransactionOwner(t, s, token))
	require.True(t, ok, "session for %s", id)
	exec := session.Executor
	_, first := lifecycleTransactionPost(t, s, token, path, "CALL gapp01b.fail()")
	_, second := lifecycleTransactionPost(t, s, token, path, "RETURN 1")
	activeAfterFailure := exec != nil && exec.HasActiveTransaction()
	idleAfterFailure := gapP01bIdle(t) - base
	switch end {
	case "commit":
		_, committed := lifecycleTransactionPost(t, s, token, path+"/commit")
		t.Logf("GAPP01B %s end=commit response=%s", kind, gapP01bErrs(committed))
	case "rollback":
		rec := makeRequest(t, s, http.MethodDelete, path, nil, token)
		t.Logf("GAPP01B %s end=rollback(DELETE) status=%d", kind, rec.Code)
	case "stop":
		t.Logf("GAPP01B %s end=server-stop err=%v", kind, s.Stop(context.Background()))
	}
	_, stillManaged := s.txSessions.GetForOwner(id, lifecycleTransactionOwner(t, s, token))
	activeAfterEnd := exec != nil && exec.HasActiveTransaction()
	idleAfterEnd := gapP01bIdle(t) - base
	if end != "stop" {
		_ = s.Stop(context.Background())
	}
	activeAfterStop := exec != nil && exec.HasActiveTransaction()
	idleAfterStop := gapP01bIdle(t) - base
	mu.Lock()
	obs := append([]string(nil), observed...)
	mu.Unlock()
	t.Logf("GAPP01B %s end=%s | first statement: %s | terminal-error observer fired %d time(s) %q | second statement in the same tx: %s | executor active tx: after failure=%t after end=%t after server stop=%t | manager still has session after end=%t | registry idle rows (delta): after failure=%d after end=%d after server stop=%d",
		kind, end, gapP01bErrs(first), len(obs), obs, gapP01bErrs(second), activeAfterFailure, activeAfterEnd, activeAfterStop, stillManaged, idleAfterFailure, idleAfterEnd, idleAfterStop)
}

func TestGapP01bTerminalBranch(t *testing.T) {
	for _, c := range []struct {
		kind string
		err  error
		ends []string
	}{
		{"terminal-hard-expired", storage.ErrMVCCSnapshotHardExpired, []string{"none", "commit", "rollback", "stop"}},
		{"terminal-graceful-cancel", storage.ErrMVCCSnapshotGracefulCancel, []string{"stop"}},
		{"control-ordinary-error", errors.New("gapp01b ordinary failure"), []string{"none", "stop"}},
		{"control-success", nil, []string{"none", "stop"}},
	} {
		for _, end := range c.ends {
			t.Run(fmt.Sprintf("%s/%s", c.kind, end), func(t *testing.T) { gapP01bCase(t, c.kind, c.err, end) })
		}
	}
}
