//go:build closeout_l2_e53_7

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// E53.7: an HTTP explicit transaction that hit the 1 s transaction timeout
// answers TransactionTimedOut at 2 s, and TransactionNotFound (404) 31 s
// later. Expected (entry): the later request reports the same terminal
// status. The schedule is the entry's (2 s, then 31 s): the second step
// outlives the server's fixed 30 s session expiry. No authentication, as in
// the entry. The parser is the process's (NORNICDB_PARSER).

const (
	l2E53_7FirstWait  = 2 * time.Second
	l2E53_7SecondWait = 31 * time.Second
)

type l2E53_7Server struct {
	srv    *Server
	router http.Handler
}

func l2E53_7NewServer(t *testing.T) *l2E53_7Server {
	t.Helper()
	cfg := nornicdb.DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.TransactionTimeout = time.Second
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	srvCfg := DefaultConfig()
	srvCfg.EmbeddingEnabled = false
	srvCfg.MCPEnabled = false
	srv, err := New(db, nil, srvCfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})
	return &l2E53_7Server{srv: srv, router: srv.buildRouter()}
}

func (s *l2E53_7Server) post(path, statement string) (int, TransactionResponse, string) {
	body, _ := json.Marshal(map[string]interface{}{"statements": []interface{}{map[string]interface{}{"statement": statement}}})
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)
	var resp TransactionResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec.Code, resp, rec.Body.String()
}

func l2E53_7Code(resp TransactionResponse) string {
	if len(resp.Errors) == 0 {
		return ""
	}
	return resp.Errors[0].Code
}

// open runs step 1 and returns the transaction's path /db/nornic/tx/<id>.
func (s *l2E53_7Server) open(t *testing.T) string {
	t.Helper()
	code, resp, raw := s.post("/db/nornic/tx", "CREATE (:TTLProbe)")
	if code != http.StatusCreated || resp.Commit == "" {
		t.Fatalf("open transaction: status %d body %s", code, raw)
	}
	commit := resp.Commit
	if i := strings.Index(commit, "/db/"); i >= 0 {
		commit = commit[i:]
	}
	return strings.TrimSuffix(commit, "/commit")
}

func l2E53_7TimedOut(code string) bool {
	return strings.Contains(code, "Transaction.TransactionTimedOut")
}

func TestL2_e53_7_Repro(t *testing.T) {
	s := l2E53_7NewServer(t)
	txPath := s.open(t)

	time.Sleep(l2E53_7FirstWait)
	status2, resp2, raw2 := s.post(txPath, "RETURN 1")
	code2 := l2E53_7Code(resp2)

	time.Sleep(l2E53_7SecondWait)
	status3, resp3, raw3 := s.post(txPath, "RETURN 1")
	code3 := l2E53_7Code(resp3)

	ok := l2E53_7TimedOut(code2) && status3 == status2 && code3 == code2
	verdict := "OK"
	if !ok {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E53.7 %s parser=%s step2=[%d %s] step3=[%d %s]", verdict, config.GetParserType(), status2, code2, status3, code3)
	if !l2E53_7TimedOut(code2) {
		t.Errorf("step 2 (after %s): status %d code %q, want a TransactionTimedOut error; body %s", l2E53_7FirstWait, status2, code2, raw2)
	}
	if status3 != status2 || code3 != code2 {
		t.Errorf("step 3 (%s later): status %d code %q, want the same terminal status as step 2 (%d %q); body %s", l2E53_7SecondWait, status3, code3, status2, code2, raw3)
	}
}

// Control: a transaction used within its timeout runs RETURN 1 and commits;
// an unknown transaction ID is TransactionNotFound (404).
func TestL2_e53_7_Control(t *testing.T) {
	s := l2E53_7NewServer(t)
	txPath := s.open(t)
	status, resp, raw := s.post(txPath, "RETURN 1")
	if status != http.StatusOK || len(resp.Errors) != 0 || len(resp.Results) != 1 {
		t.Errorf("control: RETURN 1 within the timeout: status %d body %s", status, raw)
	}
	status, resp, raw = s.post(txPath+"/commit", "RETURN 2")
	if status != http.StatusOK || len(resp.Errors) != 0 {
		t.Errorf("control: commit within the timeout: status %d body %s", status, raw)
	}
	status, resp, _ = s.post("/db/nornic/tx/l2-e53-7-unknown-id", "RETURN 1")
	t.Logf("control unknown id -> %d %s", status, l2E53_7Code(resp))
	if status != http.StatusNotFound || l2E53_7Code(resp) != "Neo.ClientError.Transaction.TransactionNotFound" {
		t.Errorf("control: unknown transaction -> %d %q, want 404 TransactionNotFound", status, l2E53_7Code(resp))
	}
}

// Cause: two owners decide the two answers. The transaction's end follows
// Database.TransactionTimeout (the storage/executor end owner): step 2 gets
// TransactionTimedOut. The HTTP session registry (txsession.Manager, created
// with a fixed 30 s TTL in server.go:1212) then retires the ended session on
// its own expiry and forgets it, so handleExecuteInTransaction's
// GetForOwner lookup (server_db.go:1765) answers TransactionNotFound; the
// timed-out terminal status is not retained past the retirement. This polls
// the same transaction once a second after step 2 (bounded at 40 s) and logs
// the status sequence and the moment it changes, against the session's
// registration in the manager (Manager.Get).
func TestL2_e53_7_Cause(t *testing.T) {
	s := l2E53_7NewServer(t)
	txPath := s.open(t)
	txID := txPath[strings.LastIndex(txPath, "/")+1:]
	start := time.Now()
	time.Sleep(l2E53_7FirstWait)
	last := ""
	deadline := start.Add(l2E53_7FirstWait + 40*time.Second)
	for time.Now().Before(deadline) {
		status, resp, _ := s.post(txPath, "RETURN 1")
		state := http.StatusText(status) + " " + l2E53_7Code(resp)
		if state != last {
			_, registered := s.srv.txSessions.Get(txID)
			t.Logf("cause parser=%s t=%s status=%d code=%q registered=%t", config.GetParserType(), time.Since(start).Round(100*time.Millisecond), status, l2E53_7Code(resp), registered)
			last = state
		}
		if status == http.StatusNotFound {
			break
		}
		time.Sleep(time.Second)
	}
}

// l2E53_7Registered reports whether the session manager still holds txID,
// asked as the handler asks (GetForOwner with the request's owner key; with
// no authentication that is auth.PrincipalID(nil)) and ownerless (Get).
func (s *l2E53_7Server) l2E53_7Registered(txID string) (bool, bool) {
	_, owned := s.srv.txSessions.GetForOwner(txID, auth.PrincipalID(nil))
	_, anyOwner := s.srv.txSessions.Get(txID)
	return owned, anyOwner
}

// Cause2: the session is dropped inside the very request that reports
// TransactionTimedOut, not by the 30 s expiry. The timed-out statement is a
// failed statement: executeTxStatements returns failed=true and
// handleExecuteInTransaction calls rollbackRequestTransaction
// (pkg/server/server_db.go:1798-1800 -> :1667-1668 Lease.RollbackAndDelete),
// whose manager.rollbackAndDelete (pkg/txsession/manager.go:775) ends in
// deleteSessionLocked (:841-845: delete(m.sessions, id)); the next request's
// GetForOwner lookup (server_db.go:1765-1768) then answers 404. The test logs
// the registration right before and right after the timed-out request and
// sends the next request at once (no wait), and does the same for an ordinary
// failing statement (RETURN 1/0) inside the timeout: the same path, the same
// deletion, the same 404.
func TestL2_e53_7_Cause2(t *testing.T) {
	for _, c := range []struct {
		name      string
		wait      time.Duration
		statement string
	}{
		{"timed-out", l2E53_7FirstWait, "RETURN 1"},
		{"failing-statement", 0, "RETURN 1/0"},
	} {
		s := l2E53_7NewServer(t)
		txPath := s.open(t)
		txID := txPath[strings.LastIndex(txPath, "/")+1:]
		if c.wait > 0 {
			time.Sleep(c.wait)
		}
		ownedBefore, anyBefore := s.l2E53_7Registered(txID)
		status2, resp2, _ := s.post(txPath, c.statement)
		ownedAfter, anyAfter := s.l2E53_7Registered(txID)
		status3, resp3, _ := s.post(txPath, "RETURN 1")
		t.Logf("L2CAUSE E53.7 %s parser=%s registered_before=(owner %t, any %t) request=[%d %s] registered_after=(owner %t, any %t) next_request_immediately=[%d %s]",
			c.name, config.GetParserType(), ownedBefore, anyBefore, status2, l2E53_7Code(resp2), ownedAfter, anyAfter, status3, l2E53_7Code(resp3))
	}
}
