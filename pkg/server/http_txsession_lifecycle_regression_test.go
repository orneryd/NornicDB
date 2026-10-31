package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

type lifecycleBlockedResponseWriter struct {
	*httptest.ResponseRecorder
	once             sync.Once
	entered, release chan struct{}
}

func (w *lifecycleBlockedResponseWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.release
	})
	return w.ResponseRecorder.Write(p)
}

func lifecycleTransactionPost(t *testing.T, s *Server, token, path string, statements ...string) (int, TransactionResponse) {
	t.Helper()
	items := make([]map[string]any, 0, len(statements))
	for _, statement := range statements {
		items = append(items, map[string]any{"statement": statement})
	}
	rec := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": items}, token)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), rec.Body.String())
	return rec.Code, response
}

func lifecycleTransactionOwner(t *testing.T, s *Server, token string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx", nil)
	req.Header.Set("Authorization", token)
	var owner string
	s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		owner = transactionOwnerKey(r, getClaims(r))
	}, auth.PermRead)(httptest.NewRecorder(), req)
	require.NotEmpty(t, owner)
	return owner
}

func TestHTTPEmptyTransactionRequestRefreshesExpiry(t *testing.T) {
	s, authenticator := setupTestServer(t)
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	s.txSessions = txsession.NewManager(4*time.Second, s.newExecutorForDatabase)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx", "CREATE (:EmptyKeepAlive)")
	require.Equal(t, http.StatusCreated, status)
	path := strings.TrimSuffix(opened.Commit, "/commit")
	defer makeRequest(t, s, http.MethodDelete, path, nil, token)

	time.Sleep(2400 * time.Millisecond)
	status, kept := lifecycleTransactionPost(t, s, token, path)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, kept.Errors)
	time.Sleep(2400 * time.Millisecond)
	status, active := lifecycleTransactionPost(t, s, token, path, "RETURN 1")
	if status != http.StatusOK || len(active.Errors) != 0 {
		t.Fatalf("empty statement request failed to refresh transaction expiry: status=%d errors=%+v", status, active.Errors)
	}
}

type lifecycleGatedBody struct {
	io.Reader
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *lifecycleGatedBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return b.Reader.Read(p)
}

func (b *lifecycleGatedBody) Close() error { return nil }

func TestHTTPExpiryDuringRequestBodyDecodeReturnsNotFound(t *testing.T) {
	for _, endpoint := range []string{"run", "commit"} {
		t.Run(endpoint, func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			t.Cleanup(func() { _ = s.Stop(context.Background()) })
			s.txSessions = txsession.NewManager(300*time.Millisecond, s.newExecutorForDatabase)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx", "CREATE (:BodyDecodeExpiry)")
			require.Equal(t, http.StatusCreated, status)
			path := strings.TrimSuffix(opened.Commit, "/commit")
			body := `{"statements":[{"statement":"RETURN 1"}]}`
			if endpoint == "commit" {
				path += "/commit"
				body = `{"statements":[]}`
			}
			gated := &lifecycleGatedBody{Reader: strings.NewReader(body), entered: make(chan struct{}), release: make(chan struct{})}
			req := httptest.NewRequest(http.MethodPost, path, gated)
			req.Header.Set("Authorization", token)
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.buildRouter().ServeHTTP(recorder, req)
			}()
			select {
			case <-gated.entered:
			case <-time.After(3 * time.Second):
				close(gated.release)
				t.Fatal("handler did not begin decoding request body")
			}
			time.Sleep(700 * time.Millisecond)
			close(gated.release)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("handler did not finish after request body was released")
			}
			var response TransactionResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			if recorder.Code != http.StatusNotFound || len(response.Errors) != 1 || response.Errors[0].Code != "Neo.ClientError.Transaction.TransactionNotFound" {
				t.Fatalf("expired transaction during %s body decode: status=%d errors=%+v", endpoint, recorder.Code, response.Errors)
			}
		})
	}
}

func TestHTTPConcurrentRunsSerializeSessionExpiryState(t *testing.T) {
	s, authenticator := setupTestServer(t)
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	s.txSessions = txsession.NewManager(time.Minute, s.newExecutorForDatabase)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx", "RETURN 1")
	require.Equal(t, http.StatusCreated, status)
	path := strings.TrimSuffix(opened.Commit, "/commit")
	router := s.buildRouter()
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 8; j++ {
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"statements":[{"statement":"RETURN 1"}]}`))
				req.Header.Set("Authorization", token)
				req.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, req)
				if recorder.Code != http.StatusOK {
					t.Errorf("concurrent transaction run returned %d: %s", recorder.Code, recorder.Body.String())
				}
			}
		}()
	}
	group.Wait()
	_ = makeRequest(t, s, http.MethodDelete, path, nil, token)
}

func TestHTTPServerStopRollsBackAndRemovesExplicitTransactions(t *testing.T) {
	s, authenticator := setupTestServer(t)
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	s.txSessions = txsession.NewManager(time.Hour, s.newExecutorForDatabase)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx", "CREATE (:StopRollsBackTx)")
	require.Equal(t, http.StatusCreated, status)
	path := strings.TrimSuffix(opened.Commit, "/commit")
	id := path[strings.LastIndex(path, "/")+1:]
	owner := lifecycleTransactionOwner(t, s, token)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, s.Stop(ctx))
	if _, ok := s.txSessions.GetForOwner(id, owner); ok {
		t.Fatal("Server.Stop left explicit transaction registered")
	}

	check, err := s.db.GetCypherExecutor().Execute(context.Background(), "MATCH (n:StopRollsBackTx) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, check.Rows[0][0], "Server.Stop did not roll back the open transaction")
}

func TestHTTPServerStopHonorsDeadlineWithHeldRequestLease(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx")
	require.Equal(t, http.StatusCreated, status)
	path := strings.TrimSuffix(opened.Commit, "/commit")

	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	httpServer := &http.Server{}
	s.lifecycleMu.Lock()
	s.httpServer = httpServer
	s.lifecycleMu.Unlock()
	require.True(t, s.shutdown.work.OnStop(s.stopHTTPServer(httpServer, cancelRequests)))

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"statements":[]}`)).WithContext(requestCtx)
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	writer := &lifecycleBlockedResponseWriter{
		ResponseRecorder: httptest.NewRecorder(),
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		s.withAuth(func(w http.ResponseWriter, r *http.Request) {
			s.handleExecuteInTransaction(w, r, "nornic", path[strings.LastIndex(path, "/")+1:])
		}, auth.PermRead)(writer, req)
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		close(writer.release)
		t.Fatal("transaction handler did not reach its response write")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopResult := make(chan error, 1)
	started := time.Now()
	go func() { stopResult <- s.Stop(ctx) }()
	blocked := false
	var stopErr error
	select {
	case stopErr = <-stopResult:
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	if blocked {
		time.Sleep(300 * time.Millisecond)
	}
	close(writer.release)
	select {
	case <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("transaction handler did not finish after releasing the response")
	}
	if blocked {
		select {
		case stopErr = <-stopResult:
		case <-time.After(3 * time.Second):
			t.Fatal("Stop remained blocked after releasing the request lease")
		}
		t.Fatalf("Stop waited for the request lease past its deadline: returned after %s with %v", time.Since(started), stopErr)
	}
	require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestHTTPTransactionLeaseRejectsDatabaseMismatch(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx")
	require.Equal(t, http.StatusCreated, status)
	txID := strings.TrimSuffix(opened.Commit, "/commit")
	txID = txID[strings.LastIndex(txID, "/")+1:]
	owner := lifecycleTransactionOwner(t, s, token)
	req := httptest.NewRequest(http.MethodPost, "/db/other/tx/"+txID, strings.NewReader(`{"statements":[]}`))
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	var lease *txsession.Lease
	var ok bool
	s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		lease, ok = s.leaseTransactionOrNotFound(w, r, "other", txID, owner)
	}, auth.PermRead)(response, req)
	require.False(t, ok)
	require.Nil(t, lease)
	require.Equal(t, http.StatusNotFound, response.Code)
	if _, exists := s.txSessions.GetForOwner(txID, owner); !exists {
		t.Fatal("database mismatch removed the original transaction")
	}
}
