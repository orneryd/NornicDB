package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/stretchr/testify/require"
)

// committedTransactionBody is the success body a transaction handler writes for
// an outcome the phase owner admitted.
const (
	committedTransactionBody = `{"results":[{"columns":["k"],"data":[{"row":[7]}]}],"errors":[]}`
	shutdownCancelMessage    = "request canceled before commit: server shutting down"
	disconnectCancelMessage  = "request canceled before commit"
	uncommittedCancelCode    = "Neo.TransientError.General.DatabaseUnavailable"
)

// TestTransactionCancelBeforeCommitAdmissionReportsError is the deterministic
// regression for a transaction request whose context ends before terminal COMMIT
// admission. The retained handler stays blocked until that cancellation, exactly
// as the transport-observed worker does during Shutdown.Stop, and then writes the
// success body it would have published, so a false success would be visible.
func TestTransactionCancelBeforeCommitAdmissionReportsError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		shutdown  bool
		wantError string
	}{
		{name: "server shutdown cancels requests", shutdown: true, wantError: shutdownCancelMessage},
		{name: "caller disconnect cancels request", wantError: disconnectCancelMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{config: &Config{WriteTimeout: 10 * time.Second}}
			requestsCtx, cancelRequests := context.WithCancel(s.shutdown.work.Context())
			defer cancelRequests()
			t.Cleanup(func() {
				cancelRequests()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, s.shutdown.work.CloseContext(ctx))
			})
			entered, exited := make(chan struct{}), make(chan struct{})
			handler := s.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
				// A handler this request never admitted must not be able to
				// publish the transaction it did not commit.
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(committedTransactionBody))
				close(exited)
			}))
			req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil).WithContext(requestsCtx)
			rr := httptest.NewRecorder()
			responded := make(chan struct{})
			go func() {
				defer close(responded)
				handler.ServeHTTP(rr, req)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("retained transaction handler did not start")
			}
			// Shutdown.Stop seals admission and then signals the work lifetime
			// the HTTP transport's request contexts derive from, before it
			// drains HTTP.
			if tc.shutdown {
				require.True(t, s.shutdown.work.CloseAdmission())
				s.shutdown.work.Cancel()
			} else {
				cancelRequests()
			}
			select {
			case <-responded:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled transaction request did not return")
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("retained transaction handler did not retire")
			}
			require.Equal(t, http.StatusServiceUnavailable, rr.Code)
			var body TransactionResponse
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
			require.NotEmpty(t, body.Errors, "a canceled transaction must report why it ended")
			require.Equal(t, uncommittedCancelCode, body.Errors[0].Code)
			require.Equal(t, tc.wantError, body.Errors[0].Message)
			require.Empty(t, body.Results, "an uncommitted transaction must not publish results")
			require.NotContains(t, rr.Body.String(), `"errors":[]`, "the interrupted handler body must be discarded")
		})
	}
}

// TestTransactionCancelAfterCommitAdmissionPreservesCommittedResponse keeps the
// accepted terminal COMMIT outcome: the same cancellation that discards an
// unadmitted handler response must not replace an admitted commit's result.
func TestTransactionCancelAfterCommitAdmissionPreservesCommittedResponse(t *testing.T) {
	s := &Server{config: &Config{WriteTimeout: 10 * time.Second}}
	requestsCtx, cancelRequests := context.WithCancel(s.shutdown.work.Context())
	defer cancelRequests()
	admission := make(chan error, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	allowResponse := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		allowResponse()
		cancelRequests()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, s.shutdown.work.CloseContext(ctx))
	})
	handler := s.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := txphase.CommitContext(r.Context()); err != nil {
			admission <- err
			return
		}
		admission <- nil
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(committedTransactionBody))
	}))
	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil).WithContext(requestsCtx)
	rr := httptest.NewRecorder()
	responded := make(chan struct{})
	go func() {
		defer close(responded)
		handler.ServeHTTP(rr, req)
	}()
	select {
	case err := <-admission:
		require.NoError(t, err, "terminal COMMIT must be admitted before cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("terminal COMMIT was not admitted")
	}
	require.True(t, s.shutdown.work.CloseAdmission())
	s.shutdown.work.Cancel()
	allowResponse()
	select {
	case <-responded:
	case <-time.After(5 * time.Second):
		t.Fatal("admitted transaction request did not return")
	}
	require.Equal(t, http.StatusOK, rr.Code)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Empty(t, body.Errors)
	require.Len(t, body.Results, 1)
	require.Contains(t, rr.Body.String(), `"row":[7]`, "the admitted commit keeps its outcome")
}

// TestPublicHTTPShutdownCancelBeforeCommitAdmissionReportsError exercises the
// public HTTP transaction path with the existing server fixture: the router is
// the production router and the transport's request context derives from the
// work lifetime exactly as the HTTP listener installs it.
func TestPublicHTTPShutdownCancelBeforeCommitAdmissionReportsError(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	entered := make(chan struct{}, 1)
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "fixture.awaitShutdown", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	defer cypher.ClearUserProcedures()

	requestsCtx, cancelRequests := context.WithCancel(s.shutdown.work.Context())
	defer cancelRequests()
	host := httptest.NewUnstartedServer(s.buildRouter())
	host.Config.BaseContext = func(net.Listener) context.Context {
		return fabric.WithTransactionLifetime(requestsCtx, s.shutdown.work.Context())
	}
	host.Start()
	t.Cleanup(func() { cancelRequests(); host.Close() })

	post := func(body string) (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, host.URL+"/db/nornic/tx/commit", strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		return host.Client().Do(req)
	}
	// The same public path returns a real success before the shutdown, so the
	// cancellation response below cannot be an artifact of a broken fixture.
	warm, err := post(`{"statements":[{"statement":"RETURN 1 AS k"}]}`)
	require.NoError(t, err)
	warmBody, err := io.ReadAll(warm.Body)
	warm.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, warm.StatusCode, "public transaction path must succeed before shutdown")
	var warmResult TransactionResponse
	require.NoError(t, json.Unmarshal(warmBody, &warmResult))
	require.Empty(t, warmResult.Errors)

	type outcome struct {
		status int
		body   []byte
		err    error
	}
	canceled := make(chan outcome, 1)
	go func() {
		response, err := post(`{"statements":[{"statement":"CALL fixture.awaitShutdown() CREATE (:ShutdownCancellationProbe {value:1})"}]}`)
		if err != nil {
			canceled <- outcome{err: err}
			return
		}
		defer response.Body.Close()
		body, readErr := io.ReadAll(response.Body)
		canceled <- outcome{status: response.StatusCode, body: body, err: readErr}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("public transaction handler never reached the blocked statement")
	}
	// Shutdown.Stop: seal admission, then signal the work lifetime whose
	// cancellation the transport's request contexts derive from.
	require.True(t, s.shutdown.work.CloseAdmission())
	s.shutdown.work.Cancel()
	select {
	case result := <-canceled:
		require.NoError(t, result.err)
		require.Equal(t, http.StatusServiceUnavailable, result.status, "a canceled transaction must not report success")
		var body TransactionResponse
		require.NoError(t, json.Unmarshal(result.body, &body))
		require.NotEmpty(t, body.Errors)
		require.Equal(t, uncommittedCancelCode, body.Errors[0].Code)
		require.Equal(t, shutdownCancelMessage, body.Errors[0].Message)
		require.Empty(t, body.Results, "a canceled transaction must not publish results")
		require.NotContains(t, string(result.body), `"errors":[]`)
	case <-time.After(10 * time.Second):
		t.Fatal("canceled public transaction request did not return")
	}
	nodes, err := s.db.GetStorage().GetNodesByLabel(t.Context(), "ShutdownCancellationProbe")
	require.NoError(t, err)
	require.Empty(t, nodes, "the canceled write was not committed")
}
