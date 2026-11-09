package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A MERGE waiting for a node key another transaction holds, over HTTP
// (NOR-284). The wait ends at the server's transaction timeout even when the
// client sets no deadline, when the request is cancelled, and with
// DeadlockDetected when it would close a cycle; one past the slow-query
// threshold is logged. A holder that commits or rolls back lets the waiter
// proceed as before.

const httpKeyWaitStuckAfter = 15 * time.Second

// httpKeyWaitLog collects the JSON log records the database writes.
type httpKeyWaitLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *httpKeyWaitLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *httpKeyWaitLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Reset()
}

// records returns the records with eventID and resets the log.
func (l *httpKeyWaitLog) records(t *testing.T, eventID string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record), scanner.Text())
		if record["event_id"] == eventID {
			out = append(out, record)
		}
	}
	l.buf.Reset()
	return out
}

// setupKeyWaitServer serves a database whose transaction timeout is timeout
// and whose slow-query threshold is logAfter, with a UNIQUE constraint on
// (:Doc {id}).
func setupKeyWaitServer(t *testing.T, timeout, logAfter time.Duration) (*Server, string, *httpKeyWaitLog) {
	t.Helper()
	log := &httpKeyWaitLog{}
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(config *nornicdb.Config) {
		config.Database.TransactionTimeout = timeout
		config.Logging.SlowQueryThreshold = logAfter
		config.Logger = slog.New(slog.NewJSONHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}, nil)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	_, err := server.db.ExecuteCypher(t.Context(), "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	return server, token, log
}

// httpKeyHolder is an open explicit transaction that has MERGEd keys.
type httpKeyHolder struct {
	t      *testing.T
	server *Server
	token  string
	commit string
}

func holdHTTPKeys(t *testing.T, server *Server, token string, keys ...string) *httpKeyHolder {
	t.Helper()
	commit, errs := openHTTPTransaction(t, server, token)
	require.Empty(t, errs)
	holder := &httpKeyHolder{t: t, server: server, token: token, commit: commit}
	for _, key := range keys {
		require.Empty(t, holder.run("MERGE (n:Doc {id: $k})", map[string]any{"k": key}))
	}
	return holder
}

// keepBusy keeps the holder's transaction working, with a statement every
// interval, until the returned stop: an HTTP client has no transaction
// timeout of its own, and a holder idle for the server's transaction timeout
// is ended by it (NOR-284 follow-up), so a test of the waiter's bound keeps
// its holder busy.
func (h *httpKeyHolder) keepBusy(interval time.Duration) (stop func()) {
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = postMerge(h.t, h.server, h.token, strings.TrimSuffix(h.commit, "/commit"), "RETURN 1", nil)
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

func (h *httpKeyHolder) run(query string, params map[string]any) []any {
	return postMerge(h.t, h.server, h.token, strings.TrimSuffix(h.commit, "/commit"), query, params)
}

func (h *httpKeyHolder) end(commit bool) []any {
	if !commit {
		response := makeRequest(h.t, h.server, http.MethodDelete, strings.TrimSuffix(h.commit, "/commit"), nil, h.token)
		var result map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			return []any{response.Body.String()}
		}
		errs, _ := result["errors"].([]any)
		return errs
	}
	return postMerge(h.t, h.server, h.token, h.commit, "RETURN 1", nil)
}

type httpKeyWaitResult struct {
	errs    []any
	elapsed time.Duration
}

// startHTTPKeyWaiter runs query auto-commit or in an explicit transaction of
// its own, under ctx, and reports its errors.
func startHTTPKeyWaiter(ctx context.Context, t *testing.T, server *Server, token string, explicit bool, query string, params map[string]any) <-chan httpKeyWaitResult {
	done := make(chan httpKeyWaitResult, 1)
	go func() {
		started := time.Now()
		endpoint := "/db/nornic/tx/commit"
		if explicit {
			commit, errs := openHTTPTransaction(t, server, token)
			if errs != nil {
				done <- httpKeyWaitResult{errs: errs}
				return
			}
			endpoint = commit
		}
		body, _ := json.Marshal(map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}})
		request := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		recorder := httptest.NewRecorder()
		server.buildRouter().ServeHTTP(recorder, request)
		var result map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
			done <- httpKeyWaitResult{errs: []any{recorder.Body.String()}, elapsed: time.Since(started)}
			return
		}
		errs, _ := result["errors"].([]any)
		done <- httpKeyWaitResult{errs: errs, elapsed: time.Since(started)}
	}()
	return done
}

func awaitHTTPKeyWaiter(t *testing.T, done <-chan httpKeyWaitResult, release func()) httpKeyWaitResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(httpKeyWaitStuckAfter):
		release()
		select {
		case <-done:
		case <-time.After(httpKeyWaitStuckAfter):
		}
		t.Fatalf("the MERGE was still waiting for the key after %s", httpKeyWaitStuckAfter)
		return httpKeyWaitResult{}
	}
}

func requireHTTPCode(t *testing.T, errs []any, code string, msgAndArgs ...any) {
	t.Helper()
	require.NotEmpty(t, errs, msgAndArgs...)
	first, _ := errs[0].(map[string]any)
	require.Equal(t, code, first["code"], "%v %v", errs, msgAndArgs)
}

func httpKeyWaitSignal(t *testing.T) <-chan struct{} {
	waits := make(chan struct{}, 64)
	t.Cleanup(storage.SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	}))
	return waits
}

func drainHTTPKeyWaits(waits <-chan struct{}) {
	for len(waits) > 0 {
		<-waits
	}
}

func awaitHTTPKeyWait(t *testing.T, waits <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-waits:
	case <-time.After(httpKeyWaitStuckAfter):
		t.Fatalf("%s never waited for the key", what)
	}
}

// requireHTTPKeyFree MERGEs key auto-commit: a lock an earlier waiter left
// held would make it wait.
func requireHTTPKeyFree(t *testing.T, server *Server, token, key string) {
	t.Helper()
	result := awaitHTTPKeyWaiter(t, startHTTPKeyWaiter(context.Background(), t, server, token, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": key}), func() {})
	require.Empty(t, result.errs, key)
	require.Less(t, result.elapsed, time.Second, "%s: a MERGE of a key nobody holds waited", key)
}

// TestHTTPMergeKeyWaitEndsAtTransactionTimeout: the holder never ends and
// the client sets no deadline. The waiter fails at the server's transaction
// timeout with TransactionTimedOut, releases the key it took before waiting,
// and the wait is logged once past the slow-query threshold.
func TestHTTPMergeKeyWaitEndsAtTransactionTimeout(t *testing.T) {
	const timeout, logAfter = time.Second, 200 * time.Millisecond
	server, token, log := setupKeyWaitServer(t, timeout, logAfter)
	for _, explicit := range []bool{false, true} {
		held, free := fmt.Sprintf("held-%v", explicit), fmt.Sprintf("free-%v", explicit)
		holder := holdHTTPKeys(t, server, token, held)
		stopBusy := holder.keepBusy(timeout / 5)
		log.reset()
		result := awaitHTTPKeyWaiter(t, startHTTPKeyWaiter(context.Background(), t, server, token, explicit, "UNWIND [$free, $held] AS k MERGE (n:Doc {id: k})", map[string]any{"free": free, "held": held}), func() {
			stopBusy()
			holder.end(false)
		})
		stopBusy()
		requireHTTPCode(t, result.errs, "Neo.ClientError.Transaction.TransactionTimedOut", "explicit=%v", explicit)
		require.GreaterOrEqual(t, result.elapsed, timeout, "explicit=%v", explicit)
		require.Less(t, result.elapsed, timeout+5*time.Second, "explicit=%v", explicit)

		records := log.records(t, "storage.transaction.lock_wait_prolonged")
		require.Len(t, records, 1, "explicit=%v", explicit)
		require.Equal(t, "WARN", records[0]["level"])
		require.Equal(t, "nornic", records[0]["database"])
		require.Equal(t, "Doc", records[0]["label"])
		require.Equal(t, "id", records[0]["property"])
		require.NotEmpty(t, records[0]["holder_transaction_id"])

		require.Equal(t, int64(0), countHTTPDocs(t, server, free), "the failed statement's MERGE is rolled back")
		requireHTTPKeyFree(t, server, token, free)
		require.Empty(t, holder.end(true))
		requireHTTPKeyFree(t, server, token, held)
		require.Equal(t, int64(1), countHTTPDocs(t, server, held))
	}
}

// TestHTTPMergeKeyWaitEndsWithItsRequest: a cancelled request ends the wait
// before the server's timeout, and the waiter's keys are released.
func TestHTTPMergeKeyWaitEndsWithItsRequest(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, time.Minute, 0)
	waits := httpKeyWaitSignal(t)
	for _, explicit := range []bool{false, true} {
		held, free := fmt.Sprintf("cancel-held-%v", explicit), fmt.Sprintf("cancel-free-%v", explicit)
		holder := holdHTTPKeys(t, server, token, held)
		drainHTTPKeyWaits(waits)
		ctx, cancel := context.WithCancel(context.Background())
		done := startHTTPKeyWaiter(ctx, t, server, token, explicit, "UNWIND [$free, $held] AS k MERGE (n:Doc {id: k})", map[string]any{"free": free, "held": held})
		awaitHTTPKeyWait(t, waits, "the request to cancel")
		cancel()
		awaitHTTPKeyWaiter(t, done, func() { holder.end(false) })
		requireHTTPKeyFree(t, server, token, free)
		require.Empty(t, holder.end(false))
	}
}

// TestHTTPMergeKeyWaitProceedsWhenTheHolderEnds: with the transaction
// timeout configured, a waiter still proceeds as soon as the holder commits
// (it matches the holder's node) or rolls back (it creates the node).
func TestHTTPMergeKeyWaitProceedsWhenTheHolderEnds(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, time.Minute, 0)
	waits := httpKeyWaitSignal(t)
	for _, commit := range []bool{true, false} {
		for _, explicit := range []bool{false, true} {
			key := fmt.Sprintf("ends-%v-%v", commit, explicit)
			holder := holdHTTPKeys(t, server, token, key)
			drainHTTPKeyWaits(waits)
			done := startHTTPKeyWaiter(context.Background(), t, server, token, explicit, "MERGE (n:Doc {id: $k}) ON MATCH SET n.matched = true ON CREATE SET n.created = true", map[string]any{"k": key})
			awaitHTTPKeyWait(t, waits, "the waiter")
			require.Empty(t, holder.end(commit))
			result := awaitHTTPKeyWaiter(t, done, func() {})
			require.Empty(t, result.errs, key)
			rows, err := server.db.ExecuteCypher(t.Context(), "MATCH (n:Doc {id: $k}) RETURN count(n), coalesce(n.matched, false), coalesce(n.created, false)", map[string]any{"k": key})
			require.NoError(t, err)
			require.Equal(t, []any{int64(1), commit, !commit}, rows.Rows[0], key)
		}
	}
}

// TestHTTPMergeKeyDeadlockFailsOneSide: two transactions each hold the key
// the other then MERGEs. One fails with DeadlockDetected; its keys are
// released, so the other proceeds and commits both nodes.
func TestHTTPMergeKeyDeadlockFailsOneSide(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, time.Minute, 0)
	waits := httpKeyWaitSignal(t)
	for _, explicit := range []bool{false, true} {
		k1, k2 := fmt.Sprintf("dl1-%v", explicit), fmt.Sprintf("dl2-%v", explicit)
		first := holdHTTPKeys(t, server, token, k1)
		drainHTTPKeyWaits(waits)
		// The second side holds k2 and waits for k1: as one auto-commit
		// statement, or an explicit transaction that MERGEd k2 first.
		var second <-chan httpKeyWaitResult
		var secondHolder *httpKeyHolder
		if explicit {
			secondHolder = holdHTTPKeys(t, server, token, k2)
			done := make(chan httpKeyWaitResult, 1)
			go func() {
				done <- httpKeyWaitResult{errs: secondHolder.run("MERGE (n:Doc {id: $k})", map[string]any{"k": k1})}
			}()
			second = done
		} else {
			second = startHTTPKeyWaiter(context.Background(), t, server, token, false, "UNWIND [$k2, $k1] AS k MERGE (n:Doc {id: k})", map[string]any{"k1": k1, "k2": k2})
		}
		awaitHTTPKeyWait(t, waits, "the second side")
		if firstErrs := first.run("MERGE (n:Doc {id: $k})", map[string]any{"k": k2}); len(firstErrs) > 0 {
			requireHTTPCode(t, firstErrs, "Neo.TransientError.Transaction.DeadlockDetected", "explicit=%v", explicit)
			first.end(false)
			require.Empty(t, awaitHTTPKeyWaiter(t, second, func() {}).errs, "explicit=%v", explicit)
			if secondHolder != nil {
				require.Empty(t, secondHolder.end(true))
			}
		} else {
			requireHTTPCode(t, awaitHTTPKeyWaiter(t, second, func() {}).errs, "Neo.TransientError.Transaction.DeadlockDetected", "explicit=%v", explicit)
			if secondHolder != nil {
				secondHolder.end(false)
			}
			require.Empty(t, first.end(true))
		}
		require.Equal(t, int64(1), countHTTPDocs(t, server, k1), "explicit=%v", explicit)
		require.Equal(t, int64(1), countHTTPDocs(t, server, k2), "explicit=%v", explicit)
	}
}

// TestHTTPMergeKeyTimeoutEndsTheTransactionWithoutClientCleanup (NOR-284,
// review round 1): an explicit HTTP transaction whose MERGE hits the server's
// transaction timeout holds nothing afterwards, though its client neither
// commits nor rolls it back: an unrelated schema change proceeds once the
// key's holder ends, and a later statement on the transaction fails.
func TestHTTPMergeKeyTimeoutEndsTheTransactionWithoutClientCleanup(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, 200*time.Millisecond, 0)
	holder := holdHTTPKeys(t, server, token, "held")
	stopBusy := holder.keepBusy(40 * time.Millisecond)
	waiter := holdHTTPKeys(t, server, token)
	requireHTTPCode(t, waiter.run("MERGE (n:Doc {id: 'held'})", nil), "Neo.ClientError.Transaction.TransactionTimedOut")
	stopBusy()
	require.Empty(t, holder.end(false))

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := server.db.ExecuteCypher(ctx, "CREATE CONSTRAINT http_unrelated FOR (n:HTTPUnrelated) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err, "the timed-out transaction kept its write admission until its client ended it")
	require.NotEmpty(t, waiter.run("RETURN 1", nil), "a statement on the timed-out transaction fails")
}
