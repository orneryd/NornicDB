// HTTP end-to-end coverage for NOR-213's write-wait contract on the public
// protocol: two sessions write one existing node, or one existing
// relationship, with different properties through the explicit-transaction
// routes (/db/{db}/tx, /db/{db}/tx/{id}, /db/{db}/tx/{id}/commit).
//
// The schedule is the original two-session one. The first writer's
// MATCH ... SET runs in an open explicit transaction and holds the entity. The
// second session's MATCH ... SET, which writes the same entity with a
// different property, waits at its write instead of failing at commit; the
// first commit releases it; the second then writes on top of that commit, so
// both properties survive. Before NOR-213 the second commit failed with a
// transaction conflict.
//
// The storage lock-wait hook - not timing - orders the two writers, and every
// wait is bounded, so a failed assertion ends the waiting request instead of
// stranding it.
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

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// nor213HTTPShape is one entity two sessions write: the statements that create
// it, write its two properties from each session, and read them back.
type nor213HTTPShape struct {
	name string
	// seed creates the entity both sessions write.
	seed string
	// hold writes the first property, under the first writer's lock.
	hold string
	// write writes the second property from the waiting session.
	write string
	// after writes a third property from a session that must not wait.
	after string
	// readWriters returns both writers' properties.
	readWriters string
	// readAll returns both writers' properties and after's.
	readAll string
}

// nor213HTTPShapes are the two entities a writer contends on: a node and a
// relationship.
func nor213HTTPShapes() []nor213HTTPShape {
	return []nor213HTTPShape{
		{
			name:        "node",
			seed:        "CREATE (:Doc {id: $k})",
			hold:        "MATCH (n:Doc {id: $k}) SET n.a = 1",
			write:       "MATCH (n:Doc {id: $k}) SET n.b = 2",
			after:       "MATCH (n:Doc {id: $k}) SET n.c = 3",
			readWriters: "MATCH (n:Doc {id: $k}) RETURN n.a, n.b",
			readAll:     "MATCH (n:Doc {id: $k}) RETURN n.a, n.b, n.c",
		},
		{
			name:        "relationship",
			seed:        "CREATE (:Doc {id: $k})-[:HAS]->(:Part)",
			hold:        "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.a = 1",
			write:       "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.b = 2",
			after:       "MATCH (:Doc {id: $k})-[r:HAS]->() SET r.c = 3",
			readWriters: "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.a, r.b",
			readAll:     "MATCH (:Doc {id: $k})-[r:HAS]->() RETURN r.a, r.b, r.c",
		},
	}
}

// nor213HTTPServer serves a database whose async writes the subtest covers:
// durable commits, or commits acknowledged through the write-behind buffer.
func nor213HTTPServer(t *testing.T, writeBehind bool) (*Server, string) {
	t.Helper()
	server, authenticator := setupTestServerWithDatabaseConfig(t, func(config *nornicdb.Config) {
		config.Database.AsyncWritesEnabled = writeBehind
	}, nil)
	return server, "Bearer " + getAuthToken(t, authenticator, "admin")
}

// nor213HTTPWriter opens an explicit HTTP transaction and returns the holder
// that runs its statements and ends it.
func nor213HTTPWriter(t *testing.T, server *Server, token string) *httpKeyHolder {
	t.Helper()
	commit, errs := openHTTPTransaction(t, server, token)
	require.Empty(t, errs, "open an explicit HTTP transaction")
	require.NotEmpty(t, commit, "open an explicit HTTP transaction")
	return &httpKeyHolder{t: t, server: server, token: token, commit: commit}
}

// nor213TxEndpoint is the URL a statement is posted to inside an open
// explicit transaction.
func nor213TxEndpoint(commit string) string {
	return strings.TrimSuffix(commit, "/commit")
}

// postCypherStatement runs one statement on endpoint under ctx and reports the
// response's errors. It is the one HTTP statement helper: an ordinary caller
// passes t.Context(), and a statement that may wait for a lock passes the
// context of the request it must be able to cancel.
func postCypherStatement(ctx context.Context, t *testing.T, server *Server, token, endpoint, query string, params map[string]any) []any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}})
	if err != nil {
		return []any{err.Error()}
	}
	request := httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", token)
	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, request)
	var result map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		return []any{recorder.Body.String()}
	}
	errs, _ := result["errors"].([]any)
	return errs
}

// nor213HTTPRow runs a read and returns its only row.
func nor213HTTPRow(t *testing.T, server *Server, token, query string, params map[string]any) []any {
	t.Helper()
	response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
		"statements": []any{map[string]any{"statement": query, "parameters": params}},
	}, token)
	var payload TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload), response.Body.String())
	require.Empty(t, payload.Errors)
	require.Len(t, payload.Results, 1)
	require.Len(t, payload.Results[0].Data, 1)
	return payload.Results[0].Data[0].Row
}

// nor213RequireNoHTTPWriterWaits runs statement in an explicit transaction of
// its own and requires that it takes the entity's write lock without waiting:
// a writer queued behind an earlier session would make it wait. The lock-wait
// hook reports a wait at once; the statement's bound ends a wait the hook
// cannot shorten (a queued writer behind a session that no longer runs).
func nor213RequireNoHTTPWriterWaits(t *testing.T, server *Server, token string, waits <-chan struct{}, statement string, params map[string]any) {
	t.Helper()
	writer := nor213HTTPWriter(t, server, token)
	ctx, cancel := context.WithTimeout(context.Background(), httpKeyWaitStuckAfter)
	defer cancel()
	drainHTTPKeyWaits(waits)
	done := make(chan []any, 1)
	go func() {
		done <- postCypherStatement(ctx, t, server, token, nor213TxEndpoint(writer.commit), statement, params)
	}()
	select {
	case errs := <-done:
		require.Empty(t, errs, "a writer of the entity failed")
	case <-waits:
		t.Fatal("a writer of the entity waited although no session held its lock")
	}
	require.Empty(t, writer.end(true))
}

// TestHTTPSameEntityWritersWaitAndRetainBothProperties drives the two-session
// schedule over the HTTP explicit-transaction routes: the second writer waits
// for the first transaction, the first commit releases it, the second
// succeeds, and both writers' properties are committed. The database runs
// durable and with write-behind commits.
func TestHTTPSameEntityWritersWaitAndRetainBothProperties(t *testing.T) {
	for _, engineShape := range []struct {
		name        string
		writeBehind bool
	}{
		{"durable", false},
		{"write-behind", true},
	} {
		t.Run(engineShape.name, func(t *testing.T) {
			server, token := nor213HTTPServer(t, engineShape.writeBehind)
			waits := httpKeyWaitSignal(t)
			for _, shape := range nor213HTTPShapes() {
				key := shape.name + "-" + engineShape.name
				params := map[string]any{"k": key}
				require.Empty(t, postCypherStatement(t.Context(), t, server, token, "/db/nornic/tx/commit", shape.seed, params), key)

				// First session: an open explicit transaction whose write
				// holds the entity until it commits.
				holder := nor213HTTPWriter(t, server, token)
				require.Empty(t, holder.run(shape.hold, params), "%s: the first writer", key)

				// Second session: the same entity, a different property.
				waiter := nor213HTTPWriter(t, server, token)
				drainHTTPKeyWaits(waits)
				done := make(chan httpKeyWaitResult, 1)
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					started := time.Now()
					errs := postCypherStatement(context.Background(), t, server, token, nor213TxEndpoint(waiter.commit), shape.write, params)
					if len(errs) == 0 {
						errs = waiter.end(true)
					}
					done <- httpKeyWaitResult{errs: errs, elapsed: time.Since(started)}
				}()
				// A failed assertion must not strand the waiting request: the
				// holder's rollback releases the entity.
				t.Cleanup(func() {
					_ = holder.end(false)
					select {
					case <-finished:
					case <-time.After(httpKeyWaitStuckAfter):
						t.Errorf("%s: the second writer's request did not end", key)
					}
				})

				awaitHTTPKeyWait(t, waits, key+": the second writer")
				require.Empty(t, holder.end(true), "%s: the first writer commits", key)
				result := awaitHTTPKeyWaiter(t, done, func() { _ = holder.end(false) })
				require.Empty(t, result.errs, "%s: the second writer", key)
				require.Equal(t, []any{float64(1), float64(2)},
					nor213HTTPRow(t, server, token, shape.readWriters, params),
					"%s: both writers' properties are committed", key)
			}
		})
	}
}

// TestHTTPSameEntityWriterCancellationReleasesItsWait: the second writer's
// request is cancelled while its statement waits for the entity. The wait
// ends, the cancelled statement writes nothing, and the entity's write lock is
// left free: the holder's commit and the next writer both proceed.
func TestHTTPSameEntityWriterCancellationReleasesItsWait(t *testing.T) {
	for _, shape := range nor213HTTPShapes() {
		t.Run(shape.name, func(t *testing.T) {
			server, token := nor213HTTPServer(t, false)
			waits := httpKeyWaitSignal(t)
			key := "cancelled-" + shape.name
			params := map[string]any{"k": key}
			require.Empty(t, postCypherStatement(t.Context(), t, server, token, "/db/nornic/tx/commit", shape.seed, params))

			holder := nor213HTTPWriter(t, server, token)
			require.Empty(t, holder.run(shape.hold, params), key)
			waiter := nor213HTTPWriter(t, server, token)
			drainHTTPKeyWaits(waits)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan httpKeyWaitResult, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				started := time.Now()
				errs := postCypherStatement(ctx, t, server, token, nor213TxEndpoint(waiter.commit), shape.write, params)
				done <- httpKeyWaitResult{errs: errs, elapsed: time.Since(started)}
			}()
			t.Cleanup(func() {
				cancel()
				_ = holder.end(false)
				select {
				case <-finished:
				case <-time.After(httpKeyWaitStuckAfter):
					t.Errorf("%s: the cancelled statement did not end", key)
				}
			})

			awaitHTTPKeyWait(t, waits, key+": the second writer")
			cancel()
			require.NotEmpty(t, awaitHTTPKeyWaiter(t, done, func() { _ = holder.end(false) }).errs,
				"%s: a cancelled statement fails rather than committing", key)
			require.Empty(t, holder.end(true), "%s: the first writer commits", key)

			// The cancelled wait released the entity: the next writer takes it
			// without waiting and commits.
			nor213RequireNoHTTPWriterWaits(t, server, token, waits, shape.after, params)
			require.Equal(t, []any{float64(1), nil, float64(3)},
				nor213HTTPRow(t, server, token, shape.readAll, params),
				"%s: the cancelled writer committed nothing", key)
		})
	}
}
