package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// participantCallbacksFixture is the one construction path for a participant
// binding case: a real FabricTransaction with the named shards opened at the
// requested access level, then the exact ParticipantCallbacks under test bound
// through the public BindParticipantCallbacks entry point. Each case supplies
// its own recorder, so it keeps every success, error and once-only expectation
// it had before it shared this construction.
type participantCallbacksFixture struct {
	tx        *FabricTransaction
	callbacks ParticipantCallbacks
}

// participantShard names one participant and the access it was opened with.
type participantShard struct {
	name  string
	write bool
}

func newParticipantCallbacksFixture(t *testing.T, txID string, shards []participantShard, callbacks ParticipantCallbacks) participantCallbacksFixture {
	t.Helper()
	tx := NewFabricTransaction(txID)
	for _, shard := range shards {
		if _, err := tx.GetOrOpen(shard.name, shard.write); err != nil {
			t.Fatalf("open participant %q: %v", shard.name, err)
		}
	}
	for _, shard := range shards {
		if err := tx.BindParticipantCallbacks(shard.name, callbacks); err != nil {
			t.Fatalf("bind participant %q: %v", shard.name, err)
		}
	}
	return participantCallbacksFixture{tx: tx, callbacks: callbacks}
}

// participantRecorder builds the ParticipantCallbacks for one participant and
// records how many times each terminal operation ran, plus the order when a
// case asks for it. Two cases share one recorder only when they observe the
// same participant; each still asserts its own run-for-run expectations.
type participantRecorder struct {
	commitErr, rollbackErr error
	outcome                ParticipantOutcome
	order                  *[]string

	commits, rollbacks int
}

func (rec *participantRecorder) callback() ParticipantCallbacks {
	return ParticipantCallbacks{
		Commit: func(*SubTransaction) error {
			rec.commits++
			if rec.order != nil {
				*rec.order = append(*rec.order, "commit")
			}
			return rec.commitErr
		},
		Rollback: func(*SubTransaction) error {
			rec.rollbacks++
			if rec.order != nil {
				*rec.order = append(*rec.order, "rollback")
			}
			return rec.rollbackErr
		},
		Outcome: rec.outcome,
	}
}

// remoteQueryEngineFixture is the one remote-engine construction path: the
// exact engine the case supplies is installed under the executor's own cache
// key for the location, so no transport is opened and the case's engine is the
// one the executor runs against. Installing before any participant opens is
// the precondition every remote participant case shares.
type remoteQueryEngineFixture struct {
	loc    *LocationRemote
	engine remoteCypherEngine
}

func newRemoteQueryEngineFixture(t *testing.T, loc *LocationRemote, engine remoteCypherEngine) remoteQueryEngineFixture {
	t.Helper()
	if loc == nil || engine == nil {
		t.Fatal("a remote query engine fixture needs both a location and an engine")
	}
	return remoteQueryEngineFixture{loc: loc, engine: engine}
}

// install caches the fixture's engine under the executor's exact cache key and
// returns the cache key so a case can assert on the release of that entry.
func (remote remoteQueryEngineFixture) install(t *testing.T, executor *RemoteFragmentExecutor) remoteEngineIdentity {
	t.Helper()
	key := cacheKey(remote.loc, "")
	executor.mu.Lock()
	executor.engineCache[key] = remote.engine
	cached := executor.engineCache[key]
	executor.mu.Unlock()
	if cached != remote.engine {
		t.Fatal("the fixture engine must be the one the executor caches")
	}
	return key
}

// execute runs one remote statement for the fixture's location inside tx,
// opening the participant at the access the mode names. Read and write cases
// differ only by mode and query, so they share this construction and keep
// their own assertions.
func (remote remoteQueryEngineFixture) execute(t *testing.T, executor *RemoteFragmentExecutor, tx *FabricTransaction, query string, mode storage.RemoteAccessMode) (*ResultStream, error) {
	t.Helper()
	return executor.Execute(participantContext(t, tx, remote.loc, mode == storage.RemoteAccessWrite), remote.loc, query, nil, "", mode)
}

// participantContext opens the participant for loc in tx and returns the
// context Execute needs. Every remote participant case opens it this way.
func participantContext(t *testing.T, tx *FabricTransaction, loc *LocationRemote, write bool) context.Context {
	t.Helper()
	sub, err := tx.GetOrOpen(ParticipantKeyFromLocation(loc), write)
	if err != nil {
		t.Fatalf("open participant: %v", err)
	}
	return WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
}

// remoteTxServerFixture is the one remote HTTP transaction API used by the
// remote participant lifecycle cases. It answers the transaction open, the
// statement, the commit and the rollback of exactly one transaction id, which
// is all a lifecycle case needs; the case supplies the database name, the id
// and the row the statement returns.
type remoteTxServerFixture struct {
	server *httptest.Server
	path   string
}

func newRemoteTxServer(t *testing.T, database, txID string, row any) remoteTxServerFixture {
	t.Helper()
	var server *httptest.Server
	path := fmt.Sprintf("/db/%s/tx/%s", database, txID)
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]any{"results": []any{}, "errors": []any{}}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == fmt.Sprintf("/db/%s/tx", database):
			response["commit"] = server.URL + path + "/commit"
		case r.Method == http.MethodPost && r.URL.Path == path:
			if row != nil {
				response["results"] = []any{map[string]any{
					"columns": []string{"x"},
					"data":    []any{map[string]any{"row": []any{row}}},
				}}
			}
		case r.Method == http.MethodPost && r.URL.Path == path+"/commit":
		case r.Method == http.MethodDelete && r.URL.Path == path:
		default:
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(server.Close)
	return remoteTxServerFixture{server: server, path: path}
}

// location is the remote a case addresses through this server.
func (server remoteTxServerFixture) location(database string) *LocationRemote {
	return &LocationRemote{URI: server.server.URL, DBName: database, AuthMode: "oidc_forwarding"}
}
