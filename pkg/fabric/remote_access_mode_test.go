package fabric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// accessModeRemote is a remote's HTTP transaction API that records, for each
// transaction it opens, its access mode, the statements run in it and how it
// ended.
type accessModeRemote struct {
	mu           sync.Mutex
	srv          *httptest.Server
	txs          []*accessModeRemoteTx
	reportedMode string
	onQuery      func()
}

type accessModeRemoteTx struct {
	mode       string
	statements []string
	ended      string
}

func newAccessModeRemote(t *testing.T) *accessModeRemote {
	remote := &accessModeRemote{}
	remote.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remote.mu.Lock()
		defer remote.mu.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/db/tenant/tx")
		if path == "" && r.Method == http.MethodPost {
			mode := r.Header.Get("access-mode")
			if mode == "" {
				mode = "WRITE"
			}
			remote.txs = append(remote.txs, &accessModeRemoteTx{mode: mode})
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{},
				"commit": fmt.Sprintf("%s/db/tenant/tx/%d/commit", remote.srv.URL, len(remote.txs))})
			return
		}
		var id int
		var action string
		if _, err := fmt.Sscanf(strings.Replace(path, "/commit", " commit", 1), "/%d %s", &id, &action); err != nil {
			_, _ = fmt.Sscanf(path, "/%d", &id)
		}
		if id < 1 || id > len(remote.txs) {
			http.NotFound(w, r)
			return
		}
		tx := remote.txs[id-1]
		switch {
		case r.Method == http.MethodDelete:
			tx.ended = "rollback"
		case action == "commit":
			tx.ended = "commit"
		default:
			if remote.onQuery != nil {
				remote.onQuery()
			}
			var body struct {
				Statements []struct {
					Statement string `json:"statement"`
				} `json:"statements"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			for _, statement := range body.Statements {
				tx.statements = append(tx.statements, statement.Statement)
			}
			result := map[string]any{"columns": []string{"n"}, "data": []any{map[string]any{"row": []any{1}}}}
			if remote.reportedMode != "" {
				result["query_type"] = remote.reportedMode
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{}, "results": []any{result}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
	}))
	t.Cleanup(remote.srv.Close)
	return remote
}

func (r *accessModeRemote) sessions() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	parts := make([]string, 0, len(r.txs))
	for _, tx := range r.txs {
		parts = append(parts, fmt.Sprintf("%s%v:%s", tx.mode, tx.statements, tx.ended))
	}
	return strings.Join(parts, " ")
}

// A remote fragment that only reads runs in a read session, which ends with
// a rollback when the statement commits: nothing run in it is kept, also on a
// remote that doesn't enforce the access mode. A fragment that writes opens
// the write session only once the remote is the statement's write
// participant (GetOrOpen); the read session ends first, and every later
// query on the remote runs in the write session, which commits.
func TestRemoteParticipantOpensWriteSessionOnlyWhenItClaimsTheWrite(t *testing.T) {
	remote := newAccessModeRemote(t)
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()
	loc := &LocationRemote{URI: remote.srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-remote-modes")
	sub, err := tx.GetOrOpen(ParticipantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	for _, request := range []struct {
		query string
		mode  storage.RemoteAccessMode
	}{
		{"MATCH (n) RETURN n", storage.RemoteAccessRead}, {"CREATE (n) RETURN n", storage.RemoteAccessWrite}, {"MATCH (n) RETURN n", storage.RemoteAccessRead},
	} {
		if _, err := re.Execute(ctx, loc, request.query, nil, "", request.mode); err != nil {
			t.Fatalf("%s: %v", request.query, err)
		}
	}
	if tx.WriteShard() != sub.ShardName {
		t.Fatalf("the remote did not become the writer: %q", tx.WriteShard())
	}
	if err := tx.Commit(nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := remote.sessions(); got != "READ[MATCH (n) RETURN n]:rollback WRITE[CREATE (n) RETURN n MATCH (n) RETURN n]:commit" {
		t.Fatalf("sessions: %s", got)
	}
}

// A read-only remote participant ends with a rollback when the statement
// commits.
func TestRemoteReadParticipantKeepsNothing(t *testing.T) {
	remote := newAccessModeRemote(t)
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()
	loc := &LocationRemote{URI: remote.srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-remote-read")
	sub, err := tx.GetOrOpen(ParticipantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	if _, err := re.Execute(ctx, loc, "MATCH (n) RETURN n", nil, "", storage.RemoteAccessRead); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := remote.sessions(); got != "READ[MATCH (n) RETURN n]:rollback" {
		t.Fatalf("sessions: %s", got)
	}
}

// A remote write when another participant holds the statement's write is
// refused before any session for it is opened.
func TestRemoteSecondWriterIsRefusedBeforeItsSessionOpens(t *testing.T) {
	remote := newAccessModeRemote(t)
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()
	loc := &LocationRemote{URI: remote.srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-remote-second")
	if _, err := tx.GetOrOpen("local:other", true); err != nil {
		t.Fatal(err)
	}
	sub, err := tx.GetOrOpen(ParticipantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	if _, err := re.Execute(ctx, loc, "CREATE (n) RETURN n", nil, "", storage.RemoteAccessWrite); err == nil || !strings.Contains(err.Error(), "Writing to more than one database per transaction is not allowed") {
		t.Fatalf("expected the second writer refused, got %v", err)
	}
	if got := remote.sessions(); got != "" {
		t.Fatalf("a session was opened: %s", got)
	}
}

func TestRemoteUncertainModeClaimsFromTheResponse(t *testing.T) {
	for _, mode := range []string{"r", "w", ""} {
		for _, otherWriter := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%q/otherWriter=%v", mode, otherWriter), func(t *testing.T) {
				remote := newAccessModeRemote(t)
				remote.reportedMode = mode
				re := NewRemoteFragmentExecutor()
				defer func() { _ = re.Close() }()
				loc := &LocationRemote{URI: remote.srv.URL, DBName: "tenant"}
				tx := NewFabricTransaction("uncertain")
				initialWriter := ""
				if otherWriter {
					initialWriter = "local:other"
					if _, err := tx.GetOrOpen(initialWriter, true); err != nil {
						t.Fatal(err)
					}
				}
				remote.onQuery = func() {
					if got := tx.WriteShard(); got != initialWriter {
						t.Errorf("uncertain query claimed before the remote answered: %q", got)
					}
				}
				sub, err := tx.GetOrOpen(ParticipantKeyFromLocation(loc), false)
				if err != nil {
					t.Fatal(err)
				}
				ctx := WithUnresolvedRemoteMode(WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub))
				_, err = re.Execute(ctx, loc, "CALL remote.mode()", nil, "", storage.RemoteAccessWrite)
				if otherWriter && mode != "r" {
					if err == nil || !strings.Contains(err.Error(), "Writing to more than one database") {
						t.Fatalf("second writer was not refused: %v", err)
					}
					if err := tx.Rollback(nil); err != nil {
						t.Fatal(err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					wantWriter := initialWriter
					if mode != "r" {
						wantWriter = sub.ShardName
					}
					if got := tx.WriteShard(); got != wantWriter {
						t.Fatalf("writer %q, want %q", got, wantWriter)
					}
					if err := tx.Commit(nil, nil); err != nil {
						t.Fatal(err)
					}
				}
				end := "commit"
				if mode == "r" || otherWriter {
					end = "rollback"
				}
				if got, want := remote.sessions(), "WRITE[CALL remote.mode()]:"+end; got != want {
					t.Fatalf("sessions %q, want %q", got, want)
				}
			})
		}
	}
}

func TestRemoteDefiniteReadRefusesAReportedWrite(t *testing.T) {
	remote := newAccessModeRemote(t)
	remote.reportedMode = "w"
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()
	loc := &LocationRemote{URI: remote.srv.URL, DBName: "tenant"}
	tx := NewFabricTransaction("read-contradiction")
	sub, err := tx.GetOrOpen(ParticipantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	if _, err := re.Execute(ctx, loc, "MATCH (n) RETURN n", nil, "", storage.RemoteAccessRead); err == nil || !strings.Contains(err.Error(), "reported a write") {
		t.Fatalf("contradicting read summary: %v", err)
	}
	if tx.WriteShard() != "" {
		t.Fatal("a definite read claimed a writer")
	}
	if err := tx.Rollback(nil); err != nil {
		t.Fatal(err)
	}
	if got := remote.sessions(); got != "READ[MATCH (n) RETURN n]:rollback" {
		t.Fatal(got)
	}
}

// unboundRemoteEngine is the remote engine of a participant the executor can
// never bind: BeginCypherTx answers with the counter the case watches, so the
// failed binding and its cleanup are both observable.
type unboundRemoteEngine struct{ tx *countingRemoteTx }

func (engine *unboundRemoteEngine) QueryCypher(context.Context, string, map[string]interface{}, storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	return nil, nil, fmt.Errorf("unowned execution")
}
func (engine *unboundRemoteEngine) BeginCypherTx(context.Context, storage.RemoteAccessMode) (storage.RemoteCypherTx, error) {
	return engine.tx, nil
}
func (*unboundRemoteEngine) Close() error { return nil }

// A participant whose binding fails is released exactly once, and a failing
// release stays reachable (errors.Is) from the returned error: the context
// missing the fabric transaction and the context whose transaction never
// opened the shard both report their own cause, roll back the remote once and
// keep no handle.
func TestRemoteUnboundParticipantCleanupPreservesFailure(t *testing.T) {
	cleanupFailure := errors.New("unbound remote rollback failed")
	for _, missingContext := range []bool{false, true} {
		for _, cleanupFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("context_missing_%t/cleanup_fails_%t", missingContext, cleanupFails), func(t *testing.T) {
				remote := &countingRemoteTx{}
				if cleanupFails {
					remote.rollbackErr = cleanupFailure
				}
				engine := newRemoteQueryEngineFixture(t, &LocationRemote{URI: "bolt://unbound.invalid:7687", DBName: "tenant", AuthMode: "oidc_forwarding"}, &unboundRemoteEngine{tx: remote})
				executor := NewRemoteFragmentExecutor()
				ctx := context.Background()
				if !missingContext {
					ctx = WithFabricTransaction(ctx, NewFabricTransaction(t.Name()))
				}
				_, err := executor.getOrCreateTxHandle(ctx, &SubTransaction{ShardName: "missing"}, engine.engine, storage.RemoteAccessWrite)
				require.Error(t, err)
				if missingContext {
					require.ErrorContains(t, err, "context is missing")
				} else {
					require.ErrorContains(t, err, "not found")
				}
				if cleanupFails {
					require.ErrorIs(t, err, cleanupFailure)
				}
				require.Equal(t, 1, remote.rollbacks)
				require.Zero(t, remote.commits)
				require.Empty(t, executor.txHandles)
				require.NoError(t, executor.Close())
				require.Equal(t, 1, remote.rollbacks)
			})
		}
	}
}
