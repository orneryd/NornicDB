package fabric

import (
	"context"
	"encoding/json"
	"fmt"
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
	mu  sync.Mutex
	srv *httptest.Server
	txs []*accessModeRemoteTx
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
			var body struct {
				Statements []struct {
					Statement string `json:"statement"`
				} `json:"statements"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			for _, statement := range body.Statements {
				tx.statements = append(tx.statements, statement.Statement)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{}, "results": []any{
				map[string]any{"columns": []string{"n"}, "data": []any{map[string]any{"row": []any{1}}}},
			}})
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
// participant (ClaimWrite); the read session ends first, and every later
// query on the remote runs in the write session, which commits.
func TestRemoteParticipantOpensWriteSessionOnlyWhenItClaimsTheWrite(t *testing.T) {
	remote := newAccessModeRemote(t)
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()
	loc := &LocationRemote{URI: remote.srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
	tx := NewFabricTransaction("tx-remote-modes")
	sub, err := tx.GetOrOpen(participantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	for _, query := range []string{"MATCH (n) RETURN n", "CREATE (n) RETURN n", "MATCH (n) RETURN n"} {
		if _, err := re.Execute(ctx, loc, query, nil, ""); err != nil {
			t.Fatalf("%s: %v", query, err)
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
	sub, err := tx.GetOrOpen(participantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	if _, err := re.Execute(ctx, loc, "MATCH (n) RETURN n", nil, ""); err != nil {
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
	if _, err := tx.ClaimWrite("local:other"); err != nil {
		t.Fatal(err)
	}
	sub, err := tx.GetOrOpen(participantKeyFromLocation(loc), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithSubTransaction(WithFabricTransaction(context.Background(), tx), sub)
	if _, err := re.Execute(ctx, loc, "CREATE (n) RETURN n", nil, ""); err == nil || !strings.Contains(err.Error(), "Writing to more than one database per transaction is not allowed") {
		t.Fatalf("expected the second writer refused, got %v", err)
	}
	if got := remote.sessions(); got != "" {
		t.Fatalf("a session was opened: %s", got)
	}
}
