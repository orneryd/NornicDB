//go:build closeout_l2_e17

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E17: CREATE INDEX / DROP INDEX over HTTP autocommit are refused with
// "schema snapshot changed before commit" while other clients write. The
// requests go to the server's HTTP router in process, as in the entry. The
// parser is the process's (NORNICDB_PARSER).
//
// Ported: the administrator's user store is the database's own "system"
// namespace (as the package's server fixture does) instead of a separate
// in-memory store; it holds only the user and is not the written namespace.

const (
	l2E17Writers    = 4
	l2E17Iterations = 50
	l2E17Rounds     = 3
)

type l2E17Server struct {
	router http.Handler
	token  string
}

func l2E17NewServer(t *testing.T) *l2E17Server {
	t.Helper()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALSyncMode = "immediate"
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("l2-e17-test-secret-key-32-bytes!!"),
	}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	if _, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil {
		t.Fatalf("create admin: %v", err)
	}

	serverConfig := DefaultConfig()
	serverConfig.Address = "127.0.0.1"
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = cfg
	srv, err := New(db, authenticator, serverConfig)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})
	tokenResp, _, err := authenticator.Authenticate(context.Background(), "admin", "password123", "127.0.0.1", "L2Test")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	return &l2E17Server{router: srv.buildRouter(), token: "Bearer " + tokenResp.AccessToken}
}

// post sends one statement to POST path and returns the status, the parsed
// answer and whether it could be parsed.
func (s *l2E17Server) post(path, statement string, params map[string]interface{}) (int, TransactionResponse, bool) {
	stmt := map[string]interface{}{"statement": statement}
	if params != nil {
		stmt["parameters"] = params
	}
	body, _ := json.Marshal(map[string]interface{}{"statements": []interface{}{stmt}})
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.token)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)
	var resp TransactionResponse
	parsed := json.Unmarshal(rec.Body.Bytes(), &resp) == nil
	return rec.Code, resp, parsed
}

type l2E17Round struct {
	refusedCreate, refusedDrop int
	lastErr                    string
	errors                     map[string]int
	writes                     int64
}

func (r l2E17Round) refused() int { return r.refusedCreate + r.refusedDrop }

// l2E17RunRound runs one round on a new store: writers goroutines writing
// CREATE (:W {v: $v}) in a loop while the main goroutine sends 50 CREATE INDEX
// / DROP INDEX pairs. schemaPath selects the schema statements' route.
func l2E17RunRound(t *testing.T, writers int, schemaRun func(s *l2E17Server, statement string) (int, TransactionResponse, bool)) l2E17Round {
	t.Helper()
	s := l2E17NewServer(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var writes int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				code, resp, ok := s.post("/db/nornic/tx/commit", "CREATE (:W {v: $v})", map[string]interface{}{"v": w*1000000 + i})
				if code == http.StatusOK && ok && len(resp.Errors) == 0 {
					mu.Lock()
					writes++
					mu.Unlock()
				}
			}
		}(w)
	}
	round := l2E17Round{errors: map[string]int{}}
	record := func(kind string, code int, resp TransactionResponse, ok bool) {
		if ok && code == http.StatusOK && len(resp.Errors) == 0 {
			return
		}
		if kind == "create" {
			round.refusedCreate++
		} else {
			round.refusedDrop++
		}
		text := fmt.Sprintf("status=%d parsed=%t", code, ok)
		if len(resp.Errors) > 0 {
			text = resp.Errors[len(resp.Errors)-1].Code + ": " + resp.Errors[len(resp.Errors)-1].Message
		}
		round.lastErr = text
		round.errors[text]++
	}
	for i := 0; i < l2E17Iterations; i++ {
		code, resp, ok := schemaRun(s, "CREATE INDEX w_v IF NOT EXISTS FOR (n:W) ON (n.v)")
		record("create", code, resp, ok)
		code, resp, ok = schemaRun(s, "DROP INDEX w_v IF EXISTS")
		record("drop", code, resp, ok)
	}
	close(stop)
	wg.Wait()
	round.writes = writes
	return round
}

func l2E17Autocommit(s *l2E17Server, statement string) (int, TransactionResponse, bool) {
	return s.post("/db/nornic/tx/commit", statement, nil)
}

func l2E17Summarize(rounds []l2E17Round) (refused, creates, drops int, writes int64, last string) {
	for _, r := range rounds {
		refused += r.refused()
		creates += r.refusedCreate
		drops += r.refusedDrop
		writes += r.writes
		if r.lastErr != "" {
			last = r.lastErr
		}
	}
	return
}

func TestL2_e17_Repro(t *testing.T) {
	rounds := make([]l2E17Round, 0, l2E17Rounds)
	for r := 0; r < l2E17Rounds; r++ {
		rounds = append(rounds, l2E17RunRound(t, l2E17Writers, l2E17Autocommit))
	}
	refused, creates, drops, writes, last := l2E17Summarize(rounds)
	total := l2E17Rounds * l2E17Iterations * 2
	verdict := "OK"
	if refused > 0 {
		verdict = "DEFECT"
	}
	perRound := make([]int, 0, len(rounds))
	for _, r := range rounds {
		perRound = append(perRound, r.refused())
	}
	t.Logf("L2RESULT E17 %s parser=%s writers=%d refused=%d/%d create=%d drop=%d per_round=%v writer_commits=%d last=%q",
		verdict, config.GetParserType(), l2E17Writers, refused, total, creates, drops, perRound, writes, last)
	if writes == 0 {
		t.Errorf("harness: the writers committed nothing, so the rounds had no concurrent writes")
	}
	if refused > 0 {
		t.Errorf("%d of %d schema statements refused with %d concurrent writers; last error %q", refused, total, l2E17Writers, last)
	}
}

// Control: the same rounds without writers refuse nothing.
func TestL2_e17_Control(t *testing.T) {
	rounds := make([]l2E17Round, 0, l2E17Rounds)
	for r := 0; r < l2E17Rounds; r++ {
		rounds = append(rounds, l2E17RunRound(t, 0, l2E17Autocommit))
	}
	refused, _, _, _, last := l2E17Summarize(rounds)
	t.Logf("control writers=0 refused=%d/%d last=%q", refused, l2E17Rounds*l2E17Iterations*2, last)
	if refused > 0 {
		t.Errorf("control: %d schema statements refused without writers; last %q", refused, last)
	}
}

// Cause: the refusal is the commit-time schema snapshot check
// (pkg/storage/badger_transaction_publication.go:109-111: a transaction that
// carries a schema change is rolled back when the namespace's commit sequence
// moved after its snapshot). An autocommit schema statement is meant to be
// admitted with AutocommitSchemaBegin (cypher openTransaction ->
// SchemaManager.BeginSchemaTransaction, pkg/cypher/transaction_admission.go:63),
// which holds the engine's write barrier from before the snapshot to the
// commit, so no data commit can land inside it. One round with writers logs
// the error-text histogram for autocommit, and for the same schema statements
// sent through an explicit HTTP transaction (POST /tx, then its commit URL),
// which keeps the ordinary begin and its conflict guard: refusals on the
// autocommit route at the same rate as on the explicit route indicate the
// autocommit statement did not get the schema admission.
func TestL2_e17_Cause(t *testing.T) {
	explicit := func(s *l2E17Server, statement string) (int, TransactionResponse, bool) {
		code, opened, ok := s.post("/db/nornic/tx", statement, nil)
		if !ok || code != http.StatusCreated || len(opened.Errors) > 0 || opened.Commit == "" {
			if code == http.StatusCreated {
				code = http.StatusOK
			}
			return code, opened, ok
		}
		commit := opened.Commit
		if i := strings.Index(commit, "/db/"); i >= 0 {
			commit = commit[i:]
		}
		req := httptest.NewRequest(http.MethodPost, commit, bytes.NewReader([]byte(`{"statements":[]}`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", s.token)
		rec := httptest.NewRecorder()
		s.router.ServeHTTP(rec, req)
		var resp TransactionResponse
		parsed := json.Unmarshal(rec.Body.Bytes(), &resp) == nil
		return rec.Code, resp, parsed
	}
	for _, route := range []struct {
		name string
		run  func(s *l2E17Server, statement string) (int, TransactionResponse, bool)
	}{
		{"autocommit", l2E17Autocommit},
		{"explicit", explicit},
	} {
		r := l2E17RunRound(t, l2E17Writers, route.run)
		keys := make([]string, 0, len(r.errors))
		for k := range r.errors {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			t.Logf("cause parser=%s route=%s error x%d: %s", config.GetParserType(), route.name, r.errors[k], k)
		}
		t.Logf("cause parser=%s route=%s refused=%d/%d (create=%d drop=%d) writer_commits=%d",
			config.GetParserType(), route.name, r.refused(), l2E17Iterations*2, r.refusedCreate, r.refusedDrop, r.writes)
	}
}
