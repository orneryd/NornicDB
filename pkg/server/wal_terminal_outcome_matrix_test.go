// Author: Claude, NOR-180 terminal outcome matrix (HTTP), 2026-10-30.
// Self-contained so it runs unchanged on release as a red-first overlay.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

type httpMatrixEngine struct {
	*storage.MemoryEngine
	wal    *storage.WAL
	begin  error
	commit func(context.Context, *storage.BadgerTransaction) error
}

func (e *httpMatrixEngine) GetWAL() *storage.WAL { return e.wal }
func (e *httpMatrixEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	if e.begin != nil {
		return nil, e.begin
	}
	return e.MemoryEngine.BeginTransaction()
}
func (e *httpMatrixEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	if e.commit != nil {
		return e.commit(ctx, tx)
	}
	return tx.CommitContext(ctx)
}

type httpMatrixSuppression struct {
	storage.Engine
	cause error
}

func (e *httpMatrixSuppression) GetInnerEngine() storage.Engine { return e.Engine }
func (e *httpMatrixSuppression) SetManualSuppression(storage.StoredEntityKind, string, bool) error {
	return e.cause
}

type httpMatrixPrimary struct{ msg string }

func (e *httpMatrixPrimary) Error() string         { return e.msg }
func (e *httpMatrixPrimary) BoltErrorCode() string { return httpMatrixPrimaryCode }

const httpMatrixPrimaryCode = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"

var httpMatrixProcedureID atomic.Uint64

type httpMatrixCell struct {
	stage    string // statement, commit, post-commit, rollback, begin, implicit-begin
	explicit bool
	closed   bool
	code     string // "" = the operation succeeds
	native   int64
}

// HTTP has no RESET, and a failed statement rolls its transaction back, so a
// remembered terminal error cannot reach COMMIT over HTTP; that delivery is
// covered by pkg/txsession TestWALTerminalRememberedErrorAtCommitKeepsClassification.
func httpMatrixCells() []httpMatrixCell {
	var cells []httpMatrixCell
	for _, explicit := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			cells = append(cells,
				httpMatrixCell{"statement", explicit, closed, httpMatrixPrimaryCode, 0},
				httpMatrixCell{"commit", explicit, closed, "Neo.DatabaseError.Transaction.TransactionCommitFailed", 0},
				httpMatrixCell{"post-commit", explicit, closed, "Neo.DatabaseError.Transaction.TransactionCommitFailed", 1},
			)
			if !explicit {
				cells = append(cells, httpMatrixCell{"implicit-begin", explicit, closed, "Neo.DatabaseError.Transaction.TransactionStartFailed", 0})
			}
			if explicit {
				rollback := ""
				if closed {
					rollback = "Neo.DatabaseError.Transaction.TransactionRollbackFailed"
				}
				cells = append(cells,
					httpMatrixCell{"rollback", explicit, closed, rollback, 0},
					httpMatrixCell{"begin", explicit, closed, "Neo.DatabaseError.Transaction.TransactionStartFailed", 0},
				)
			}
		}
	}
	return cells
}

func TestHTTPWALTerminalOutcomeMatrix(t *testing.T) {
	for _, cell := range httpMatrixCells() {
		name := fmt.Sprintf("%s/explicit=%v/closed=%v", cell.stage, cell.explicit, cell.closed)
		t.Run(name, func(t *testing.T) {
			wal, err := storage.NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			engine := &httpMatrixEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), wal: wal}
			store := storage.NewNamespacedEngine(engine, "nornic")
			_, err = store.CreateNode(&storage.Node{ID: "matrix-seed", Labels: []string{"WalMatrixSeed"}})
			require.NoError(t, err)
			closeWAL := func() {
				if cell.closed {
					require.NoError(t, wal.Close())
				}
			}
			var backing storage.Engine = store
			causeText := ""
			query := "CREATE (:WalMatrix {m: true})"
			switch cell.stage {
			case "statement":
				name := fmt.Sprintf("nor180.matrixhttp%d", httpMatrixProcedureID.Add(1))
				causeText = "matrix statement failure"
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
					func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
						if _, err := exec.Execute(ctx, "CREATE (:WalMatrix {m: true})", nil); err != nil {
							return nil, err
						}
						closeWAL()
						return nil, &httpMatrixPrimary{causeText}
					}))
				query = "CALL " + name + "() YIELD value RETURN value"
			case "commit":
				causeText = "matrix native commit failure"
				engine.commit = func(_ context.Context, tx *storage.BadgerTransaction) error {
					closeWAL()
					_ = tx.Rollback()
					return errors.New(causeText)
				}
			case "post-commit":
				causeText = "matrix post-commit action rejected"
				backing = &httpMatrixSuppression{Engine: store, cause: errors.New(causeText)}
				engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
					err := tx.CommitContext(ctx)
					if err == nil {
						closeWAL()
					}
					return err
				}
				query = "MATCH (n:WalMatrixSeed) SET n.m = true WITH n CALL kp.suppress(n)"
			case "begin":
				causeText = "matrix begin rejected"
			case "implicit-begin":
				// Open: the native snapshot cannot start. Closed: the snapshot
				// starts and the WAL begin record cannot be written.
				causeText = "matrix implicit begin rejected"
				if cell.closed {
					causeText = storage.ErrWALClosed.Error()
				} else {
					engine.begin = errors.New(causeText)
				}
			}

			s, auth := setupTestServer(t)
			newExec := func(string) (*cypher.StorageExecutor, error) {
				if cell.stage == "begin" && !cell.closed {
					return nil, errors.New(causeText)
				}
				return cypher.NewStorageExecutor(backing), nil
			}
			s.txSessions = txsession.NewManager(time.Minute, newExec)
			s.executorsMu.Lock()
			s.executors["nornic"] = cypher.NewStorageExecutor(backing)
			s.executorsMu.Unlock()
			host := httptest.NewServer(s.buildRouter())
			t.Cleanup(host.Close)
			token := getAuthToken(t, auth, "admin")
			request := func(method, path string, statements []string) TransactionResponse {
				t.Helper()
				payload := TransactionRequest{Statements: []StatementRequest{}}
				for _, statement := range statements {
					payload.Statements = append(payload.Statements, StatementRequest{Statement: statement})
				}
				body, err := json.Marshal(payload)
				require.NoError(t, err)
				if method == http.MethodDelete {
					body = nil
				}
				if !strings.HasPrefix(path, "http") {
					path = host.URL + path
				}
				req, err := http.NewRequest(method, path, strings.NewReader(string(body)))
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")
				resp, err := host.Client().Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				var result TransactionResponse
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
				return result
			}

			var response TransactionResponse
			switch {
			case cell.stage == "begin":
				closeWAL()
				response = request(http.MethodPost, "/db/nornic/tx", nil)
			case !cell.explicit:
				if cell.stage == "implicit-begin" {
					closeWAL()
				}
				response = request(http.MethodPost, "/db/nornic/tx/commit", []string{query})
			default:
				opened := request(http.MethodPost, "/db/nornic/tx", nil)
				require.Empty(t, opened.Errors)
				txPath := strings.TrimSuffix(opened.Commit, "/commit")
				require.NotEmpty(t, txPath)
				response = request(http.MethodPost, txPath, []string{query})
				if cell.stage == "statement" {
					break
				}
				require.Empty(t, response.Errors)
				switch cell.stage {
				case "commit", "post-commit":
					response = request(http.MethodPost, opened.Commit, nil)
				case "rollback":
					closeWAL()
					response = request(http.MethodDelete, txPath, nil)
				}
			}
			truth := "MATCH (n:WalMatrix {m: true}) RETURN count(n)"
			if cell.stage == "post-commit" {
				truth = "MATCH (n:WalMatrixSeed {m: true}) RETURN count(n)"
			}
			rows, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine.MemoryEngine, "nornic")).Execute(context.Background(), truth, nil)
			require.NoError(t, err)
			require.Equal(t, cell.native, rows.Rows[0][0], "native commit truth")
			if cell.code == "" {
				require.Empty(t, response.Errors)
				return
			}
			require.NotEmpty(t, response.Errors)
			messages := make([]string, len(response.Errors))
			for i, e := range response.Errors {
				messages[i] = e.Message
			}
			message := strings.Join(messages, "\n")
			if causeText != "" && !(cell.stage == "begin" && cell.closed) {
				require.Contains(t, message, causeText, "the primary cause is retained")
			}
			// An explicit statement failure only marks its transaction failed;
			// the abort record is attempted by the later DELETE or COMMIT.
			if cell.closed && !(cell.stage == "statement" && cell.explicit) {
				require.Contains(t, message, "WAL tx", "the WAL record failure is reported")
			}
			require.Equal(t, cell.code, response.Errors[0].Code, "required Neo4j status")
		})
	}
}
