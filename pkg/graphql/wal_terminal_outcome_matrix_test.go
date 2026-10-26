// Author: Claude, NOR-180 terminal outcome matrix (GraphQL), 2026-10-30.
// Self-contained so it runs unchanged on release as a red-first overlay.
// GraphQL reports no Neo4j status code; the contract's public outcome is the
// error itself: the primary cause, the WAL record failure and native truth.
// GraphQL statements commit through the database manager's storage, which
// publishes natively and never calls the inner engine's CommitTransaction, so
// every fault here is injected where that route reaches it: a procedure in the
// statement, a commit-time constraint, the post-commit action, native begin.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type graphqlMatrixEngine struct {
	*storage.MemoryEngine
	wal   *storage.WAL
	begin error
}

func (e *graphqlMatrixEngine) GetWAL() *storage.WAL { return e.wal }
func (e *graphqlMatrixEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	if e.begin != nil {
		return nil, e.begin
	}
	return e.MemoryEngine.BeginTransaction()
}

// graphqlMatrixSuppression rejects the post-commit suppression action, which
// runs after the native commit and before the COMMIT record; before closes the
// WAL first in the closed cells.
type graphqlMatrixSuppression struct {
	*graphqlMatrixEngine
	before func()
	cause  error
}

func (e *graphqlMatrixSuppression) GetInnerEngine() storage.Engine { return e.graphqlMatrixEngine }
func (e *graphqlMatrixSuppression) SetManualSuppression(storage.StoredEntityKind, string, bool) error {
	e.before()
	return e.cause
}

var graphqlMatrixProcedureID atomic.Uint64

func TestGraphQLWALTerminalOutcomeMatrix(t *testing.T) {
	for _, stage := range []string{"statement", "commit", "post-commit", "implicit-begin"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%v", stage, closed), func(t *testing.T) {
				wal, err := storage.NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })
				engine := &graphqlMatrixEngine{MemoryEngine: storage.NewMemoryEngine(), wal: wal}
				t.Cleanup(func() { _ = engine.MemoryEngine.Close() })
				var base storage.Engine = engine
				closeWAL := func() {
					if closed {
						require.NoError(t, wal.Close())
					}
				}
				query := "CREATE (:WalMatrix {m: true})"
				var setup []string
				var beginErr error
				var cause string
				var native int64
				switch stage {
				case "statement":
					name := fmt.Sprintf("nor180.matrixgraphql%d", graphqlMatrixProcedureID.Add(1))
					cause = "matrix statement failure"
					require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
						func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
							if _, err := exec.Execute(ctx, "CREATE (:WalMatrix {m: true})", nil); err != nil {
								return nil, err
							}
							closeWAL()
							return nil, errors.New(cause)
						}))
					query = "CALL " + name + "() YIELD value RETURN value"
				case "commit":
					// Auto-commit defers constraint validation to commit, so the
					// duplicate fails the native commit on the GraphQL route.
					// Closed: a write procedure in the statement closes the WAL
					// before the commit, so the abort record fails too.
					cause = "Constraint violation"
					setup = []string{"CREATE CONSTRAINT nor180_matrix_k FOR (n:WalMatrix) REQUIRE n.k IS UNIQUE", "CREATE (:WalMatrix {k: 1})"}
					query = "CREATE (:WalMatrix {k: 1, m: true})"
					if closed {
						name := fmt.Sprintf("nor180.matrixgraphqlclose%d", graphqlMatrixProcedureID.Add(1))
						require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
							func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
								closeWAL()
								return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
							}))
						query = "CALL " + name + "() YIELD value CREATE (:WalMatrix {k: 1, m: true})"
					}
				case "post-commit":
					cause = "matrix post-commit action rejected"
					base = &graphqlMatrixSuppression{graphqlMatrixEngine: engine, before: closeWAL, cause: errors.New(cause)}
					query = "MATCH (n:WalMatrixSeed) SET n.m = true WITH n CALL kp.suppress(n)"
					native = 1
				case "implicit-begin":
					// Open: the native snapshot cannot start. Closed: the
					// snapshot starts and the WAL begin record cannot be written.
					cause = "matrix implicit begin rejected"
					if closed {
						cause = storage.ErrWALClosed.Error()
					} else {
						beginErr = errors.New(cause)
					}
				}
				manager, err := multidb.NewDatabaseManager(context.Background(), base, nil)
				require.NoError(t, err)
				store, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				for _, statement := range append([]string{"CREATE (:WalMatrixSeed)"}, setup...) {
					_, err = cypher.NewStorageExecutor(store).Execute(context.Background(), statement, nil)
					require.NoError(t, err)
				}
				engine.begin = beginErr
				if stage == "implicit-begin" {
					closeWAL()
				}
				h := newHandler(nil, manager, nil)
				payload, err := json.Marshal(map[string]string{"query": fmt.Sprintf("mutation { executeCypher(input: {statement: %q}) { columns rowCount } }", query)})
				require.NoError(t, err)
				request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				h.ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code)
				truth := "MATCH (n:WalMatrix {m: true}) RETURN count(n)"
				if stage == "post-commit" {
					truth = "MATCH (n:WalMatrixSeed {m: true}) RETURN count(n)"
				}
				rows, err := cypher.NewStorageExecutor(store).Execute(context.Background(), truth, nil)
				require.NoError(t, err)
				require.Equal(t, native, rows.Rows[0][0], "native commit truth")
				var wire struct {
					Errors []struct{ Message string } `json:"errors"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
				require.NotEmpty(t, wire.Errors, "a failed outcome is never a success")
				messages := make([]string, len(wire.Errors))
				for i, e := range wire.Errors {
					messages[i] = e.Message
				}
				message := strings.Join(messages, "\n")
				require.Contains(t, message, cause, "the primary cause is retained")
				if closed {
					require.Contains(t, message, "WAL tx", "the WAL record failure is reported")
				}
			})
		}
	}
}
