package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestHTTPCompositeRequestCleanupFailures(t *testing.T) {
	server, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "cleanup_leaf"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "cleanup_comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "cleanup_leaf", Type: "local", AccessMode: "read_write"}}))
	server.txSessions = txsession.NewManager(time.Hour, server.newExecutorForDatabase, server.shutdown.work.Context())
	failure := errors.New("transport rollback cleanup not confirmed")
	var rollbackErr error
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "transport.cleanupFault", Mode: cypher.ProcedureModeRead, Returns: []cypher.ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		tx, ok := fabric.FabricTransactionFromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("missing composite transaction in procedure context")
		}
		_, err := tx.GetOrOpen("remote:cleanup-proof", false)
		if err != nil {
			return nil, err
		}
		err = tx.BindParticipantCallbacks("remote:cleanup-proof", fabric.ParticipantCallbacks{Rollback: func(*fabric.SubTransaction) error { return rollbackErr }, Commit: func(*fabric.SubTransaction) error { return nil }})
		return &cypher.ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, err
	}))
	t.Cleanup(cypher.ClearUserProcedures)
	router := server.buildRouter()
	send := func(method, path, body string) (int, TransactionResponse) {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response), w.Body.String())
		return w.Code, response
	}
	for _, route := range []string{"delete", "statement_failure", "invalid_execute", "invalid_commit"} {
		for _, fault := range []bool{false, true} {
			t.Run(route+map[bool]string{false: "/clean", true: "/cleanup_error"}[fault], func(t *testing.T) {
				rollbackErr = nil
				if fault {
					rollbackErr = failure
				}
				_, opened := send(http.MethodPost, "/db/cleanup_comp/tx", `{"statements":[{"statement":"USE cleanup_comp CALL transport.cleanupFault() YIELD v RETURN v"}]}`)
				require.Empty(t, opened.Errors)
				target, err := url.Parse(opened.Commit)
				require.NoError(t, err)
				path := strings.TrimSuffix(target.Path, "/commit")
				method, body := http.MethodPost, `{"statements":[{"statement":"RETURN 1 / 0 AS failed"}]}`
				if route == "delete" {
					method, body = http.MethodDelete, ""
				}
				if strings.HasPrefix(route, "invalid_") {
					body = `{"statements":`
				}
				if route == "invalid_commit" {
					path = target.Path
				}
				status, response := send(method, path, body)
				primary := 0
				if route != "delete" {
					primary = 1
				}
				extra := 0
				if fault {
					extra = 1
				}
				require.Len(t, response.Errors, primary+extra)
				if strings.HasPrefix(route, "invalid_") {
					require.Equal(t, http.StatusOK, status, "transaction format errors retain the existing Neo4j HTTP envelope")
					require.Equal(t, "Neo.ClientError.Request.InvalidFormat", response.Errors[0].Code)
				}
				if fault {
					require.Contains(t, response.Errors[len(response.Errors)-1].Message, failure.Error())
				}
				require.Nil(t, response.Receipt)
				require.Nil(t, response.Optimistic)
				_, ended := send(http.MethodPost, strings.TrimSuffix(target.Path, "/commit"), `{"statements":[]}`)
				require.Len(t, ended.Errors, 1)
				require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", ended.Errors[0].Code)
			})
		}
	}
}

// A panic in the bounded rollback worker must be contained in that worker.
// A subprocess makes the fail-first process-crash reproduction suite-safe.
func TestHTTPCompositeRollbackPanicContained(t *testing.T) {
	if os.Getenv("NORNICDB_TEST_ROLLBACK_PANIC_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHTTPCompositeRollbackPanicContained$", "-test.timeout=30s")
		cmd.Env = append(os.Environ(), "NORNICDB_TEST_ROLLBACK_PANIC_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}
	server, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "panic_cleanup_leaf"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "panic_cleanup_comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "panic_cleanup_leaf", Type: "local", AccessMode: "read_write"}}))
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "transport.rollbackPanic", Mode: cypher.ProcedureModeRead, Returns: []cypher.ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		tx, ok := fabric.FabricTransactionFromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("missing composite transaction in procedure context")
		}
		_, err := tx.GetOrOpen("remote:panic-cleanup", false)
		if err != nil {
			return nil, err
		}
		if err := tx.BindParticipantCallbacks("remote:panic-cleanup", fabric.ParticipantCallbacks{Rollback: func(*fabric.SubTransaction) error { panic("bounded rollback panic") }}); err != nil {
			return nil, err
		}
		return &cypher.ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, nil
	}))
	t.Cleanup(cypher.ClearUserProcedures)
	openedRecord := makeRequest(t, server, http.MethodPost, "/db/panic_cleanup_comp/tx", map[string]any{"statements": []map[string]any{{"statement": "USE panic_cleanup_comp CALL transport.rollbackPanic() YIELD v RETURN v"}}}, token)
	var opened TransactionResponse
	require.NoError(t, json.Unmarshal(openedRecord.Body.Bytes(), &opened))
	require.Empty(t, opened.Errors)
	target, err := url.Parse(opened.Commit)
	require.NoError(t, err)
	ended := makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(target.Path, "/commit"), nil, token)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(ended.Body.Bytes(), &response))
	require.Len(t, response.Errors, 1)
	require.Contains(t, response.Errors[0].Message, "bounded rollback panic")
}

func TestCompositePublicStatementPanicCannotCommit(t *testing.T) {
	s, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "public_panic_leaf"}))
	require.NoError(t, s.dbManager.CreateCompositeDatabase(t.Context(), "public_panic_comp", []multidb.ConstituentRef{{Alias: "one", DatabaseName: "public_panic_leaf", Type: "local", AccessMode: "read_write"}}))
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "transport.statementPanic", Mode: cypher.ProcedureModeRead}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
		panic("public composite statement panic")
	}))
	t.Cleanup(cypher.ClearUserProcedures)
	driver, err := neo4j.NewDriverWithContext(fabricLifetimeBolt(t, s), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
	for _, route := range []string{"http", "bolt"} {
		t.Run(route, func(t *testing.T) {
			query := "USE public_panic_comp CALL { USE public_panic_comp.one CREATE (:PublicPanic {run:$run}) RETURN 1 AS w } CALL { USE public_panic_comp.one CALL transport.statementPanic() RETURN 1 AS v } RETURN w,v"
			if route == "http" {
				openedRecord := makeRequest(t, s, http.MethodPost, "/db/public_panic_comp/tx", map[string]any{"statements": []any{}}, token)
				var opened TransactionResponse
				require.NoError(t, json.Unmarshal(openedRecord.Body.Bytes(), &opened))
				require.Empty(t, opened.Errors)
				target, err := url.Parse(opened.Commit)
				require.NoError(t, err)
				failed := makeRequest(t, s, http.MethodPost, strings.TrimSuffix(target.Path, "/commit"), map[string]any{"statements": []map[string]any{{"statement": query, "parameters": map[string]any{"run": t.Name()}}}}, token)
				require.GreaterOrEqual(t, failed.Code, 400)
				ended := makeRequest(t, s, http.MethodPost, target.Path, map[string]any{"statements": []any{}}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(ended.Body.Bytes(), &response))
				require.NotEmpty(t, response.Errors)
				require.Contains(t, response.Errors[0].Message, "was rolled back")
			} else {
				session := driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: "public_panic_comp"})
				defer session.Close(context.Background())
				tx, err := session.BeginTransaction(context.Background())
				require.NoError(t, err)
				result, runErr := tx.Run(context.Background(), query, map[string]any{"run": t.Name()})
				if runErr == nil {
					_, runErr = result.Consume(context.Background())
				}
				require.Error(t, runErr)
				require.Error(t, tx.Commit(context.Background()))
				_ = tx.Rollback(context.Background())
			}
			persisted := makeRequest(t, s, http.MethodPost, "/db/public_panic_leaf/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "MATCH (n:PublicPanic {run:$run}) RETURN count(n) AS c", "parameters": map[string]any{"run": t.Name()}}}}, token)
			var response TransactionResponse
			require.NoError(t, json.Unmarshal(persisted.Body.Bytes(), &response))
			require.Empty(t, response.Errors)
			require.Len(t, response.Results, 1)
			require.Equal(t, float64(0), response.Results[0].Data[0].Row[0])
		})
	}
}
