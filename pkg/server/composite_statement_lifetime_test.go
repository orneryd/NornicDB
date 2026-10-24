package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A composite statement sent over Bolt or the HTTP API keeps nothing it
// wrote when it fails, whatever fails and wherever: its parts write one
// database (a local constituent through Cypher and a WRITE user procedure,
// or a remote NornicDB over Bolt or HTTP through Cypher), other parts read,
// and a failure is injected before, between and after them as an error, a
// panic (a procedure's), a constraint violation (on the local target) or,
// over Bolt, the transaction's timeout. Autocommit and in an explicit
// transaction, with write-behind on and off. A panicking statement's HTTP
// transaction can still be ended: its COMMIT reports the failure at once.
func TestCompositeStatementFailureAtEveryPositionKeepsNothing(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(cypher.ClearUserProcedures)
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name: "lifetimeserver.panic", Signature: "lifetimeserver.panic() :: (z :: INTEGER)", Mode: cypher.ProcedureModeRead,
		Returns: []cypher.ProcedureColumn{{Name: "z", Type: "INTEGER"}},
	}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
		panic("lifetime test: injected procedure panic")
	}))
	previous := cypher.PluginFunctionLookup
	cypher.PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "lifetimeserver.slow" {
			return func(value int64) int64 {
				time.Sleep(1500 * time.Millisecond)
				return value
			}, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { cypher.PluginFunctionLookup = previous })

	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_behind=%v", writeBehind), func(t *testing.T) {
			server, authenticator := setupTestServerWithDatabaseConfig(t, func(config *nornicdb.Config) {
				config.Database.AsyncWritesEnabled = writeBehind
			}, nil)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			remote, remoteAuth := setupTestServer(t)
			remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
			remoteRouter := remote.buildRouter()
			remoteHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Header.Set("Authorization", remoteToken)
				remoteRouter.ServeHTTP(w, r)
			}))
			t.Cleanup(remoteHTTP.Close)
			require.NoError(t, server.dbManager.CreateDatabase("ltone"))
			require.NoError(t, server.dbManager.CreateDatabase("lttwo"))
			require.NoError(t, server.dbManager.CreateCompositeDatabase("ltcmp", []multidb.ConstituentRef{
				{Alias: "one", DatabaseName: "ltone", Type: "local", AccessMode: "read_write"},
				{Alias: "two", DatabaseName: "lttwo", Type: "local", AccessMode: "read_write"},
				{Alias: "nbolt", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: matrixBoltServer(t, remote)},
				{Alias: "nhttp", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: remoteHTTP.URL},
			}))

			// post runs one statement over the HTTP API: its status, and its
			// response when the body is one.
			post := func(target *Server, auth, path, statement string, params map[string]any) (int, TransactionResponse) {
				rec := makeRequest(t, target, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}, auth)
				var resp TransactionResponse
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)
				return rec.Code, resp
			}
			admin := func(target *Server, auth, path, statement string, params map[string]any) TransactionResponse {
				code, resp := post(target, auth, path, statement, params)
				require.Less(t, code, 300, "%s: %v", statement, resp.Errors)
				require.Empty(t, resp.Errors, statement)
				return resp
			}
			admin(server, token, "/db/ltone/tx/commit", "CREATE CONSTRAINT lt_unique IF NOT EXISTS FOR (n:LtUnique) REQUIRE n.k IS UNIQUE", nil)
			admin(server, token, "/db/ltone/tx/commit", "CREATE OR REPLACE PROCEDURE lifetimeserver.write($run, $at) MODE WRITE AS CREATE (:LtMark {run: $run, at: $at}) RETURN 1 AS w", nil)
			marks := func(target, run string) float64 {
				t.Helper()
				// What write-behind still buffers lands first.
				for engine := storage.Engine(server.db.GetBaseStorageForManager()); engine != nil; {
					if flusher, ok := engine.(interface{ FlushWriteBehind() error }); ok {
						require.NoError(t, flusher.FlushWriteBehind())
						break
					}
					inner, ok := engine.(storage.EngineUnwrapper)
					if !ok {
						break
					}
					engine = inner.GetInnerEngine()
				}
				params := map[string]any{"run": run}
				if target == "one" {
					return admin(server, token, "/db/ltone/tx/commit", "MATCH (n:LtMark {run: $run}) RETURN count(n) AS c", params).Results[0].Data[0].Row[0].(float64)
				}
				left := admin(remote, remoteToken, "/db/nornic/tx/commit", "MATCH (n:LtMark {run: $run}) RETURN count(n) AS c", params).Results[0].Data[0].Row[0].(float64)
				admin(remote, remoteToken, "/db/nornic/tx/commit", "MATCH (n:LtMark {run: $run}) DETACH DELETE n", params)
				return left
			}

			driver, err := neo4j.NewDriverWithContext(matrixBoltServer(t, server), neo4j.NoAuth())
			require.NoError(t, err)
			t.Cleanup(func() { _ = driver.Close(ctx) })
			// routes run a statement autocommit or in an explicit
			// transaction, whose COMMIT's failure is the outcome when the
			// statement itself succeeded.
			routes := map[string]func(statement string, params map[string]any, explicit bool) error{
				"bolt": func(statement string, params map[string]any, explicit bool) error {
					session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "ltcmp"})
					defer func() { _ = session.Close(ctx) }()
					timeout := neo4j.WithTxTimeout(500 * time.Millisecond)
					if !explicit {
						result, err := session.Run(ctx, statement, params, timeout)
						if err == nil {
							_, err = result.Consume(ctx)
						}
						return err
					}
					tx, err := session.BeginTransaction(ctx, timeout)
					if err != nil {
						return err
					}
					result, err := tx.Run(ctx, statement, params)
					if err == nil {
						_, err = result.Consume(ctx)
					}
					if err != nil {
						_ = tx.Rollback(ctx)
						return err
					}
					return tx.Commit(ctx)
				},
				"http": func(statement string, params map[string]any, explicit bool) error {
					if !explicit {
						code, resp := post(server, token, "/db/ltcmp/tx/commit", statement, params)
						if code >= 400 || len(resp.Errors) > 0 {
							return fmt.Errorf("HTTP %d: %v", code, resp.Errors)
						}
						return nil
					}
					_, opened := post(server, token, "/db/ltcmp/tx", "RETURN 1", nil)
					require.Empty(t, opened.Errors)
					commit, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					code, resp := post(server, token, strings.TrimSuffix(commit.Path, "/commit"), statement, params)
					failed := code >= 400 || len(resp.Errors) > 0
					ended := make(chan error, 1)
					go func() {
						code, resp := post(server, token, commit.Path, "RETURN 1", nil)
						if code >= 400 || len(resp.Errors) > 0 {
							ended <- fmt.Errorf("HTTP %d: %v", code, resp.Errors)
							return
						}
						ended <- nil
					}()
					select {
					case commitErr := <-ended:
						if failed {
							require.Error(t, commitErr, "a failed statement's transaction can't commit")
							return fmt.Errorf("HTTP %d: %v", code, resp.Errors)
						}
						return commitErr
					case <-time.After(10 * time.Second):
						t.Fatal("the transaction's COMMIT never ran: its session is still held")
						return nil
					}
				},
			}

			for _, target := range []string{"one", "nbolt", "nhttp"} {
				writer := func(i int) string {
					return fmt.Sprintf("CALL { USE ltcmp.%s CREATE (:LtMark {run: $run, at: %d}) RETURN 1 AS a%d }", target, i, i)
				}
				procedure := func(i int) string {
					if target == "one" {
						return fmt.Sprintf("CALL { USE ltcmp.one CALL lifetimeserver.write($run, %d) YIELD w RETURN w AS a%d }", i, i)
					}
					return writer(i)
				}
				reader := func(i int) string {
					return fmt.Sprintf("CALL { USE ltcmp.two MATCH (n:LtMark) RETURN count(n) AS a%d }", i)
				}
				parts := []string{writer(0), reader(1), procedure(2), writer(3)}
				failures := map[string]string{
					"error": "CALL { USE ltcmp.two RETURN 1 / $zero AS f }",
					"panic": "CALL { USE ltcmp.two CALL lifetimeserver.panic() YIELD z RETURN z AS f }",
				}
				if target == "one" {
					failures["constraint"] = "CALL { USE ltcmp.one CREATE (:LtUnique {k: $run}) CREATE (:LtUnique {k: $run}) RETURN 1 AS f }"
				}
				for route, send := range routes {
					kinds := failures
					if route == "bolt" {
						kinds = map[string]string{"timeout": "CALL { USE ltcmp.two RETURN lifetimeserver.slow(1) AS f }"}
						for kind, failure := range failures {
							kinds[kind] = failure
						}
					}
					for _, explicit := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/%s/explicit=%v/no_failure", target, route, explicit), func(t *testing.T) {
							run := fmt.Sprintf("lt-ok-%d", time.Now().UnixNano())
							require.NoError(t, send(strings.Join(parts, " ")+" RETURN 1 AS done", map[string]any{"run": run, "zero": int64(0)}, explicit))
							require.Equal(t, float64(3), marks(target, run), "every write is kept")
						})
						for kind, failure := range kinds {
							for position := 0; position <= len(parts); position++ {
								t.Run(fmt.Sprintf("%s/%s/explicit=%v/%s_at_%d", target, route, explicit, kind, position), func(t *testing.T) {
									run := fmt.Sprintf("lt-%s-%d-%d", kind, position, time.Now().UnixNano())
									statement := append(append(append([]string{}, parts[:position]...), failure), parts[position:]...)
									require.Error(t, send(strings.Join(statement, " ")+" RETURN 1 AS done", map[string]any{"run": run, "zero": int64(0)}, explicit), "the statement fails")
									require.Zero(t, marks(target, run), "nothing the failed statement wrote is kept")
								})
							}
						}
					}
				}
			}
		})
	}
}
