package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// lifetimeFixture is a composite comp of two local constituents (one, two),
// with the Neo4j reference server as remote constituents over Bolt (remote)
// and HTTP (http) when it is configured, on a Badger engine with
// write-behind on or off.
type lifetimeFixture struct {
	engine  *storage.BadgerEngine
	mgr     *multidb.DatabaseManager
	admin   *StorageExecutor
	targets []string
}

func newLifetimeFixture(t *testing.T, writeBehind bool) *lifetimeFixture {
	t.Helper()
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	config := multidb.DefaultConfig()
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
	}
	mgr, err := multidb.NewDatabaseManager(engine, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	f := &lifetimeFixture{engine: engine, mgr: mgr, targets: []string{"one"}}
	refs := []multidb.ConstituentRef{}
	for _, db := range []string{"one", "two"} {
		require.NoError(t, mgr.CreateDatabase(db))
		refs = append(refs, multidb.ConstituentRef{Alias: db, DatabaseName: db, Type: "local", AccessMode: "read_write"})
	}
	for _, remote := range []struct{ alias, env string }{{"remote", "NORNICDB_NEO4J_REFERENCE_URI"}, {"http", "NORNICDB_NEO4J_REFERENCE_HTTP_URI"}} {
		if uri := os.Getenv(remote.env); uri != "" {
			refs = append(refs, multidb.ConstituentRef{Alias: remote.alias, DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri})
			f.targets = append(f.targets, remote.alias)
		}
	}
	require.NoError(t, mgr.CreateCompositeDatabase("comp", refs))
	store, err := mgr.GetStorage("one")
	require.NoError(t, err)
	f.admin = NewStorageExecutor(store)
	f.admin.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	f.admin.cache = nil
	_, err = f.admin.Execute(context.Background(), "CREATE CONSTRAINT FOR (n:LtUnique) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	return f
}

// session is an executor of the composite database.
func (f *lifetimeFixture) session(t *testing.T) *StorageExecutor {
	t.Helper()
	store, err := f.mgr.GetStorage("comp")
	require.NoError(t, err)
	session := NewStorageExecutor(store)
	session.SetDatabaseManager(f.admin.dbManager)
	session.cache = nil
	return session
}

// marks counts run's LtMark nodes on constituent alias, after write-behind
// lands, and removes them from a remote one so runs don't share data.
func (f *lifetimeFixture) marks(t *testing.T, alias, run string) int64 {
	t.Helper()
	require.NoError(t, f.engine.FlushWriteBehind())
	params := map[string]interface{}{"run": run}
	result, err := f.admin.Execute(context.Background(), "USE comp."+alias+" MATCH (n:LtMark {run: $run}) RETURN count(n) AS c", params)
	require.NoError(t, err)
	var count int64
	switch value := result.Rows[0][0].(type) {
	case int64:
		count = value
	case float64:
		count = int64(value)
	default:
		t.Fatalf("count is %T", value)
	}
	if alias != "one" && alias != "two" {
		_, err = f.admin.Execute(context.Background(), "USE comp."+alias+" MATCH (n:LtMark {run: $run}) DETACH DELETE n", params)
		require.NoError(t, err)
	}
	return count
}

// run executes statement on a composite session, in an explicit
// transaction when explicit is set (whose COMMIT's error is the outcome when
// the statement succeeded). A panic is recovered and reported as such.
func (f *lifetimeFixture) run(t *testing.T, ctx context.Context, statement string, params map[string]interface{}, explicit bool) (err error, panicked interface{}) {
	t.Helper()
	session := f.session(t)
	if explicit {
		_, beginErr := session.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, beginErr)
	}
	func() {
		defer func() { panicked = recover() }()
		_, err = session.Execute(ctx, statement, params)
	}()
	if explicit {
		_, commitErr := session.Execute(context.Background(), "COMMIT", nil)
		if err == nil && panicked == nil {
			err = commitErr
		}
		if panicked != nil || err != nil {
			require.Error(t, commitErr, "a failed statement's transaction can't commit")
		}
	}
	return err, panicked
}

// lifetimeProcedures registers the procedures the lifetime tests run:
// lifetime.write writes a node through the storage the procedure is given,
// lifetime.panic panics, and lifetime.cancel(x) cancels the statement's
// context (cancel) before returning x.
func lifetimeProcedures(t *testing.T, cancel *func()) {
	t.Helper()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "lifetime.write", Signature: "lifetime.write(run :: STRING, target :: STRING, at :: INTEGER) :: (w :: INTEGER)", Mode: ProcedureModeWrite,
		Params:  []ProcedureParam{{Name: "run", Type: "STRING"}, {Name: "target", Type: "STRING"}, {Name: "at", Type: "INTEGER"}},
		MinArgs: 3, MaxArgs: 3, Returns: []ProcedureColumn{{Name: "w", Type: "INTEGER"}},
	}, func(ctx context.Context, e *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("lt-%v-%v", args[0], args[2])), Labels: []string{"LtMark"},
			Properties: map[string]interface{}{"run": args[0], "database_id": args[1], "at": args[2]}}
		if _, err := e.getStorage(ctx).CreateNode(node); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{"w"}, Rows: [][]interface{}{{int64(1)}}}, nil
	}))
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "lifetime.panic", Signature: "lifetime.panic() :: (z :: INTEGER)", Mode: ProcedureModeRead,
		Returns: []ProcedureColumn{{Name: "z", Type: "INTEGER"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		panic("lifetime test: injected procedure panic")
	}))
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "lifetime.cancel" {
			return func(value int64) int64 {
				if *cancel != nil {
					(*cancel)()
				}
				return value
			}, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })
}

// A composite statement keeps nothing it wrote when it fails, whatever fails
// and wherever: N parts write one database (a local constituent, or a remote
// one over Bolt or HTTP) through Cypher and through a procedure writing the
// storage it is given, other parts read the other databases, and a failure
// is injected before, between and after them as an error (a division by
// zero), a panic (a procedure's), a cancellation (of the statement's
// context) or a constraint violation (on a local target). Autocommit and in
// an explicit transaction, with write-behind on and off; the same statement
// without the failure keeps every write.
func TestFabricStatementFailureAtEveryPositionKeepsNothing(t *testing.T) {
	var cancel func()
	lifetimeProcedures(t, &cancel)
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_behind=%v", writeBehind), func(t *testing.T) {
			f := newLifetimeFixture(t, writeBehind)
			for _, target := range f.targets {
				// The parts, in order: writers of target and readers of the
				// other local constituent. A remote constituent runs only
				// statements it knows: its storage is written through the
				// composite router (USE comp), a local one's through its own.
				writer := func(i int) string {
					return fmt.Sprintf("CALL { USE comp.%s CREATE (:LtMark {run: $run, at: %d}) RETURN 1 AS a%d }", target, i, i)
				}
				procedure := func(i int) string {
					if target == "one" {
						return fmt.Sprintf("CALL { USE comp.one CALL lifetime.write($run, 'one', %d) YIELD w RETURN w AS a%d }", i, i)
					}
					return fmt.Sprintf("CALL { USE comp CALL lifetime.write($run, '%s', %d) YIELD w RETURN w AS a%d }", target, i, i)
				}
				reader := func(i int) string {
					return fmt.Sprintf("CALL { USE comp.two MATCH (n:LtMark) RETURN count(n) AS a%d }", i)
				}
				parts := []string{writer(0), reader(1), procedure(2), reader(3), writer(4)}
				failures := map[string]string{
					"error":  "CALL { USE comp.two RETURN 1 / $zero AS f }",
					"panic":  "CALL { USE comp.two CALL lifetime.panic() YIELD z RETURN z AS f }",
					"cancel": "CALL { USE comp.two RETURN lifetime.cancel(1) AS f }",
				}
				if target == "one" {
					failures["constraint"] = "CALL { USE comp.one CREATE (:LtUnique {k: $run}) CREATE (:LtUnique {k: $run}) RETURN 1 AS f }"
				}
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/explicit=%v/no_failure", target, explicit), func(t *testing.T) {
						run := fmt.Sprintf("lt-ok-%d", time.Now().UnixNano())
						err, panicked := f.run(t, context.Background(), strings.Join(parts, " ")+" RETURN 1 AS done", map[string]interface{}{"run": run, "zero": int64(0)}, explicit)
						require.Nil(t, panicked)
						require.NoError(t, err)
						require.Equal(t, int64(3), f.marks(t, target, run), "every write is kept")
					})
					for kind, failure := range failures {
						for position := 0; position <= len(parts); position++ {
							t.Run(fmt.Sprintf("%s/explicit=%v/%s_at_%d", target, explicit, kind, position), func(t *testing.T) {
								run := fmt.Sprintf("lt-%s-%d-%d", kind, position, time.Now().UnixNano())
								statement := append(append(append([]string{}, parts[:position]...), failure), parts[position:]...)
								ctx, stop := context.WithCancel(context.Background())
								defer stop()
								cancel = stop
								defer func() { cancel = nil }()
								err, panicked := f.run(t, ctx, strings.Join(statement, " ")+" RETURN 1 AS done", map[string]interface{}{"run": run, "zero": int64(0)}, explicit)
								require.True(t, err != nil || panicked != nil, "the statement fails")
								if kind == "panic" {
									require.NotNil(t, panicked, "the panic reaches the caller")
								}
								require.Zero(t, f.marks(t, target, run), "nothing the failed statement wrote is kept")
							})
						}
					}
				}
			}
		})
	}
}

// Three parts, the middle one on each constituent failing or not, keep
// every write or none; a constraint failure in the middle part keeps none.
// (Review of round 8.)
func TestFabricThreePartStatementIsAtomic(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_behind=%v", writeBehind), func(t *testing.T) {
			f := newLifetimeFixture(t, writeBehind)
			for _, explicit := range []bool{false, true} {
				for _, middle := range append([]string{"two"}, f.targets...) {
					for _, failure := range []bool{false, true} {
						t.Run(fmt.Sprintf("explicit=%v/middle=%s/failure=%v", explicit, middle, failure), func(t *testing.T) {
							run := fmt.Sprintf("lt-three-%d", time.Now().UnixNano())
							params := map[string]interface{}{"run": run, "d": int64(1)}
							if failure {
								params["d"] = int64(0)
							}
							err, panicked := f.run(t, context.Background(), "CALL { USE comp.one CREATE (:LtMark {run: $run, stage: 1}) RETURN 1 AS w } CALL { USE comp."+middle+" RETURN 1 / $d AS z } CALL { WITH z USE comp.one CREATE (:LtMark {run: $run, stage: 3}) RETURN z AS done } RETURN done", params, explicit)
							require.Nil(t, panicked)
							want := int64(2)
							if failure {
								require.Error(t, err)
								want = 0
							} else {
								require.NoError(t, err)
							}
							require.Equal(t, want, f.marks(t, "one", run))
							require.Zero(t, f.marks(t, "two", run))
						})
					}
				}
				t.Run(fmt.Sprintf("explicit=%v/constraint", explicit), func(t *testing.T) {
					run := fmt.Sprintf("lt-three-constraint-%d", time.Now().UnixNano())
					err, panicked := f.run(t, context.Background(), "CALL { USE comp.one CREATE (:LtMark {run: $run}) CREATE (:LtUnique {k: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one CREATE (:LtUnique {k: $run}) RETURN w AS z } CALL { WITH z USE comp.two RETURN z AS done } RETURN done", map[string]interface{}{"run": run}, explicit)
					require.Nil(t, panicked)
					require.Error(t, err)
					require.Zero(t, f.marks(t, "one", run))
				})
			}
		})
	}
}

// A cancellation while the second part runs, and a transactional CALL nested
// in a part (directly or through apoc.cypher.run), fail the statement and
// keep nothing. (Review of round 8.)
func TestFabricStatementCancelAndNestedBatchKeepNothing(t *testing.T) {
	var cancel func()
	lifetimeProcedures(t, &cancel)
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_behind=%v", writeBehind), func(t *testing.T) {
			f := newLifetimeFixture(t, writeBehind)
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%v/cancel_in_part_two", explicit), func(t *testing.T) {
					run := fmt.Sprintf("lt-cancel-%d", time.Now().UnixNano())
					ctx, stop := context.WithCancel(context.Background())
					defer stop()
					cancel = stop
					defer func() { cancel = nil }()
					err, panicked := f.run(t, ctx, "CALL { USE comp.one CREATE (:LtMark {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.two RETURN lifetime.cancel(w) AS z } CALL { WITH z USE comp.one RETURN z AS done } RETURN done", map[string]interface{}{"run": run}, explicit)
					require.Nil(t, panicked)
					require.Error(t, err)
					require.Zero(t, f.marks(t, "one", run))
				})
				for _, dynamic := range []bool{false, true} {
					t.Run(fmt.Sprintf("explicit=%v/nested_batch/dynamic=%v", explicit, dynamic), func(t *testing.T) {
						run := fmt.Sprintf("lt-batch-%d", time.Now().UnixNano())
						body := "UNWIND [1] AS i CALL { WITH i CALL { WITH i CREATE (:LtMark {run: $run}) } IN TRANSACTIONS OF 1 ROW } IN TRANSACTIONS OF 1 ROW RETURN 1 AS z"
						query := "CALL { USE comp.one CREATE (:LtMark {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one " + body + " } CALL { WITH z USE comp.two RETURN z AS done } RETURN done"
						if dynamic {
							query = "CALL { USE comp.one CREATE (:LtMark {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.z AS z } CALL { WITH z USE comp.two RETURN z AS done } RETURN done"
						}
						err, panicked := f.run(t, context.Background(), query, map[string]interface{}{"run": run, "body": body}, explicit)
						require.Nil(t, panicked)
						require.Error(t, err)
						require.Zero(t, f.marks(t, "one", run))
					})
				}
			}
		})
	}
}

// A procedure that panics in a later part fails the statement: autocommit,
// every participant is rolled back; in an explicit transaction, the
// transaction is failed, so COMMIT keeps nothing and reports it. The panic
// reaches the caller. (Review of round 8.)
func TestFabricStatementPanicEndsEveryParticipant(t *testing.T) {
	var cancel func()
	lifetimeProcedures(t, &cancel)
	var coordinator *fabric.FabricTransaction
	var mu sync.Mutex
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "lifetime.panicWatched", Signature: "lifetime.panicWatched() :: (z :: INTEGER)", Mode: ProcedureModeRead,
		Returns: []ProcedureColumn{{Name: "z", Type: "INTEGER"}},
	}, func(ctx context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		mu.Lock()
		coordinator, _ = fabric.FabricTransactionFromContext(ctx)
		mu.Unlock()
		panic("lifetime test: injected procedure panic")
	}))
	for _, writeBehind := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("write_behind=%v/explicit=%v", writeBehind, explicit), func(t *testing.T) {
				f := newLifetimeFixture(t, writeBehind)
				run := fmt.Sprintf("lt-panic-%d", time.Now().UnixNano())
				err, panicked := f.run(t, context.Background(), "CALL { USE comp.one CREATE (:LtMark {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.two CALL lifetime.panicWatched() YIELD z RETURN z } CALL { WITH z USE comp.one RETURN z AS done } RETURN done", map[string]interface{}{"run": run}, explicit)
				require.NotNil(t, panicked, "the panic reaches the caller")
				require.NoError(t, err)
				require.Zero(t, f.marks(t, "one", run), "a failed statement never commits an earlier part")
				mu.Lock()
				defer mu.Unlock()
				if !explicit {
					require.NotNil(t, coordinator)
					require.Equal(t, "rolledback", coordinator.State(), "every participant is ended")
				}
			})
		}
	}
}

// A procedure writing the storage it is given on the composite database (the
// router) writes a remote constituent in the statement's remote session: a
// later failure keeps nothing there, Bolt and HTTP, autocommit and explicit.
// A local constituent is the control. (Review of round 8.)
func TestFabricProcedureStorageWriteJoinsTheStatement(t *testing.T) {
	var cancel func()
	lifetimeProcedures(t, &cancel)
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_behind=%v", writeBehind), func(t *testing.T) {
			f := newLifetimeFixture(t, writeBehind)
			for _, explicit := range []bool{false, true} {
				for _, target := range f.targets {
					for _, failure := range []bool{false, true} {
						t.Run(fmt.Sprintf("explicit=%v/target=%s/failure=%v", explicit, target, failure), func(t *testing.T) {
							run := fmt.Sprintf("lt-storage-%d", time.Now().UnixNano())
							d := int64(1)
							if failure {
								d = 0
							}
							err, panicked := f.run(t, context.Background(), "USE comp CALL lifetime.write($run, $target, 1) YIELD w RETURN w / $d AS bad", map[string]interface{}{"run": run, "target": target, "d": d}, explicit)
							require.Nil(t, panicked)
							want := int64(1)
							if failure {
								require.Error(t, err)
								want = 0
							} else {
								require.NoError(t, err)
							}
							require.Equal(t, want, f.marks(t, target, run))
						})
					}
				}
			}
		})
	}
}
