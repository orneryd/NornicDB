package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A remote constituent's engine inside a composite statement is one per
// constituent, which the statement recognises as its own. A constituent
// whose engine is not a remote engine cannot join the statement's remote
// session: every operation on it fails, and its engine is never reached.
func TestRemoteConstituentEngineJoinsTheStatement(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "far")
	c := &cypherFabricExecutor{base: NewStorageExecutor(inner), autoCommit: true, remote: fabric.NewRemoteFragmentExecutor()}
	t.Cleanup(func() { _ = c.remote.Close() })
	composite := c.statementFor(ctx, fabric.NewFabricTransaction("far"))

	unjoined := composite.remote("remote:far", inner)
	require.Same(t, unjoined, composite.remote("remote:far", inner), "one engine per constituent")
	_, err := unjoined.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Far"}})
	require.ErrorContains(t, err, "storage engine has unexpected type for 'remote:far'")
	_, err = unjoined.GetNode("n1")
	require.ErrorContains(t, err, "storage engine has unexpected type for 'remote:far'")
	count, err := inner.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count, "the engine is never reached")

	raw, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: "http://127.0.0.1:1", Database: "far"})
	require.NoError(t, err)
	bound := composite.remote("remote:raw", raw)
	require.NotSame(t, raw, bound, "the statement's engine is not the remote's own")
	require.Same(t, composite, membershipOfStorage(bound).statement)
	require.Nil(t, membershipOfStorage(raw).statement)
	exec, _, err := composite.on(ctx, "far", NewStorageExecutor(bound), bound)
	require.NoError(t, err)
	require.Same(t, storage.Engine(bound), exec.storage, "a statement on it stays on it")
}

// A write a composite statement refuses fails the procedures that run
// statements in batches (apoc.periodic.iterate, apoc.periodic.commit), which
// count other failures per operation; and a schema change, a knowledge-policy
// command or a suppression on a second database is refused before it is made.
func TestCompositeRefusalsFailTheirStatement(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	for name, fragment := range map[string]string{
		"periodic_iterate": "CALL apoc.periodic.iterate($iterate, 'CREATE (:AcAtom {run: $r})', {batchSize: 1}) YIELD total RETURN total AS w",
		"periodic_commit":  "CALL apoc.periodic.commit('CREATE (:AcAtom {run: $run}) RETURN 0 AS count', {run: $run}) YIELD updates RETURN updates AS w",
		"fulltext_index":   "CALL db.index.fulltext.createNodeIndex($index, ['AcAtom'], ['run']) YIELD name RETURN 1 AS w",
		"schema_command":   "CALL apoc.cypher.doIt('CREATE INDEX ' + $index + ' FOR (n:AcAtom) ON (n.run)', {}) YIELD value RETURN 1 AS w",
	} {
		t.Run(name, func(t *testing.T) {
			run := "refusal-" + name
			params := map[string]interface{}{"run": run}
			params["iterate"] = "UNWIND ['" + run + "'] AS r RETURN r"
			params["index"] = "refusal_" + name
			query := "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { WITH z USE comp.one " + fragment + " } RETURN w, z"
			_, err := fixture.exec.Execute(ctx, query, params)
			if strings.HasPrefix(name, "periodic_") {
				// #1565 rejects independent batches before invocation: their
				// commits cannot belong to the composite rollback owner.
				require.ErrorContains(t, err, "unsupported query type in transaction")
			} else {
				require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
			}
			require.Equal(t, int64(0), fixture.writes(t, "comp.one", run))
			require.Equal(t, int64(0), fixture.writes(t, "comp.two", run))
		})
	}
}

// Value fragments can still access graph storage through expression subqueries.
// They must read the selected participant and its pending writes, and nested
// writes must end with the same statement owner.
func TestFabricValueFragmentRunsInTheStatement(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		for _, shape := range []struct{ name, second string }{
			{"match_control", "CALL { WITH w USE comp.one MATCH (n:AcAtom {run:$run}) RETURN count(n) AS c }"},
			{"count_import", "CALL { WITH w USE comp.one RETURN COUNT { (n:AcAtom {run:$run}) } AS c }"},
			{"count", "CALL { USE comp.one RETURN COUNT { (n:AcAtom {run:$run}) } AS c }"},
			{"exists", "CALL { USE comp.one RETURN CASE WHEN EXISTS { (n:AcAtom {run:$run}) } THEN 1 ELSE 0 END AS c }"},
		} {
			t.Run(fmt.Sprintf("%s/explicit_%t", shape.name, explicit), func(t *testing.T) {
				e := f.compositeSession(t)
				t.Cleanup(func() {
					if e.txContext != nil && e.txContext.active {
						_, err := e.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
				if explicit {
					_, err := e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := e.Execute(ctx, "CALL { USE comp.one CREATE (:AcAtom {run:$run}) RETURN 1 AS w } "+shape.second+" RETURN w,c", map[string]interface{}{"run": t.Name()})
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.EqualValues(t, 1, result.Rows[0][1], "the value expression reads the selected participant's pending write")
				if explicit {
					_, err = e.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				require.Equal(t, int64(1), f.writes(t, "one", t.Name()))
			})
		}
		t.Run(fmt.Sprintf("nested_write/explicit_%t", explicit), func(t *testing.T) {
			e := f.compositeSession(t)
			t.Cleanup(func() {
				if e.txContext != nil && e.txContext.active {
					_, err := e.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
			if explicit {
				_, err := e.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := e.Execute(ctx, "CALL { USE comp.one RETURN COLLECT { CREATE (n:AcAtom {run:$run}) RETURN n } AS ns } CALL { USE comp.two RETURN 1 / $zero AS failed } RETURN failed", map[string]interface{}{"run": t.Name(), "zero": int64(0)})
			require.Error(t, err)
			if explicit {
				_, err = e.Execute(ctx, "COMMIT", nil)
				require.Error(t, err)
			}
			require.Zero(t, f.writes(t, "one", t.Name()))
			require.Zero(t, f.writes(t, "two", t.Name()))
		})
	}
	_, err := f.exec.Execute(ctx, "USE two CREATE (:AcAtom {run:'value-database'})", nil)
	require.NoError(t, err)
	for _, db := range []string{"comp", "two", "one"} {
		t.Run("selected_database_"+db, func(t *testing.T) {
			store, err := f.mgr.GetStorage(db)
			require.NoError(t, err)
			e := NewStorageExecutor(store)
			e.SetDatabaseManager(f.exec.dbManager)
			result, err := e.Execute(ctx, "CALL { USE comp.one RETURN COUNT { (n:AcAtom {run:'value-database'}) } AS c } RETURN c", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.EqualValues(t, 0, result.Rows[0][0])
		})
	}
}

type ownershipBeginCapture struct {
	storage.Engine
	begin   func() (*storage.BadgerTransaction, error)
	started *storage.BadgerTransaction
}

func (engine *ownershipBeginCapture) BeginTransaction() (*storage.BadgerTransaction, error) {
	tx, err := engine.begin()
	engine.started = tx
	return tx, err
}

func TestFabricParticipantBeginPreservesMetadataAndBindingCleanup(t *testing.T) {
	for _, registered := range []bool{false, true} {
		t.Run(fmt.Sprintf("registered_%t", registered), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			engine := &ownershipBeginCapture{Engine: base, begin: base.BeginTransaction}
			tx := fabric.NewFabricTransaction(t.Name())
			c := &cypherFabricExecutor{base: NewStorageExecutor(engine), autoCommit: true, transaction: tx}
			sub := &fabric.SubTransaction{ShardName: "local:one"}
			if registered {
				var err error
				sub, err = tx.GetOrOpen(sub.ShardName, false)
				require.NoError(t, err)
			}
			runningTransactions.mu.RLock()
			before := make(map[uint64]bool, len(runningTransactions.byNumber))
			for id := range runningTransactions.byNumber {
				before[id] = true
			}
			runningTransactions.mu.RUnlock()
			t.Cleanup(func() {
				if engine.started != nil && engine.started.IsActive() {
					require.NoError(t, engine.started.Rollback())
				}
				runningTransactions.mu.RLock()
				var issued []*runningTransaction
				for id, entry := range runningTransactions.byNumber {
					if !before[id] {
						issued = append(issued, entry)
					}
				}
				runningTransactions.mu.RUnlock()
				for _, entry := range issued {
					runningTransactions.end(entry)
				}
			})
			participant, err := c.ensureLocalShardTxExecutor(WithTransactionMetadata(context.Background(), map[string]any{"client": "ownership-proof"}), sub, "one", engine)
			require.NotNil(t, engine.started)
			require.Equal(t, "ownership-proof", engine.started.GetMetadata()["client"], "canonical BEGIN applies caller metadata")
			if registered {
				require.NoError(t, err)
				require.True(t, participant.txContext.coordinatorOwned)
				require.NotNil(t, participant.txContext.running)
				require.NoError(t, tx.Rollback(nil))
				require.False(t, engine.started.IsActive())
			} else {
				require.ErrorContains(t, err, "not found")
				require.False(t, engine.started.IsActive(), "a native transaction without a bound coordinator is rolled back")
				runningTransactions.mu.RLock()
				after := len(runningTransactions.byNumber)
				runningTransactions.mu.RUnlock()
				require.Equal(t, len(before), after, "failed ownership transfer releases registration")
			}
		})
	}
}

func TestExplicitStatementPanicCannotCommitEarlierWrites(t *testing.T) {
	f := newFabricAtomicityFixture(t)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "ownershippanic.fail", Mode: ProcedureModeRead}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		panic("owned statement panic")
	}))
	t.Cleanup(ClearUserProcedures)
	for _, composite := range []bool{false, true} {
		t.Run(fmt.Sprintf("composite_%t", composite), func(t *testing.T) {
			e := f.compositeSession(t)
			if !composite {
				store, err := f.mgr.GetStorage("one")
				require.NoError(t, err)
				e = NewStorageExecutor(store)
				e.SetDatabaseManager(f.exec.dbManager)
			}
			ctx := context.Background()
			_, err := e.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				if e.txContext != nil && e.txContext.active {
					_, rollbackErr := e.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, rollbackErr)
				}
			})
			query := "CREATE (:AcAtom {run:$run}) WITH 1 AS v CALL ownershippanic.fail() RETURN v"
			if composite {
				query = "USE comp.one " + query
			}
			require.PanicsWithValue(t, "owned statement panic", func() { _, _ = e.Execute(ctx, query, map[string]interface{}{"run": t.Name()}) })
			_, err = e.Execute(ctx, "COMMIT", nil)
			require.ErrorContains(t, err, "was rolled back")
			require.Zero(t, f.writes(t, "one", t.Name()))
		})
	}
}
