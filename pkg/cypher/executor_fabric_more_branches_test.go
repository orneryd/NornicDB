package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFabricHelpers_MoreRewriteAndKeywordBranches(t *testing.T) {
	// stripLeadingWithImportsForFabricRecord guard branches
	require.Equal(t, "RETURN 1", stripLeadingWithImportsForFabricRecord("RETURN 1", map[string]interface{}{}))
	require.Equal(t, "MATCH (n) RETURN n", stripLeadingWithImportsForFabricRecord("MATCH (n) RETURN n", map[string]interface{}{"x": 1}))
	require.Equal(t, "WITH x", stripLeadingWithImportsForFabricRecord("WITH x", map[string]interface{}{"x": 1}))
	require.Equal(t, "WITH x CREATE (n)", stripLeadingWithImportsForFabricRecord("WITH x CREATE (n)", map[string]interface{}{"x": 1}))
	require.Equal(t, "WITH x, y MATCH (n) RETURN n", stripLeadingWithImportsForFabricRecord("WITH x, y MATCH (n) RETURN n", map[string]interface{}{"x": 1}))
	require.Equal(t, "WITH x AS y MATCH (n) RETURN n", stripLeadingWithImportsForFabricRecord("WITH x AS y MATCH (n) RETURN n", map[string]interface{}{"x": 1, "y": 2}))
	require.Equal(t, "USE shard RETURN $x", stripLeadingWithImportsForFabricRecord("WITH x USE shard RETURN x", map[string]interface{}{"x": 1}))

	// findLeadingWithEndLocal branches
	require.Equal(t, -1, findLeadingWithEndLocal("WITH x + 1"))
	require.Greater(t, findLeadingWithEndLocal("WITH x, y MATCH (n) RETURN n"), 0)
	require.Greater(t, findLeadingWithEndLocal("WITH apoc.map.fromPairs([[1,2]]) RETURN 1"), 0)
	for _, query := range []string{
		"WITH x OPTIONAL MATCH (n) RETURN n",
		"WITH x WHERE x IS NOT NULL RETURN x",
		"WITH x USE nornic RETURN x",
		"WITH x WITH x AS y RETURN y",
		"WITH x CALL db.labels()",
		"WITH x CREATE (n)",
		"WITH x MERGE (n:N {id:1})",
		"WITH x UNWIND [1] AS i RETURN i",
		"WITH x SET x = 1 RETURN x",
		"WITH x DELETE x",
		"WITH x DETACH DELETE x",
	} {
		require.Greater(t, findLeadingWithEndLocal(query), 0)
	}
	require.Greater(t, findLeadingWithEndLocal("WITH 'MATCH' AS x RETURN x"), 0)
	require.Greater(t, findLeadingWithEndLocal("WITH \"MATCH\" AS x RETURN x"), 0)
	require.Greater(t, findLeadingWithEndLocal("WITH `MATCH` AS x RETURN x"), 0)
	require.Greater(t, findLeadingWithEndLocal("WITH [1, 2, 3] AS xs MATCH (n) RETURN n"), 0)
	require.Greater(t, findLeadingWithEndLocal("WITH {k: 1} AS m MATCH (n) RETURN n"), 0)
	require.Equal(t, -1, findLeadingWithEndLocal("WITH 'unterminated"))

	// startsWithKeywordAtLocal boundaries
	require.False(t, startsWithKeywordAtLocal("AMATCH", 1, "MATCH"))
	require.False(t, startsWithKeywordAtLocal("MATCHED", 0, "MATCH"))
	require.True(t, startsWithKeywordAtLocal(" MATCH ", 1, "MATCH"))
	require.False(t, startsWithKeywordAtLocal("MATCH", -1, "MATCH"))

	// splitCommaTopLevelLocal with nesting/quotes
	parts := splitCommaTopLevelLocal("a, apoc.map.fromPairs([[1,2],[3,4]]), 'x,y', `q,r`, b")
	require.Equal(t, []string{"a", " apoc.map.fromPairs([[1,2],[3,4]])", " 'x,y'", " `q,r`", " b"}, parts)
	parts = splitCommaTopLevelLocal("a, \"x,y\", {k:[1,2,3]}, b")
	require.Equal(t, []string{"a", " \"x,y\"", " {k:[1,2,3]}", " b"}, parts)
}

func TestFabricHelpers_MoreContextAndWrapperBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	// preparedFabricFromContext branches
	prepared, err := exec.preparedFabricFromContext(context.Background())
	require.NoError(t, err)
	require.Nil(t, prepared)

	ctxBad := context.WithValue(context.Background(), fabricPreparedExecKey{}, 123)
	prepared, err = exec.preparedFabricFromContext(ctxBad)
	require.Error(t, err)
	require.Nil(t, prepared)

	ctxGood := context.WithValue(context.Background(), fabricPreparedExecKey{}, &fabricPreparedExec{})
	prepared, err = exec.preparedFabricFromContext(ctxGood)
	require.NoError(t, err)
	require.NotNil(t, prepared)

	// executeViaPreparedFabricWithTx nil prepared branch
	_, err = exec.executeViaPreparedFabricWithTx(context.Background(), "RETURN 1", nil, fabric.NewFabricTransaction("tx-1"), true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not prepared")

	// normalizeFabricRowWrapper branches including sourceId fallback lookup
	stream := &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{map[string]interface{}{"sourceId": "s1", "rows": []interface{}{map[string]interface{}{"sourceId": "s1", "name": "neo"}}}}}}
	exec.normalizeFabricRowWrapper("MATCH (n) RETURN name", stream)
	require.Equal(t, []string{"name"}, stream.Columns)
	require.Equal(t, [][]interface{}{{"neo"}}, stream.Rows)

	stream = &fabric.ResultStream{Columns: []string{"not_wrapper"}, Rows: [][]interface{}{{"x"}}}
	exec.normalizeFabricRowWrapper("RETURN x", stream)
	require.Equal(t, []string{"not_wrapper"}, stream.Columns)

	stream = &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{123}}}
	exec.normalizeFabricRowWrapper("RETURN x", stream)
	require.Equal(t, []string{"__fabric_row"}, stream.Columns)

	// lookupColumnBySourceIDInRows []map branch and missing keys
	v, ok := lookupColumnBySourceIDInRows([]map[string]interface{}{{"sourceId": "a", "col": int64(7)}}, "a", "col")
	require.True(t, ok)
	require.Equal(t, int64(7), v)
	_, ok = lookupColumnBySourceIDInRows([]interface{}{42}, "a", "col")
	require.False(t, ok)
}

// A local sub-transaction hands its completion to the coordinator that
// dispatched it. A coordinator without the participant refuses it, and the
// sub-transaction just begun is rolled back rather than left open.
func TestFabricHelpers_LocalShardCompletionBinding(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	c := &cypherFabricExecutor{base: NewStorageExecutor(store), autoCommit: true}
	tx := fabric.NewFabricTransaction("tx-bind")
	sub, err := tx.GetOrOpen("s1", true)
	require.NoError(t, err)
	exec, err := c.ensureLocalShardTxExecutor(context.Background(), tx, sub, "nornic", store)
	require.NoError(t, err)
	again, err := c.ensureLocalShardTxExecutor(context.Background(), tx, sub, "nornic", store)
	require.NoError(t, err)
	require.Same(t, exec, again)
	_, err = exec.Execute(context.Background(), "CREATE (:BindProbe)", nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(nil, nil))
	result, err := c.base.Execute(context.Background(), "MATCH (n:BindProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	_, err = c.ensureLocalShardTxExecutor(context.Background(), fabric.NewFabricTransaction("tx-missing"), &fabric.SubTransaction{ShardName: "missing"}, "nornic", store)
	require.Error(t, err)
	require.NotContains(t, c.localShardTxExecutors(), "missing")
}

// A statement of a composite statement opens its write when it runs: one
// that writes claims its database (FabricTransaction.ClaimWrite) and runs in
// the database's sub-transaction, and so does a later statement there, which
// reads the write; one that reads runs outside it. A write on a second
// database is refused. CALL { ... } IN TRANSACTIONS claims its database but
// commits its own batches. In an explicit transaction every statement runs in
// its database's sub-transaction. A remote constituent a USE selects is
// claimed by its own identity and has no local sub-transaction.
func TestFabricScopeOpensWritesWhenTheyRun(t *testing.T) {
	catalog := fabric.NewCatalog()
	catalog.Register("one", &fabric.LocationLocal{DBName: "one"})
	catalog.Register("comp.one", &fabric.LocationLocal{DBName: "one"})
	catalog.Register("two", &fabric.LocationLocal{DBName: "two"})
	catalog.Register("comp.far", &fabric.LocationRemote{URI: "bolt://far:7687", DBName: "far"})
	tx := fabric.NewFabricTransaction("scope")
	c := &cypherFabricExecutor{autoCommit: true}
	scope := func(db string, joins bool) *fabricScope {
		return &fabricScope{executor: c, tx: tx, catalog: catalog, dbName: db, joins: joins}
	}
	open := func(s *fabricScope, statement string, inTransaction bool) (string, bool) {
		t.Helper()
		participant, join, err := s.open(statement, inTransaction)
		require.NoError(t, err, statement)
		return participant, join
	}

	participant, join := open(scope("two", true), "MATCH (n) RETURN n", false)
	require.Equal(t, "local:two", participant)
	require.False(t, join, "a read runs outside the sub-transaction")
	require.Empty(t, tx.WriteShard(), "a read claims nothing")

	participant, join = open(scope("comp.one", true), "CREATE (n) RETURN n", false)
	require.Equal(t, "local:one", participant, "an alias claims its database")
	require.True(t, join)
	require.Equal(t, "local:one", tx.WriteShard())
	_, join = open(scope("one", true), "MATCH (n) RETURN n", false)
	require.True(t, join, "a read on the write participant reads its writes")
	_, join = open(scope("one", true), "CREATE (n) RETURN n", true)
	require.False(t, join, "a statement already in a transaction stays in it")
	_, join = open(scope("one", false), "UNWIND [1] AS d CALL { WITH d CREATE (n) } IN TRANSACTIONS", false)
	require.False(t, join, "IN TRANSACTIONS commits its own batches")

	_, _, err := scope("two", true).open("CREATE (n) RETURN n", false)
	require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
	_, _, err = scope("comp.far", true).open("CALL db.labels() YIELD label RETURN label", false)
	require.NoError(t, err, "a remote read")
	_, _, err = scope("comp.far", true).open("CREATE (n) RETURN n", false)
	require.ErrorIs(t, err, fabric.ErrSecondWriteShard, "a remote write claims the remote")

	c.autoCommit = false
	_, join = open(scope("two", true), "MATCH (n) RETURN n", false)
	require.True(t, join, "an explicit transaction runs every statement in it")
	_, join = open(scope("comp.far", true), "MATCH (n) RETURN n", false)
	require.False(t, join, "a remote has no local sub-transaction")
}

// A statement's access mode comes from its text: its write clauses and the
// registered mode of every procedure it calls, an unknown procedure counting
// as a write. On a remote constituent only a built-in's mode is trusted: a
// user or plugin procedure, and a READ built-in that runs a statement it is
// given, count as writes.
func TestFabricAccessModeFromPlan(t *testing.T) {
	read := ProcedureSpec{Name: "access.mode.userRead", Signature: "access.mode.userRead() :: (x :: INTEGER)", Mode: ProcedureModeRead,
		Returns: []ProcedureColumn{{Name: "x", Type: "INTEGER"}}}
	write := read
	write.Name, write.Signature, write.Mode = "access.mode.userWrite", "access.mode.userWrite() :: (x :: INTEGER)", ProcedureModeWrite
	for _, spec := range []ProcedureSpec{read, write} {
		require.NoError(t, RegisterUserProcedure(spec, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	}
	t.Cleanup(ClearUserProcedures)
	for _, tc := range []struct {
		query         string
		local, remote bool
	}{
		{"MATCH (n) RETURN n", false, false},
		{"MATCH (n) RETURN 'CREATE (m)' AS text", false, false},
		{"CREATE (n) RETURN n", true, true},
		{"MATCH (n:Missing) SET n.v = 1", true, true},
		{"MATCH (n) FOREACH (x IN [1] | SET n.v = x)", true, true},
		{"CREATE INDEX access_mode_idx FOR (n:L) ON (n.p)", true, true},
		{"CALL db.labels() YIELD label RETURN label", false, false},
		{"CALL db.index.fulltext.createNodeIndex('i', ['L'], ['p'])", true, true},
		{"MATCH (n) CALL kp.suppress(n) RETURN count(*) AS c", true, true},
		{"CALL apoc.cypher.doIt('CREATE (n) RETURN n', {}) YIELD value RETURN value", true, true},
		{"CALL apoc.cypher.run('RETURN 1 AS x', {}) YIELD value RETURN value", false, true},
		{"CALL apoc.export.json.query('RETURN 1', null, {stream: true}) YIELD data RETURN data", false, true},
		{"CALL access.mode.unknown() YIELD x RETURN x", true, true},
		{"CALL { MATCH (n) RETURN n } RETURN n", false, false},
		{"CALL access.mode.userRead() YIELD x RETURN x", false, true},
		{"CALL { CALL access.mode.userWrite() YIELD x RETURN x } RETURN x", true, true},
		{"/*c*/CREATE/*d*/(n) RETURN n", true, true},
		{"MATCH (n) RETURN n.create AS c, 'SET' AS s, $remove AS r", false, false},
	} {
		require.Equal(t, tc.local, fragmentWrites(tc.query, false), "local: %s", tc.query)
		require.Equal(t, tc.remote, fragmentWrites(tc.query, true), "remote: %s", tc.query)
	}
}

// configureStatementTransaction refuses a transaction that has already ended.
func TestConfigureStatementTransactionRefusesEndedTransaction(t *testing.T) {
	tx, err := newTestMemoryEngine(t).BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Error(t, configureStatementTransaction(tx))
}

// The access mode of a statement text is decided once per procedure
// registry generation: a procedure registered later changes it.
func TestFabricAccessModeFollowsTheRegistry(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	query := "CALL access.mode.later() YIELD x RETURN x"
	require.True(t, fragmentWrites(query, false), "an unknown procedure counts as a write")
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "access.mode.later", Signature: "access.mode.later() :: (x :: INTEGER)",
		Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "x", Type: "INTEGER"}}},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	require.False(t, fragmentWrites(query, false), "a registered READ procedure reads")
}
