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

	// The canonical comma reader handles nesting, strings and quoted names.
	parts := splitTopLevelComma("a, apoc.map.fromPairs([[1,2],[3,4]]), 'x,y', `q,r`, b")
	require.Equal(t, []string{"a", "apoc.map.fromPairs([[1,2],[3,4]])", "'x,y'", "`q,r`", "b"}, parts)
	parts = splitTopLevelComma("a, \"x,y\", {k:[1,2,3]}, b")
	require.Equal(t, []string{"a", "\"x,y\"", "{k:[1,2,3]}", "b"}, parts)
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

	exec.fabricRecordBindings = map[string]interface{}{"name": nil, "x": nil}
	// normalizeFabricRowWrapper branches including sourceId fallback lookup
	stream := &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{map[string]interface{}{"sourceId": "s1", "rows": []interface{}{map[string]interface{}{"sourceId": "s1", "name": "neo"}}}}}}
	require.NoError(t, exec.normalizeFabricRowWrapper("MATCH (n) RETURN name", stream))
	require.Equal(t, []string{"name"}, stream.Columns)
	require.Equal(t, [][]interface{}{{"neo"}}, stream.Rows)

	stream = &fabric.ResultStream{Columns: []string{"not_wrapper"}, Rows: [][]interface{}{{"x"}}}
	require.NoError(t, exec.normalizeFabricRowWrapper("RETURN x", stream))
	require.Equal(t, []string{"not_wrapper"}, stream.Columns)

	stream = &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{123}}}
	require.NoError(t, exec.normalizeFabricRowWrapper("RETURN x", stream))
	require.Equal(t, []string{"__fabric_row"}, stream.Columns)

	// lookupColumnBySourceIDInRows []map branch and missing keys
	v, ok := lookupColumnBySourceIDInRows([]map[string]interface{}{{"sourceId": "a", "col": int64(7)}}, "a", "col")
	require.True(t, ok)
	require.Equal(t, int64(7), v)
	_, ok = lookupColumnBySourceIDInRows([]interface{}{42}, "a", "col")
	require.False(t, ok)
}

// A local participant begins its transaction on its first write, which
// claims its database, and hands the transaction's completion to the
// coordinator: nothing is visible before the coordinator commits, and a
// rollback keeps nothing. The statement shares one participant per
// database.
func TestFabricHelpers_LocalShardCompletionBinding(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	c := &cypherFabricExecutor{base: NewStorageExecutor(store), autoCommit: true}
	count := func() interface{} {
		t.Helper()
		result, err := c.base.Execute(ctx, "MATCH (n:BindProbe) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0]
	}

	tx := fabric.NewFabricTransaction("tx-bind")
	composite := c.statementFor(ctx, tx)
	participant, err := composite.participant("nornic", store)
	require.NoError(t, err)
	again, err := composite.participant("nornic", store)
	require.NoError(t, err)
	require.Same(t, participant, again)
	exec, runCtx := participant.run(ctx, composite)
	_, err = exec.Execute(runCtx, "MATCH (n:BindProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Empty(t, tx.WriteShard(), "a read claims nothing")
	require.NotNil(t, participant.storage.lazy.tx.Load(), "a read begins the transaction, claiming nothing")
	_, err = exec.Execute(runCtx, "CREATE (:BindProbe)", nil)
	require.NoError(t, err)
	require.Equal(t, "local:nornic", tx.WriteShard())
	require.Equal(t, int64(0), count(), "nothing is visible before the coordinator commits")
	require.NoError(t, tx.Commit(nil, nil))
	require.Equal(t, int64(1), count())

	tx = fabric.NewFabricTransaction("tx-rollback")
	c = &cypherFabricExecutor{base: c.base, autoCommit: true}
	composite = c.statementFor(ctx, tx)
	participant, err = composite.participant("nornic", store)
	require.NoError(t, err)
	exec, runCtx = participant.run(ctx, composite)
	_, err = exec.Execute(runCtx, "CREATE (:BindProbe)", nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(nil))
	require.Equal(t, int64(1), count(), "a rollback keeps nothing")
}

// A composite statement claims a database where a write reaches its
// storage: a read claims nothing, the first write claims its database and
// begins its transaction, a later read there sees the write, and a write to
// a second database is refused before it is made, through its participant
// storage or the composite router; a remote constituent without a remote
// engine can't join the statement and is refused. A statement run
// inside the composite statement runs no CALL { ... } IN TRANSACTIONS batch
// of its own: the composite statement commits as one.
func TestCompositeParticipantsClaimAtTheirFirstWrite(t *testing.T) {
	ctx := context.Background()
	inner := newTestMemoryEngine(t)
	one := storage.NewNamespacedEngine(inner, "one")
	two := storage.NewNamespacedEngine(inner, "two")
	far := storage.NewNamespacedEngine(inner, "far")
	catalog := fabric.NewCatalog()
	catalog.Register("one", &fabric.LocationLocal{DBName: "one"})
	catalog.Register("comp.one", &fabric.LocationLocal{DBName: "one"})
	catalog.Register("two", &fabric.LocationLocal{DBName: "two"})
	catalog.Register("comp.two", &fabric.LocationLocal{DBName: "two"})
	catalog.Register("comp.far", &fabric.LocationRemote{URI: "bolt://far:7687", DBName: "far"})
	// The remote's executor names its own database: it routes as the remote.
	catalog.Register("far", &fabric.LocationRemote{URI: "bolt://far:7687", DBName: "far"})
	newStatement := func() (*fabric.FabricTransaction, *compositeStatement) {
		tx := fabric.NewFabricTransaction("claims")
		c := &cypherFabricExecutor{base: NewStorageExecutor(one), autoCommit: true, catalog: catalog}
		return tx, c.statementFor(ctx, tx)
	}
	run := func(composite *compositeStatement, database string, engine storage.Engine, query string) (*ExecuteResult, error) {
		t.Helper()
		exec, runCtx, err := composite.on(ctx, database, NewStorageExecutor(engine))
		require.NoError(t, err)
		return exec.Execute(runCtx, query, nil)
	}
	count := func(engine storage.Engine, label string) interface{} {
		t.Helper()
		result, err := NewStorageExecutor(engine).Execute(ctx, "MATCH (n:"+label+") RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0]
	}

	tx, composite := newStatement()
	_, err := run(composite, "two", two, "MATCH (n) RETURN count(n)")
	require.NoError(t, err)
	_, err = run(composite, "two", two, "MATCH (n:Missing) SET n.x = 1")
	require.NoError(t, err)
	require.Empty(t, tx.WriteShard(), "reads and a write that changes nothing claim nothing")
	_, err = run(composite, "comp.one", one, "CREATE (:Claim)")
	require.NoError(t, err)
	require.Equal(t, "local:one", tx.WriteShard(), "an alias claims its database")
	result, err := run(composite, "one", one, "MATCH (n:Claim) RETURN count(n) AS c")
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0], "a read on the write participant reads its writes")
	_, err = run(composite, "two", two, "CREATE (:Claim)")
	require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
	_, err = run(composite, "comp.far", far, "CREATE (:Claim)")
	require.ErrorContains(t, err, "storage engine has unexpected type", "a remote constituent that can't join the statement's remote session writes nothing")
	root := storage.NewCompositeEngine(map[string]storage.Engine{"two": two}, map[string]string{"two": "two"}, map[string]string{"two": "read_write"})
	root.SetCompositeName("comp")
	_, err = composite.router(root).CreateNode(&storage.Node{ID: "routed", Labels: []string{"Claim"}})
	require.ErrorIs(t, err, fabric.ErrSecondWriteShard, "the composite routes to its constituents' participants")
	require.NoError(t, tx.Commit(nil, nil))
	require.Equal(t, int64(1), count(one, "Claim"))
	require.Equal(t, int64(0), count(two, "Claim"))
	require.Equal(t, int64(0), count(far, "Claim"))

	tx, composite = newStatement()
	for _, query := range []string{
		"UNWIND [1] AS i CALL { WITH i MATCH (n) RETURN count(n) AS c } IN TRANSACTIONS OF 1 ROW RETURN c",
		"UNWIND [1, 2] AS i CALL { WITH i CREATE (:Batch {i: i}) } IN TRANSACTIONS OF 1 ROW",
	} {
		_, err = run(composite, "two", two, query)
		require.ErrorContains(t, err, "transactional CALL on composite targets is not supported", query)
	}
	require.Empty(t, tx.WriteShard(), "a refused batch claims nothing")
	require.NoError(t, tx.Rollback(nil))
	require.Equal(t, int64(0), count(two, "Batch"), "and writes nothing")
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
