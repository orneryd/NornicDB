package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
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
	_, err = exec.executeViaFabricWithTx(context.Background(), "RETURN 1", nil, fabric.NewFabricTransaction("tx-1"), true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not prepared")

	exec.fabricRecordBindings = map[string]interface{}{"name": nil, "x": nil}
	// normalizeFabricRowWrapper branches including sourceId fallback lookup
	stream := &fabric.ResultStream{Columns: []string{"__fabric_row"}, RowWrapper: true, Rows: [][]interface{}{{map[string]interface{}{"sourceId": "s1", "rows": []interface{}{map[string]interface{}{"sourceId": "s1", "name": "neo"}}}}}}
	require.NoError(t, exec.normalizeFabricRowWrapper(t.Context(), "MATCH (n) RETURN name", stream))
	require.Equal(t, []string{"name"}, stream.Columns)
	require.Equal(t, [][]interface{}{{"neo"}}, stream.Rows)

	stream = &fabric.ResultStream{Columns: []string{"not_wrapper"}, Rows: [][]interface{}{{"x"}}}
	require.NoError(t, exec.normalizeFabricRowWrapper(t.Context(), "RETURN x", stream))
	require.Equal(t, []string{"not_wrapper"}, stream.Columns)

	stream = &fabric.ResultStream{Columns: []string{"__fabric_row"}, RowWrapper: true, Rows: [][]interface{}{{123}}}
	require.NoError(t, exec.normalizeFabricRowWrapper(t.Context(), "RETURN x", stream))
	require.Equal(t, []string{"__fabric_row"}, stream.Columns)

	// A query column named like the wrapper is the query's own.
	stream = &fabric.ResultStream{Columns: []string{"__fabric_row"}, Rows: [][]interface{}{{map[string]interface{}{"name": "neo"}}}}
	exec.normalizeFabricRowWrapper(t.Context(), "RETURN name", stream)
	require.Equal(t, []string{"__fabric_row"}, stream.Columns)

	// lookupColumnBySourceIDInRows []map branch and missing keys
	v, ok := lookupColumnBySourceIDInRows([]map[string]interface{}{{"sourceId": "a", "col": int64(7)}}, "a", "col")
	require.True(t, ok)
	require.Equal(t, int64(7), v)
	_, ok = lookupColumnBySourceIDInRows([]interface{}{42}, "a", "col")
	require.False(t, ok)
}

func TestFabricHelpers_TransactionOwnerAndCallbacks(t *testing.T) {
	c := &cypherFabricExecutor{}
	require.Nil(t, c.fabricTransaction())

	c.base = NewStorageExecutor(newTestMemoryEngine(t))
	require.Nil(t, c.fabricTransaction())

	c.base.txContext = &TransactionContext{active: true, tx: struct{}{}}
	require.Nil(t, c.fabricTransaction())

	tx := fabric.NewFabricTransaction("tx-bind")
	_, err := tx.GetOrOpen("s1", false)
	require.NoError(t, err)
	c.base.txContext = &TransactionContext{active: true, tx: tx}
	require.Same(t, tx, c.fabricTransaction())
	require.NoError(t, c.fabricTransaction().BindParticipantCallbacks("s1", fabric.ParticipantCallbacks{Commit: func(*fabric.SubTransaction) error { return nil }, Rollback: func(*fabric.SubTransaction) error { return nil }}))
	err = c.fabricTransaction().BindParticipantCallbacks("missing", fabric.ParticipantCallbacks{Commit: nil, Rollback: nil})
	require.Error(t, err)
}
