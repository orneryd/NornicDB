package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanonicalTypedProcedureArgumentBoundaries(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	spec := ProcedureSpec{Name: "typed", MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "value", Type: "ANY"}}}
	for _, query := range []string{"CALL typed()", "CALL typed(count(1))", "CALL typed(missing)", "CALL typed(1 / 0)"} {
		t.Run(query, func(t *testing.T) {
			arguments, err := exec.extractBoundProcedureInvocationArguments(context.Background(), spec, query)
			require.Error(t, err)
			require.Nil(t, arguments)
		})
	}
	arguments, err := exec.extractBoundProcedureInvocationArguments(withQueryParams(context.Background(), map[string]interface{}{"value": int64(7)}), spec, "CALL typed")
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(7)}, arguments)
	for _, arguments := range [][]interface{}{nil, {true, int64(1), []float32{1}}, {"index", true, []float32{1}}} {
		_, err := exec.callVectorQueryArguments(context.Background(), arguments, false)
		requireSyntaxErrorStatus(t, err, "invalid typed vector arguments")
	}
	for _, relationships := range []bool{false, true} {
		_, err := exec.callVectorQueryArguments(context.Background(), []interface{}{"index", int64(1), "text"}, relationships)
		require.ErrorContains(t, err, "embedder")
	}
}

func TestProcedurePipelinePreservesTypedRowArguments(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(context.Background(), "CREATE (a:ProcTyped {name:'a',value:1}), (b:ProcTyped {name:'b',value:2}), (c:ProcTyped {name:'c',value:3}), (a)-[:PROC_TYPED]->(b), (b)-[:PROC_TYPED]->(c)", nil)
	require.NoError(t, err)
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	var calls [][]interface{}
	require.NoError(t, RegisterUserProcedure(
		ProcedureSpec{Name: "custom.pipeline_entities", Mode: ProcedureModeRead, MinArgs: 3, MaxArgs: 3},
		func(_ context.Context, _ *StorageExecutor, _ string, arguments []interface{}) (*ExecuteResult, error) {
			calls = append(calls, arguments)
			return &ExecuteResult{Columns: []string{"name"}, Rows: [][]interface{}{{arguments[0].(*storage.Node).Properties["name"]}}}, nil
		},
	))
	query := "MATCH (n:ProcTyped)-[r:PROC_TYPED]->(m:ProcTyped) CALL custom.pipeline_entities(n,r,{target:m,values:[n.value,m.value],tag:$tag,parameter:$precision}) YIELD name RETURN name ORDER BY name"
	params := map[string]interface{}{"tag": "typed", "precision": []float32{0.12345679}}
	result, err := exec.executeRequiredPipeline(withQueryParams(context.Background(), params), query)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a"}, {"b"}}, result.Rows)
	public, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err)
	require.Equal(t, result.Columns, public.Columns)
	require.Equal(t, result.Rows, public.Rows)
	require.Len(t, calls, 4)
	for _, arguments := range calls {
		require.Len(t, arguments, 3)
		node := arguments[0].(*storage.Node)
		edge := arguments[1].(*storage.Edge)
		payload := arguments[2].(map[string]interface{})
		target := payload["target"].(*storage.Node)
		require.Equal(t, node.ID, edge.StartNode)
		require.Equal(t, target.ID, edge.EndNode)
		require.Equal(t, []interface{}{node.Properties["value"], target.Properties["value"]}, payload["values"])
		require.Equal(t, "typed", payload["tag"])
		require.Equal(t, params["precision"], payload["parameter"])
	}
}

func TestCallUnionPreservesTypedValuesAndWriteStats(t *testing.T) {
	t.Run("typed distinct values", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		result, err := exec.executeRequiredPipeline(context.Background(), "CALL { RETURN 1 AS value UNION RETURN '1' AS value } RETURN value")
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {"1"}}, result.Rows)
		public, err := exec.Execute(context.Background(), "CALL { RETURN 1 AS value UNION RETURN '1' AS value } RETURN value", nil)
		require.NoError(t, err)
		require.Equal(t, result.Rows, public.Rows)
	})
	t.Run("recorded failure stops later writes", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		result, err := exec.executeRequiredPipeline(context.Background(), `CALL {
			RETURN 1 / 0 AS value
			UNION ALL
			CREATE (n:CallUnionErrorCopy {value:1}) RETURN n.value AS value
		} RETURN value`)
		require.ErrorContains(t, err, "/ by zero")
		if result != nil {
			require.Empty(t, result.Rows)
		}
		persisted, err := exec.Execute(context.Background(), "MATCH (n:CallUnionErrorCopy) RETURN count(n) AS count", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, persisted.Rows)
	})
	t.Run("branch writes", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		result, err := exec.Execute(context.Background(), `UNWIND [1, 2] AS x CALL (x) {
			CREATE (n:CallUnionWrite {value:x, branch:1}) RETURN n.value AS value
			UNION ALL
			CREATE (n:CallUnionWrite {value:x, branch:2}) RETURN n.value AS value
		} RETURN count(*) AS count`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
		require.Equal(t, 4, result.Stats.NodesCreated)
		persisted, err := exec.Execute(context.Background(), "MATCH (n:CallUnionWrite) RETURN n.value, n.branch ORDER BY n.value, n.branch", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}, {int64(2), int64(1)}, {int64(2), int64(2)}}, persisted.Rows)
	})
}

func TestCallUnionRunsInSharedPipeline(t *testing.T) {
	for _, fixture := range []struct {
		name      string
		separator string
		want      [][]interface{}
	}{
		{name: "distinct", separator: "UNION", want: [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}}},
		{name: "all", separator: "UNION ALL", want: [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(1)}, {int64(2), int64(2)}, {int64(2), int64(2)}}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			query := "UNWIND [1, 2] AS x CALL (x) { RETURN x AS y " + fixture.separator + " RETURN x AS y } RETURN x, y ORDER BY x, y"
			result, err := exec.executeRequiredPipeline(context.Background(), query)
			require.NoError(t, err)
			require.Equal(t, []string{"x", "y"}, result.Columns)
			require.Equal(t, fixture.want, result.Rows)
			public, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, result.Columns, public.Columns)
			require.Equal(t, result.Rows, public.Rows)
		})
	}
}

// TestCallSubqueryImportsAnyOuterVariable: a CALL subquery after MATCH sees
// every variable the MATCH binds, not only its first node, as in Neo4j
// 5.26 (#648).
func TestCallSubqueryImportsAnyOuterVariable(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"CALL { RETURN 1 AS x } RETURN *", [][]interface{}{{int64(1)}}},
		{"CALL { RETURN 1 AS x } WITH x AS y RETURN y", [][]interface{}{{int64(1)}}},
		{"CALL { RETURN 1 AS x } WITH x AS y WHERE y = 1 RETURN y", [][]interface{}{{int64(1)}}},
		{"CALL { RETURN 1 AS x } UNWIND [x, x + 1] AS y RETURN x, y ORDER BY y", [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}}},
		{"CALL { RETURN 1 AS x } MATCH (n:C648) RETURN x, n.id AS id ORDER BY id", [][]interface{}{{int64(1), "a"}, {int64(1), "b"}, {int64(1), "c"}}},
		{"CALL { CREATE (n:C648 {id:'write'}) RETURN n } RETURN n.id AS id", [][]interface{}{{"write"}}},
		{"MATCH (o:C648 {id:'b'}) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { WITH o RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL { WITH o RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { RETURN o AS z } RETURN z.id AS z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (i) { RETURN i.id AS z } RETURN z, o.id AS o ORDER BY o", [][]interface{}{{"a", "b"}, {"a", "c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) WITH o CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'}), (o:C648 {id:'c'}) CALL (o) { RETURN o.id AS z } RETURN i.id AS i, z", [][]interface{}{{"a", "c"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n.id AS i CALL (i) { RETURN i AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n.id AS i CALL { WITH i RETURN i AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) OPTIONAL MATCH (n)-[:NONE]->(t) WITH n, t CALL { WITH n, t WITH n, t WHERE t IS NULL RETURN n.id AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, {k: 'v'} AS m CALL (m) { RETURN m.k AS j } RETURN j", [][]interface{}{{"v"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, 'b' AS s CALL (s) { RETURN s AS j } RETURN j", [][]interface{}{{"b"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, elementId(n) AS s CALL (s) { RETURN s AS j } RETURN j = elementId(n) AS j", [][]interface{}{{true}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, {id: elementId(n)} AS m CALL (m) { RETURN m AS j } RETURN j.id = elementId(n) AS j", [][]interface{}{{true}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, [elementId(n)] AS l CALL (l) { RETURN size(l) AS j } RETURN j", [][]interface{}{{int64(1)}}},
		{"MATCH (i:C648 {id:'a'}) RETURN COLLECT { MATCH (i)-->(o) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z } AS l", [][]interface{}{{[]interface{}{"b", "c"}}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call648"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:C648 {id:'a'})-[:USES]->(b:C648 {id:'b'}), (a)-[:USES]->(c:C648 {id:'c'})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
		if tc.query == "CALL { RETURN 1 AS x } RETURN *" {
			require.Equal(t, []string{"x"}, result.Columns)
		}
	}
}

func TestCallSubqueryTailPreservesEmptyWildcardSchemaAndWriteStats(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call648_tail"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CALL { MATCH (n:C648 {id:'missing'}) RETURN n AS x } RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, result.Columns)
	require.Empty(t, result.Rows)

	result, err = exec.Execute(ctx, "CALL { CREATE (n:C648 {id:'write'}) RETURN n } WITH n AS created RETURN created.id AS id", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, result.Columns)
	require.Equal(t, [][]interface{}{{"write"}}, result.Rows)
	require.EqualValues(t, 1, result.Stats.NodesCreated)
}

func TestCallSubqueryRefreshesOuterNodeAfterWrite(t *testing.T) {
	for _, explicitTx := range []bool{false, true} {
		name := "auto-commit"
		if explicitTx {
			name = "explicit-transaction"
		}
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "call648_refresh")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:C648Refresh {id:'a', x:5}), (:C648Refresh {id:'b', x:3})", nil)
			require.NoError(t, err)

			if explicitTx {
				_, err = exec.handleBegin()
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, "MATCH (n:C648Refresh {id:'a'}) CALL (n) { SET n.y = 2 } RETURN n.y AS y", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"y"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

			result, err = exec.Execute(ctx, "MATCH (n:C648Refresh {id:'a'}) CALL { WITH n SET n.y = 3 } RETURN n.y AS y", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)

			result, err = exec.Execute(ctx, "MATCH (n:C648Refresh {id:'a'}) CALL (n) { SET n:C648Added } RETURN n:C648Added AS hasLabel", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{true}}, result.Rows)

			result, err = exec.Execute(ctx, "MATCH (n:C648Refresh {id:'a'}), (m:C648Refresh {id:'b'}) CALL (n) { SET n.y = 4 } RETURN n.y AS y, m.id AS id", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"y", "id"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(4), "b"}}, result.Rows)
			if explicitTx {
				_, err = exec.handleCommit()
				require.NoError(t, err)
			}
		})
	}
}
