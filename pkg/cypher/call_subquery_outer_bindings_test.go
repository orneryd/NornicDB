package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineProcedureEmptyInputContracts(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	fixtureAdmission108Context, fixtureAdmission108 := statementTestContext(ctx, "MATCH (n:Missing) CALL dbms.components()")
	_ = fixtureAdmission108
	_, err := exec.executeRequiredPipeline(fixtureAdmission108, fixtureAdmission108Context, "MATCH (n:Missing) CALL dbms.components()")
	requireSyntaxErrorStatus(t, err, "non-void procedure requires explicit YIELD inside a query")
	fixtureAdmission109Context, fixtureAdmission109 := statementTestContext(ctx, "MATCH (n:Missing) CALL dbms.components() YIELD name AS component, versions RETURN component, versions")
	_ = fixtureAdmission109
	result, err := exec.executeRequiredPipeline(fixtureAdmission109, fixtureAdmission109Context, "MATCH (n:Missing) CALL dbms.components() YIELD name AS component, versions RETURN component, versions")
	require.NoError(t, err)
	require.Equal(t, []string{"component", "versions"}, result.Columns)
	require.Empty(t, result.Rows)
	fixtureAdmission110Context, fixtureAdmission110 := statementTestContext(ctx, "MATCH (n:Missing) CALL db.unknownPipelineProcedure() YIELD value RETURN value")
	_ = fixtureAdmission110
	_, err = exec.executeRequiredPipeline(fixtureAdmission110, fixtureAdmission110Context, "MATCH (n:Missing) CALL db.unknownPipelineProcedure() YIELD value RETURN value")
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Procedure.ProcedureNotFound")
	fixtureAdmission111Context, fixtureAdmission111 := statementTestContext(ctx, "MATCH (n:Missing) CALL dbms.components(1) YIELD name RETURN name")
	_ = fixtureAdmission111
	_, err = exec.executeRequiredPipeline(fixtureAdmission111, fixtureAdmission111Context, "MATCH (n:Missing) CALL dbms.components(1) YIELD name RETURN name")
	requireSyntaxErrorStatus(t, err, "invalid argument count even on empty input")
}

func TestPipelineProcedureRequiresWithAfterSet(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ProcedureBefore {value:1})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:ProcedureBefore) SET n.value = 2 CALL dbms.components() YIELD name RETURN n.value AS value", nil)
	requireSyntaxErrorStatus(t, err, "SET requires WITH before CALL")
	unchanged, err := exec.Execute(ctx, "MATCH (n:ProcedureBefore) RETURN n.value AS value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, unchanged.Rows)
	updated, err := exec.Execute(ctx, "MATCH (n:ProcedureBefore) SET n.value = 2 WITH n CALL dbms.components() YIELD name RETURN n.value AS value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, updated.Rows)
}

func TestPipelineDBMSProcedureRunsPerInputRow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	var calls int64
	require.NoError(t, RegisterUserProcedure(
		ProcedureSpec{Name: "custom.dbms_count", Mode: ProcedureModeDBMS, MinArgs: 0, MaxArgs: 0, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			calls++
			return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{calls}}}, nil
		},
	))
	result, err := exec.Execute(context.Background(), "UNWIND [1,2] AS input CALL custom.dbms_count() YIELD value RETURN input, value ORDER BY input", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}}, result.Rows)
	require.Equal(t, int64(2), calls)
}

func TestCanonicalTypedProcedureArgumentBoundaries(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	spec := ProcedureSpec{Name: "typed", MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "value", Type: "ANY"}}}
	for _, query := range []string{"CALL typed()", "CALL typed(count(1))", "CALL typed(missing)", "CALL typed(1 / 0)"} {
		t.Run(query, func(t *testing.T) {
			fixtureAdmission1859Context, fixtureAdmission1859 := statementTestContext(context.Background(), query)
			_ = fixtureAdmission1859Context
			arguments, err := exec.extractBoundProcedureInvocationArguments(fixtureAdmission1859, fixtureAdmission1859Context, spec, query)
			require.Error(t, err)
			require.Nil(t, arguments)
		})
	}
	fixtureAdmission1860Context, fixtureAdmission1860 := statementTestContext(withQueryParams(context.Background(), map[string]interface{}{"value": int64(7)}), "CALL typed")
	_ = fixtureAdmission1860Context
	arguments, err := exec.extractBoundProcedureInvocationArguments(fixtureAdmission1860, fixtureAdmission1860Context, spec, "CALL typed")
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(7)}, arguments)
	_, err = exec.callVectorQueryArguments(context.Background(), nil, false)
	requireSyntaxErrorStatus(t, err, "no vector arguments")
	// A value of another type reaching the procedure is a TypeError; the
	// statement's static check refuses it first, as Neo4j does (Type
	// mismatch, a SyntaxError).
	for _, arguments := range [][]interface{}{{true, int64(1), []float32{1}}, {"index", true, []float32{1}}} {
		_, err := exec.callVectorQueryArguments(context.Background(), arguments, false)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
	}
	for _, query := range []string{
		"WITH true AS v CALL db.index.vector.queryNodes(v, 2, [1.0, 2.0]) YIELD node RETURN node",
		"WITH true AS v CALL db.index.vector.queryNodes('i', v, [1.0, 2.0]) YIELD node RETURN node",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		requireSyntaxErrorStatus(t, err, query)
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
	fixtureAdmission112Context, fixtureAdmission112 := statementTestContext(withQueryParams(context.Background(), params), query)
	_ = fixtureAdmission112
	result, err := exec.executeRequiredPipeline(fixtureAdmission112, fixtureAdmission112Context, query)
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
		fixtureAdmission113Context, fixtureAdmission113 := statementTestContext(context.Background(), "CALL { RETURN 1 AS value UNION RETURN '1' AS value } RETURN value")
		_ = fixtureAdmission113
		result, err := exec.executeRequiredPipeline(fixtureAdmission113, fixtureAdmission113Context, "CALL { RETURN 1 AS value UNION RETURN '1' AS value } RETURN value")
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {"1"}}, result.Rows)
		public, err := exec.Execute(context.Background(), "CALL { RETURN 1 AS value UNION RETURN '1' AS value } RETURN value", nil)
		require.NoError(t, err)
		require.Equal(t, result.Rows, public.Rows)
	})
	t.Run("recorded failure stops later writes", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		fixtureAdmission114Context, fixtureAdmission114 := statementTestContext(context.Background(), `CALL {
			RETURN 1 / 0 AS value
			UNION ALL
			CREATE (n:CallUnionErrorCopy {value:1}) RETURN n.value AS value
		} RETURN value`)
		_ = fixtureAdmission114
		result, err := exec.executeRequiredPipeline(fixtureAdmission114, fixtureAdmission114Context, `CALL {
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
			fixtureAdmission115Context, fixtureAdmission115 := statementTestContext(context.Background(), query)
			_ = fixtureAdmission115
			result, err := exec.executeRequiredPipeline(fixtureAdmission115, fixtureAdmission115Context, query)
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
				_, err = exec.handleCommit(context.Background())
				require.NoError(t, err)
			}
		})
	}
}
