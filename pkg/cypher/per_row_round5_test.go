package cypher

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Expected values are Neo4j 5.26.30's for the same statements, never another
// NornicDB route. Statements separated by ";;" run in order on one store;
// the last one's rows are compared.
func runPerRowRound5Statements(t *testing.T, statements string) ([][]interface{}, error) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5"))
	var result *ExecuteResult
	var err error
	for _, statement := range strings.Split(statements, ";;") {
		result, err = exec.Execute(context.Background(), strings.TrimSpace(statement), nil)
		if err != nil {
			return nil, err
		}
	}
	return result.Rows, nil
}

// A subquery inside an expression the row evaluator hands to the shared
// evaluator: the shared evaluator evaluates that text itself and hands back
// only its parts (handedBackByRowEvaluator). Each of these overflowed the
// stack when the two evaluators handed the same text back and forth; a
// small maximum stack makes such a cycle fail at once.
func TestPerRowRound5SubqueryInHandedBackExpression(t *testing.T) {
	previous := debug.SetMaxStack(1 << 20)
	defer debug.SetMaxStack(previous)
	for _, tc := range []struct {
		name, statements string
		want             [][]interface{}
	}{
		{"point_x", "RETURN point({x: COUNT { RETURN 1 }, y: 0}).x AS v", [][]interface{}{{1.0}}},
		{"point_y", "RETURN point({x: 1, y: COUNT { RETURN 1 }}).y AS v", [][]interface{}{{1.0}}},
		{"point_crs", "RETURN point({x: COUNT { RETURN 1 }, y: 0}).crs AS v", [][]interface{}{{"cartesian"}}},
		{"point_latitude", "RETURN point({latitude: COUNT { RETURN 1 }, longitude: 0}).latitude AS v", [][]interface{}{{1.0}}},
		{"point_where", "UNWIND [1] AS x WITH x WHERE point({x: COUNT { RETURN x }, y: 0}).x = 1.0 RETURN x", [][]interface{}{{int64(1)}}},
		{"point_order", "UNWIND [1,2] AS x RETURN x ORDER BY point({x: COUNT { WITH x WHERE x > 1 RETURN x }, y: 0}).x DESC", [][]interface{}{{int64(2)}, {int64(1)}}},
		{"point_case", "UNWIND [1] AS x RETURN CASE WHEN point({x: COUNT { RETURN x }, y: 0}).x = 1.0 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}}},
		{"point_match", "CREATE (:E20 {v:1}) ;; MATCH (n:E20) RETURN point({x: COUNT { MATCH (n) RETURN n }, y: 0}).x AS v", [][]interface{}{{1.0}}},
		{"point_distance", "RETURN point.distance(point({x: COUNT { RETURN 1 }, y: 0}), point({x: 0, y: 0})) AS v", [][]interface{}{{1.0}}},
		{"point_comprehension", "RETURN [p IN [point({x: COUNT { RETURN 1 }, y: 0})] | p.x] AS v", [][]interface{}{{[]interface{}{1.0}}}},
		{"trim_head_collect", "RETURN trim(head(COLLECT { UNWIND [' a '] AS s RETURN s })) AS v", [][]interface{}{{"a"}}},
		{"value_type", "RETURN valueType(COUNT { RETURN 1 }) AS v", [][]interface{}{{"INTEGER NOT NULL"}}},
		{"null_if", "RETURN nullIf(COUNT { RETURN 1 }, 1) AS v", [][]interface{}{{nil}}},
		{"cosine", "RETURN vector.similarity.cosine(COLLECT { UNWIND [1.0,0.0] AS i RETURN i }, [1.0, 0.0]) AS v", [][]interface{}{{1.0}}},
		{"coalesce_trim", "RETURN coalesce(null, trim(head(COLLECT { UNWIND [' a '] AS s RETURN s }))) AS v", [][]interface{}{{"a"}}},
		{"size_trim", "RETURN size(trim(head(COLLECT { UNWIND [' ab '] AS s RETURN s }))) AS v", [][]interface{}{{int64(2)}}},
		{"where_trim", "UNWIND [1] AS x WITH x WHERE trim(head(COLLECT { UNWIND [' a '] AS s RETURN s })) = 'a' RETURN x", [][]interface{}{{int64(1)}}},
		{"set_trim", "CREATE (n:A26) SET n.v = trim(head(COLLECT { UNWIND [' a '] AS s RETURN s })) RETURN n.v AS v", [][]interface{}{{"a"}}},
		{"order_trim", "UNWIND [2,1] AS x RETURN x ORDER BY trim(toString(COUNT { WITH x WHERE x > 1 RETURN x })), x", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"to_integer_trim", "RETURN toInteger(trim(toString(COUNT { RETURN 1 }))) AS v", [][]interface{}{{int64(1)}}},
		{"ltrim_coalesce", "RETURN ltrim(coalesce(head(COLLECT { UNWIND [' a'] AS s RETURN s }), '')) AS v", [][]interface{}{{"a"}}},
		{"trim_case", "RETURN trim(CASE WHEN COUNT { RETURN 1 } = 1 THEN ' a ' END) AS v", [][]interface{}{{"a"}}},
		{"btrim", "RETURN btrim(head(COLLECT { UNWIND ['xax'] AS s RETURN s }), 'x') AS v", [][]interface{}{{"a"}}},
		{"normalize", "RETURN normalize(head(COLLECT { UNWIND ['a'] AS s RETURN s })) AS v", [][]interface{}{{"a"}}},
		{"char_length", "RETURN char_length(head(COLLECT { UNWIND ['ab'] AS s RETURN s })) AS v", [][]interface{}{{int64(2)}}},
		{"correlated_collect", "CREATE (:A41 {v:' a '}) ;; MATCH (n:A41) RETURN trim(head(COLLECT { MATCH (n) RETURN n.v })) AS v", [][]interface{}{{"a"}}},
		{"abs_coalesce_count", "RETURN abs(coalesce(COUNT { RETURN 1 },0)) AS v", [][]interface{}{{int64(1)}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.name, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				rows, err := runPerRowRound5Statements(t, tc.statements)
				require.NoError(t, err)
				require.Equal(t, tc.want, rows)
			})
		}
	}
}

// One policy decides which operand errors a fold discards
// (isolateOperandErrors): a subquery's error counts in an AND, whichever
// route evaluates it, and a deciding OR operand discards it.
func TestPerRowRound5SubqueryOperandErrors(t *testing.T) {
	previous := debug.SetMaxStack(1 << 20)
	defer debug.SetMaxStack(previous)
	for _, tc := range []struct {
		name, statements string
		want             [][]interface{}
		err              string
	}{
		{name: "where_exists_and", statements: "UNWIND [0,0] AS x WITH x WHERE EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x <> 0 RETURN x", err: "/ by zero"},
		{name: "where_count_and", statements: "UNWIND [0,0] AS x WITH x WHERE COUNT { WITH x WHERE 1/x=1 RETURN x } > 0 AND x <> 0 RETURN x", err: "/ by zero"},
		{name: "return_exists_and_tostring", statements: "UNWIND [0,0] AS x RETURN trim(toString(EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x <> 0)) AS v", err: "/ by zero"},
		{name: "where_exists_or", statements: "UNWIND [0,0] AS x WITH x WHERE EXISTS { WITH x WHERE 1/x=1 RETURN x } OR x = 0 RETURN x", want: [][]interface{}{{int64(0)}, {int64(0)}}},
		{name: "context_case_chain", statements: "UNWIND [0,0] AS x RETURN trim(toString(CASE WHEN 1/x < x < -1 THEN 1 ELSE 0 END)) AS v", want: [][]interface{}{{"0"}, {"0"}}},
		{name: "where_arithmetic_and", statements: "UNWIND [0,0] AS x WITH x WHERE (1/x = 1) AND x <> 0 RETURN x", want: [][]interface{}{}},
		{name: "return_arithmetic_or", statements: "UNWIND [0,0] AS x RETURN (1/x = 1) OR (x = 0) AS v", want: [][]interface{}{{true}, {true}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.name, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				rows, err := runPerRowRound5Statements(t, tc.statements)
				if tc.err != "" {
					requireRound3Neo4jError(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, rows)
			})
		}
	}
}

// The predicate plan (planRowPredicate) is the route production takes for
// a complete plan; its literal answers are Neo4j's, as the program corpus's.
func TestRowPredicatePlansMatchNeo4j(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	planned := 0
	for _, tc := range rowPredicateNeo4jCorpus {
		plan := planRowPredicate(tc.expression)
		if plan == nil {
			continue
		}
		planned++
		t.Run(tc.expression, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			row := rowValueCorpusRows(t, store)[0]
			row[rowStatementContextKey] = ctx
			require.Equal(t, tc.want, exec.evaluateRowPredicatePlan(ctx, plan, row))
			require.NoError(t, getExpressionFailure(ctx))
		})
	}
	require.NotZero(t, planned, "the corpus must reach the planned route")
}

// A relationship read with its record has endpoints without a storage
// check; a deleted relationship's view has none (relationshipEndpoint).
func TestPerRowRound5EndpointReadsNoRelationshipRecord(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	_ = store
	edge := &storage.Edge{ID: "r5-missing", Type: "R", StartNode: "absent-start", EndNode: "absent-end", Properties: map[string]interface{}{"k": "v"}}
	require.False(t, mayBeDeletedRelationshipView(edge))
	view := (&deletedEntityViews{}).edge(edge)
	require.True(t, mayBeDeletedRelationshipView(view))
	require.Nil(t, exec.relationshipEndpoint(view, false))
}

// Operands that can't fail need no error slot (operandCannotFail), so the
// context evaluator's comparisons and a pattern predicate's operands cost no
// more than on the PR base (develop 59d59237: 4, 4, 6, 0, 0 and 42, 43).
func TestPerRowRound5FoldAllocationBudget(t *testing.T) {
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5alloc"))
	background := context.Background()
	_, err := exec.Execute(background, "CREATE (:R5N {v:1})-[:R]->(:R5N {v:2})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(background, "MATCH (n:R5N {v:1}) RETURN n", nil)
	require.NoError(t, err)
	node := result.Rows[0][0].(*storage.Node)
	ctx := withExpressionFailureSlot(background)
	measure := func(name string, maximum float64, run func()) {
		for range 10 {
			run()
		}
		allocations := testing.AllocsPerRun(100, run)
		t.Logf("%s: %g allocations; limit %g", name, allocations, maximum)
		if allocations > maximum {
			t.Errorf("%s: got %g allocations, maximum %g", name, allocations, maximum)
		}
	}
	nodes := map[string]*storage.Node{"n": node}
	for _, tc := range []struct {
		expression string
		maximum    float64
	}{
		{"CASE WHEN n.v > 0 THEN 1 ELSE 0 END", 4},
		{"CASE WHEN 0 < n.v < 5 THEN 1 ELSE 0 END", 4},
		{"CASE WHEN n.v > 0 AND n.v < 5 THEN 1 ELSE 0 END", 6},
		{"n.v > 0 AND n.v < 5", 0},
		{"n.v > 5 OR n.v < 5", 0},
	} {
		measure("context "+tc.expression, tc.maximum, func() { exec.evaluateExpressionWithContext(ctx, tc.expression, nodes, nil) })
	}
	row := map[string]interface{}{"n": node}
	for _, tc := range []struct {
		expression string
		maximum    float64
	}{
		{"(n)-->() OR n.v = 1", 42},
		{"n.v = 2 OR (n)-[:R]->()", 43},
	} {
		require.True(t, exec.evaluateRowPredicate(ctx, tc.expression, row))
		measure("predicate "+tc.expression, tc.maximum, func() { exec.evaluateRowPredicate(ctx, tc.expression, row) })
	}
	require.NoError(t, getExpressionFailure(ctx))
}
