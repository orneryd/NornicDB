package cypher

// Unit and behavior tests for the traversal-seeded OPTIONAL MATCH pipeline
// helpers (optional_match_traversal*.go): pattern parsing, shape routing,
// projection fast paths and compilation, aggregation accumulation and
// finalization, and the runtime edge branches of the general Apply + Optional
// path. Assertions mirror Neo4j semantics (see the Neo4j operator citations
// in the implementation files).

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newUnitExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	return NewStorageExecutor(ns), context.Background()
}

func TestGh713TraversalProjectionUsesSharedFailureBoundary(t *testing.T) {
	for _, route := range []string{"direct", "compiled"} {
		for _, test := range []struct {
			expression string
			value      interface{}
			code       string
		}{
			{"missing", nil, "Neo.ClientError.Statement.SyntaxError"},
			{"missingFunction(n)", nil, "Neo.ClientError.Statement.SyntaxError"},
			{"[, ]", nil, "Neo.ClientError.Statement.SyntaxError"},
			{"1 / 0", nil, "Neo.ClientError.Statement.ArithmeticError"},
			{"n.missing", nil, ""},
			{"m.name", nil, ""},
			{"r.weight", int64(2), ""},
			{"r.missing", nil, ""},
			{"missingRel.weight", nil, ""},
			{"value + 1", int64(2), ""},
		} {
			t.Run(route+"/"+test.expression, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				ctx = withExpressionFailureSlot(ctx)
				row := traversalOptRow{
					nodes: map[string]*storage.Node{
						"n": {ID: "n", Properties: map[string]interface{}{"name": "Alice"}},
						"m": nil,
					},
					rels: map[string]*storage.Edge{
						"r":          {ID: "r", Properties: map[string]interface{}{"weight": int64(2)}},
						"missingRel": nil,
					},
					values: map[string]interface{}{"value": int64(1)},
				}
				var value interface{}
				if route == "compiled" {
					fixtureAdmission764Context, fixtureAdmission764 := expressionTestContext(ctx, test.expression)
					_ = fixtureAdmission764
					value = exec.compileTraversalProjection(fixtureAdmission764, fixtureAdmission764Context, test.expression)(row)
				} else {
					fixtureAdmission765Context, fixtureAdmission765 := expressionTestContext(ctx, test.expression)
					_ = fixtureAdmission765
					value = exec.evaluateTraversalRowExpression(fixtureAdmission765, fixtureAdmission765Context, test.expression, row)
				}
				require.Equal(t, test.value, value)
				if test.code == "" {
					require.NoError(t, getExpressionFailure(ctx))
				} else {
					requireStatusCode(t, getExpressionFailure(ctx), test.code)
				}
			})
		}
	}
}

func TestGh713TraversalProjectionErrorsPreserveGraph(t *testing.T) {
	for _, route := range []string{"autocommit", "explicit transaction"} {
		for _, test := range []struct {
			projection string
			code       string
		}{
			{"missingFunction(n)", "Neo.ClientError.Statement.SyntaxError"},
			{"1 / $zero", "Neo.ClientError.Statement.ArithmeticError"},
			{"missing.name", ""},
		} {
			t.Run(route+"/"+test.projection, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, "CREATE (:ProjectionSeed {id: 1})-[:LINK]->(:ProjectionTarget {id: 2})", nil)
				require.NoError(t, err)
				if route == "explicit transaction" {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				query := "MATCH (n:ProjectionSeed)-[:LINK]->(target) OPTIONAL MATCH (target)-[:NONE]->(missing) "
				if test.code != "" {
					query += "SET n.flag = true "
				}
				result, err := exec.Execute(ctx, query+"RETURN "+test.projection+" AS value", map[string]interface{}{"zero": int64(0)})
				if test.code == "" {
					require.NoError(t, err)
					require.Equal(t, []string{"value"}, result.Columns)
					require.Equal(t, [][]interface{}{{nil}}, result.Rows)
				} else {
					requireStatusCode(t, err, test.code)
				}
				if route == "explicit transaction" {
					command := "COMMIT"
					if test.code != "" {
						command = "ROLLBACK"
					}
					_, err = exec.Execute(ctx, command, nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (n:ProjectionSeed) RETURN n.id AS id, n.flag AS flag", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1), nil}}, stored.Rows)
			})
		}
	}
}

func TestGh713AggregateNumberRejectsNonNumeric(t *testing.T) {
	for _, value := range []interface{}{nil, true, "4", []interface{}{int64(4)}} {
		floating, integer, integral, valid := pipelineAggregateNumber(value)
		require.Zero(t, floating, "%T", value)
		require.Zero(t, integer, "%T", value)
		require.False(t, integral, "%T", value)
		require.False(t, valid, "%T", value)
	}
}

func TestGh713SharedAggregateAdapterBoundaries(t *testing.T) {
	t.Run("empty projection rejected", func(t *testing.T) {
		_, emptyProjectionStatement := statementTestContext(context.Background(), "RETURN")
		executor, ctx := newUnitExecutor(t)
		rows, err := executor.aggregateTraversalOptionalRows(emptyProjectionStatement, ctx, nil, nil)
		require.Nil(t, rows)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	})
	t.Run("unaliased aggregate", func(t *testing.T) {
		executor, ctx := newUnitExecutor(t)
		items := []returnItem{{expr: "count(*)", alias: "count(*)"}}
		plan := returnProjectionPlanFromItems(items)
		require.True(t, plan.valid)
		require.Equal(t, []string{"count(*)"}, plan.columns)
		fixtureAdmission1658Context, fixtureAdmission1658 := statementTestContext(ctx, "RETURN "+("count(*)"))
		_ = fixtureAdmission1658Context
		rows, err := executor.aggregateTraversalOptionalRows(fixtureAdmission1658, fixtureAdmission1658Context, []traversalOptRow{{values: map[string]interface{}{}}}, items)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	})
	t.Run("expression failure preserved", func(t *testing.T) {
		_, emptyProjectionStatement := statementTestContext(context.Background(), "RETURN")
		executor, ctx := newUnitExecutor(t)
		ctx = withExpressionFailureSlot(ctx)
		failure := newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "invalid aggregate input")
		recordExpressionFailure(ctx, failure)
		rows, err := executor.aggregateTraversalOptionalRows(emptyProjectionStatement, ctx, nil, nil)
		require.Nil(t, rows)
		require.ErrorIs(t, err, failure)
	})
}

func TestGh713PipelineSumNumericTypes(t *testing.T) {
	tests := []struct {
		name     string
		values   []interface{}
		distinct bool
		want     interface{}
	}{
		{"int", []interface{}{int(4), int(5)}, false, int64(9)},
		{"int8", []interface{}{int8(4), int8(5)}, false, int64(9)},
		{"int16", []interface{}{int16(4), int16(5)}, false, int64(9)},
		{"int32", []interface{}{int32(4), int32(5)}, false, int64(9)},
		{"int64", []interface{}{int64(4), int64(5)}, false, int64(9)},
		{"uint", []interface{}{uint(4), uint(5)}, false, int64(9)},
		{"uint8", []interface{}{uint8(4), uint8(5)}, false, int64(9)},
		{"uint16", []interface{}{uint16(4), uint16(5)}, false, int64(9)},
		{"uint32", []interface{}{uint32(4), uint32(5)}, false, int64(9)},
		{"uint64", []interface{}{uint64(4), uint64(5)}, false, int64(9)},
		{"large signed", []interface{}{int64(9007199254740993), int64(1)}, false, int64(9007199254740994)},
		{"large unsigned", []interface{}{uint64(9007199254740993), uint64(1)}, false, int64(9007199254740994)},
		{"float first", []interface{}{float64(0.5), int32(4)}, false, float64(4.5)},
		{"float last", []interface{}{int16(4), float32(0.5)}, false, float64(4.5)},
		{"distinct and null", []interface{}{nil, int64(9007199254740993), int64(9007199254740993), int64(1)}, true, int64(9007199254740994)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			state := pipelineAggregateState{name: "sum", expression: "value", distinct: test.distinct}
			for _, value := range test.values {
				fixtureAdmission1659Context, fixtureAdmission1659 := statementTestContext(ctx, "RETURN "+("value"))
				_ = fixtureAdmission1659Context
				require.True(t, state.add(fixtureAdmission1659, fixtureAdmission1659Context, exec, pipelineRow{"value": value}))
			}
			fixtureAdmission1660Context, fixtureAdmission1660 := statementTestContext(ctx, "RETURN "+("value"))
			_ = fixtureAdmission1660Context
			actual, ok := state.result(fixtureAdmission1660, fixtureAdmission1660Context, exec)
			require.True(t, ok)
			require.Equal(t, test.want, actual)
		})
	}
}

func TestExtractRelationshipVariables_Shapes(t *testing.T) {
	require.Equal(t, []string{"r"}, extractRelationshipVariables("(a)-[r]->(b)"), "bare variable")
	require.Equal(t, []string{"r"}, extractRelationshipVariables("(a)-[r:T]->(b)"), "typed variable")
	require.Equal(t, []string{"r"}, extractRelationshipVariables("(a)-[r*1..3]->(b)"), "variable-length variable")
	require.Empty(t, extractRelationshipVariables("(a)-[:T]->(b)"), "anonymous typed relationship")
	require.Empty(t, extractRelationshipVariables("(a)-[*2]->(b)"), "anonymous variable-length")
	require.Equal(t, []string{"r1", "r2"}, extractRelationshipVariables("(a)-[r1:X]->(b)<-[r2:Y]-(c)"), "chain")
}

func TestInvertOptionalDirection(t *testing.T) {
	require.Equal(t, "in", invertOptionalDirection("out"))
	require.Equal(t, "out", invertOptionalDirection("in"))
	require.Equal(t, "both", invertOptionalDirection("both"))
}

func TestIsSimpleTraversalIdentifier(t *testing.T) {
	require.True(t, isSimpleTraversalIdentifier("abc"))
	require.True(t, isSimpleTraversalIdentifier("_a1"))
	require.False(t, isSimpleTraversalIdentifier(""))
	require.False(t, isSimpleTraversalIdentifier("9a"), "digit first")
	require.False(t, isSimpleTraversalIdentifier("a-b"), "operator char")
}

func TestParseOptionalClauseEndpoints_ErrorsAndRelForms(t *testing.T) {
	exec, ctx := newUnitExecutor(t)

	fixtureAdmission1336Context, fixtureAdmission1336 := traversalTestContext(ctx, "no parens here", "", nil)
	_ = fixtureAdmission1336Context
	_, err := exec.parseOptionalClauseEndpoints(fixtureAdmission1336, fixtureAdmission1336Context, "no parens here")
	require.Error(t, err, "no node endpoint")
	fixtureAdmission1337Context, fixtureAdmission1337 := traversalTestContext(ctx, "(a", "", nil)
	_ = fixtureAdmission1337Context
	_, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1337, fixtureAdmission1337Context, "(a")
	require.Error(t, err, "unterminated source endpoint")
	fixtureAdmission1338Context, fixtureAdmission1338 := traversalTestContext(ctx, "(a)-[r:T]->", "", nil)
	_ = fixtureAdmission1338Context
	_, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1338, fixtureAdmission1338Context, "(a)-[r:T]->")
	require.Error(t, err, "no target endpoint")
	fixtureAdmission1339Context, fixtureAdmission1339 := traversalTestContext(ctx, "(a)-[r:T]->(b", "", nil)
	_ = fixtureAdmission1339Context
	_, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1339, fixtureAdmission1339Context, "(a)-[r:T]->(b")
	require.Error(t, err, "unterminated target endpoint")

	fixtureAdmission1340Context, fixtureAdmission1340 := traversalTestContext(ctx, "(a)-[r:T {w:1}]->(b)", "", nil)
	_ = fixtureAdmission1340Context
	eps, err := exec.parseOptionalClauseEndpoints(fixtureAdmission1340, fixtureAdmission1340Context, "(a)-[r:T {w:1}]->(b)")
	require.NoError(t, err)
	require.Equal(t, "T", eps.relType, "props stripped from relationship type")

	fixtureAdmission1341Context, fixtureAdmission1341 := traversalTestContext(ctx, "(a)-[r:T*1..2]->(b)", "", nil)
	_ = fixtureAdmission1341Context
	eps, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1341, fixtureAdmission1341Context, "(a)-[r:T*1..2]->(b)")
	require.NoError(t, err)
	require.Equal(t, "T", eps.relType, "variable-length hops stripped from type")

	fixtureAdmission1342Context, fixtureAdmission1342 := traversalTestContext(ctx, "(a)-[r*1..2]->(b)", "", nil)
	_ = fixtureAdmission1342Context
	eps, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1342, fixtureAdmission1342Context, "(a)-[r*1..2]->(b)")
	require.NoError(t, err)
	require.Equal(t, "r", eps.relVar, "variable-length without type keeps the variable")
	require.Empty(t, eps.relType)

	fixtureAdmission1343Context, fixtureAdmission1343 := traversalTestContext(ctx, "(a)-[r]->(b)", "", nil)
	_ = fixtureAdmission1343Context
	eps, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1343, fixtureAdmission1343Context, "(a)-[r]->(b)")
	require.NoError(t, err)
	require.Equal(t, "r", eps.relVar, "bare relationship variable")

	fixtureAdmission1344Context, fixtureAdmission1344 := traversalTestContext(ctx, "(a)<-[:T]-(b)", "", nil)
	_ = fixtureAdmission1344Context
	eps, err = exec.parseOptionalClauseEndpoints(fixtureAdmission1344, fixtureAdmission1344Context, "(a)<-[:T]-(b)")
	require.NoError(t, err)
	require.Equal(t, "in", eps.direction)
}

func TestExtendTraversalRow_NoNewBindings(t *testing.T) {
	n := &storage.Node{ID: "n1"}
	row := traversalOptRow{nodes: map[string]*storage.Node{"a": n}, rels: map[string]*storage.Edge{}}
	out := extendTraversalRow(row, "", nil, "", nil)
	require.Equal(t, n, out.nodes["a"], "copy preserves existing bindings")
	require.Len(t, out.nodes, 1, "no new bindings added")
}

func TestFastTraversalExprValue_AllShapes(t *testing.T) {
	node := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "Ada"}}
	embedded := &storage.Node{ID: "n2", Properties: map[string]interface{}{}, EmbedMeta: map[string]any{"has_embedding": true}}
	chunked := &storage.Node{ID: "n3", Properties: map[string]interface{}{}, ChunkEmbeddings: [][]float32{{0.5}}}
	scalar := &storage.Node{ID: "n4", Properties: map[string]interface{}{"value": int64(7)}}
	edge := &storage.Edge{ID: "e1", Type: "REL", Properties: map[string]interface{}{"w": int64(2)}}
	row := traversalOptRow{
		nodes: map[string]*storage.Node{"n": node, "emb": embedded, "chk": chunked, "sc": scalar, "nilNode": nil},
		rels:  map[string]*storage.Edge{"r": edge, "nilRel": nil},
	}

	v, ok := fastTraversalExprValue("n.name", row)
	require.True(t, ok)
	require.Equal(t, "Ada", v)

	v, ok = fastTraversalExprValue("n.missing", row)
	require.True(t, ok)
	require.Nil(t, v, "missing property projects null")

	v, ok = fastTraversalExprValue("nilNode.name", row)
	require.True(t, ok)
	require.Nil(t, v, "null binding projects null")

	v, ok = fastTraversalExprValue("emb.has_embedding", row)
	require.True(t, ok)
	require.Equal(t, true, v, "has_embedding reads EmbedMeta")

	v, ok = fastTraversalExprValue("chk.has_embedding", row)
	require.True(t, ok)
	require.Equal(t, true, v, "has_embedding falls back to chunk embeddings")

	v, ok = fastTraversalExprValue("r.w", row)
	require.True(t, ok)
	require.EqualValues(t, 2, v)

	v, ok = fastTraversalExprValue("r.missing", row)
	require.True(t, ok)
	require.Nil(t, v)

	v, ok = fastTraversalExprValue("nilRel.w", row)
	require.True(t, ok)
	require.Nil(t, v)

	_, ok = fastTraversalExprValue("unbound.x", row)
	require.False(t, ok, "unbound variable falls back to the evaluator")

	_, ok = fastTraversalExprValue("1a.b", row)
	require.False(t, ok, "invalid identifier falls back")

	v, ok = fastTraversalExprValue("n", row)
	require.True(t, ok)
	require.Equal(t, node, v, "bare node variable returns the node")

	v, ok = fastTraversalExprValue("nilNode", row)
	require.True(t, ok)
	require.Nil(t, v)

	v, ok = fastTraversalExprValue("sc", row)
	require.True(t, ok)
	require.Equal(t, scalar, v, "a node whose only property is value stays a node")

	row.values = map[string]interface{}{"x": int64(7)}
	v, ok = fastTraversalExprValue("x", row)
	require.True(t, ok)
	require.EqualValues(t, 7, v, "a row value resolves from row.values")

	v, ok = fastTraversalExprValue("r", row)
	require.True(t, ok)
	require.Equal(t, edge, v, "bare relationship variable returns the edge")

	v, ok = fastTraversalExprValue("nilRel", row)
	require.True(t, ok)
	require.Nil(t, v)

	_, ok = fastTraversalExprValue("unboundVar", row)
	require.False(t, ok)

	_, ok = fastTraversalExprValue("a+b", row)
	require.False(t, ok, "non-identifier falls back")
}

func TestScanOptionalPatternShape_QuoteAware(t *testing.T) {
	groups, brackets := scanOptionalPatternShape(`(a)-[r:T {p:'([('}]->(b)`)
	require.Equal(t, 2, groups, "parens inside quoted strings are not node groups")
	require.Equal(t, 1, brackets, "brackets inside quoted strings are not rel sections")

	groups, brackets = scanOptionalPatternShape(`(a {s:"x(\"y["})`)
	require.Equal(t, 1, groups)
	require.Equal(t, 0, brackets)

	// A bracketless hop is one relationship, so (a)-->(b) takes the seeded
	// single-hop path like (a)-[]->(b).
	for _, pattern := range []string{"(a)-->(b)", "(a)<--(b)", "(a)--(b)", "(a)<-->(b)", "(a)-[r:T]->(b)"} {
		groups, brackets = scanOptionalPatternShape(pattern)
		require.Equal(t, [2]int{2, 1}, [2]int{groups, brackets}, pattern)
	}
	groups, brackets = scanOptionalPatternShape("(a)-->(b)<--(c)")
	require.Equal(t, [2]int{3, 2}, [2]int{groups, brackets})
}

func TestFirstParenGroup(t *testing.T) {
	require.Equal(t, "(a:L)", firstParenGroup("(a:L)-[r]->(b)"))
	require.Equal(t, "", firstParenGroup("no group"))
	require.Equal(t, "", firstParenGroup("(unterminated"))
}

func TestEnsureLeadingNodeNamed(t *testing.T) {
	require.Equal(t, "(a:L)-[:T]->(b)", ensureLeadingNodeNamed("(a:L)-[:T]->(b)"), "named pattern unchanged")
	require.Equal(t, "("+traversalAnonVar+":L)-[:T]->(:M)", ensureLeadingNodeNamed("(:L)-[:T]->(:M)"), "anonymous pattern gets a synthetic variable")
	require.Equal(t, "no parens", ensureLeadingNodeNamed("no parens"), "patterns without a node group pass through")
}

func TestCandidateAgreesWithRow(t *testing.T) {
	n1 := &storage.Node{ID: "n1"}
	n2 := &storage.Node{ID: "n2"}
	e1 := &storage.Edge{ID: "e1"}
	e2 := &storage.Edge{ID: "e2"}
	row := traversalOptRow{nodes: map[string]*storage.Node{"a": n1, "nilA": nil}, rels: map[string]*storage.Edge{"r": e1}}

	require.True(t, candidateAgreesWithRow(row, traversalOptRow{nodes: map[string]*storage.Node{"a": n1}, rels: map[string]*storage.Edge{"r": e1}}, []string{"a"}, []string{"r"}))
	require.False(t, candidateAgreesWithRow(row, traversalOptRow{nodes: map[string]*storage.Node{"a": n2}}, []string{"a"}, nil), "different node identity")
	require.False(t, candidateAgreesWithRow(row, traversalOptRow{nodes: map[string]*storage.Node{"a": n1}}, []string{"nilA"}, nil), "null left binding never agrees")
	require.False(t, candidateAgreesWithRow(row, traversalOptRow{nodes: map[string]*storage.Node{"a": n1}, rels: map[string]*storage.Edge{"r": e2}}, []string{"a"}, []string{"r"}), "different rel identity")
}

func TestFindAggregateSpans_Boundaries(t *testing.T) {
	require.Empty(t, findAggregateSpans("discount(x)"), "word boundary: discount is not count")
	require.Empty(t, findAggregateSpans(`'count(x)'`), "aggregates inside string literals are ignored")
	require.Empty(t, findAggregateSpans("count(x"), "unbalanced call is not a span")
	require.Len(t, findAggregateSpans("count (x)"), 1, "whitespace before parens tolerated")
	require.Len(t, findAggregateSpans("count(sum(x))"), 1, "nested aggregate reported as one outer span")
	require.Len(t, findAggregateSpans("{c: count(*), s: sum(x)}"), 2, "aggregates inside map literals located")

	spans := findAggregateSpans("1 + count(x)")
	require.Len(t, spans, 1)
	require.Equal(t, "count(x)", "1 + count(x)"[spans[0].start:spans[0].end])
}

func TestParseTraversalAggregateCall_Forms(t *testing.T) {
	for _, expression := range []string{"notacall", "bogus(x)", "count(x) + 1", "count()"} {
		_, _, _, ok := parsePipelineAggregate(expression)
		require.False(t, ok, expression)
	}

	name, inner, distinct, ok := parsePipelineAggregate("count(*)")
	require.True(t, ok)
	require.Equal(t, "count", name)
	require.Equal(t, "*", inner)
	require.False(t, distinct)

	name, inner, distinct, ok = parsePipelineAggregate("sum(DISTINCT x.v)")
	require.True(t, ok)
	require.Equal(t, "sum", name)
	require.True(t, distinct)
	require.Equal(t, "x.v", inner)

	name, inner, distinct, ok = parsePipelineAggregate("stdevp(x.v)")
	require.True(t, ok)
	require.Equal(t, "stdevp", name)
	require.Equal(t, "x.v", inner)
	require.False(t, distinct)
}

func TestTraversalAggAccum_NullAndDistinct(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	state := pipelineAggregateState{name: "collect", expression: "value", distinct: true}
	fixtureAdmission1661Context, fixtureAdmission1661 := statementTestContext(ctx, "RETURN "+("value"))
	_ = fixtureAdmission1661Context
	require.True(t, state.add(fixtureAdmission1661, fixtureAdmission1661Context, executor, pipelineRow{"value": nil}))
	fixtureAdmission1662Context, fixtureAdmission1662 := statementTestContext(ctx, "RETURN "+("value"))
	_ = fixtureAdmission1662Context
	values, ok := state.result(fixtureAdmission1662, fixtureAdmission1662Context, executor)
	require.True(t, ok)
	require.Empty(t, values, "aggregates skip nulls")
	for _, value := range []string{"x", "x", "y"} {
		fixtureAdmission1663Context, fixtureAdmission1663 := statementTestContext(ctx, "RETURN "+("value"))
		_ = fixtureAdmission1663Context
		require.True(t, state.add(fixtureAdmission1663, fixtureAdmission1663Context, executor, pipelineRow{"value": value}))
	}
	fixtureAdmission1664Context, fixtureAdmission1664 := statementTestContext(ctx, "RETURN "+("value"))
	_ = fixtureAdmission1664Context
	values, ok = state.result(fixtureAdmission1664, fixtureAdmission1664Context, executor)
	require.True(t, ok)
	require.Equal(t, []interface{}{"x", "y"}, values, "DISTINCT deduplicates")
}

func TestFinalizeTraversalAggregate_AllFunctions(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	fin := func(fn string, vals []interface{}, star bool, rows int64) interface{} {
		state := pipelineAggregateState{name: fn, expression: "value"}
		if star {
			state.expression = "*"
			for row := int64(0); row < rows; row++ {
				fixtureAdmission1665Context, fixtureAdmission1665 := statementTestContext(ctx, "RETURN "+("value"))
				_ = fixtureAdmission1665Context
				require.True(t, state.add(fixtureAdmission1665, fixtureAdmission1665Context, exec, pipelineRow{}))
			}
		} else {
			for _, value := range vals {
				fixtureAdmission1666Context, fixtureAdmission1666 := statementTestContext(ctx, "RETURN "+("value"))
				_ = fixtureAdmission1666Context
				require.True(t, state.add(fixtureAdmission1666, fixtureAdmission1666Context, exec, pipelineRow{"value": value}))
			}
		}
		fixtureAdmission1667Context, fixtureAdmission1667 := statementTestContext(ctx, "RETURN "+("value"))
		_ = fixtureAdmission1667Context
		value, ok := state.result(fixtureAdmission1667, fixtureAdmission1667Context, exec)
		require.True(t, ok)
		return value
	}

	require.EqualValues(t, 3, fin("count", nil, true, 3), "count(*) counts rows")
	require.EqualValues(t, 2, fin("count", []interface{}{"a", "b"}, false, 9))
	require.Equal(t, []interface{}{}, fin("collect", nil, false, 0), "collect identity is []")
	require.Equal(t, []interface{}{"a"}, fin("collect", []interface{}{"a"}, false, 1))
	require.EqualValues(t, 5, fin("sum", []interface{}{int64(2), int(3)}, false, 2))
	require.EqualValues(t, 0, fin("sum", nil, false, 0), "sum identity is 0")
	require.Equal(t, 5.5, fin("sum", []interface{}{int64(2), 3.5}, false, 2), "mixed types sum as float")
	require.Nil(t, fin("avg", nil, false, 0), "avg identity is null")
	require.Equal(t, 2.5, fin("avg", []interface{}{int64(2), int64(3)}, false, 2))
	for _, name := range []string{"sum", "avg"} {
		state := pipelineAggregateState{name: name, expression: "value"}
		fixtureAdmission1668Context, fixtureAdmission1668 := statementTestContext(ctx, "RETURN "+("value"))
		_ = fixtureAdmission1668Context
		require.False(t, state.add(fixtureAdmission1668, fixtureAdmission1668Context, exec, pipelineRow{"value": "abc"}), "%s of a non-numeric value is a TypeError, as in Neo4j", name)
	}
	require.Nil(t, fin("min", nil, false, 0))
	require.EqualValues(t, 2, fin("min", []interface{}{int64(3), int64(2), int64(5)}, false, 3))
	require.EqualValues(t, 5, fin("max", []interface{}{int64(3), int64(2), int64(5)}, false, 3))
	require.Nil(t, fin("stdev", nil, false, 0), "stdev of empty input is null")
	require.Equal(t, 0.0, fin("stdev", []interface{}{int64(4)}, false, 1), "stdev of one value is 0.0")
	unknown := pipelineAggregateState{name: "unknownagg", expression: "value"}
	fixtureAdmission1669Context, fixtureAdmission1669 := statementTestContext(ctx, "RETURN "+("value"))
	_ = fixtureAdmission1669Context
	require.False(t, unknown.add(fixtureAdmission1669, fixtureAdmission1669Context, exec, pipelineRow{"value": int64(1)}))
	fixtureAdmission1670Context, fixtureAdmission1670 := statementTestContext(ctx, "RETURN "+("value"))
	_ = fixtureAdmission1670Context
	value, ok := unknown.result(fixtureAdmission1670, fixtureAdmission1670Context, exec)
	require.False(t, ok, "unknown functions are not silently handled")
	require.Nil(t, value)
}

func TestStdevTraversalAggregateValues_Contract(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	deviation := func(values []interface{}, population bool) interface{} {
		state := pipelineAggregateState{name: "stdev", expression: "value"}
		if population {
			state.name = "stdevp"
		}
		for _, value := range values {
			fixtureAdmission1671Context, fixtureAdmission1671 := statementTestContext(ctx, "RETURN "+("value"))
			_ = fixtureAdmission1671Context
			require.True(t, state.add(fixtureAdmission1671, fixtureAdmission1671Context, exec, pipelineRow{"value": value}))
		}
		fixtureAdmission1672Context, fixtureAdmission1672 := statementTestContext(ctx, "RETURN "+("value"))
		_ = fixtureAdmission1672Context
		value, ok := state.result(fixtureAdmission1672, fixtureAdmission1672Context, exec)
		require.True(t, ok)
		return value
	}
	require.Nil(t, deviation(nil, false), "no values: null (StdevFunction count==0)")
	nonNumeric := pipelineAggregateState{name: "stdev", expression: "value"}
	fixtureAdmission1673Context, fixtureAdmission1673 := statementTestContext(ctx, "RETURN "+("value"))
	_ = fixtureAdmission1673Context
	require.False(t, nonNumeric.add(fixtureAdmission1673, fixtureAdmission1673Context, exec, pipelineRow{"value": "x"}), "a non-numeric value is a TypeError, as in Neo4j")
	require.Equal(t, 0.0, deviation([]interface{}{int64(9)}, false), "single value: 0.0")
	require.InDelta(t, 2.8284, deviation([]interface{}{int64(2), int64(6)}, false).(float64), 0.001, "sample divisor n-1")
	require.InDelta(t, 2.0, deviation([]interface{}{int64(2), int64(6)}, true).(float64), 0.001, "population divisor n")
}

func TestTryCompileTraversalExpr_LiteralsAndFallbacks(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	empty := traversalOptRow{nodes: map[string]*storage.Node{}, rels: map[string]*storage.Edge{}}

	eval := func(expr string) interface{} {
		fixtureAdmission766Context, fixtureAdmission766 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission766
		return exec.compileTraversalProjection(fixtureAdmission766, fixtureAdmission766Context, expr)(empty)
	}
	require.Equal(t, "lit", eval("'lit'"))
	require.Equal(t, "dq", eval(`"dq"`))
	require.Equal(t, true, eval("true"))
	require.Equal(t, false, eval("FALSE"))
	require.Nil(t, eval("null"))
	require.EqualValues(t, 42, eval("42"))
	require.Equal(t, 3.5, eval("3.5"))

	fixtureAdmission1345Context, fixtureAdmission1345 := expressionTestContext(ctx, "")
	_ = fixtureAdmission1345Context
	_, ok := exec.tryCompileTraversalExpr(fixtureAdmission1345, fixtureAdmission1345Context, "")
	require.False(t, ok, "empty expression is not compilable")
	fixtureAdmission1346Context, fixtureAdmission1346 := expressionTestContext(ctx, "a + b")
	_ = fixtureAdmission1346Context
	_, ok = exec.tryCompileTraversalExpr(fixtureAdmission1346, fixtureAdmission1346Context, "a + b")
	require.False(t, ok, "operator expressions fall back to the evaluator")
	fixtureAdmission1347Context, fixtureAdmission1347 := expressionTestContext(ctx, "(x)")
	_ = fixtureAdmission1347Context
	_, ok = exec.tryCompileTraversalFunctionCall(fixtureAdmission1347, fixtureAdmission1347Context, "(x)")
	require.False(t, ok, "not a call")
	fixtureAdmission1348Context, fixtureAdmission1348 := expressionTestContext(ctx, "CASE WHEN a THEN 1 ELSE 2 END")
	_ = fixtureAdmission1348Context
	_, ok = exec.tryCompileTraversalExpr(fixtureAdmission1348, fixtureAdmission1348Context, "CASE WHEN a THEN 1 ELSE 2 END")
	require.False(t, ok, "CASE expressions are left to the evaluator")
}

// TestTraversalProjection_LiteralAndComplexItems exercises the projection
// paths end to end: literal items, operator items, CASE items, an
// evaluator-fallback function, and an uncompilable coalesce argument.
func TestTraversalProjection_LiteralAndComplexItems(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN 'lit' AS s, 42 AS i, true AS b, null AS nl,
		       CASE WHEN target.name = "Dog" THEN 1 ELSE 0 END AS flag,
		       exists(target.name) AS ex,
		       coalesce(CASE WHEN target.name = "X" THEN 1 ELSE null END, 99) AS bump
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "lit", row["s"])
	require.EqualValues(t, 42, row["i"])
	require.Equal(t, true, row["b"])
	require.Nil(t, row["nl"])
	require.EqualValues(t, 1, row["flag"], "CASE items evaluate through the evaluator fallback")
	require.Equal(t, true, row["ex"], "registry-miss functions defer to the legacy evaluator")
	require.EqualValues(t, 99, row["bump"], "uncompilable coalesce argument evaluates per row")
}

// TestTraversalOptionalMatch_NoReturnClause: a compound optional-match query
// without RETURN produces an empty result rather than an error.
func TestTraversalOptionalMatch_NoReturnClause(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	_, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
	`, nil)
	// An OPTIONAL MATCH with nothing after it is an incomplete statement:
	// a SyntaxError, as in Neo4j (#514).
	require.Error(t, err)
	require.Contains(t, err.Error(), "SyntaxError")
}

// TestTraversalOptionalMatch_EmptySeed: an empty primary MATCH yields zero
// rows through every downstream clause.
func TestTraversalOptionalMatch_EmptySeed(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:NoSuch"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo)
		RETURN target.name AS n, repo.id AS r
	`, nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows)
}

// TestTraversalOptionalMatch_SkipModifier: SKIP applies after projection,
// including a SKIP past the end of the row set.
func TestTraversalOptionalMatch_SkipModifier(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS n ORDER BY n SKIP 1 LIMIT 5
	`, nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows, "one Dog row, skipped past")

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS n SKIP 9
	`, nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows, "SKIP past the end clamps to empty")
}

// TestTraversalOptionalMatch_BothEndpointsBoundFilter: a clause whose both
// endpoints are already bound acts as a relationship filter between them.
func TestTraversalOptionalMatch_BothEndpointsBoundFilter(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (e)<-[:CONTAINS]-(sourceFile:OMFile)
		OPTIONAL MATCH (sourceFile)-[cr:CONTAINS]->(target)
		RETURN target.name AS n, sourceFile.relative_path AS p, type(cr) AS crType
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "Dog", row["n"])
	require.Equal(t, "svc.py", row["p"])
	require.Equal(t, "CONTAINS", row["crType"], "both-bound clause binds the relationship variable")
}

// TestTraversalOptionalMatch_PreBoundRelVarRoutesGeneral: re-using a bound
// relationship variable in a later OPTIONAL MATCH is valid and joins by
// relationship identity.
func TestTraversalOptionalMatch_PreBoundRelVarRoutesGeneral(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (src:OMClass)-[rel]->(dst:OMClass)
		RETURN target.name AS n, src.uid AS srcUid, dst.name AS dstName
	`, nil)
	require.NoError(t, err, "re-using a bound relationship variable must not error")
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "Dog", row["n"])
	require.Equal(t, "cls:ServiceDog", row["srcUid"], "identity join recovers the endpoints of the bound relationship")
	require.Equal(t, "Dog", row["dstName"])
}

// TestTraversalOptionalMatch_DisconnectedRelNullFill: a disconnected
// relationship pattern with no matches null-fills node AND relationship
// variables while preserving left rows.
func TestTraversalOptionalMatch_DisconnectedRelNullFill(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (g:OMGhost)-[gr:GHOST_REL]->(h:OMGhost)
		RETURN target.name AS n, g.id AS gid, type(gr) AS grType
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "Dog", row["n"])
	require.Nil(t, row["gid"])
	require.Nil(t, row["grType"], "unmatched relationship variable projects null")
}

// TestTraversalOptionalMatch_GeneralPathWhere: WHERE on a disconnected clause
// referencing outer variables filters candidates per row; a predicate that
// rejects everything null-fills.
func TestTraversalOptionalMatch_GeneralPathWhere(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo)-[rc:REPO_CONTAINS]->(f:OMFile) WHERE f.language = e.language
		RETURN target.name AS n, repo.id AS repoId
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "repo:1", rowMap(t, res, 0)["repoId"], "outer-variable WHERE passes for matching language")

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo)-[rc:REPO_CONTAINS]->(f:OMFile) WHERE f.language = "go"
		RETURN target.name AS n, repo.id AS repoId
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Nil(t, rowMap(t, res, 0)["repoId"], "WHERE rejecting all candidates null-fills")
}

// TestSingleNodeOptionalClause_Variants: property filters, WHERE, and
// anonymous single-node patterns.
func TestSingleNodeOptionalClause_Variants(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo {id:"repo:1"})
		RETURN repo.name AS rn
	`, nil)
	require.NoError(t, err)
	require.Equal(t, "repo1", res.Rows[0][0], "property-filtered scan binds")

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo {id:"nope"})
		RETURN repo.name AS rn
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Nil(t, res.Rows[0][0], "non-matching properties null-fill")

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo) WHERE repo.name = "repo1"
		RETURN repo.id AS ri
	`, nil)
	require.NoError(t, err)
	require.Equal(t, "repo:1", res.Rows[0][0], "WHERE on the scanned node applies")

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (repo:OMRepo) WHERE repo.name = "other"
		RETURN repo.id AS ri
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Nil(t, res.Rows[0][0], "WHERE rejecting the scan null-fills")

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (:OMRepo)
		RETURN target.name AS n
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "anonymous single-node pattern preserves multiplicity via one match")
	require.Equal(t, "Dog", res.Rows[0][0])
}

// TestTraversalAggregate_ShapeVariants: grouped count(*), DISTINCT, mixed
// expressions with count(*), and empty-input identities.
func TestTraversalAggregate_ShapeVariants(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS n, count(*) AS rows, count(DISTINCT tf.language) AS langs
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.EqualValues(t, 1, row["rows"])
	require.EqualValues(t, 1, row["langs"])

	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN count(*) + 1 AS bumpedRows
	`, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Rows[0][0], "count(*) inside a larger expression")

	// Empty ungrouped input: identity values flow through mixed expressions.
	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:NoSuch"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN count(tf) + 1 AS c, collect(tf.relative_path) AS paths, avg(rel.weight) AS a
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "ungrouped aggregation over empty input yields one identity row")
	row = rowMap(t, res, 0)
	require.EqualValues(t, 1, row["c"], "count identity 0 + 1")
	require.Equal(t, []interface{}{}, row["paths"])
	require.Nil(t, row["a"])

	// Empty input WITH grouping keys: zero rows.
	res, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:NoSuch"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS n, count(tf) AS c
	`, nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows, "grouped aggregation over empty input yields zero rows")
}

// TestTraversalAggregate_EmptyArgumentErrors: count() is the one genuinely
// rejected form — Neo4j itself refuses it at compile time.
func TestTraversalAggregate_EmptyArgumentErrors(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	_, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN count() AS c
	`, nil)
	require.Error(t, err)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")

	_, err = exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN count() + 1 AS c
	`, nil)
	require.Error(t, err, "empty argument inside a mixed expression is rejected the same way")
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}

func TestGh713TraversalAggregatePlannerParity(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []int64
		expr   string
		want   interface{}
	}{
		{"continuous percentile", []int64{1, 3}, "percentileCont(n.x, 0.5)", float64(2)},
		{"discrete percentile", []int64{1, 3}, "percentileDisc(n.x, 0.5)", int64(1)},
		{"exact integer sum", []int64{9007199254740993, 1}, "sum(n.x)", int64(9007199254740994)},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor, ctx := newUnitExecutor(t)
			rows := make([]traversalOptRow, 0, len(test.values))
			bindings := make([]pipelineRow, 0, len(test.values))
			for index, value := range test.values {
				row := traversalOptRow{nodes: map[string]*storage.Node{"n": {ID: storage.NodeID("row-" + strconv.Itoa(index)), Properties: map[string]interface{}{"x": value}}}}
				rows = append(rows, row)
				bindings = append(bindings, pipelineRowFromTraversalOptionalRow(row))
			}
			items := executor.parseReturnItems(test.expr + " AS value")
			fixtureAdmission1674Context, fixtureAdmission1674 := statementTestContext(ctx, "RETURN "+test.expr+" AS value")
			_ = fixtureAdmission1674Context
			shared, err := executor.projectMergeReturn(fixtureAdmission1674, fixtureAdmission1674Context, bindings, "RETURN "+test.expr+" AS value")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.want}}, shared.Rows)
			fixtureAdmission1882Context, fixtureAdmission1882 := statementTestContext(ctx, test.expr+" AS value")
			_ = fixtureAdmission1882Context
			legacy, err := executor.aggregateTraversalOptionalRows(fixtureAdmission1882, fixtureAdmission1882Context, rows, items)
			require.NoError(t, err)
			require.Equal(t, shared.Rows, legacy)
		})
	}
}

func TestExtractRelationshipVariables_Whitespace(t *testing.T) {
	require.Equal(t, []string{"r"}, extractRelationshipVariables("(a)-[ r :T ]->(b)"), "whitespace around the variable is tolerated")
	require.Empty(t, extractRelationshipVariables("(a)-[ :T ]->(b)"), "whitespace before an anonymous type yields no variable")
}

// TestTraversalOptionalMatch_NullSeedPropagatesThroughChain: a variable
// null-bound by an earlier optional miss seeds a later clause; the later
// clause propagates nulls instead of erroring or dropping the row
// (OptionalExpandAllPipe's NO_VALUE source behavior).
func TestTraversalOptionalMatch_NullSeedPropagatesThroughChain(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(ghost:OMGhostFile)
		OPTIONAL MATCH (ghost)<-[:REPO_CONTAINS]-(r2:OMRepo)
		RETURN target.name AS n, ghost.uid AS g, r2.id AS r2id
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "Dog", row["n"])
	require.Nil(t, row["g"], "first optional missed: ghost is null")
	require.Nil(t, row["r2id"], "null seed propagates null bindings through the chained clause")
}

// TestTraversalOptionalMatch_AnonymousSeedPattern: a fully anonymous primary
// traversal still supports trailing OPTIONAL MATCH clauses.
func TestTraversalOptionalMatch_AnonymousSeedPattern(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	res, err := exec.Execute(ctx, `
		MATCH (:OMClass {uid:"cls:ServiceDog"})-[:INHERITS]->(:OMClass)
		OPTIONAL MATCH (repo:OMRepo)
		RETURN repo.id AS ri
	`, nil)
	require.NoError(t, err, "anonymous seed patterns must not error")
	// The seed executes as RETURN * (no named variables); the engine yields
	// one seed row here, and the disconnected optional binds on it.
	require.Len(t, res.Rows, 1)
	require.Equal(t, "repo:1", res.Rows[0][0])
}

func TestFindAggregateSpans_NameWithoutCall(t *testing.T) {
	require.Empty(t, findAggregateSpans("a + count"), "an aggregate name with no argument list is not a span")
	require.Empty(t, findAggregateSpans("count"), "a bare trailing aggregate name is not a span")
}

func TestApplySingleNodeOptionalClause_NoParenGroup(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	rows := []traversalOptRow{{nodes: map[string]*storage.Node{"a": {ID: "n1"}}, rels: map[string]*storage.Edge{}}}
	fixtureAdmission1675Context, fixtureAdmission1675 := statementTestContext(ctx, "MATCH "+("garbage"))
	_ = fixtureAdmission1675Context
	out, err := exec.applySingleNodeOptionalClause(fixtureAdmission1675, fixtureAdmission1675Context, rows, optionalMatchClause{pattern: "garbage"})
	require.NoError(t, err)
	require.Equal(t, rows, out, "a pattern without a node group passes rows through")
}

func TestApplyGeneralOptionalClause_EdgeInputs(t *testing.T) {
	exec, ctx := newUnitExecutor(t)

	fixtureAdmission1676Context, fixtureAdmission1676 := statementTestContext(ctx, "MATCH "+("(x:L)-[:T]->(y:M)"))
	_ = fixtureAdmission1676Context
	out, err := exec.applyGeneralOptionalClause(fixtureAdmission1676, fixtureAdmission1676Context, nil, optionalMatchClause{pattern: "(x:L)-[:T]->(y:M)"})
	require.NoError(t, err)
	require.Empty(t, out, "no left rows: nothing to join")

	rows := []traversalOptRow{{nodes: map[string]*storage.Node{"a": {ID: "n1"}}, rels: map[string]*storage.Edge{}}}
	fixtureAdmission1677Context, fixtureAdmission1677 := statementTestContext(ctx, "MATCH "+("novars"))
	_ = fixtureAdmission1677Context
	out, err = exec.applyGeneralOptionalClause(fixtureAdmission1677, fixtureAdmission1677Context, rows, optionalMatchClause{pattern: "novars"})
	require.NoError(t, err)
	require.Equal(t, rows, out, "a pattern with no variables and no node group passes rows through")
}

func TestCompiledVarProjection_UnboundFallsBackToEvaluator(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	ctx = withExpressionFailureSlot(ctx)
	row := traversalOptRow{nodes: map[string]*storage.Node{}, rels: map[string]*storage.Edge{}}
	fixtureAdmission1349Context, fixtureAdmission1349 := expressionTestContext(ctx, "zzz.prop")
	_ = fixtureAdmission1349Context
	compiled := exec.compileTraversalProjection(fixtureAdmission1349, fixtureAdmission1349Context, "zzz.prop")(row)
	require.Nil(t, compiled, "an unbound variable projection must not return query text")
	requireStatusCode(t, getExpressionFailure(ctx), "Neo.ClientError.Statement.SyntaxError")
}

func TestOptionalMatchProjectionUsesSharedEntityPredicateSemantics(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, `CREATE (:Single)-[:OTHER]->(:Target)`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (n:Single)-[r]->(x)
		OPTIONAL MATCH (n)-[:TYPE]-(m)
		RETURN r:OTHER AS relationshipTypeMatches, r:other AS relationshipTypeCaseMismatch, m:TYPE AS missingNodeLabel
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"relationshipTypeMatches", "relationshipTypeCaseMismatch", "missingNodeLabel"}, result.Columns)
	require.Equal(t, [][]interface{}{{true, false, nil}}, result.Rows)
}

func TestOptionalMatchAggregateCaseTreatsNullBindingAsNull(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	row := traversalOptRow{
		nodes: map[string]*storage.Node{"n": nil},
		rels:  map[string]*storage.Edge{},
	}
	fixtureAdmission767Context, fixtureAdmission767 := expressionTestContext(ctx, "n IS NULL")
	_ = fixtureAdmission767
	require.True(t, exec.evaluateRowPredicate(fixtureAdmission767, fixtureAdmission767Context, "n IS NULL", pipelineRow{"n": nil}))
	caseExpression := `CASE WHEN n IS NULL THEN null ELSE {value: n.value} END`
	parsed, err := parseCaseExpression(caseExpression)
	require.NoError(t, err)
	require.Equal(t, "n IS NULL", parsed.whenClauses[0].condition)
	direct, evaluated := rowValue(t, exec, caseExpression, pipelineRow{"n": nil})
	require.True(t, evaluated)
	require.Nil(t, direct)
	fixtureAdmission768Context, fixtureAdmission768 := expressionTestContext(ctx, caseExpression)
	_ = fixtureAdmission768
	project := exec.compileTraversalProjection(fixtureAdmission768, fixtureAdmission768Context, caseExpression)
	require.Nil(t, project(row))
}

// TestSupport_DisconnectedOptionalBareVarReturn pins the exact review shape
// "MATCH (a)-[r]->(b) OPTIONAL MATCH (x:X) RETURN a, x": a trailing OPTIONAL
// MATCH with no previously bound variable is a legal disconnected optional
// pattern (Neo4j plans it as Apply + Optional; OptionalPipe.scala null-fills
// when the pattern has no matches). Bare-variable projections return real
// node entities.
func TestSupport_DisconnectedOptionalBareVarReturn(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, `CREATE (a:PN {name:"a"})-[:T {n:1}]->(b:PN {name:"b"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (x:XX {name:"only-x"})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `MATCH (a:PN {name:"a"})-[r]->(b) OPTIONAL MATCH (x:XX) RETURN a, x`, nil)
	require.NoError(t, err, "a disconnected OPTIONAL MATCH must never be rejected")
	require.Len(t, res.Rows, 1)
	aNode, ok := res.Rows[0][0].(*storage.Node)
	require.True(t, ok, "bare a projects the node entity")
	require.Equal(t, "a", aNode.Properties["name"])
	xNode, ok := res.Rows[0][1].(*storage.Node)
	require.True(t, ok, "bare x projects the independently matched node entity")
	require.Equal(t, "only-x", xNode.Properties["name"])

	// And the null-fill side: no OMNever nodes exist.
	res, err = exec.Execute(ctx, `MATCH (a:PN {name:"a"})-[r]->(b) OPTIONAL MATCH (x:OMNever) RETURN a, x`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "left row preserved")
	require.Nil(t, res.Rows[0][1], "unmatched disconnected variable is null")
}

// TestSupport_ReboundRelVariableIsIdentityConstraint pins the parallel-edge
// invariant the executor already enforces for chained MATCH
// (multi_match_relationship_binding_bug_test.go: "same relationship variable
// only counts rows for the same edge" / "rejects rows that would overwrite
// the bound edge") on the traversal OPTIONAL MATCH path. Neo4j's planner
// makes the same choice: a pattern relationship whose variable is already in
// availableSymbols is planned as ProjectEndpoints over the bound edge, never
// as a fresh expansion (cypher-planner .../idp/expandSolverStep.scala,
// plansForNodeConnection). With two parallel :T edges between a and b,
// re-referencing r must yield one row per originally bound edge and must not
// overwrite r.
func TestSupport_ReboundRelVariableIsIdentityConstraint(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, `CREATE (a:PN {name:"a"})-[:T {n:1}]->(b:PN {name:"b"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:PN {name:"a"}) MATCH (b:PN {name:"b"}) CREATE (a)-[:T {n:2}]->(b)`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `MATCH (a:PN)-[r:T]->(b:PN) OPTIONAL MATCH (a)-[r]->(b) RETURN count(*) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 2, res.Rows[0][0],
		"one row per originally bound edge — re-referencing r must not fan out across parallel edges")

	res, err = exec.Execute(ctx, `MATCH (a:PN)-[r:T {n:1}]->(b:PN) OPTIONAL MATCH (a)-[r]->(b) RETURN r.n AS n`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0],
		"r keeps its original edge binding — the n:2 parallel edge must not overwrite it")
}

// TestSupport_StdevMixedCaseAggregate: aggregate names are case-insensitive
// on the traversal OPTIONAL MATCH path, matching the executor-wide registry
// (aggregateFnNames in clauses.go).
func TestSupport_StdevMixedCaseAggregate(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, `CREATE (a:PN {name:"a"})-[:T {n:1}]->(b:PN {name:"b"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:PN {name:"a"}) MATCH (b:PN {name:"b"}) CREATE (a)-[:T {n:2}]->(b)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (x:XX {name:"only-x"})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (a:PN {name:"a"})-[r:T]->(b)
		OPTIONAL MATCH (x:XX)
		RETURN stDev(r.n) AS s, STDEVP(r.n) AS sp
	`, nil)
	require.NoError(t, err, "mixed-case stDev/STDEVP must be supported like the rest of the executor")
	require.Len(t, res.Rows, 1)
	require.InDelta(t, 0.70710678, res.Rows[0][0].(float64), 1e-6, "sample stdev of {1,2}")
	require.InDelta(t, 0.5, res.Rows[0][1].(float64), 1e-6, "population stdev of {1,2}")
}
