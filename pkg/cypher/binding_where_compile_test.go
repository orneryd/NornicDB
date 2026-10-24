package cypher

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGh728CompiledComparisonHandlersPreserveUnknown(t *testing.T) {
	exec := &StorageExecutor{}
	node := &storage.Node{ID: "node", Properties: map[string]interface{}{"present": int64(1)}}
	for _, test := range []struct {
		clause string
		want   bool
	}{
		{"NOT n.missing = 1", false},
		{"NOT n.missing <> 1", false},
		{"NOT n.missing > 1", false},
		{"NOT (n.missing = 1 OR n.present = 2)", false},
		{"NOT (n.missing = 1 AND n.present = 1)", false},
		{"NOT (n.missing = 1 AND n.present = 2)", true},
		{"n.missing = 1 OR n.present = 1", true},
		{"NOT n.present = 2", true},
		{"n.present = 1", true},
	} {
		t.Run(test.clause, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			fixtureAdmission79Context, fixtureAdmission79 := expressionTestContext(ctx, test.clause)
			_ = fixtureAdmission79
			predicate, supported := exec.tryCompileBindingWhere(fixtureAdmission79Context, test.clause)
			require.True(t, supported)
			fixtureAdmission80Context, fixtureAdmission80 := expressionTestContext(ctx, test.clause)
			_ = fixtureAdmission80
			assert.Equal(t, test.want, exec.evaluateRowPredicate(fixtureAdmission80, fixtureAdmission80Context, test.clause, map[string]interface{}{"n": node}))
			assert.Equal(t, test.want, predicate(binding{"n": node}, nil))
			require.NoError(t, getExpressionFailure(ctx))
		})
	}
}

func TestGh728SharedComparisonHandlerTypedValues(t *testing.T) {
	for _, test := range []struct {
		name, operator string
		left, right    interface{}
		want           interface{}
	}{
		{"null equality", "=", nil, int64(1), nil},
		{"null inequality", "<>", nil, int64(1), nil},
		{"string equality", "=", "same", "same", true},
		{"string inequality", "!=", "left", "right", true},
		{"bool equality", "=", true, false, false},
		{"bool inequality", "<>", true, false, true},
		{"large integer", "=", int64(9007199254740993), int64(9007199254740992), false},
		{"mixed numeric", "=", int64(1), float64(1), true},
		{"ordered numeric", "<=", int64(1), float64(2), true},
		{"nested unknown", "=", []interface{}{nil}, []interface{}{nil}, nil},
		{"nested unequal", "<>", []interface{}{int64(1)}, []interface{}{int64(2)}, true},
		{"node identity", "=", &storage.Node{ID: "same"}, &storage.Node{ID: "same", Properties: map[string]interface{}{"x": int64(1)}}, true},
		{"edge identity", "=", &storage.Edge{ID: "same"}, &storage.Edge{ID: "same", Type: "LINK"}, true},
		{"different edges", "<>", &storage.Edge{ID: "left"}, &storage.Edge{ID: "right"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, comparisonEvaluationHandler(test.operator).evaluate(test.left, test.right))
		})
	}
}

func TestGh728ComparisonHandlerAdmission(t *testing.T) {
	exec := &StorageExecutor{}
	for _, clause := range []string{"", "n.x >", "> n.x", "n.x ?? n.y", "n.x + 1 = 1", "n.x = n.y + 1"} {
		t.Run(clause, func(t *testing.T) {
			predicate, supported := exec.compileBindingComparisonTruth(clause)
			require.False(t, supported)
			require.Nil(t, predicate)
		})
	}
	predicate, supported := exec.compileBindingComparisonTruth("n.present = m.missing")
	require.True(t, supported)
	assert.Equal(t, truthUnknown, predicate(binding{"n": &storage.Node{Properties: map[string]interface{}{"present": int64(1)}}}, nil))
}

func TestGh728SharedArithmeticPredicatePlan(t *testing.T) {
	clause := "size(n.name) + n.count >= 0"
	plan := planRowPredicate(clause)
	require.NotNil(t, plan)
	require.True(t, plan.complete)
	for _, test := range []struct {
		name       string
		properties map[string]interface{}
		want       bool
		code       string
	}{
		{"accepted", map[string]interface{}{"name": "node", "count": int64(1)}, true, ""},
		{"rejected", map[string]interface{}{"name": "node", "count": int64(-5)}, false, ""},
		{"null function", map[string]interface{}{"count": int64(1)}, false, ""},
		{"null arithmetic", map[string]interface{}{"name": "node"}, false, ""},
		{"invalid arithmetic", map[string]interface{}{"name": "node", "count": true}, false, "Neo.ClientError.Statement.TypeError"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			row := map[string]interface{}{"n": &storage.Node{ID: "node", Properties: test.properties}}
			exec := &StorageExecutor{}
			fixtureAdmission81Context, fixtureAdmission81 := expressionTestContext(ctx, clause)
			_ = fixtureAdmission81
			assert.Equal(t, test.want, exec.evaluateRowPredicate(fixtureAdmission81, fixtureAdmission81Context, clause, row))
			failure := getExpressionFailure(ctx)
			if test.code == "" {
				require.NoError(t, failure)
			} else {
				require.Error(t, failure)
				require.True(t, strings.HasPrefix(statusText(failure), test.code), statusText(failure))
			}
		})
	}
}

func TestGh728SharedArithmeticPredicateZeroAllocations(t *testing.T) {
	exec := &StorageExecutor{}
	ctx := context.Background()
	clause := "size(n.name) + n.count >= 0"
	var fixtureAdmission82 admittedRelationshipStatement
	ctx, fixtureAdmission82 = expressionTestContext(ctx, clause)
	_ = fixtureAdmission82
	for _, length := range []int{4, 1024} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			row := map[string]interface{}{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{
				"name": strings.Repeat("x", length), "count": int64(4096),
			}}}
			require.True(t, exec.evaluateRowPredicate(fixtureAdmission82, ctx, clause, row))
			accepted := true
			allocations := testing.AllocsPerRun(100, func() {
				accepted = accepted && exec.evaluateRowPredicate(fixtureAdmission82, ctx, clause, row)
			})
			require.True(t, accepted)
			require.Zero(t, allocations)
		})
	}
}

func TestGh728ContextWherePreservesSharedTruth(t *testing.T) {
	exec := &StorageExecutor{}
	nodes := map[string]*storage.Node{"n": {ID: "node", Properties: map[string]interface{}{"count": int64(1)}}}
	for _, test := range []struct {
		name, clause, code string
		want               bool
	}{
		{"empty clause", "", "", true},
		{"null equality negation", "NOT (n.missing = 1)", "", false},
		{"null inequality negation", "NOT (n.missing <> 1)", "", false},
		{"null arithmetic negation", "NOT (n.missing + 1 > 0)", "", false},
		{"null membership negation", "NOT (n.missing IN [1, null])", "", false},
		{"arithmetic parameters", "n.count + $offset = $expected", "", true},
		{"arithmetic failure negation", "NOT (n.count / 0 > 0)", "Neo.ClientError.Statement.ArithmeticError", false},
		{"non boolean", "1", "Neo.ClientError.Statement.TypeError", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := withExpressionFailureSlot(withQueryParams(context.Background(), map[string]interface{}{"offset": int64(1), "expected": int64(2)}))
			fixtureAdmission83Context, fixtureAdmission83 := expressionTestContext(ctx, test.clause)
			_ = fixtureAdmission83
			require.Equal(t, test.want, exec.evaluateWhereForContext(fixtureAdmission83, fixtureAdmission83Context, test.clause, nodes))
			failure := getExpressionFailure(ctx)
			if test.code == "" {
				require.NoError(t, failure)
			} else {
				require.Error(t, failure)
				require.True(t, strings.HasPrefix(statusText(failure), test.code), statusText(failure))
			}
		})
	}
}

func TestGh728ContextWherePropertyNamesZeroAllocations(t *testing.T) {
	exec := &StorageExecutor{}
	ctx := context.Background()
	nodes := map[string]*storage.Node{"n": {ID: "node", Properties: map[string]interface{}{"count": int64(1), "collect": int64(1), "exists": int64(1)}}}
	for _, name := range []string{"count", "collect", "exists"} {
		t.Run(name, func(t *testing.T) {
			clause := "n." + name + " >= 0"
			rowContext, fixtureAdmission84 := expressionTestContext(ctx, clause)
			_ = fixtureAdmission84
			plan := planRowPredicate(clause)
			require.NotNil(t, plan)
			require.True(t, plan.complete)
			require.True(t, exec.evaluateWhereForContext(fixtureAdmission84, rowContext, clause, nodes))
			require.Zero(t, testing.AllocsPerRun(100, func() {
				if !exec.evaluateWhereForContext(fixtureAdmission84, rowContext, clause, nodes) {
					t.Fatal("property name must not be mistaken for a function")
				}
			}))
		})
	}
}

func TestGh728BindingFilterPreservesQuotedWhitespace(t *testing.T) {
	exec := &StorageExecutor{}
	for _, separator := range []string{"\t", "\n", "\r"} {
		t.Run(strconv.Quote(separator), func(t *testing.T) {
			name := "first" + separator + "second"
			row := binding{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{"name": name}}}
			clause := "n.name + '' = '" + name + "'"
			fixtureAdmission1039Context, fixtureAdmission1039 := expressionTestContext(context.Background(), clause)
			_ = fixtureAdmission1039Context
			require.Len(t, exec.filterBindingsByWhere(fixtureAdmission1039, fixtureAdmission1039Context, []binding{row}, clause, nil), 1)
		})
	}
}

func TestGh728WhereNormalizationPreservesQuotedText(t *testing.T) {
	for _, test := range []struct {
		name, clause, expected string
	}{
		{"plain", "n.name = $name", "n.name = $name"},
		{"outside", "n.name\t=\r\n$name", "n.name = $name"},
		{"single quote", "n.name = 'a\tb\nc'", "n.name = 'a\tb\nc'"},
		{"double quote", "n.name = \"a\tb\"", "n.name = \"a\tb\""},
		{"escaped quote", "n.name = 'a\\'b\tc'\nAND n.count > 0", "n.name = 'a\\'b\tc' AND n.count > 0"},
		{"doubled quote", "n.name = 'a''\tb'\nAND n.count > 0", "n.name = 'a''\tb' AND n.count > 0"},
		{"backticks", "n.`a\tb` = 1\nAND n.count > 0", "n.`a\tb` = 1 AND n.count > 0"},
		{"doubled backtick", "n.`a``\tb` = 1\tAND n.count > 0", "n.`a``\tb` = 1 AND n.count > 0"},
		{"unterminated quote", "n.name = 'a\tb", "n.name = 'a\tb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, normalizeBindingWhereClause(test.clause))
		})
	}
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if normalizeBindingWhereClause("n.name = $name") != "n.name = $name" {
			t.Fatal("normalization changed plain predicate")
		}
	}))
}

func TestGh728BindingWhereUsesSharedTypedPredicate(t *testing.T) {
	tests := []struct {
		name   string
		clause string
		params map[string]interface{}
		want   bool
		code   string
	}{
		{name: "arithmetic property", clause: "a.age + 1 = 31", want: true},
		{name: "typed arithmetic parameters", clause: "a.age + $increment = $expected", params: map[string]interface{}{"increment": int64(1), "expected": int64(31)}, want: true},
		{name: "null negation", clause: "NOT (a.missing + 1 = 0)"},
		{name: "null disjunction", clause: "NOT (a.missing + 1 = 0 OR false)"},
		{name: "quoted operator", clause: "a.name = 'alice=admin'", want: true},
		{name: "typed map predicate", clause: "all(k IN keys($props) WHERE a[k] = $props[k])", params: map[string]interface{}{"props": map[string]interface{}{"age": int64(30), "name": "alice=admin"}}, want: true},
		{name: "non boolean", clause: "a.name", code: "Neo.ClientError.Statement.TypeError"},
		{name: "arithmetic failure", clause: "1 / 0 > 0", code: "Neo.ClientError.Statement.ArithmeticError"},
	}
	for _, test := range tests {
		for _, route := range []string{"direct", "compiled", "shared", "with"} {
			t.Run(test.name+"/"+route, func(t *testing.T) {
				exec, _ := newUnitExecutor(t)
				ctx := withExpressionFailureSlot(context.Background())
				row := binding{"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"age": int64(30), "name": "alice=admin"}}}
				var got bool
				switch route {
				case "direct":
					fixtureAdmission85Context, fixtureAdmission85 := expressionTestContext(ctx, test.clause)
					_ = fixtureAdmission85
					got = exec.evaluateBindingWhere(fixtureAdmission85, fixtureAdmission85Context, row, test.clause, test.params)
				case "compiled":
					fixtureAdmission86Context, fixtureAdmission86 := expressionTestContext(ctx, test.clause)
					_ = fixtureAdmission86
					got = exec.getCompiledBindingWhere(fixtureAdmission86, fixtureAdmission86Context, test.clause)(row, test.params)
				case "shared":
					values := parameterRowsOf(test.params, false)
					values["a"] = row["a"]
					fixtureAdmission1040Context, fixtureAdmission1040 := expressionTestContext(withQueryParams(ctx, test.params), test.clause)
					_ = fixtureAdmission1040Context
					got = exec.evaluateMatchRowPredicate(fixtureAdmission1040, fixtureAdmission1040Context, test.clause, values)
				case "with":
					var err error
					fixtureAdmission87Context, fixtureAdmission87 := expressionTestContext(withQueryParams(ctx, test.params), test.clause)
					_ = fixtureAdmission87
					got, err = exec.evaluateWithWhere(fixtureAdmission87, fixtureAdmission87Context, test.clause, map[string]interface{}{"a": row["a"]})
					if test.code != "" {
						require.Error(t, err)
						require.True(t, strings.HasPrefix(statusText(err), test.code), statusText(err))
					} else {
						require.NoError(t, err)
					}
					if err != nil && getExpressionFailure(ctx) == nil {
						recordExpressionFailure(ctx, err)
					}
				}
				require.Equal(t, test.want, got)
				failure := getExpressionFailure(ctx)
				if test.code == "" {
					require.NoError(t, failure)
				} else {
					require.Error(t, failure)
					require.True(t, strings.HasPrefix(statusText(failure), test.code), statusText(failure))
				}
			})
		}
	}
}

func TestGh728SharedPredicatesPublicReadback(t *testing.T) {
	for _, test := range []struct {
		name, query string
		params      map[string]interface{}
		rows        [][]interface{}
		code        string
	}{
		{name: "typed arithmetic", query: "MATCH (n:WhereScratch) WHERE n.age + $offset = $expected RETURN n.name AS name", params: map[string]interface{}{"offset": int64(1), "expected": int64(31)}, rows: [][]interface{}{{"alice=admin"}}},
		{name: "null negation", query: "MATCH (n:WhereScratch) WHERE NOT (n.missing + 1 = 0 OR false) RETURN n.name AS name", rows: [][]interface{}{}},
		{name: "typed map WITH", query: "MATCH (n:WhereScratch) WITH n WHERE all(k IN keys($props) WHERE n[k] = $props[k]) RETURN n.name AS name", params: map[string]interface{}{"props": map[string]interface{}{"age": int64(30), "name": "alice=admin"}}, rows: [][]interface{}{{"alice=admin"}}},
		{name: "quoted property", query: "MATCH (n:WhereScratch) WHERE n.`a+b` + 1 = 4 RETURN n.name AS name", rows: [][]interface{}{{"alice=admin"}}},
		{name: "escaped literal", query: `RETURN 'a\' + b' + 'z' AS name`, rows: [][]interface{}{{"a' + bz"}}},
		{name: "failed writes rollback", query: "MATCH (n:WhereScratch) SET n.flag = true WITH n WHERE 1 / $zero > 0 RETURN n.name AS name", params: map[string]interface{}{"zero": int64(0)}, code: "Neo.ClientError.Statement.ArithmeticError"},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, "CREATE (:WhereScratch {age: 30, name: 'alice=admin', `a+b`: 3}), (:WhereScratch {age: 20, name: 'bob'}), (:WhereScratch {name: 'missing'})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, test.query, test.params)
			if test.code != "" {
				require.Error(t, err)
				require.True(t, strings.HasPrefix(statusText(err), test.code), statusText(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"name"}, result.Columns)
				require.Equal(t, test.rows, result.Rows)
			}
			readback, err := exec.Execute(ctx, "MATCH (n:WhereScratch) RETURN n.name, n.flag ORDER BY n.name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"alice=admin", nil}, {"bob", nil}, {"missing", nil}}, readback.Rows)
		})
	}
}

func TestGh728RelationshipFilterKeepsTypedBindings(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	created, err := exec.Execute(ctx, "CREATE (a:TypedScratch)-[r:LINK {age:41}]->(b:TypedScratch) RETURN a, r, b", nil)
	require.NoError(t, err)
	nodes := binding{"a": created.Rows[0][0].(*storage.Node), "b": created.Rows[0][2].(*storage.Node)}
	edge := created.Rows[0][1].(*storage.Edge)
	rels := relationshipBinding{"r": edge}
	for _, clause := range []string{"r.age = 41", "type(r) = 'LINK'", "type(r) = 'LINK' AND r.age + 1 = 42"} {
		t.Run(clause, func(t *testing.T) {
			fixtureAdmission971Context, fixtureAdmission971 := expressionTestContext(ctx, clause)
			_ = fixtureAdmission971Context
			selected, selectedRels := exec.filterBindingsByWhereWithRels(fixtureAdmission971, fixtureAdmission971Context, []binding{nodes}, []relationshipBinding{rels}, clause, nil)
			require.Len(t, selected, 1)
			require.Len(t, selectedRels, 1)
			require.Same(t, edge, selectedRels[0]["r"])
			require.Len(t, nodes, 2)
		})
	}
}

func TestGh728BindingFilterAdmissionAndParameterOwnership(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	created, err := exec.Execute(ctx, "CREATE (a:ScratchAdmission {age:30})-[:LINK]->(b:ScratchAdmission {age:20}) RETURN a, b", nil)
	require.NoError(t, err)
	row := binding{"a": created.Rows[0][0].(*storage.Node), "b": created.Rows[0][1].(*storage.Node)}
	for _, clause := range []string{"a.age = 30", "(a)-[:LINK]->(b)"} {
		// A graph predicate is a fragment of an admitted statement. Compile its
		// source before exercising the private filter, as public execution does.
		statement, fixtureAdmission88 := compileRelationshipStatement(ctx, "MATCH (a:ScratchAdmission), (b:ScratchAdmission) WHERE "+clause+" RETURN a", nil)
		_ = fixtureAdmission88
		predicate := exec.newBindingFilterPredicate(statement, clause, nil)
		if clause == "a.age = 30" {
			require.NotNil(t, predicate.plan)
		} else {
			require.Nil(t, predicate.plan)
		}
		require.True(t, predicate.matches(fixtureAdmission88, row, nil))
	}
	params := map[string]interface{}{"offset": int64(1), "expected": int64(31)}
	fixtureAdmission89Context, fixtureAdmission89 := expressionTestContext(ctx, "a.age + $offset = $expected")
	_ = fixtureAdmission89
	predicate := exec.newBindingFilterPredicate(fixtureAdmission89Context, "a.age + $offset = $expected", params)
	require.NotNil(t, predicate.plan)
	require.True(t, predicate.matches(fixtureAdmission89, row, params))
	require.False(t, predicate.matches(fixtureAdmission89, binding{"a": row["b"]}, params))
	require.Nil(t, predicate.values)
	require.Equal(t, params, predicate.queryParameters)
	require.Equal(t, map[string]interface{}{"offset": int64(1), "expected": int64(31)}, params)
	require.Len(t, params, 2)
	require.Len(t, row, 2)
}

func TestGh728SharedMembershipObservesParameterChanges(t *testing.T) {
	exec := &StorageExecutor{}
	row := binding{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{"key": "first"}}}
	keys := []interface{}{"first"}
	params := map[string]interface{}{"keys": keys}
	clause := "n.key IN $keys"
	fixtureAdmission1041Context, fixtureAdmission1041 := expressionTestContext(context.Background(), clause)
	_ = fixtureAdmission1041Context
	require.Len(t, exec.filterBindingsByWhere(fixtureAdmission1041, fixtureAdmission1041Context, []binding{row}, clause, params), 1)
	keys[0] = "second"
	fixtureAdmission1042Context, fixtureAdmission1042 := expressionTestContext(context.Background(), clause)
	_ = fixtureAdmission1042Context
	require.Empty(t, exec.filterBindingsByWhere(fixtureAdmission1042, fixtureAdmission1042Context, []binding{row}, clause, params))
	keys[0] = "first"
	fixtureAdmission1043Context, fixtureAdmission1043 := expressionTestContext(context.Background(), clause)
	_ = fixtureAdmission1043Context
	require.Len(t, exec.filterBindingsByWhere(fixtureAdmission1043, fixtureAdmission1043Context, []binding{row}, clause, params), 1)
}

func TestGh728SharedWithArithmeticParametersZeroAllocations(t *testing.T) {
	exec := &StorageExecutor{}
	params := map[string]interface{}{"offset": int64(1024), "minimum": int64(2048)}
	ctx := withExpressionFailureSlot(withQueryParams(context.Background(), params))
	values := map[string]interface{}{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{"count": int64(1024)}}}
	clause := "n.count + $offset >= $minimum"
	var fixtureAdmission90 admittedRelationshipStatement
	ctx, fixtureAdmission90 = expressionTestContext(ctx, clause)
	_ = fixtureAdmission90
	accepted, err := exec.evaluateWithWhere(fixtureAdmission90, ctx, clause, values)
	require.NoError(t, err)
	require.True(t, accepted)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		accepted, err := exec.evaluateWithWhere(fixtureAdmission90, ctx, clause, values)
		if err != nil || !accepted {
			t.Fatal("shared parameterized arithmetic must accept the row")
		}
	}))
	require.Len(t, values, 1)
	require.Equal(t, map[string]interface{}{"offset": int64(1024), "minimum": int64(2048)}, params)
}

func TestGh728SharedPreparedMembershipZeroAllocations(t *testing.T) {
	exec := &StorageExecutor{}
	row := binding{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{"key": "first"}}}
	params := map[string]interface{}{"keys": []interface{}{"first", nil}}
	fixtureAdmission91Context, fixtureAdmission91 := expressionTestContext(context.Background(), "n.key IN $keys")
	_ = fixtureAdmission91
	predicate := exec.newBindingFilterPredicate(fixtureAdmission91Context, "n.key IN $keys", params)
	require.True(t, predicate.matches(fixtureAdmission91, row, params))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !predicate.matches(fixtureAdmission91, row, params) {
			t.Fatal("prepared membership must keep the row")
		}
	}))
}

func TestGh728SharedMembershipConcurrentParameterScopes(t *testing.T) {
	exec := &StorageExecutor{}
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			key := strconv.Itoa(worker)
			row := binding{"n": &storage.Node{ID: storage.NodeID(key), Properties: map[string]interface{}{"key": key}}}
			keys := []interface{}{key}
			params := map[string]interface{}{"keys": keys}
			for iteration := 0; iteration < 16; iteration++ {
				fixtureAdmission92Context, fixtureAdmission92 := expressionTestContext(context.Background(), "n.key IN $keys")
				_ = fixtureAdmission92
				predicate := exec.newBindingFilterPredicate(fixtureAdmission92Context, "n.key IN $keys", params)
				keys[0] = "changed"
				if !predicate.matches(fixtureAdmission92, row, params) {
					t.Error("another invocation changed a prepared index")
					return
				}
				keys[0] = key
			}
		}(worker)
	}
	workers.Wait()
}

func TestGh728BindingFilterScopesAreInvocationLocal(t *testing.T) {
	exec, _ := newUnitExecutor(t)
	params := map[string]interface{}{"offset": int64(1), "expected": int64(31)}
	ctx := withQueryParams(context.Background(), params)
	parameters := parameterRowValues(ctx)
	rows := []binding{
		{"n": &storage.Node{ID: "first", Properties: map[string]interface{}{"age": int64(30)}}},
		{"n": &storage.Node{ID: "second", Properties: map[string]interface{}{"age": int64(20)}}},
		{"n": nil},
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 16; iteration++ {
				fixtureAdmission1035Context, fixtureAdmission1035 := expressionTestContext(ctx, "n.age + $offset = $expected")
				_ = fixtureAdmission1035Context
				selected := exec.filterBindingsByWhere(fixtureAdmission1035, fixtureAdmission1035Context, rows, "n.age + $offset = $expected", params)
				if len(selected) != 1 || selected[0]["n"] != rows[0]["n"] {
					t.Errorf("unexpected filtered rows: %v", selected)
					return
				}
			}
		}()
	}
	workers.Wait()
	require.Equal(t, map[string]interface{}{"$offset": int64(1), "$expected": int64(31)}, parameters)
	require.Len(t, params, 2)
	require.Len(t, rows[0], 1)
	require.Len(t, rows[1], 1)
	require.Nil(t, rows[2]["n"])
}

func TestCompiledBindingWhere_SupportedAndFallback(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	bindingRow := binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"status": "active", "name": "alice", "age": int64(30)}},
		"b": &storage.Node{ID: "n2", Properties: map[string]interface{}{"status": "pending", "name": "bob", "age": int64(40)}},
	}
	params := map[string]interface{}{"statuses": []interface{}{"active", "pending"}, "prefix": "al"}

	ctx := context.Background()
	fixtureAdmission93Context, fixtureAdmission93 := expressionTestContext(ctx, "a.status IN $statuses")
	_ = fixtureAdmission93
	assert.True(t, exec.evaluateBindingWhere(fixtureAdmission93, fixtureAdmission93Context, bindingRow, "a.status IN $statuses", params))
	fixtureAdmission94Context, fixtureAdmission94 := expressionTestContext(ctx, "b.age > a.age")
	_ = fixtureAdmission94
	assert.True(t, exec.evaluateBindingWhere(fixtureAdmission94, fixtureAdmission94Context, bindingRow, "b.age > a.age", params))
	fixtureAdmission95Context, fixtureAdmission95 := expressionTestContext(ctx, "a.name STARTS WITH $prefix")
	_ = fixtureAdmission95
	assert.True(t, exec.evaluateBindingWhere(fixtureAdmission95, fixtureAdmission95Context, bindingRow, "a.name STARTS WITH $prefix", params))
	fixtureAdmission96Context, fixtureAdmission96 := expressionTestContext(ctx, "a.name IS NOT NULL")
	_ = fixtureAdmission96
	assert.True(t, exec.evaluateBindingWhere(fixtureAdmission96, fixtureAdmission96Context, bindingRow, "a.name IS NOT NULL", nil))
	fixtureAdmission97Context, fixtureAdmission97 := expressionTestContext(ctx, "a.missing IS NULL")
	_ = fixtureAdmission97
	assert.True(t, exec.evaluateBindingWhere(fixtureAdmission97, fixtureAdmission97Context, bindingRow, "a.missing IS NULL", nil))

	fixtureAdmission98Context, fixtureAdmission98 := expressionTestContext(ctx, "a.status IN $statuses AND b.age > a.age AND a.name STARTS WITH $prefix")
	_ = fixtureAdmission98
	compiled := exec.getCompiledBindingWhere(fixtureAdmission98, fixtureAdmission98Context, "a.status IN $statuses AND b.age > a.age AND a.name STARTS WITH $prefix")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, params))

	fixtureAdmission99Context, fixtureAdmission99 := expressionTestContext(ctx, "a.name IS NOT NULL")
	_ = fixtureAdmission99
	compiled = exec.getCompiledBindingWhere(fixtureAdmission99, fixtureAdmission99Context, "a.name IS NOT NULL")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, nil))

	fixtureAdmission100Context, fixtureAdmission100 := expressionTestContext(ctx, "a.missing IS NULL")
	_ = fixtureAdmission100
	compiled = exec.getCompiledBindingWhere(fixtureAdmission100, fixtureAdmission100Context, "a.missing IS NULL")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, nil))
	fixtureAdmission101Context, fixtureAdmission101 := expressionTestContext(ctx, "a.status IN $statuses AND b.status = a.status AND a.name IS NOT NULL AND b.name IS NOT NULL")
	_ = fixtureAdmission101
	supported, ok := exec.getCompiledBindingWhereIfSupported(fixtureAdmission101Context, "a.status IN $statuses AND b.status = a.status AND a.name IS NOT NULL AND b.name IS NOT NULL")
	require.True(t, ok)
	assert.True(t, supported(binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"status": "active", "name": "alice"}},
		"b": &storage.Node{ID: "n2", Properties: map[string]interface{}{"status": "active", "name": "bob"}},
	}, params))

	fixtureAdmission102Context, fixtureAdmission102 := expressionTestContext(ctx, "a = b")
	_ = fixtureAdmission102
	compiled = exec.getCompiledBindingWhere(fixtureAdmission102, fixtureAdmission102Context, "a = b")
	require.NotNil(t, compiled)
	assert.False(t, compiled(bindingRow, nil))
	assert.True(t, compiled(binding{"a": bindingRow["a"], "b": bindingRow["a"]}, nil))
	fixtureAdmission103Context, fixtureAdmission103 := expressionTestContext(ctx, "size(a.name) > 0")
	_ = fixtureAdmission103
	assert.True(t, exec.evaluateBindingWhere(fixtureAdmission103, fixtureAdmission103Context, bindingRow, "size(a.name) > 0", nil))
}

func TestCompiledBindingWhere_UnsupportedFallsBackCompliantly(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	bindingRow := binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}},
	}
	ctx := context.Background()
	fixtureAdmission104Context, fixtureAdmission104 := expressionTestContext(ctx, "a.name")
	_ = fixtureAdmission104
	compiled := exec.getCompiledBindingWhere(fixtureAdmission104, fixtureAdmission104Context, "a.name")
	require.NotNil(t, compiled)
	assert.False(t, compiled(bindingRow, nil))
	fixtureAdmission105Context, fixtureAdmission105 := expressionTestContext(ctx, "a.missing")
	_ = fixtureAdmission105
	assert.False(t, exec.evaluateBindingWhere(fixtureAdmission105, fixtureAdmission105Context, bindingRow, "a.missing", nil))
	fixtureAdmission106Context, fixtureAdmission106 := expressionTestContext(ctx, "a.name")
	_ = fixtureAdmission106
	assert.False(t, exec.evaluateBindingWhere(fixtureAdmission106, fixtureAdmission106Context, bindingRow, "a.name", nil))
}

func TestBindingWhereStringPredicateFallsBackForExpressionOperand(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	row := binding{
		"n": &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}},
	}

	fixtureAdmission107Context, fixtureAdmission107 := expressionTestContext(ctx, "toUpper(n.name) STARTS WITH 'AL'")
	_ = fixtureAdmission107
	assert.True(t, exec.evaluateBindingWhereGeneric(fixtureAdmission107, fixtureAdmission107Context, row, "toUpper(n.name) STARTS WITH 'AL'", nil))
}

func TestCompiledBindingWhere_NodeOrderingUsesNumericComparisonForNumericIDs(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	assert.True(t, exec.compareNodeIDs("2", "10", "<"))
	assert.False(t, exec.compareNodeIDs("2", "10", ">"))
	assert.True(t, exec.compareNodeIDs("2", "10", "<="))
	assert.False(t, exec.compareNodeIDs("2", "10", ">="))
	assert.True(t, exec.compareNodeIDs("alpha", "beta", "<"))
	assert.False(t, exec.compareNodeIDs("alpha", "beta", ">"))
}
