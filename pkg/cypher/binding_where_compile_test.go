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
			predicate, supported := exec.tryCompileBindingWhere(ctx, test.clause)
			require.True(t, supported)
			assert.Equal(t, test.want, exec.evaluateRowPredicate(ctx, test.clause, map[string]interface{}{"n": node}))
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
			assert.Equal(t, test.want, exec.evaluateRowPredicate(ctx, clause, row))
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
	for _, length := range []int{4, 1024} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			row := map[string]interface{}{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{
				"name": strings.Repeat("x", length), "count": int64(4096),
			}}}
			require.True(t, exec.evaluateRowPredicate(ctx, clause, row))
			accepted := true
			allocations := testing.AllocsPerRun(100, func() {
				accepted = accepted && exec.evaluateRowPredicate(ctx, clause, row)
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
			require.Equal(t, test.want, exec.evaluateWhereForContext(ctx, test.clause, nodes))
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
			plan := planRowPredicate(clause)
			require.NotNil(t, plan)
			require.True(t, plan.complete)
			require.True(t, exec.evaluateWhereForContext(ctx, clause, nodes))
			require.Zero(t, testing.AllocsPerRun(100, func() {
				if !exec.evaluateWhereForContext(ctx, clause, nodes) {
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
			require.Len(t, exec.filterBindingsByWhere(context.Background(), []binding{row}, clause, nil), 1)
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
					got = exec.evaluateBindingWhere(ctx, row, test.clause, test.params)
				case "compiled":
					got = exec.getCompiledBindingWhere(ctx, test.clause)(row, test.params)
				case "shared":
					values := parameterRowsOf(test.params, false)
					values["a"] = row["a"]
					got = exec.evaluateMatchRowPredicate(withQueryParams(ctx, test.params), test.clause, values)
				case "with":
					var err error
					got, err = exec.evaluateWithWhere(withQueryParams(ctx, test.params), test.clause, map[string]interface{}{"a": row["a"]})
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
			selected, selectedRels := exec.filterBindingsByWhereWithRels(ctx, []binding{nodes}, []relationshipBinding{rels}, clause, nil)
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
		predicate := exec.newBindingFilterPredicate(ctx, clause, nil)
		if clause == "a.age = 30" {
			require.NotNil(t, predicate.plan)
		} else {
			require.Nil(t, predicate.plan)
		}
		require.True(t, predicate.matches(row, nil))
	}
	params := map[string]interface{}{"offset": int64(1), "expected": int64(31)}
	predicate := exec.newBindingFilterPredicate(ctx, "a.age + $offset = $expected", params)
	require.NotNil(t, predicate.plan)
	require.True(t, predicate.matches(row, params))
	require.False(t, predicate.matches(binding{"a": row["b"]}, params))
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
	require.Len(t, exec.filterBindingsByWhere(context.Background(), []binding{row}, clause, params), 1)
	keys[0] = "second"
	require.Empty(t, exec.filterBindingsByWhere(context.Background(), []binding{row}, clause, params))
	keys[0] = "first"
	require.Len(t, exec.filterBindingsByWhere(context.Background(), []binding{row}, clause, params), 1)
}

func TestGh728SharedWithArithmeticParametersZeroAllocations(t *testing.T) {
	exec := &StorageExecutor{}
	params := map[string]interface{}{"offset": int64(1024), "minimum": int64(2048)}
	ctx := withExpressionFailureSlot(withQueryParams(context.Background(), params))
	values := map[string]interface{}{"n": &storage.Node{ID: "node", Properties: map[string]interface{}{"count": int64(1024)}}}
	clause := "n.count + $offset >= $minimum"
	accepted, err := exec.evaluateWithWhere(ctx, clause, values)
	require.NoError(t, err)
	require.True(t, accepted)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		accepted, err := exec.evaluateWithWhere(ctx, clause, values)
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
	predicate := exec.newBindingFilterPredicate(context.Background(), "n.key IN $keys", params)
	require.True(t, predicate.matches(row, params))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !predicate.matches(row, params) {
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
				predicate := exec.newBindingFilterPredicate(context.Background(), "n.key IN $keys", params)
				keys[0] = "changed"
				if !predicate.matches(row, params) {
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
				selected := exec.filterBindingsByWhere(ctx, rows, "n.age + $offset = $expected", params)
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
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	bindingRow := binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"status": "active", "name": "alice", "age": int64(30)}},
		"b": &storage.Node{ID: "n2", Properties: map[string]interface{}{"status": "pending", "name": "bob", "age": int64(40)}},
	}
	params := map[string]interface{}{"statuses": []interface{}{"active", "pending"}, "prefix": "al"}

	ctx := context.Background()
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.status IN $statuses", params))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "b.age > a.age", params))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.name STARTS WITH $prefix", params))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.name IS NOT NULL", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.missing IS NULL", nil))

	compiled := exec.getCompiledBindingWhere(ctx, "a.status IN $statuses AND b.age > a.age AND a.name STARTS WITH $prefix")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, params))

	compiled = exec.getCompiledBindingWhere(ctx, "a.name IS NOT NULL")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, nil))

	compiled = exec.getCompiledBindingWhere(ctx, "a.missing IS NULL")
	require.NotNil(t, compiled)
	assert.True(t, compiled(bindingRow, nil))
	supported, ok := exec.getCompiledBindingWhereIfSupported(ctx, "a.status IN $statuses AND b.status = a.status AND a.name IS NOT NULL AND b.name IS NOT NULL")
	require.True(t, ok)
	assert.True(t, supported(binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"status": "active", "name": "alice"}},
		"b": &storage.Node{ID: "n2", Properties: map[string]interface{}{"status": "active", "name": "bob"}},
	}, params))

	compiled = exec.getCompiledBindingWhere(ctx, "a = b")
	require.NotNil(t, compiled)
	assert.False(t, compiled(bindingRow, nil))
	assert.True(t, compiled(binding{"a": bindingRow["a"], "b": bindingRow["a"]}, nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, bindingRow, "size(a.name) > 0", nil))
}

func TestCompiledBindingWhere_UnsupportedFallsBackCompliantly(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	bindingRow := binding{
		"a": &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}},
	}
	ctx := context.Background()
	compiled := exec.getCompiledBindingWhere(ctx, "a.name")
	require.NotNil(t, compiled)
	assert.False(t, compiled(bindingRow, nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.missing", nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, bindingRow, "a.name", nil))
}

func TestBindingWhereStringPredicateFallsBackForExpressionOperand(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	ctx := context.Background()
	row := binding{
		"n": &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}},
	}

	assert.True(t, exec.evaluateBindingWhereGeneric(ctx, row, "toUpper(n.name) STARTS WITH 'AL'", nil))
}

func TestCompiledBindingWhere_NodeOrderingUsesNumericComparisonForNumericIDs(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	assert.True(t, exec.compareNodeIDs("2", "10", "<"))
	assert.False(t, exec.compareNodeIDs("2", "10", ">"))
	assert.True(t, exec.compareNodeIDs("2", "10", "<="))
	assert.False(t, exec.compareNodeIDs("2", "10", ">="))
	assert.True(t, exec.compareNodeIDs("alpha", "beta", "<"))
	assert.False(t, exec.compareNodeIDs("alpha", "beta", ">"))
}
