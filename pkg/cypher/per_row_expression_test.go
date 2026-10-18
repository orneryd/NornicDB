package cypher

import (
	"context"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// perRowTestExecutor: (a:P {id: 'a'})-[:R {kind: 'k'}]->(b:P {id: 'b'}),
// and (c:P {id: 'c'})-[:R]->(gone), whose end node is then removed from
// storage underneath the relationship.
func perRowTestExecutor(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	nodes := []*storage.Node{
		{ID: "a", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "a"}},
		{ID: "b", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "b"}},
		{ID: "c", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "c"}},
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		t.Fatal(err)
	}
	if err := store.BulkCreateEdges([]*storage.Edge{{ID: "r1", Type: "R", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{"kind": "k"}}}); err != nil {
		t.Fatal(err)
	}
	return NewStorageExecutor(store), store
}

// startNode / endNode in the row evaluator give what the context evaluator
// gives for the same row: the endpoint by id, null for a missing endpoint,
// and the context evaluator's answer for any argument that isn't a bound
// relationship.
func TestRowRelationshipEndpointMatchesContextEvaluator(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	edge, err := store.GetEdge("r1")
	if err != nil {
		t.Fatal(err)
	}
	dangling := &storage.Edge{ID: "r9", Type: "R", StartNode: "c", EndNode: "missing"}
	values := map[string]interface{}{
		"r": edge, "d": dangling, "n": int64(1), "m": map[string]interface{}{"type": "R"}, "z": (*storage.Edge)(nil),
	}
	for _, expr := range []string{
		"startNode(r)", "endNode(r)", "startNode( r )", "STARTNODE(r)", "endnode(r)",
		"startNode(d)", "endNode(d)", "startNode(z)", "startNode(unbound)",
		"startNode(r).id", "endNode(r).id + '|' + startNode(r).id",
	} {
		got, gotOK, gotErr := exec.evaluateRowValue(expr, values)
		want, wantErr := exec.evaluateRowFallback(expr, values)
		wantOK := true
		if text, isText := want.(string); isText && text == expr {
			want, wantOK = nil, false
		}
		if gotErr != nil || wantErr != nil {
			t.Fatalf("%s: errors %v / %v", expr, gotErr, wantErr)
		}
		if gotOK != wantOK || !sameRowValue(got, want) {
			t.Fatalf("%s: row evaluator %#v (%v), context evaluator %#v (%v)", expr, got, gotOK, want, wantOK)
		}
	}
	// Wrong argument types must fail through the same shared contract in both
	// evaluators; neither path may silently turn an ordinary value into null.
	for _, expr := range []string{"startNode(n)", "endNode(n)", "startNode(m)", "endNode(m)"} {
		_, _, gotErr := exec.evaluateRowValue(expr, values)
		_, wantErr := exec.evaluateRowFallback(expr, values)
		if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() ||
			!strings.Contains(gotErr.Error(), "Neo.ClientError.Statement.TypeError") {
			t.Fatalf("%s: expected matching TypeError, got %v / %v", expr, gotErr, wantErr)
		}
	}
	if node, _, _ := exec.evaluateRowValue("startNode(r)", values); node.(*storage.Node).ID != "a" {
		t.Fatalf("startNode(r) = %v", node)
	}
	if node, _, _ := exec.evaluateRowValue("endNode(d)", values); node != nil {
		t.Fatalf("endNode(d) of a missing node = %#v", node)
	}

	ctx := context.Background()
	result, err := exec.Execute(ctx, "MATCH (x)-[r:R]->(y) RETURN startNode(r) = x AS s, endNode(r) = y AS e, startNode(r).id + '|' + endNode(r).id AS k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]interface{}{{true, true, "a|b"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows %v, want %v", result.Rows, want)
	}
	result, err = exec.Execute(ctx, "MATCH ()-[r:R]->() WHERE startNode(r).id = 'a' AND endNode(r).id <> 'zz' RETURN count(*)", nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]interface{}{{int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows %v, want %v", result.Rows, want)
	}
}

// sameRowValue compares evaluator results, nodes and relationships by id.
func sameRowValue(left, right interface{}) bool {
	switch l := left.(type) {
	case *storage.Node:
		r, ok := right.(*storage.Node)
		if !ok || (l == nil) != (r == nil) {
			return false
		}
		return l == nil || l.ID == r.ID && reflect.DeepEqual(l.Properties, r.Properties)
	case *storage.Edge:
		r, ok := right.(*storage.Edge)
		return ok && (l == nil) == (r == nil) && (l == nil || l.ID == r.ID)
	}
	return reflect.DeepEqual(left, right)
}

// rowValueCorpus: expressions of every form the row value programs compile,
// and some they read from the text, with nulls, type errors and
// unresolved parts.
var rowValueCorpus = []string{
	"1", "'a'", "null", "true", "x", "$p", "$keys", "missing", "n.id", "n.missing", "m.a.b", "m.a", "r.kind", "u.id", "(n.id)", "((1))",
	"n.id + '|' + r.kind", "startNode(r).id + '|' + endNode(r).id + '|' + r.kind", "startNode(r)", "endNode(r).id", "startNode(n)", "startNode(r.kind)",
	"1 + 2 * 3", "x - 1", "x / 0", "1 / 0", "1.0 / 0", "x % 2", "'a' - 1", "x * null", "'a' + 1", "[1] + 2", "x + y", "x / y",
	"x = 1", "x <> 1", "1 < x < 3", "x = null", "n.id = 'a'", "n.id <> 'zz'", "id(n) = id(n)", "x = 'a'", "1 = 1.0", "[1, 2] = [1, 2.0]",
	"x IS NULL", "missing IS NULL", "n.missing IS NOT NULL", "x IN [1, 2]", "x NOT IN [1, null]", "'k1' IN $keys", "null IN $keys", "x IN null", "n.id IN $keys",
	"n.id STARTS WITH 'a'", "x STARTS WITH 'a'", "n.id ENDS WITH null", "n.id CONTAINS 'x'",
	"x = 1 AND y = 2", "x = 1 OR missing", "NOT x = 1", "NOT NOT x = 1", "x = 1 XOR y = 2", "true OR x / 0 = 1", "false AND x / 0 = 1", "x AND true", "NOT (x = 1 OR y = 2)", "NOT 1",
	"any(k IN $keys WHERE k = 'k1')", "NOT any(k IN $keys WHERE k = 'zz')", "all(k IN $keys WHERE k STARTS WITH 'k')", "none(k IN $keys WHERE k = n.id + '|' + r.kind)",
	"single(k IN [1, 2, 1] WHERE k = 1)", "any(k IN [1, null] WHERE k = 2)", "any(k IN null WHERE k = 1)", "any(k IN missing WHERE k = 1)", "all(k IN [1, 'a'] WHERE k > 0)",
	"any(k IN $keys WHERE k = startNode(r).id)", "any(k IN [1, 2] WHERE any(j IN [2, 3] WHERE j = k + x))", "any(k IN [1, 2] WHERE k / 0 = 1)", "any(k IN [] WHERE 1 / 0 = 1)",
	"any(k IN [1, 2] WHERE k)", "any(x IN [5] WHERE x = 5)", "all(k IN $keys WHERE size(k) = 2)",
	"[k IN $keys WHERE k <> 'k1' | k + '!']", "[k IN [1, 2, 3] WHERE k > x]", "[k IN [1, 2] | k * y]", "[k IN missing | k]", "[k IN [1, 2] WHERE k]", "[k IN x]", "size([k IN $keys WHERE k = n.id + '|' + r.kind | k])",
	"[1, x, n.id]", "[]", "[1, missing]", "[x, 1 / 0]",
	"coalesce(missing, n.missing, x)", "coalesce(null, null)", "coalesce(n.id, 1 / 0)", "size(null)", "size('abc')", "size($keys)", "size(1)", "head([])", "last([1, 2])", "reverse('abc')", "reverse(null)",
	"type(r)", "type(m)", "type(x)", "type(null)", "toLower(n.id)", "toLower(startNode(r).id) + 'x'", "rand() < 2", "abs(-1)",
	"CASE WHEN x = 1 THEN 'one' ELSE 'other' END", "x[0]", "$keys[0]", "m['a']", "-x", "2 ^ 3", "n {.id}", "{a: x}", "`x`",
	"reduce(acc = 0, k IN [1, 2, 3] | acc + k + x)", "reduce(acc = '', k IN $keys | acc + k)",
	"reduce(acc = 0, k IN null | acc + k)", "reduce(acc = 0, k IN missing | acc + k)", "reduce(acc = missing, k IN [1] | acc)", "reduce(acc = 0, k IN [] | 1 / 0)",
	"reduce(acc = 0, k IN [1, 2] | acc + k / 0)", "reduce(acc = 0, k IN [1, 'a'] | acc + k)", "reduce(acc = [], k IN $keys | acc + [k + n.id])",
	"reduce(acc = 0, k IN [1, 2] | acc + reduce(s = k, j IN [k, x] | s + j))", "REDUCE(acc = 0, k IN [1] | acc + k)", "reduce(acc = 0, k IN [1] | unknown(k))",
	"reduce(acc 0, k IN [1] | acc)", "any(k IN [1, 2] WHERE reduce(s = 0, j IN [k, x] | s + j) > 2)",
	"allReduce(acc = 0, k IN [1, 2, 3] | acc + k, acc < 10)", "allReduce(acc = 0, k IN [1, 2, 3] | acc + k, acc < 2)", "allReduce(acc = 0, k IN [1, null] | k, acc > 0)",
	"allReduce(acc = 0, k IN [1] | acc + k, 'a')", "allReduce(acc = 0, k IN [1] | acc + k, $p)", "allReduce(acc = 0, k IN [] | acc, false)",
	// Errors and unresolved parts reaching each compiled form.
	"[1, 'b']", "{a: 1}", "any(1)", "any(k IN)", "CASE END", "CASE WHEN x THEN END", "CASE 1 / 0 WHEN 1 THEN 2 END", "CASE [, ] WHEN 1 THEN 2 END",
	"CASE x WHEN 1 / 0 THEN 2 END", "CASE x WHEN [, ] THEN 2 END", "1 / 0 IS NULL", "1 / 0 IN [1]", "1 IN 1 / 0", "1 / 0 STARTS WITH 'a'", "'a' STARTS WITH 1 / 0",
	"[, ] STARTS WITH 'a'", "(1 / 0) + 1", "head([1 / 0]).x", "head([1]).x", "head([m]).a", "size(1 / 0)", "any(k IN [1 / 0] WHERE k = 1)", "any(k IN [1] WHERE [, ])",
	"[k IN [1 / 0] | k]", "[k IN [1] WHERE 1 / 0 = k]", "[k IN [1] WHERE [, ] | k]", "type(z)", "n.id",
	// A subscript whose key holds brackets of its own is one subscript
	// (rowSubscriptStart), as is a slice; subquery and pattern comprehension
	// atoms are read by their evaluators' own readers.
	"m[n['id']]", "m[$keys[0]]", "m['a']['b']", "$keys[size($keys) - 1]", "m[[k IN keys(m) | k][0]]", "$keys[0..1][0]", "[[1, 2], [3]][x][0]",
	"any(k IN keys(n) WHERE m[n[k]] IS NULL)", "none(k IN keys(n) WHERE m[n[k]] IS NOT NULL)", "[k IN keys(r) | m[r[k]]]", "reduce(s = 0, k IN keys(n) | s + size(n[k]))",
	"COUNT { MATCH (q) }", "[(n)-[:R]->(o) | o.id]", "size([(n)-[:R]->(o) | o.id]) > 0",
}

func rowValueCorpusRows(t *testing.T, store storage.Engine) []map[string]interface{} {
	t.Helper()
	node, err := store.GetNode("a")
	if err != nil {
		t.Fatal(err)
	}
	edge, err := store.GetEdge("r1")
	if err != nil {
		t.Fatal(err)
	}
	keys := []interface{}{"k1", "k2", "a|k"}
	return []map[string]interface{}{
		{"x": int64(1), "y": int64(2), "n": node, "r": edge, "m": map[string]interface{}{"a": map[string]interface{}{"b": "c"}, "type": "R"}, "$p": "p", "$keys": keys},
		{"x": nil, "y": "s", "n": node, "r": edge, "m": map[string]interface{}{"a": int64(3)}, "$keys": []interface{}{"k1", nil}, cypher25RowKey: true},
		{"x": "a", "n": nil, "r": nil, "u": int64(4), "z": (*storage.Edge)(nil), "n.id": "bound"},
		{"x": int64(1), "n + '|' + r.kind": "bound", "x = 1": "bound", "missing": int64(7), "$keys": keys, "n": node, "r": edge},
	}
}

// Supplementary adapter-contract check, including non-Cypher internal bindings.
// Semantic expectations live in TestRowValueProgramsMatchNeo4j.
// A row value program gives what the row evaluator reading the text gives,
// for every expression and row: value, resolution and error.
func TestRowValueProgramAdapterConsistency(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	rows := rowValueCorpusRows(t, store)
	for _, expr := range rowValueCorpus {
		for index, row := range rows {
			rowValueProgramsDisabled.Store(true)
			want, wantOK, wantErr := exec.evaluateRowValue(expr, row)
			rowValueProgramsDisabled.Store(false)
			compiler := rowValueCompiler{}
			root, _ := compiler.compile(expr)
			got, gotOK, gotErr := root.eval(exec, row, nil, nil)
			if gotOK != wantOK || !sameRowValue(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Errorf("%s (row %d): program %#v, %v, %v; text %#v, %v, %v", expr, index, got, gotOK, gotErr, want, wantOK, wantErr)
			}
		}
	}
}

// The forms of #1214 compile: no part of them is read from the text per
// evaluation, and a list predicate's part that doesn't read the element
// variable is evaluated once per list.
func TestRowValueProgramsCompileIssueForms(t *testing.T) {
	for _, expr := range []string{
		"startNode(r).id + '|' + endNode(r).id + '|' + r.kind",
		"NOT any(k IN $keys WHERE k = startNode(r).id + '|' + endNode(r).id + '|' + r.kind)",
		"coalesce(r.origin, '') <> 'automatic' OR r.stage = 'interpretation'",
		"none(k IN $keys WHERE k = r.kind)", "all(k IN $keys WHERE k <> r.kind)", "single(k IN $keys WHERE k = r.kind)",
		"[k IN $keys WHERE k <> r.kind | k + '|']", "reduce(acc = '', k IN $keys | acc + k + r.kind)",
		"allReduce(acc = 0, k IN $keys | acc + size(k), acc < 10000)",
	} {
		compiler := rowValueCompiler{}
		root, _ := compiler.compile(expr)
		if text := firstRowTextNode(root); text != "" {
			t.Fatalf("%s: %q is read from the text", expr, text)
		}
	}
	compiler := rowValueCompiler{}
	root, _ := compiler.compile("any(k IN $keys WHERE k = startNode(r).id + '|' + r.kind)")
	quantifier := root.(*rowBoundNode).inner.(*rowQuantifierNode)
	if quantifier.loop.slots != 1 {
		t.Fatalf("memo slots %d, want the key once", quantifier.loop.slots)
	}
}

// firstRowTextNode returns the text of a part of nodes read from the text.
func firstRowTextNode(nodes ...rowValueNode) string {
	for _, node := range nodes {
		if text := firstRowTextNodeOf(node); text != "" {
			return text
		}
	}
	return ""
}

func firstRowTextNodeOf(node rowValueNode) string {
	switch n := node.(type) {
	case *rowTextNode:
		return n.text
	case *rowFallbackNode:
		if !isValidIdentifier(n.text) && !strings.HasPrefix(n.text, "$") {
			return n.text
		}
	case *rowBoundNode:
		return firstRowTextNode(n.inner)
	case *rowMemoNode:
		return firstRowTextNode(n.inner)
	case *rowListNode:
		return firstRowTextNode(n.items...)
	case *rowLogicalNode:
		return firstRowTextNode(n.operands...)
	case *rowComparisonNode:
		return firstRowTextNode(n.operands...)
	case *rowNullTestNode:
		return firstRowTextNode(n.operand)
	case *rowMembershipNode:
		return firstRowTextNode(n.needle, n.haystack)
	case *rowStringPredicateNode:
		return firstRowTextNode(n.left, n.right)
	case *rowArithmeticNode:
		return firstRowTextNode(n.left, n.right)
	case *rowPropertyNode:
		return firstRowTextNode(n.base)
	case *rowUnaryFunctionNode:
		return firstRowTextNode(n.operand)
	case *rowCoalesceNode:
		return firstRowTextNode(n.operands...)
	case *rowQuantifierNode:
		return firstRowTextNode(n.list, n.predicate)
	case *rowComprehensionNode:
		return firstRowTextNode(n.list, n.predicate, n.projection)
	case *rowReduceNode:
		return firstRowTextNode(n.initial, n.list, n.step, n.predicate)
	}
	return ""
}

// rowPredicateCorpus: WHERE predicates of every form the predicate programs
// compile, and some they leave to the text path.
var rowPredicateCorpus = []string{
	"true", "false", "x = 1", "x <> 1", "x = 1 AND y = 2", "x = 1 OR y = 2", "(x = 1)", "NOT x = 1", "x = 1 XOR y = 2", "x", "missing",
	"n.id = 'a' AND r.kind <> 'zz'", "startNode(r).id <> 'zz'", "startNode(r).id + '|' + endNode(r).id + '|' + r.kind <> 'zz'",
	"NOT any(k IN $keys WHERE k = 'zz')", "NOT any(k IN $keys WHERE k = startNode(r).id + '|' + r.kind)", "any(k IN $keys WHERE k = n.id)",
	"NOT startNode(r).id + '|' + r.kind IN $keys", "n.id IN $keys", "x NOT IN [2, 3]", "x IN null", "toLower(n.id) IN ['a']",
	"n.id STARTS WITH 'a'", "x ENDS WITH 'b'", "n.id CONTAINS toLower('A')", "x IS NULL", "n.missing IS NOT NULL", "coalesce(n.missing, '') = ''",
	"n:P", "n:Q", "x + 1", "x + 1 > 1", "1 < x + 1 < 3", "id(n) = id(n)", "x / 0 = 1", "x = 1 OR x / 0 = 1", "x = 2 AND x / 0 = 1", "'a' = 1",
	"true OR (x = 1 AND y = 2)", "false AND (x / 0 = 1)", "true OR (startNode(r).id = 'a' AND any(k IN $keys WHERE k = n.id))",
	"CASE WHEN x = 1 THEN true ELSE false END", "CASE WHEN x = 1 THEN y = 2 ELSE false END OR n.id = 'zz'",
	"n.id = 'zz' OR CASE x WHEN 1 THEN true END", "x = 1 AND CASE WHEN y = 2 THEN 'yes' END = 'yes'",
	"(CASE WHEN x = 1 THEN 1 ELSE 2 END) + 1 = 2 OR false", "size([k IN $keys WHERE k <> 'zz']) > 0", "reduce(s = 0, k IN [1, 2] | s + k) = 3",
	"x = 1 AND (y = 2 OR missing = 3)", "NOT (x = 1 OR y = 2)", "NOT NOT x = 1", "x = 1 XOR NOT y = 2", "x AND y",
	"()", "COUNT { MATCH (q) } > 0", "n.id IS NORMALIZED", "1 / 0 IN [1]", "x IN [1 / 0]", "[, ] IN [1]", "x / 0 = 1 XOR y = 2", "x + 1 = 2 XOR CASE WHEN y = 2 THEN true END",
	"x + 1 = 2 XOR CASE WHEN true THEN 1 / 0 END = 1", "x / 0 = 1 XOR CASE WHEN x = 1 THEN true END", "x + [, ] = 1 XOR CASE WHEN x = 1 THEN true END",
	"n.id = 'zz' OR CASE x WHEN 1 / 0 THEN true END", "x = 1 AND CASE WHEN true THEN [, ] END = 1", "CASE WHEN x = 1 THEN 1 END = 1 OR CASE WHEN x = 2 THEN 1 END = 1",
	"any(k IN keys(n) WHERE m[n[k]] IS NULL)", "none(k IN keys(r) WHERE m[r[k]] IS NOT NULL)", "m[n['id']] IS NULL", "$keys[x] = 'k2'",
}

// Supplementary adapter-contract check; TestRowPredicateProgramsMatchNeo4j
// supplies the independent semantic expectations.
// A predicate program holds when the text path holds, recording the same
// error, for every predicate and row.
func TestRowPredicateProgramAdapterConsistency(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	rows := rowValueCorpusRows(t, store)
	evaluate := func(expr string, row map[string]interface{}, program bool) (bool, error) {
		ctx := withExpressionFailureSlot(context.Background())
		if program {
			return compileRowPredicateProgram(expr).holds(exec, ctx, row), getExpressionFailure(ctx)
		}
		rowValueProgramsDisabled.Store(true)
		defer rowValueProgramsDisabled.Store(false)
		return exec.evaluateRowPredicate(ctx, expr, row), getExpressionFailure(ctx)
	}
	for _, expr := range rowPredicateCorpus {
		// CASE has a hand-written oracle in TestRowPredicateCaseExpectedResults:
		// evaluateRowPredicate compiles CASE even when value programs are disabled.
		if mayContainCaseKeyword(expr) {
			continue
		}
		for index, row := range rows {
			want, wantErr := evaluate(expr, row, false)
			got, gotErr := evaluate(expr, row, true)
			if got != want || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
				t.Errorf("%s (row %d): program %v, %v; text %v, %v", expr, index, got, gotErr, want, wantErr)
			}
		}
	}
}

// An AND / OR operand the predicate doesn't reach isn't evaluated, its
// CASE blocks included, as in Neo4j 5.26.30: a division by zero in an
// unreached CASE is no error; in a reached one it is.
func TestRowPredicateUnreachedCaseIsNotEvaluated(t *testing.T) {
	exec, _ := perRowTestExecutor(t)
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"UNWIND [0] AS x WITH x WHERE true OR CASE WHEN 1/x = 1 THEN true END RETURN count(*) AS c":   int64(1),
		"UNWIND [0] AS x WITH x WHERE false AND CASE WHEN 1/x = 1 THEN true END RETURN count(*) AS c": int64(0),
		"UNWIND [0] AS x WITH x WHERE x = 0 OR CASE WHEN 1/x = 1 THEN true END RETURN count(*) AS c":  int64(1),
		"UNWIND [0] AS x WITH x WHERE true OR 1/x = 1 RETURN count(*) AS c":                           int64(1),
		"UNWIND [0] AS x WITH x WHERE x = 1 OR CASE WHEN 1/x = 1 THEN true END RETURN count(*) AS c":  nil,
	} {
		// Three runs: the first evaluation and the compiled ones agree.
		for run := 0; run < 3; run++ {
			result, err := exec.Execute(ctx, query, map[string]interface{}{"run": int64(run)})
			if want == nil {
				if err == nil {
					t.Fatalf("%s: no error, rows %v", query, result.Rows)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if len(result.Rows) != 1 || result.Rows[0][0] != want {
				t.Fatalf("%s: rows %v, want %v", query, result.Rows, want)
			}
		}
	}
}

// A list predicate, comprehension or reduce over a compiled program
// allocates per list, not per element: its parts that don't read the
// element are evaluated once per list, and its element parts allocate
// nothing (#1214).
func TestRowValueProgramListAllocationsDoNotGrowWithList(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	edge, err := store.GetEdge("r1")
	if err != nil {
		t.Fatal(err)
	}
	// AllocsPerRun counts process-wide allocations. Finish any collection
	// left by earlier tests, then keep GC work and pool eviction outside this
	// bounded measurement. Restore the caller's GC setting even on failure.
	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)
	runtime.GC()
	keys := func(n int) []interface{} {
		list := make([]interface{}, n)
		for i := range list {
			list[i] = "nomatch-" + strings.Repeat("x", i%7)
		}
		return list
	}
	for expr, perElement := range map[string]float64{
		"any(k IN $keys WHERE k = startNode(r).id + '|' + endNode(r).id + '|' + r.kind)":  0,
		"none(k IN $keys WHERE k = startNode(r).id)":                                      0,
		"all(k IN $keys WHERE k <> r.kind)":                                               0,
		"single(k IN $keys WHERE k = r.kind)":                                             0,
		"size([k IN $keys WHERE k = r.kind])":                                             0,
		"size([k IN $keys WHERE k = r.kind | k + '|'])":                                   0,
		"reduce(found = false, k IN $keys | found OR k = startNode(r).id + '|' + r.kind)": 0,
		"allReduce(found = false, k IN $keys | k = endNode(r).id, NOT found)":             0,
		"startNode(r).id + '|' + r.kind IN $keys":                                         0,
		"all(k IN $keys WHERE k IS NOT NULL OR EXISTS { MATCH (n) })":                     0,
		"all(k IN $keys WHERE k IS NOT NULL OR COUNT { MATCH (n) } > 0)":                  0,
	} {
		root, _ := (&rowValueCompiler{}).compile(expr)
		shortRow := map[string]interface{}{"r": edge, "$keys": keys(20)}
		longRow := map[string]interface{}{"r": edge, "$keys": keys(200)}
		evaluate := func(row map[string]interface{}) {
			if _, ok, err := root.eval(exec, row, nil, nil); !ok || err != nil {
				t.Fatalf("%s: unresolved or %v", expr, err)
			}
		}
		// Warm both inputs before either sample, including storage caches and
		// the parts a program delegates to the text evaluator.
		for range 10 {
			evaluate(shortRow)
			evaluate(longRow)
		}
		short := testing.AllocsPerRun(100, func() { evaluate(shortRow) })
		long := testing.AllocsPerRun(100, func() { evaluate(longRow) })
		t.Logf("%s: 20 elements = %g allocations, 200 = %g", expr, short, long)
		if strings.Contains(expr, " OR EXISTS {") || strings.Contains(expr, " OR COUNT {") {
			if short > 3 || long > 3 {
				t.Errorf("%s: skipped subquery allocated %g/%g times, want at most 3 at each list length", expr, short, long)
			}
		}
		if growth := (long - short) / 180; growth > perElement {
			t.Errorf("%s: %.0f allocations for 20 elements, %.0f for 200: %.2f per element, want at most %.0f", expr, short, long, growth, perElement)
		}
	}
}

// The context evaluator's entries that read an expression's shape once per
// text (rowContextShape) give what they gave per evaluation: a list
// predicate whose predicate needs the statement context, a negative
// substring position, a pattern comprehension.
func TestRowContextShapeForms(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	node, err := store.GetNode("a")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for run := 0; run < 2; run++ {
		if value := exec.evaluateExpressionWithContext(ctx, "any(k IN [1] WHERE datetime() IS NOT NULL)", map[string]*storage.Node{"n": node}, nil); value != true {
			t.Fatalf("run %d: any() with datetime() = %v", run, value)
		}
		failure := withExpressionFailureSlot(ctx)
		if _, ok := exec.evaluateRowExpressionWithContext(failure, "substring(n.id, -1)", pipelineRow{"n": node}); ok || getExpressionFailure(failure) == nil {
			t.Fatalf("run %d: substring(n.id, -1) resolved without an error", run)
		}
		value, ok := exec.evaluateRowExpressionWithContext(ctx, "[(n)-[:R]->(m) | m.id]", pipelineRow{"n": node})
		if !ok || !reflect.DeepEqual(value, []interface{}{"b"}) {
			t.Fatalf("run %d: pattern comprehension = %v, %v", run, value, ok)
		}
	}
}
