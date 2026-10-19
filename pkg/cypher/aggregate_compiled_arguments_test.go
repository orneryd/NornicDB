package cypher

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aggregateArgumentModes are the evaluation routes the compiled aggregate
// arguments must agree with: map literals compiled and each row's scope
// built once (the default), compiled without the scope, and the row
// evaluator reading every text as before programs.
var aggregateArgumentModes = []struct {
	name           string
	text, unscoped bool
}{
	{name: "compiled-scoped"},
	{name: "compiled-unscoped", unscoped: true},
	{name: "text", text: true, unscoped: true},
}

// renderOrderedValue renders a result value with each map's keys in their
// recorded order (orders), so two routes agree only if they give the same
// values and the same key orders.
func renderOrderedValue(value interface{}, orders map[uintptr][]string) string {
	switch typed := value.(type) {
	case map[string]interface{}:
		keys, ordered := orders[uintptr(reflect.ValueOf(typed).UnsafePointer())]
		if !ordered {
			keys = make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
		}
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+":"+renderOrderedValue(typed[key], orders))
		}
		return fmt.Sprintf("{%v|%s}", ordered, strings.Join(parts, ","))
	case []interface{}:
		parts := make([]string, len(typed))
		for i, item := range typed {
			parts[i] = renderOrderedValue(item, orders)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *storage.Node:
		return "node:" + string(typed.ID)
	}
	return fmt.Sprintf("%T:%v", value, value)
}

func runAggregateArgumentQuery(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}) string {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	if err != nil {
		return "error: " + err.Error()
	}
	rows := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		cells := make([]string, len(row))
		for j, cell := range row {
			cells[j] = renderOrderedValue(cell, result.MapKeyOrders)
		}
		rows[i] = strings.Join(cells, " | ")
	}
	return strings.Join(result.Columns, ",") + "\n" + strings.Join(rows, "\n")
}

// TestAggregateCompiledArgumentsMatchText: aggregate arguments, grouping
// keys and map literals give the same rows, map key orders and errors
// whether map literals are compiled and the row's scope built once, or
// every text is read per row as before, on both parsers.
func TestAggregateCompiledArgumentsMatchText(t *testing.T) {
	params := map[string]interface{}{"p": int64(7), "q": 0.5, "m": map[string]interface{}{"a": "x"}}
	queries := []string{
		"UNWIND range(1, 6) AS i WITH i % 2 AS k, i RETURN k, collect({i: i, sq: i * i, p: $p, k: k}) AS xs ORDER BY k",
		"UNWIND range(1, 4) AS i RETURN collect({a: i, b: 1, a: i + 1}) AS xs",
		"UNWIND range(1, 4) AS i RETURN collect({}) AS xs, count(*) AS n",
		"UNWIND range(1, 4) AS i RETURN collect({m: {x: i, y: [i, $p]}, l: [{z: i}]}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({c: COUNT { MATCH (n:P) RETURN n }, i: i}) AS xs",
		"MATCH (n:P) WITH n ORDER BY n.id RETURN collect(n {.id, extra: $p}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({`a b`: i, 'q': 1, \"r\": $m.a}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({a: i / (i - 2)}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({a: i.x}) AS xs",
		"UNWIND range(1, 8) AS i RETURN percentileCont(i, $q) AS c, percentileDisc(i, 0.25) AS d",
		"UNWIND range(1, 6) AS i RETURN {k: i % 2, p: $p} AS key, count(*) AS n, collect(i) AS is ORDER BY key.k",
		"UNWIND range(1, 3) AS i RETURN collect([x IN range(1, i) | {x: x, p: $p}]) AS xs",
		"UNWIND range(1, 4) AS i RETURN collect(CASE WHEN i > 2 THEN {big: i} ELSE {small: i, p: $p} END) AS xs",
		"UNWIND range(1, 6) AS i RETURN collect(DISTINCT {k: i % 2}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({t: date('2020-01-01'), i: i}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({v: $m.a, w: $missing}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({a: null, b: i, c: null}) AS xs, collect({a: null}) AS ns",
		"UNWIND range(1, 3) AS i RETURN collect($m) AS ms, collect({m: $m, i: i}) AS wrapped",
		"UNWIND range(1, 3) AS i RETURN collect({a: i, A: -i, `a`: i * 10}) AS xs",
		"UNWIND range(1, 3) AS i WITH i, {a: i} AS m RETURN collect(m[null]) AS xs, collect({k: m['a']}) AS ks",
		"UNWIND range(1, 3) AS i RETURN collect({i: i, n: [(a:P)-[:R]->(b) | b.id]}) AS xs",
		"UNWIND range(1, 3) AS i RETURN collect({i: i, e: EXISTS { MATCH (:P)-[:R]->() }}) AS xs",
		"UNWIND range(1, 3) AS i WITH i, {i: i} AS m RETURN collect(m {.*, j: i * 2}) AS xs",
		"MATCH (n:P) WITH n ORDER BY n.id RETURN collect({id: id(n) IS NOT NULL, label: labels(n)}) AS xs",
	}
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			outputs := make([][]string, len(aggregateArgumentModes))
			for index, mode := range aggregateArgumentModes {
				func() {
					rowValueProgramsDisabled.Store(mode.text)
					rowEvaluationScopesDisabled.Store(mode.unscoped)
					defer rowValueProgramsDisabled.Store(false)
					defer rowEvaluationScopesDisabled.Store(false)
					exec, _ := perRowTestExecutor(t)
					exec.cache = nil
					for _, query := range queries {
						queryParams := map[string]interface{}{}
						for name, value := range params {
							queryParams[name] = value
						}
						outputs[index] = append(outputs[index], runAggregateArgumentQuery(t, exec, query, queryParams))
					}
				}()
			}
			for index, query := range queries {
				for mode := 1; mode < len(aggregateArgumentModes); mode++ {
					assert.Equal(t, outputs[mode][index], outputs[0][index], "%s vs %s: %s", aggregateArgumentModes[mode].name, aggregateArgumentModes[0].name, query)
				}
			}
			// The routes ran the statements: rows, not only errors.
			require.True(t, strings.Contains(outputs[0][0], "{true|i:int64:1,sq:int64:1,p:int64:7,k:int64:1}"), outputs[0][0])
		})
	}
}

// TestAggregateCompiledArgumentsKeepEachRowsValues: the reused scope gives
// every row its own values; a map literal is a new map for every row.
func TestAggregateCompiledArgumentsKeepEachRowsValues(t *testing.T) {
	exec, _ := perRowTestExecutor(t)
	exec.cache = nil
	result, err := exec.Execute(context.Background(),
		"UNWIND range(1, 5) AS i RETURN collect({i: i, p: $p}) AS xs, collect([i, $p]) AS ls", map[string]interface{}{"p": "p"})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	maps := result.Rows[0][0].([]interface{})
	require.Len(t, maps, 5)
	seen := map[uintptr]bool{}
	for index, value := range maps {
		entry := value.(map[string]interface{})
		require.Equal(t, map[string]interface{}{"i": int64(index + 1), "p": "p"}, entry)
		pointer := uintptr(reflect.ValueOf(entry).UnsafePointer())
		require.False(t, seen[pointer], "row %d shares a map", index)
		seen[pointer] = true
		require.Equal(t, []string{"i", "p"}, result.MapKeyOrders[pointer])
	}
	require.Equal(t, []interface{}{
		[]interface{}{int64(1), "p"}, []interface{}{int64(2), "p"}, []interface{}{int64(3), "p"}, []interface{}{int64(4), "p"}, []interface{}{int64(5), "p"},
	}, result.Rows[0][1])
}

// TestAggregateScopeNeverAliasedPastTheRow: nothing an aggregation keeps
// past its row shares the reused per-row scope: collected map literals,
// map variables and projections, values written by CREATE and SET, and
// rows handed to CALL and COLLECT subqueries each keep their own row's
// value over 1,000 rows, on both parsers.
func TestAggregateScopeNeverAliasedPastTheRow(t *testing.T) {
	const rows = 1000
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec, _ := perRowTestExecutor(t)
			exec.cache = nil
			ctx := context.Background()
			params := map[string]interface{}{"p": "p", "n": int64(rows)}
			distinctMaps := func(t *testing.T, values []interface{}, key string) {
				t.Helper()
				require.Len(t, values, rows)
				seen := map[uintptr]int{}
				for index, value := range values {
					entry, ok := value.(map[string]interface{})
					require.True(t, ok, "element %d is %T", index, value)
					require.Equal(t, int64(index+1), entry[key], "element %d", index)
					require.Equal(t, "p", entry["p"], "element %d", index)
					pointer := uintptr(reflect.ValueOf(entry).UnsafePointer())
					if other, shared := seen[pointer]; shared {
						t.Fatalf("elements %d and %d are one map", other, index)
					}
					seen[pointer] = index
				}
			}
			result, err := exec.Execute(ctx, "UNWIND range(1, $n) AS x WITH x, {k: x, p: $p} AS row "+
				"RETURN collect({k: x, p: $p}) AS literals, collect(row) AS rows, collect(row {.*, j: x}) AS projected, "+
				"collect(COLLECT { RETURN {k: x, p: $p} AS m }) AS subqueries", params)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			for column, key := range map[int]string{0: "k", 1: "k", 2: "j"} {
				distinctMaps(t, result.Rows[0][column].([]interface{}), key)
			}
			nested := result.Rows[0][3].([]interface{})
			require.Len(t, nested, rows)
			flat := make([]interface{}, rows)
			for index, value := range nested {
				list := value.([]interface{})
				require.Len(t, list, 1, "element %d", index)
				flat[index] = list[0]
			}
			distinctMaps(t, flat, "k")

			result, err = exec.Execute(ctx, "UNWIND range(1, $n) AS x CALL (x) { RETURN {k: x, p: $p} AS m } "+
				"RETURN collect(m) AS called, collect({k: x, p: $p, m: m}) AS wrapped", params)
			require.NoError(t, err)
			distinctMaps(t, result.Rows[0][0].([]interface{}), "k")
			distinctMaps(t, result.Rows[0][1].([]interface{}), "k")

			// One aggregation per row, each releasing its scope's maps for the
			// next: every inner list keeps its own row's maps.
			result, err = exec.Execute(ctx, "UNWIND range(1, $n) AS x CALL (x) { UNWIND [x, x + 1] AS y "+
				"RETURN collect({k: y, p: $p}) AS ys, collect(y + 1) AS zs } RETURN collect(ys) AS inner, collect(zs) AS sums", params)
			require.NoError(t, err)
			inner := result.Rows[0][0].([]interface{})
			require.Len(t, inner, rows)
			firsts := make([]interface{}, rows)
			for index, value := range inner {
				list := value.([]interface{})
				require.Len(t, list, 2, "element %d", index)
				second := list[1].(map[string]interface{})
				require.Equal(t, int64(index+2), second["k"], "element %d", index)
				require.False(t, reflect.ValueOf(list[0]).UnsafePointer() == reflect.ValueOf(second).UnsafePointer(), "element %d", index)
				firsts[index] = list[0]
			}
			distinctMaps(t, firsts, "k")
			for index, value := range result.Rows[0][1].([]interface{}) {
				require.Equal(t, []interface{}{int64(index + 2), int64(index + 3)}, value, "element %d", index)
			}

			_, err = exec.Execute(ctx, "UNWIND range(1, $n) AS x CREATE (t:ScopeAlias {k: x, p: $p}) SET t.m = x * 2", params)
			require.NoError(t, err)
			result, err = exec.Execute(ctx, "MATCH (t:ScopeAlias) WITH t ORDER BY t.k "+
				"RETURN collect(t {.k, .p, .m}) AS stored, collect(t.m) AS doubled", params)
			require.NoError(t, err)
			distinctMaps(t, result.Rows[0][0].([]interface{}), "k")
			for index, value := range result.Rows[0][1].([]interface{}) {
				require.Equal(t, int64(2*(index+1)), value, "element %d", index)
			}
		})
	}
}
