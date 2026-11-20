package cypher

// H2 variant of 46467ce23 "Classify grouped numeric literals consistently
// with execution" (units of statement_execution_contract.go and the callers
// of canonicalizeNumericLiterals, numeric_literal_rewrite.go). The fix's own
// assertion (numeric_literal_effect_contract_test.go: a statement whose only
// difference is the digit-grouping spelling keeps its write effect and
// result) applied to the sibling statement shapes: grouped literals in
// LIMIT/SKIP, range(), FOREACH, CALL subqueries, MERGE ON CREATE, PROFILE,
// explicit transactions, negative literals; and the text the rewrite must
// not touch (strings, backticked names, variables, parameters, comments).
// Oracle: the grouped spelling returns the same rows and leaves the same
// stored graph as the plain spelling, each on a fresh store; for the
// untouched-text cases, the stated expected rows.

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func h2GraphState(exec *StorageExecutor, ctx context.Context) ([]string, error) {
	rows, err := h2Exec(exec, ctx, "MATCH (n) RETURN labels(n) AS l, properties(n) AS p", nil)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, fmt.Sprint(row))
	}
	sort.Strings(out)
	return out, nil
}

type h2LiteralRun struct {
	columns []string
	rows    [][]interface{}
	state   []string
	err     string
}

func h2RunLiteral(t *testing.T, setup []string, statements []string, params map[string]interface{}) h2LiteralRun {
	t.Helper()
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "h2literal"))
	for _, statement := range setup {
		if _, err := exec.Execute(ctx, statement, nil); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	var run h2LiteralRun
	for _, statement := range statements {
		result, err := exec.Execute(ctx, statement, params)
		if err != nil {
			run.err = err.Error()
			break
		}
		run.columns, run.rows = result.Columns, result.Rows
	}
	state, err := h2GraphState(exec, ctx)
	if err != nil {
		run.err += " / state: " + err.Error()
	}
	run.state = state
	return run
}

func TestHuntH2GroupedNumericLiteralSiblings(t *testing.T) {
	cases, leads := 0, 0
	setup := []string{"UNWIND range(1, 30) AS i CREATE (:Audit {i: i})"}
	pairs := []struct {
		name    string
		setup   []string
		grouped []string
		plain   []string
	}{
		{name: "create-return", grouped: []string{"CREATE (a:Audit {amount: 1_000}) RETURN a.amount AS v"}, plain: []string{"CREATE (a:Audit {amount: 1000}) RETURN a.amount AS v"}},
		{name: "negative-literal", grouped: []string{"CREATE (a:Audit {amount: -1_000}) RETURN a.amount AS v"}, plain: []string{"CREATE (a:Audit {amount: -1000}) RETURN a.amount AS v"}},
		{name: "fraction-and-exponent", grouped: []string{"CREATE (a:Audit {x: 1_000.5, y: 1.0_5, z: 1e1_0}) RETURN a.x, a.y, a.z"}, plain: []string{"CREATE (a:Audit {x: 1000.5, y: 1.05, z: 1e10}) RETURN a.x, a.y, a.z"}},
		{name: "hex-octal", grouped: []string{"CREATE (a:Audit {x: 0x1_F, y: 0o1_7}) RETURN a.x, a.y"}, plain: []string{"CREATE (a:Audit {x: 0x1F, y: 0o17}) RETURN a.x, a.y"}},
		{name: "unwind-range-create", grouped: []string{"UNWIND range(1_0, 1_2) AS x CREATE (:Audit {v: x})"}, plain: []string{"UNWIND range(10, 12) AS x CREATE (:Audit {v: x})"}},
		{name: "limit-set", setup: setup, grouped: []string{"MATCH (n:Audit) WITH n ORDER BY n.i LIMIT 1_0 SET n.k = 1"}, plain: []string{"MATCH (n:Audit) WITH n ORDER BY n.i LIMIT 10 SET n.k = 1"}},
		{name: "skip-delete", setup: setup, grouped: []string{"MATCH (n:Audit) WITH n ORDER BY n.i SKIP 2_0 DELETE n"}, plain: []string{"MATCH (n:Audit) WITH n ORDER BY n.i SKIP 20 DELETE n"}},
		{name: "where-delete", setup: setup, grouped: []string{"MATCH (n:Audit) WHERE n.i > 2_5 DELETE n"}, plain: []string{"MATCH (n:Audit) WHERE n.i > 25 DELETE n"}},
		{name: "where-set-label", setup: setup, grouped: []string{"MATCH (n:Audit) WHERE n.i = 1_5 SET n:Marked"}, plain: []string{"MATCH (n:Audit) WHERE n.i = 15 SET n:Marked"}},
		{name: "where-remove-property", setup: setup, grouped: []string{"MATCH (n:Audit) WHERE n.i < 1_0 REMOVE n.i"}, plain: []string{"MATCH (n:Audit) WHERE n.i < 10 REMOVE n.i"}},
		{name: "foreach-create", grouped: []string{"FOREACH (x IN [1_0, 2_0] | CREATE (:Audit {v: x}))"}, plain: []string{"FOREACH (x IN [10, 20] | CREATE (:Audit {v: x}))"}},
		{name: "call-subquery-create", grouped: []string{"CALL { CREATE (:Audit {v: 1_0}) } RETURN 1 AS done"}, plain: []string{"CALL { CREATE (:Audit {v: 10}) } RETURN 1 AS done"}},
		{name: "call-in-transactions", grouped: []string{"UNWIND [1_0, 2_0] AS x CALL (x) { CREATE (:Audit {v: x}) } IN TRANSACTIONS OF 1_0 ROWS"}, plain: []string{"UNWIND [10, 20] AS x CALL (x) { CREATE (:Audit {v: x}) } IN TRANSACTIONS OF 10 ROWS"}},
		{name: "merge-on-create", grouped: []string{"MERGE (a:Audit {v: 0x1_F}) ON CREATE SET a.c = 1.5e1_0 ON MATCH SET a.c = 0 RETURN a.c"}, plain: []string{"MERGE (a:Audit {v: 0x1F}) ON CREATE SET a.c = 1.5e10 ON MATCH SET a.c = 0 RETURN a.c"}},
		{name: "profile-create", grouped: []string{"PROFILE CREATE (:Audit {v: 1_0})"}, plain: []string{"PROFILE CREATE (:Audit {v: 10})"}},
		{name: "explain-create-writes-nothing", grouped: []string{"EXPLAIN CREATE (:Audit {v: 1_0})"}, plain: []string{"EXPLAIN CREATE (:Audit {v: 10})"}},
		{name: "explicit-transaction", grouped: []string{"BEGIN", "CREATE (:Audit {v: 1_0})", "COMMIT"}, plain: []string{"BEGIN", "CREATE (:Audit {v: 10})", "COMMIT"}},
		{name: "explicit-transaction-rollback", grouped: []string{"BEGIN", "CREATE (:Audit {v: 1_0})", "ROLLBACK"}, plain: []string{"BEGIN", "CREATE (:Audit {v: 10})", "ROLLBACK"}},
		{name: "list-index-and-slice", grouped: []string{"WITH range(0, 2_0) AS xs CREATE (:Audit {a: xs[1_0], b: size(xs[1_0..1_5])})"}, plain: []string{"WITH range(0, 20) AS xs CREATE (:Audit {a: xs[10], b: size(xs[10..15])})"}},
		{name: "case-expression", grouped: []string{"CREATE (:Audit {v: CASE WHEN 1_0 > 9 THEN 1_000 ELSE 0 END})"}, plain: []string{"CREATE (:Audit {v: CASE WHEN 10 > 9 THEN 1000 ELSE 0 END})"}},
		{name: "map-and-list-property", grouped: []string{"CREATE (:Audit {xs: [1_0, 2_0]}) WITH {a: 3_0} AS m CREATE (:Audit {m: m.a})"}, plain: []string{"CREATE (:Audit {xs: [10, 20]}) WITH {a: 30} AS m CREATE (:Audit {m: m.a})"}},
	}
	forEachParser(t, func(t *testing.T) {
		for _, pair := range pairs {
			t.Run(pair.name, func(t *testing.T) {
				cases++
				grouped := h2RunLiteral(t, pair.setup, pair.grouped, nil)
				plain := h2RunLiteral(t, pair.setup, pair.plain, nil)
				if grouped.err != plain.err || !reflect.DeepEqual(grouped.rows, plain.rows) || !reflect.DeepEqual(grouped.state, plain.state) {
					leads++
					h2Lead(map[string]any{"harness": "numeric-literal", "unit": "46467ce23", "case": pair.name, "setup": pair.setup,
						"grouped": pair.grouped, "plain": pair.plain,
						"grouped_rows": fmt.Sprint(grouped.rows), "plain_rows": fmt.Sprint(plain.rows),
						"grouped_error": grouped.err, "plain_error": plain.err,
						"grouped_state_size": len(grouped.state), "plain_state_size": len(plain.state),
						"grouped_state_head": h2Head(grouped.state), "plain_state_head": h2Head(plain.state)})
				}
			})
		}
		// Text the rewrite must leave as written.
		untouched := []struct {
			name      string
			statement string
			params    map[string]interface{}
			columns   []string
			rows      [][]interface{}
		}{
			{name: "string-literals", statement: `RETURN '1_000' AS s, "2_0" AS t`, columns: []string{"s", "t"}, rows: [][]interface{}{{"1_000", "2_0"}}},
			{name: "backticked-column", statement: "RETURN 1 AS `1_0`", columns: []string{"1_0"}, rows: [][]interface{}{{int64(1)}}},
			{name: "variable-with-digits", statement: "WITH 5 AS x1_0 RETURN x1_0", columns: []string{"x1_0"}, rows: [][]interface{}{{int64(5)}}},
			{name: "parameter-with-digits", statement: "RETURN $p1_0 AS v", params: map[string]interface{}{"p1_0": int64(7)}, columns: []string{"v"}, rows: [][]interface{}{{int64(7)}}},
			{name: "column-name-keeps-spelling", statement: "RETURN 1_000", columns: []string{"1_000"}, rows: [][]interface{}{{int64(1000)}}},
			{name: "comment", statement: "RETURN 1_000 AS n // 2_0", columns: []string{"n"}, rows: [][]interface{}{{int64(1000)}}},
			{name: "block-comment", statement: "RETURN /* 3_0 */ 1_000 AS n", columns: []string{"n"}, rows: [][]interface{}{{int64(1000)}}},
			{name: "backticked-property-key", statement: "CREATE (n:Audit {`a1_0`: 1, `2_0`: 2}) RETURN n.`a1_0` AS a, n.`2_0` AS b", columns: []string{"a", "b"}, rows: [][]interface{}{{int64(1), int64(2)}}},
			{name: "string-with-grouped-number-inside-create", statement: "CREATE (n:Audit {s: 'id 1_000'}) RETURN n.s AS s", columns: []string{"s"}, rows: [][]interface{}{{"id 1_000"}}},
			{name: "label-with-digits", statement: "CREATE (n:L1_0) RETURN labels(n) AS l", columns: []string{"l"}, rows: [][]interface{}{{[]interface{}{"L1_0"}}}},
		}
		for _, c := range untouched {
			t.Run("untouched/"+c.name, func(t *testing.T) {
				cases++
				run := h2RunLiteral(t, nil, []string{c.statement}, c.params)
				if run.err != "" || !reflect.DeepEqual(run.columns, c.columns) || fmt.Sprint(run.rows) != fmt.Sprint(c.rows) {
					leads++
					h2Lead(map[string]any{"harness": "numeric-literal", "unit": "46467ce23", "case": "untouched/" + c.name, "statement": c.statement,
						"columns": run.columns, "want_columns": c.columns, "rows": fmt.Sprint(run.rows), "want_rows": fmt.Sprint(c.rows), "error": run.err})
				}
			})
		}
	})
	h2Coverage(map[string]any{"harness": "numeric-literal", "commit": "46467ce23", "cases": cases, "leads": leads})
}

func h2Head(state []string) []string {
	if len(state) > 5 {
		return state[:5]
	}
	return state
}
