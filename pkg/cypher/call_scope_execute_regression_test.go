package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// callScopeExecuteCase is one statement of the scoped-CALL scope contract,
// asserted through the public Execute path: either its complete result
// contract (columns and rows, in order) or the Neo4j status it must reject
// with. Expected values come from the scoped-CALL reference contract, not from
// the current implementation: imports remain readable throughout the body,
// survive an inner WITH horizon, and follow their quoted spelling, while a
// body's exported wildcard columns and a UNION's branch columns decide the
// statement's validity.
type callScopeExecuteCase struct {
	name    string
	setup   []string
	query   string
	columns []string
	rows    [][]interface{}
	code    string
}

// runCallScopeExecuteCases runs every case through Execute on both supported
// parsers. Each case gets its own store so a write in one case cannot change
// another case's rows.
func runCallScopeExecuteCases(t *testing.T, cases []callScopeExecuteCase) {
	t.Helper()
	forEachParser(t, func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_scope_execute"))
				for _, statement := range tc.setup {
					_, err := exec.Execute(context.Background(), statement, nil)
					require.NoError(t, err, statement)
				}
				result, err := exec.Execute(context.Background(), tc.query, nil)
				if tc.code != "" {
					require.Error(t, err, tc.query)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, tc.code, code, tc.query)
					return
				}
				require.NoError(t, err, tc.query)
				require.NotNil(t, result, tc.query)
				require.Equal(t, tc.columns, result.Columns, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			})
		}
	})
}

// A CALL body sees only what its own header imports, so a nested subquery must
// import again before it reads an enclosing body's variable. The enclosing
// body's exported wildcard columns are its own variables, not its imports, so
// the outer UNION's two branches of this shape both export one column.
func TestScopedCallExecuteNestedLocality(t *testing.T) {
	runCallScopeExecuteCases(t, []callScopeExecuteCase{
		{
			name:  "nested-body-does-not-see-unimported-variable",
			query: "WITH 1 AS a CALL (a) { CALL () { RETURN a AS v } RETURN v } RETURN v",
			code:  "Neo.ClientError.Statement.SyntaxError",
		},
		{
			name:    "nested-body-imports-enclosing-import",
			query:   "WITH 1 AS a CALL (a) { CALL (a) { RETURN a + 1 AS v } RETURN v } RETURN v",
			columns: []string{"v"},
			rows:    [][]interface{}{{int64(2)}},
		},
		{
			name:    "nested-body-wildcard-union-columns",
			query:   "WITH 1 AS a CALL (a) { CALL () { WITH 2 AS a RETURN a AS b } RETURN * UNION RETURN 3 AS b } RETURN a, b ORDER BY b",
			columns: []string{"a", "b"},
			rows:    [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(3)}},
		},
		{
			name:    "unscoped-nested-body-control",
			query:   "WITH 1 AS a CALL { CALL { RETURN 2 AS a } RETURN a AS b } RETURN a, b",
			columns: []string{"a", "b"},
			rows:    [][]interface{}{{int64(1), int64(2)}},
		},
	})
}

// A scoped import stays readable after the body's own WITH horizon, including
// a DISTINCT/ORDER BY horizon, and through the body's DELETE, SET and CREATE
// clauses. The body's own declarations are what a horizon projects; the import
// is the body's readable outer binding, not an exported column.
func TestScopedCallExecuteRetainedImports(t *testing.T) {
	runCallScopeExecuteCases(t, []callScopeExecuteCase{
		{
			name:    "read-import-after-inner-with",
			query:   "WITH 11 AS x CALL (x) { WITH 2 AS local RETURN x AS saved } RETURN saved",
			columns: []string{"saved"},
			rows:    [][]interface{}{{int64(11)}},
		},
		{
			name:    "read-import-with-inner-local",
			query:   "WITH 11 AS x CALL (x) { WITH 2 AS local RETURN x * local AS saved } RETURN saved",
			columns: []string{"saved"},
			rows:    [][]interface{}{{int64(22)}},
		},
		{
			name:    "read-import-after-distinct-order-by-horizon",
			query:   "WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y ORDER BY x RETURN x * y AS a } RETURN a ORDER BY a",
			columns: []string{"a"},
			rows:    [][]interface{}{{int64(22)}, {int64(33)}},
		},
		{
			name:    "delete-imported-node-after-inner-with",
			setup:   []string{"CREATE (:RetainedDelete)"},
			query:   "MATCH (n:RetainedDelete) WITH n CALL (n) { WITH 1 AS marker DELETE n } RETURN count(*) AS kept",
			columns: []string{"kept"},
			rows:    [][]interface{}{{int64(1)}},
		},
		{
			name:    "delete-imported-variable-in-body-return",
			setup:   []string{"CREATE (:RetainedVictim)"},
			query:   "MATCH (n:RetainedVictim) WITH n CALL (n) { DELETE n RETURN 1 AS kept } RETURN kept",
			columns: []string{"kept"},
			rows:    [][]interface{}{{int64(1)}},
		},
		{
			name:    "set-imported-node-with-inner-local",
			setup:   []string{"CREATE (:RetainedSet {seen:0})"},
			query:   "MATCH (n:RetainedSet) WITH n CALL (n) { WITH 7 AS marker SET n.seen = marker } RETURN n.seen AS seen",
			columns: []string{"seen"},
			rows:    [][]interface{}{{int64(7)}},
		},
	})
}

// A scope clause names variables with the same quoting rules as the rest of
// the statement, so a backtick-quoted import (including an escaped backtick)
// must resolve to the binding of that decoded name everywhere the body uses it.
func TestScopedCallExecuteQuotedImports(t *testing.T) {
	runCallScopeExecuteCases(t, []callScopeExecuteCase{
		{
			name:    "quoted-scope-clause-name",
			query:   "WITH 1 AS `outer name` CALL (`outer name`) { RETURN `outer name` + 1 AS value } RETURN value",
			columns: []string{"value"},
			rows:    [][]interface{}{{int64(2)}},
		},
		{
			name:    "escaped-backtick-scope-clause-name",
			setup:   []string{"CREATE (:QuotedA {n:1})"},
			query:   "MATCH (a:QuotedA {n:1}) WITH a AS `a``b` CALL (`a``b`) { RETURN `a``b`.n + 1 AS value } RETURN value",
			columns: []string{"value"},
			rows:    [][]interface{}{{int64(2)}},
		},
		{
			name:    "quoted-import-in-body-pattern",
			setup:   []string{"CREATE (:QuotedA {n:1})-[:QUOTED]->(:QuotedB)"},
			query:   "MATCH (a:QuotedA {n:1}) WITH a AS `a b` CALL (`a b`) { MATCH (`a b`)-[:QUOTED]->() RETURN count(*) AS c } RETURN c",
			columns: []string{"c"},
			rows:    [][]interface{}{{int64(1)}},
		},
		{
			name:    "quoted-import-of-all-variables",
			query:   "WITH 1 AS `outer name` CALL (*) { RETURN `outer name` + 1 AS value } RETURN value",
			columns: []string{"value"},
			rows:    [][]interface{}{{int64(2)}},
		},
		{
			name:    "quoted-import-after-inner-with",
			query:   "WITH 1 AS `outer name` CALL (`outer name`) { WITH 2 AS local RETURN `outer name` + local AS value } RETURN value",
			columns: []string{"value"},
			rows:    [][]interface{}{{int64(3)}},
		},
		{
			name:    "quoted-import-in-both-union-branches",
			query:   "WITH 1 AS `outer name` CALL (`outer name`) { RETURN `outer name` + 1 AS value UNION RETURN `outer name` + 2 AS value } RETURN value ORDER BY value",
			columns: []string{"value"},
			rows:    [][]interface{}{{int64(2)}, {int64(3)}},
		},
	})
}

// The branch columns of a scoped body's UNION decide its validity. A body's
// RETURN * exports the columns the body itself binds, so a branch that also
// declares v and a branch that only projects v agree, and the statement is
// valid. Truly mismatched branches stay rejected, and the unscoped form keeps
// its existing behavior.
func TestScopedCallExecuteUnionWildcardColumns(t *testing.T) {
	runCallScopeExecuteCases(t, []callScopeExecuteCase{
		{
			name:    "scope-clause-star-first-branch",
			query:   "WITH 1 AS a CALL (a) { WITH 5 AS v RETURN * UNION RETURN 6 AS v } RETURN a, v ORDER BY v",
			columns: []string{"a", "v"},
			rows:    [][]interface{}{{int64(1), int64(5)}, {int64(1), int64(6)}},
		},
		{
			name:    "all-variables-star-first-branch",
			query:   "WITH 1 AS a CALL (*) { WITH 5 AS v RETURN * UNION RETURN 6 AS v } RETURN a, v ORDER BY v",
			columns: []string{"a", "v"},
			rows:    [][]interface{}{{int64(1), int64(5)}, {int64(1), int64(6)}},
		},
		{
			name:    "unscoped-star-first-branch-control",
			query:   "WITH 1 AS a CALL { WITH 5 AS v RETURN * UNION RETURN 6 AS v } RETURN a, v ORDER BY v",
			columns: []string{"a", "v"},
			rows:    [][]interface{}{{int64(1), int64(5)}, {int64(1), int64(6)}},
		},
		{
			name:    "mismatched-branch-columns-stay-rejected",
			query:   "WITH 1 AS a CALL (a) { WITH 5 AS v RETURN v UNION RETURN 6 AS v, 7 AS w } RETURN a, v, w",
			columns: nil,
			rows:    nil,
			code:    "Neo.ClientError.Statement.SyntaxError",
		},
	})
}

// WITH ... ORDER BY may precede a scoped CALL: the horizon sorts the outer
// rows, the scope clause imports the horizon's variable, and the body runs per
// row. DISTINCT, the sort direction and an empty input keep their own
// semantics, and an unimported name inside the body is still rejected.
func TestScopedCallExecuteAfterDistinctOrderBy(t *testing.T) {
	runCallScopeExecuteCases(t, []callScopeExecuteCase{
		{
			name:    "distinct-order-by-before-scope-clause",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (point) { RETURN point + 1 AS next } RETURN point, next ORDER BY point",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}},
		},
		{
			name:    "explicit-ascending-order-by-before-scope-clause",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point ASC CALL (point) { RETURN point + 1 AS next } RETURN point, next ORDER BY point",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}},
		},
		{
			name:    "distinct-order-by-before-all-variable-scope",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (*) { RETURN point + 1 AS next } RETURN point, next ORDER BY point",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}},
		},
		{
			name:    "order-by-without-distinct-before-scope-clause",
			query:   "UNWIND [2,1,2] AS point WITH point ORDER BY point CALL (point) { RETURN point + 1 AS next } RETURN point, next ORDER BY point",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}, {int64(2), int64(3)}},
		},
		{
			name:    "descending-order-by-before-scope-clause",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point DESC CALL (point) { RETURN point + 1 AS next } RETURN point, next",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(2), int64(3)}, {int64(1), int64(2)}},
		},
		{
			name:    "empty-input-before-scope-clause",
			query:   "UNWIND [] AS point WITH DISTINCT point ORDER BY point CALL (point) { RETURN point + 1 AS next } RETURN point, next",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{},
		},
		{
			name:    "unscoped-body-before-trailing-sort",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL { RETURN point + 1 AS next } RETURN point, next ORDER BY point",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}},
		},
		{
			name:    "descending-horizon-with-trailing-descending-sort",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point DESC CALL (point) { RETURN point + 1 AS next } RETURN point, next ORDER BY point DESC",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(2), int64(3)}, {int64(1), int64(2)}},
		},
		{
			name:    "trailing-skip-after-scoped-body",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (point) { RETURN point + 1 AS next } RETURN point, next SKIP 1",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(2), int64(3)}},
		},
		{
			name:    "trailing-limit-after-scoped-body",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (point) { RETURN point + 1 AS next } RETURN point, next LIMIT 1",
			columns: []string{"point", "next"},
			rows:    [][]interface{}{{int64(1), int64(2)}},
		},
		{
			name:    "unimported-body-name-stays-rejected",
			query:   "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (point) { RETURN missing + 1 AS next } RETURN point, next",
			columns: nil,
			rows:    nil,
			code:    "Neo.ClientError.Statement.SyntaxError",
		},
	})
}

// The horizon before a scoped CALL carries its own ORDER BY tail and the outer
// projection after the body carries another; neither boundary changes what the
// body imports or how often it runs. A body that writes runs once per distinct
// outer row, a body rejected for an unimported name writes nothing, and the
// scope clause still imports only the variables it names.
func TestScopedCallExecuteOrderByBoundaryKeepsScope(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := context.Background()
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_scope_execute"))
		result, err := exec.Execute(ctx, "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (point) { CREATE (:OrderByBoundarySeen {point: point}) RETURN point + 1 AS next } RETURN point, next ORDER BY point", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"point", "next"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}}, result.Rows)
		written, err := exec.Execute(ctx, "MATCH (n:OrderByBoundarySeen) RETURN count(n) AS written", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, written.Rows, "the body writes once per distinct outer row")

		_, err = exec.Execute(ctx, "UNWIND [1] AS point WITH DISTINCT point ORDER BY point CALL (point) { CREATE (:OrderByBoundaryLeak) RETURN point + missing AS next } RETURN point, next", nil)
		require.Error(t, err)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		leaked, err := exec.Execute(ctx, "MATCH (n:OrderByBoundaryLeak) RETURN count(n) AS written", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, leaked.Rows, "a rejected body ran nothing")

		// The scope clause still imports only the variables it names, even
		// with its own ORDER BY after the horizon.
		_, err = exec.Execute(ctx, "UNWIND [1] AS point WITH DISTINCT point, 5 AS local ORDER BY point CALL (point) { RETURN local AS v } RETURN point, v", nil)
		require.Error(t, err)
		code, _ = nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
	})
}
