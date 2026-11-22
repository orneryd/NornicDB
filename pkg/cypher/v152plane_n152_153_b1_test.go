package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-86 / D19: reproduction and classification of the reported
// imported-variable rebinding difference. This file asserts only behavior the
// current source already documents and other tests already pin; it does not
// require, prefer, or implement a Neo4j-parity correction.
//
// Reported reproduction:
//
//	WITH 3 AS x CALL (*) { WITH 4 AS x RETURN x AS a } RETURN a
//
// NornicDB intentionally returns 4; Neo4j rejects the same statement because
// the body declares an imported variable again. The extension is documented in
// docs/neo4j-migration/cypher-compatibility.md (scoped CALL bodies), owned by
// subqueryScope.outer in match_semantic_validation.go, pinned by
// TestScopedCallBodyRedeclaresImportedName, and recorded as an intended #907
// difference by the Cypher differential sweep and its known_mismatches entries.
// The policy decision (keep the documented extension or adopt Neo4j parity) is
// still open on NOR-86; the assertions below are the current product contract,
// not a claim that the difference is corrected.
//
// The expression-subquery fence is separate and must keep rejecting:
//
//	WITH 7 AS x RETURN EXISTS { WITH 1 AS x RETURN x } AS v
//
// The same declaration is rejected by COUNT and COLLECT bodies too. A CALL
// body nested inside one of those expressions keeps its own import-list scope,
// so the CALL rebinding extension still applies there.
//
// The reported downstream fragment
//
//	CALL (x, eff_view) { ... WITH s, head(collect(eff_view)) AS eff_view ... }
//
// is not a runnable statement. The aggregate cases below are labelled
// reconstructions of that shape; the same clause exists in the generated
// corpus statement at pkg/cypher/testdata/slow_query_large_statement.cypher
// ("WITH each.shared AS shared, head(collect(eff_view)) AS eff_view"), which is
// only read for slow-query log emission and is not replayed here.

type nor86RowCase struct {
	name    string
	query   string
	columns []string
	rows    [][]interface{}
}

// newNOR86Executor returns a fresh fixture per case: a namespaced in-memory
// engine, so no case can read another's data or metadata.
func newNOR86Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	namespace := "nor86_" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
}

func runNOR86RowCases(t *testing.T, cases []nor86RowCase) {
	t.Helper()
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			result, err := newNOR86Executor(t).Execute(context.Background(), testCase.query, nil)
			require.NoError(t, err, testCase.query)
			require.Equal(t, testCase.columns, result.Columns, testCase.query)
			require.Equal(t, testCase.rows, result.Rows, testCase.query)
		})
	}
}

func requireNOR86Rejected(t *testing.T, query, message string) {
	t.Helper()
	_, err := newNOR86Executor(t).Execute(context.Background(), query, nil)
	require.Error(t, err, query)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	require.Contains(t, err.Error(), message, query)
}

// TestNOR86ReportedRebindingReproduction pins the exact reported statement and
// the value it returns on both parsers through public Execute.
func TestNOR86ReportedRebindingReproduction(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		runNOR86RowCases(t, []nor86RowCase{
			{
				// The reported reproduction. Neo4j rejects imported-variable
				// shadowing; NornicDB keeps the documented #907 extension.
				name:    "reported call star rebinding returns four",
				query:   "WITH 3 AS x CALL (*) { WITH 4 AS x RETURN x AS a } RETURN a",
				columns: []string{"a"},
				rows:    [][]interface{}{{int64(4)}},
			},
			{
				// The rebinding is local to the body: the enclosing query
				// keeps its own x.
				name:    "reported call star keeps outer x",
				query:   "WITH 3 AS x CALL (*) { WITH 4 AS x RETURN x AS a } RETURN a, x",
				columns: []string{"a", "x"},
				rows:    [][]interface{}{{int64(4), int64(3)}},
			},
		})
	})
}

// TestNOR86RebindingPopulation covers the documented population for the same
// contract: explicit and wildcard import lists, the legacy unscoped form, a
// second WITH in the body, UNWIND, derived and aggregated values, an import
// list with more than one name, and UNION branches.
func TestNOR86RebindingPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		runNOR86RowCases(t, []nor86RowCase{
			{
				name:    "explicit import list",
				query:   "WITH 3 AS x CALL (x) { WITH 4 AS x RETURN x AS a } RETURN a",
				columns: []string{"a"},
				rows:    [][]interface{}{{int64(4)}},
			},
			{
				// Legacy CALL: no import list, outer names are implicitly
				// readable. NornicDB accepts the body's redeclaration here;
				// this task recorded acceptance only and executed no Neo4j
				// reference for the legacy body-internal rebinding.
				name:    "legacy unscoped call",
				query:   "WITH 3 AS x CALL { WITH 4 AS x RETURN x AS a } RETURN a",
				columns: []string{"a"},
				rows:    [][]interface{}{{int64(4)}},
			},
			{
				name:    "unwind rebinding",
				query:   "WITH 3 AS x CALL (*) { UNWIND [4] AS x RETURN x AS a } RETURN a",
				columns: []string{"a"},
				rows:    [][]interface{}{{int64(4)}},
			},
			{
				name:    "derived value rebinding",
				query:   "WITH 3 AS x CALL (x) { WITH x + 1 AS x RETURN x AS a } RETURN a",
				columns: []string{"a"},
				rows:    [][]interface{}{{int64(4)}},
			},
			{
				name:    "aggregated value rebinding",
				query:   "WITH 3 AS x CALL (x) { WITH sum(x) AS x RETURN x AS a } RETURN a",
				columns: []string{"a"},
				rows:    [][]interface{}{{int64(3)}},
			},
			{
				// Only the named import is rebound; the other import and the
				// outer binding keep their values.
				name:    "multiple imports rebind one name",
				query:   "WITH 3 AS x, 5 AS y CALL (x, y) { WITH x AS y RETURN y AS a } RETURN a, x, y",
				columns: []string{"a", "x", "y"},
				rows:    [][]interface{}{{int64(3), int64(3), int64(5)}},
			},
			{
				name:    "rebinding in a later body with",
				query:   "WITH 3 AS x CALL (*) { WITH 4 AS x WITH x AS y RETURN y AS a } RETURN a, x",
				columns: []string{"a", "x"},
				rows:    [][]interface{}{{int64(4), int64(3)}},
			},
			{
				// Both UNION branches rebind the same import; the outer value
				// is still 3 in every row.
				name:    "union branches rebind",
				query:   "WITH 3 AS x CALL (x) { WITH 4 AS x RETURN x AS a UNION WITH 5 AS x RETURN x AS a } RETURN a, x ORDER BY a",
				columns: []string{"a", "x"},
				rows:    [][]interface{}{{int64(4), int64(3)}, {int64(5), int64(3)}},
			},
		})
	})
}

// TestNOR86ExpressionSubqueryShadowingFence pins the negative control and its
// sibling expression bodies. These bodies may not declare an enclosing name
// again; the canonical owner is subqueryScope.redeclarationError. A CALL body
// nested in one of them keeps the CALL import-list rule instead.
func TestNOR86ExpressionSubqueryShadowingFence(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const message = "already declared in outer scope"
		for _, query := range []string{
			"WITH 7 AS x RETURN EXISTS { WITH 1 AS x RETURN x } AS v",
			"WITH 7 AS x RETURN COUNT { WITH 1 AS x RETURN x } AS v",
			"WITH 7 AS x RETURN COLLECT { WITH 1 AS x RETURN x } AS v",
			"WITH 7 AS x RETURN EXISTS { UNWIND [1] AS x RETURN x } AS v",
		} {
			t.Run(fmt.Sprintf("rejects %q", query), func(t *testing.T) {
				requireNOR86Rejected(t, query, message)
			})
		}
		runNOR86RowCases(t, []nor86RowCase{
			{
				// Passing the same value through keeps the binding; only a
				// new value under the enclosing name is shadowing.
				name:    "same value pass through is accepted",
				query:   "WITH 7 AS x RETURN COLLECT { WITH x AS x RETURN x } AS v",
				columns: []string{"v"},
				rows:    [][]interface{}{{[]interface{}{int64(7)}}},
			},
			{
				// A nested CALL body has its own import-list scope, so the
				// CALL rebinding extension still applies inside an expression
				// subquery.
				name:    "nested call in exists keeps call rule",
				query:   "WITH 7 AS x RETURN EXISTS { CALL (x) { WITH 4 AS x RETURN x AS a } RETURN a } AS v",
				columns: []string{"v"},
				rows:    [][]interface{}{{true}},
			},
			{
				name:    "nested call in count keeps call rule",
				query:   "WITH 7 AS x RETURN COUNT { CALL (x) { WITH 4 AS x RETURN x AS a } RETURN a } AS v",
				columns: []string{"v"},
				rows:    [][]interface{}{{int64(1)}},
			},
			{
				name:    "nested call in collect keeps call rule",
				query:   "WITH 7 AS x RETURN COLLECT { CALL (x) { WITH 4 AS x RETURN x AS a } RETURN a } AS v",
				columns: []string{"v"},
				rows:    [][]interface{}{{[]interface{}{int64(4)}}},
			},
		})
	})
}

// TestNOR86DownstreamAggregateReconstruction reconstructs the reported
// downstream shape. Every statement here is a reconstruction; the original
// fragment is not runnable and its full statement was not replayed.
func TestNOR86DownstreamAggregateReconstruction(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		runNOR86RowCases(t, []nor86RowCase{
			{
				name:    "explicit imports aggregate over imported view",
				query:   "WITH 3 AS x, [1, 2] AS eff_view CALL (x, eff_view) { WITH x AS s, head(collect(eff_view)) AS eff_view RETURN s, eff_view AS picked } RETURN s, picked",
				columns: []string{"s", "picked"},
				rows:    [][]interface{}{{int64(3), []interface{}{int64(1), int64(2)}}},
			},
			{
				name:    "wildcard imports aggregate over imported view",
				query:   "WITH 3 AS x, [1, 2] AS eff_view CALL (*) { WITH x AS s, head(collect(eff_view)) AS eff_view RETURN s, eff_view AS picked } RETURN s, picked",
				columns: []string{"s", "picked"},
				rows:    [][]interface{}{{int64(3), []interface{}{int64(1), int64(2)}}},
			},
			{
				// Legacy CALL's first WITH is the importing WITH; a later
				// WITH may aggregate and redeclare the imported view.
				name:    "legacy call aggregate after importing with",
				query:   "WITH 3 AS x, [1, 2] AS eff_view CALL { WITH x, eff_view WITH x AS s, head(collect(eff_view)) AS eff_view RETURN s, eff_view AS picked } RETURN s, picked",
				columns: []string{"s", "picked"},
				rows:    [][]interface{}{{int64(3), []interface{}{int64(1), int64(2)}}},
			},
			{
				// The rebound name is the aggregate's output: collecting the
				// imported list yields one list, not the two elements.
				name:    "aggregate output is the rebound value",
				query:   "WITH 3 AS x, [1, 2] AS eff_view CALL (x, eff_view) { WITH x AS s, collect(eff_view) AS eff_view RETURN s, size(eff_view) AS n } RETURN s, n",
				columns: []string{"s", "n"},
				rows:    [][]interface{}{{int64(3), int64(1)}},
			},
		})
		t.Run("export under imported name is rejected", func(t *testing.T) {
			// Boundary, not a rebinding acceptance: a CALL column that
			// redeclares an outer name with a different value is the
			// returned-column rule shared with Neo4j, not the #907
			// body-local rebinding extension.
			requireNOR86Rejected(t,
				"WITH 3 AS x, [1, 2] AS eff_view CALL (x, eff_view) { WITH x AS s, head(collect(eff_view)) AS eff_view RETURN s, eff_view } RETURN s, eff_view",
				"already declared in outer scope")
		})
	})
}
