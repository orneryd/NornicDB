package cypher

// call_import_foreach_transactions_test.go — regression tests for the two
// statement shapes that lost a correlated CALL's imports (B22/report28): a
// FOREACH in the body of a variable-scope CALL (CALL (n) / CALL (*)), and a
// WITH * body of CALL … IN TRANSACTIONS. Both run through the public Execute
// path on both parsers, and each case checks the returned rows and the state
// the statement stored. Error cases keep the imports a body may not read.

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newCallImportExecutor is the empty database one case runs on.
func newCallImportExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_import_foreach"))
}

// runCallImportCase executes setup, then query, then check (against the same
// store) on an empty database of its own.
func runCallImportCase(t *testing.T, setup []string, query string, check func(t *testing.T, exec *StorageExecutor, result *ExecuteResult)) {
	t.Helper()
	exec := newCallImportExecutor(t)
	for _, statement := range setup {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	check(t, exec, result)
}

// requireCallImportError requires query to be rejected as an undefined
// variable named variable, before it stores anything.
func requireCallImportError(t *testing.T, setup []string, query, variable, stored string, wantStored [][]interface{}) {
	t.Helper()
	exec := newCallImportExecutor(t)
	for _, statement := range setup {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
	_, err := exec.Execute(context.Background(), query, nil)
	require.Error(t, err, query)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	require.Contains(t, err.Error(), "`"+variable+"` not defined", query)
	require.Equal(t, wantStored, storedRows(t, exec, stored), query)
}

// storedRows reads the values a statement stored.
func storedRows(t *testing.T, exec *StorageExecutor, query string) [][]interface{} {
	t.Helper()
	stored, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	return stored.Rows
}

// A FOREACH in a scoped CALL body sets the variable its scope clause imports,
// through the whole body: directly, after an inner WITH, and for CALL (*). The
// returned row and the stored property both carry the last iteration's value.
func TestCallImportForeachKeepsScopedImport(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		cases := []struct {
			name  string
			setup []string
			query string
		}{
			{
				name:  "scope-clause-import",
				setup: []string{"CREATE (:ZZForeach {v:0})"},
				query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v",
			},
			{
				name:  "wildcard-scope-import",
				setup: []string{"CREATE (:ZZForeach {v:0})"},
				query: "MATCH (n:ZZForeach) CALL (*) { FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v",
			},
			{
				name:  "import-survives-inner-with",
				setup: []string{"CREATE (:ZZForeach {v:0})"},
				query: "MATCH (n:ZZForeach) CALL (n) { WITH 42 AS marker FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v",
			},
			{
				name:  "importing-with-body",
				setup: []string{"CREATE (:ZZForeach {v:0})"},
				query: "MATCH (n:ZZForeach) CALL { WITH n FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v",
			},
			{
				// The statement the report ran first: the CALL's import comes
				// from a WITH that carries a created variable.
				name:  "created-then-carried-import",
				setup: nil,
				query: "CREATE (n:ZZForeach {v:0}) WITH n CALL (n) { FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				runCallImportCase(t, tc.setup, tc.query, func(t *testing.T, exec *StorageExecutor, result *ExecuteResult) {
					require.Equal(t, []string{"v"}, result.Columns, tc.query)
					require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, tc.query)
					require.Equal(t, [][]interface{}{{int64(2)}}, storedRows(t, exec, "MATCH (n:ZZForeach) RETURN n.v AS v"), tc.query)
				})
			})
		}
	})
}

// The controls of the FOREACH regression: a body that imports its variable
// still writes it without a FOREACH, a FOREACH body writes what it creates,
// and a FOREACH body reads nothing it was not given — neither an outer
// variable its scope clause did not name nor a name no clause bound.
func TestCallImportForeachControls(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("set-in-body-writes-import", func(t *testing.T) {
			runCallImportCase(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (n) { SET n.v = 2 } RETURN n.v AS v",
				func(t *testing.T, exec *StorageExecutor, result *ExecuteResult) {
					require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
					require.Equal(t, [][]interface{}{{int64(2)}}, storedRows(t, exec, "MATCH (n:ZZForeach) RETURN n.v AS v"))
				})
		})
		t.Run("foreach-body-creates-without-import", func(t *testing.T) {
			runCallImportCase(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | CREATE (:ZZOther {v: i})) } RETURN n.v AS v",
				func(t *testing.T, exec *StorageExecutor, result *ExecuteResult) {
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}},
						storedRows(t, exec, "MATCH (o:ZZOther) RETURN o.v AS v ORDER BY v"))
				})
		})
		t.Run("foreach-body-cannot-write-unimported-variable", func(t *testing.T) {
			requireCallImportError(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | SET m.v = i) } RETURN n.v AS v",
				"m", "MATCH (n:ZZForeach) RETURN n.v AS v", [][]interface{}{{int64(0)}})
		})
		t.Run("outer-bound-variable-not-imported", func(t *testing.T) {
			requireCallImportError(t, []string{"CREATE (:ZZForeach {v:0}), (:ZZOther {v:0})"},
				"MATCH (n:ZZForeach), (m:ZZOther) CALL (n) { FOREACH (i IN [1,2] | SET m.v = i) } RETURN n.v AS v",
				"m", "MATCH (m:ZZOther) RETURN m.v AS v", [][]interface{}{{int64(0)}})
		})
		t.Run("iterator-shadows-import-only-inside-loop", func(t *testing.T) {
			runCallImportCase(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (n) { FOREACH (n IN [1,2] | CREATE (:ZZOther {v:n})) } RETURN n.v AS v",
				func(t *testing.T, exec *StorageExecutor, result *ExecuteResult) {
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, storedRows(t, exec, "MATCH (m:ZZOther) RETURN m.v AS v ORDER BY v"))
				})
		})
		t.Run("nested-loop-retains-call-import", func(t *testing.T) {
			runCallImportCase(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | FOREACH (j IN [1,2] | SET n.v = i * 10 + j)) } RETURN n.v AS v",
				func(t *testing.T, exec *StorageExecutor, result *ExecuteResult) {
					require.Equal(t, [][]interface{}{{int64(22)}}, result.Rows)
					require.Equal(t, [][]interface{}{{int64(22)}}, storedRows(t, exec, "MATCH (n:ZZForeach) RETURN n.v AS v"))
				})
		})
		t.Run("scope-clause-names-a-missing-variable", func(t *testing.T) {
			requireCallImportError(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (m) { RETURN 1 AS v } RETURN v",
				"m", "MATCH (n:ZZForeach) RETURN n.v AS v", [][]interface{}{{int64(0)}})
		})
		t.Run("foreach-iteration-list-names-a-missing-variable", func(t *testing.T) {
			requireCallImportError(t, []string{"CREATE (:ZZForeach {v:0})"},
				"MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN noSuchList | SET n.v = i) } RETURN n.v AS v",
				"noSuchList", "MATCH (n:ZZForeach) RETURN n.v AS v", [][]interface{}{{int64(0)}})
		})
	})
}

// A WITH * body of a batched CALL imports every outer variable, so each batch
// stores the row's own value. WITH x and the unbatched body are the controls
// that already ran.
func TestTransactionalCallWildcardImportKeepsOuterVariable(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		cases := []struct {
			name  string
			query string
		}{
			{
				name:  "wildcard-import-in-transactions",
				query: "UNWIND [1,2] AS x CALL { WITH * CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value",
			},
			{
				name:  "named-import-in-transactions",
				query: "UNWIND [1,2] AS x CALL { WITH x CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value",
			},
			{
				name:  "wildcard-import-unbatched",
				query: "UNWIND [1,2] AS x CALL { WITH * CREATE (:ZZB {v:x}) RETURN x AS value } RETURN value ORDER BY value",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				runCallImportCase(t, nil, tc.query, func(t *testing.T, exec *StorageExecutor, result *ExecuteResult) {
					require.Equal(t, []string{"value"}, result.Columns, tc.query)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows, tc.query)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}},
						storedRows(t, exec, "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v"), tc.query)
				})
			})
		}
	})
}

// The control of the WITH * regression: the wildcard imports only what the
// outer query binds, so a name it does not bind stays undefined and the batch
// stores nothing.
func TestTransactionalCallWildcardImportControl(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		requireCallImportError(t, nil,
			"UNWIND [1,2] AS x CALL { WITH * CREATE (:ZZB {v:nosuchVariable}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value",
			"nosuchVariable", "MATCH (n:ZZB) RETURN n.v AS v", [][]interface{}{})
	})
}
