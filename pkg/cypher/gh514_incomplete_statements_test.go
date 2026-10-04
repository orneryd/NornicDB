package cypher

// gh514_incomplete_statements_test.go — regression tests for the remaining
// #514 reopen (2026-10-03): a statement that ends inside a clause must be a
// SyntaxError with nothing written, an unevaluable CREATE property-map value
// must be a SyntaxError instead of stored text, and a bare CALL is a syntax
// error, not a procedure lookup. Neo4j rejects all of these.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh514IncompleteExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "gh514incomplete")
	return NewStorageExecutor(store)
}

func TestGh514_StatementsEndingInsideAClauseAreSyntaxErrors(t *testing.T) {
	exec := newGh514IncompleteExecutor(t)
	ctx := context.Background()
	for _, setup := range []string{
		"CREATE (:K {id: 1})-[:R]->(:K {id: 2})",
		"CREATE (:BFn {uid: 'a'})",
		"CREATE (:PDRecord {id: 'x'})",
		"CREATE (:P {id: 1, name: 'a'})",
	} {
		_, err := exec.Execute(ctx, setup, nil)
		require.NoError(t, err)
	}

	for _, statement := range []string{
		"CREATE",
		"MATCH (n:X)",
		"MATCH (n) WITH n",
		"CREATE (n:Z) WITH n",
		"MATCH (m:BFn) OPTIONAL MATCH (n)-[:CALLS]->(m {uid: 'b'})",
		"MATCH (n:PDRecord {id: identifier})",
		"OPTIONAL MATCH",
		"WITH n, count(m) AS c",
		"create.go",
		"CALL",
	} {
		t.Run(statement, func(t *testing.T) {
			_, err := exec.Execute(ctx, statement, nil)
			require.Error(t, err, "statement must be a syntax error: %s", statement)
			require.Contains(t, err.Error(), "SyntaxError", "statement must be classified as a SyntaxError: %s", statement)
		})
	}

	// The rejected statements wrote nothing beyond the setup: no :Z node from
	// CREATE (n:Z) WITH n, no unlabeled node from create.go.
	result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows, "rejected statements must not write")
	result, err = exec.Execute(ctx, "MATCH (n:Z) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "CREATE (n:Z) WITH n must not write")
}

func TestGh514_UnevaluableCreateMapValuesAreSyntaxErrors(t *testing.T) {
	exec := newGh514IncompleteExecutor(t)
	ctx := context.Background()

	for _, statement := range []string{
		"CREATE (n:T {v: undefinedvar})",
		"FOREACH (x IN ['a'] | CREATE (:F {v: y}))",
		"FOREACH (x IN ['a'] | CREATE (a:F {v: y}))",
	} {
		t.Run(statement, func(t *testing.T) {
			_, err := exec.Execute(ctx, statement, nil)
			require.Error(t, err, "statement must be a syntax error: %s", statement)
			require.Contains(t, err.Error(), "SyntaxError", "statement must be classified as a SyntaxError: %s", statement)
		})
	}

	// Nothing may have been written by the rejected statements.
	result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "rejected CREATEs must not write")
}

func TestGh514_ValidCreateMapExpressionsStillStoreValues(t *testing.T) {
	ctx := context.Background()

	checks := []struct {
		statement string
		expected  interface{}
	}{
		{"CREATE (n:T {v: 2 * 3}) RETURN n.v", int64(6)},
		{"CREATE (n:T {v: -(2 * 3)}) RETURN n.v", int64(-6)},
		{"CREATE (n:T {v: [x IN [1, 2] | x * 2]}) RETURN n.v", []interface{}{int64(2), int64(4)}},
		{"CREATE (n:T {v: reduce(s = 0, x IN [1, 2] | s + x)}) RETURN n.v", int64(3)},
		{"CREATE (n:T {v: {k: 1}.k}) RETURN n.v", int64(1)},
		{"CREATE (n:T {v: 'x'}) RETURN n.v", "x"},
		{"UNWIND [3] AS x CREATE (n:T {v: x * 3}) RETURN n.v", int64(9)},
		{"CREATE (a:T {name: 'x'}), (b:U {name: a.name}) RETURN b.name", "x"},
	}
	for _, check := range checks {
		t.Run(fmt.Sprintf("value:%s", check.statement), func(t *testing.T) {
			exec := newGh514IncompleteExecutor(t)
			result, err := exec.Execute(ctx, check.statement, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{check.expected}}, result.Rows)
		})
	}
}

func TestGh514_KnowledgePolicyDDLMissingNameIsSyntaxError(t *testing.T) {
	exec := newGh514IncompleteExecutor(t)
	_, err := exec.Execute(context.Background(), "CREATE PROMOTION PROFILE", nil)
	require.Error(t, err)
	classified, ok := err.(*classifiedCypherError)
	if ok {
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())
	} else {
		require.Contains(t, err.Error(), "SyntaxError")
	}
}
