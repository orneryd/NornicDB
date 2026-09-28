package cypher

// Non-boolean WHERE (#514, #728): a WHERE predicate that evaluates to a
// non-boolean, non-null value is Neo4j's Type mismatch (TypeError) and fails
// the statement — it never silently keeps rows. Null stays falsy.

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNonBooleanWhereRaisesTypeMismatch(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_bool"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:W {v: 1}), (:W {v: 2})", nil)
	require.NoError(t, err)

	cases := []struct {
		query    string
		wantType string
	}{
		{"MATCH (n:W) WHERE 42 RETURN count(n) AS c", "Integer"},
		{"MATCH (n:W) WHERE n.v RETURN count(n) AS c", "Integer"},
		{"MATCH (n:W) WHERE 'true' RETURN count(n) AS c", "String"},
		{"MATCH (n:W) WHERE n RETURN count(n) AS c", "Node"},
		{"MATCH (n:W) WHERE [1] RETURN count(n) AS c", "List"},
		{"WITH 42 AS x WHERE x RETURN x", "Integer"},
		{"UNWIND [1, 2] AS x WITH x WHERE x RETURN count(*) AS c", "Integer"},
		{"MATCH (n:W) WHERE {a: 1} RETURN count(n) AS c", "Map"},
	}
	for _, tc := range cases {
		_, err := exec.Execute(ctx, tc.query, nil)
		require.Error(t, err, tc.query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", code, tc.query)
		require.Contains(t, err.Error(), "expected Boolean but was "+tc.wantType, tc.query)
	}
}

func TestBooleanAndNullWhereStillWork(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_bool_ok"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:W {v: 1}), (:W {v: 2})", nil)
	require.NoError(t, err)

	run := func(query string) int64 {
		t.Helper()
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, res.Rows, 1, query)
		return res.Rows[0][0].(int64)
	}
	require.Equal(t, int64(2), run("MATCH (n:W) WHERE true RETURN count(n) AS c"))
	require.Equal(t, int64(0), run("MATCH (n:W) WHERE false RETURN count(n) AS c"))
	require.Equal(t, int64(0), run("MATCH (n:W) WHERE null RETURN count(n) AS c"))
	require.Equal(t, int64(2), run("MATCH (n:W) WHERE n.v > 0 RETURN count(n) AS c"))
	require.Equal(t, int64(1), run("MATCH (n:W) WHERE n.v = 1 RETURN count(n) AS c"))
	require.Equal(t, int64(2), run("UNWIND [1, 2] AS x WITH x WHERE x > 0 RETURN count(*) AS c"))
	require.Equal(t, int64(1), run("WITH 1 AS x WHERE x = 1 RETURN x"))
}

func TestNonBooleanWhereFailsExplicitTransaction(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_bool_tx"))
	ctx := context.Background()
	if _, err := exec.handleBegin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := exec.Execute(ctx, "CREATE (:W {v: 1})", nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := exec.Execute(ctx, "MATCH (n:W) WHERE 42 RETURN count(n) AS c", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
	// The transaction is failed: COMMIT rolls it back.
	if _, err := exec.handleCommit(); err != nil {
		t.Logf("commit err (expected failed tx): %v", err)
	}
	res, err := exec.Execute(ctx, "MATCH (n:W) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.Rows[0][0], "the failed statement's writes roll back")
}
