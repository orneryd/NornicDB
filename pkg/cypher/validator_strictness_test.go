package cypher

// #514 family, validator side: forms Neo4j rejects but the Nornic validator
// accepted — the NOT IN operator, trailing/leading commas in list literals,
// adjacent string literals (no doubled-quote escape in Cypher), and a
// dangling UNWIND with no following clause.

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func strictnessExec(t *testing.T, name string) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "strict_"+name))
}

func requireSyntaxErrorStatus(t *testing.T, err error, query string) {
	t.Helper()
	require.Error(t, err, query)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
}

func TestNotInOperatorRejected(t *testing.T) {
	exec := strictnessExec(t, "notin")
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:T) WHERE n.id NOT IN [1] RETURN count(n) AS c",
		"MATCH (n:T) WHERE NOT  IN [1] RETURN count(n) AS c",
		"RETURN 1 NOT IN [1] AS x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSyntaxErrorStatus(t, err, query)
	}
	// The valid negation keeps working, and quoted/comment text never matches.
	for _, query := range []string{
		"MATCH (n:T) WHERE NOT n.id IN [1] RETURN count(n) AS c",
		"MATCH (n:T) WHERE NOT (n.id IN [1]) RETURN count(n) AS c",
		"RETURN 'NOT IN' AS s",
		"RETURN 1 AS x // NOT IN here is a comment",
		"RETURN 'a' + 'b' AS s",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func TestListLiteralTrailingCommaRejected(t *testing.T) {
	exec := strictnessExec(t, "listcomma")
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {v: 1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"RETURN [1, 2,] AS l",
		"UNWIND [1, 2,] AS x RETURN x",
		"MATCH (n:T) WHERE n.v IN [1, 2,] RETURN count(n) AS c",
		"RETURN [, 1] AS l",
		"RETURN [[1],] AS l",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSyntaxErrorStatus(t, err, query)
	}
	for _, query := range []string{
		"RETURN [1, 2] AS l",
		"RETURN [[1], [2]] AS l",
		"RETURN {a: 1, b: 2} AS m",
		"RETURN '[,]' AS s",
		"MATCH (n:T) WHERE n.v IN [1, 2] RETURN count(n) AS c",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func TestAdjacentStringLiteralsRejected(t *testing.T) {
	exec := strictnessExec(t, "adjstr")
	ctx := context.Background()
	for _, query := range []string{
		"RETURN 'a''b' AS s",
		`RETURN "a""b" AS s`,
		`RETURN 'a''b''c' AS s`,
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSyntaxErrorStatus(t, err, query)
	}
	// Backslash is the Cypher string escape; an empty literal stays valid.
	for _, query := range []string{
		`RETURN 'a\'b' AS s`,
		`RETURN '' AS s`,
		`RETURN 'a' + 'b' AS s`,
	} {
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, res.Rows, 1, query)
	}
}

func TestDanglingUnwindRejected(t *testing.T) {
	exec := strictnessExec(t, "danglunwind")
	ctx := context.Background()
	for _, query := range []string{
		"UNWIND [1] AS x",
		"UNWIND [1, 2] AS x",
		"MATCH (n) UNWIND [1] AS x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireSyntaxErrorStatus(t, err, query)
	}
	for _, query := range []string{
		"UNWIND [1] AS x RETURN x",
		"UNWIND [1, 2] AS x CREATE (:V {x: x})",
		"UNWIND [1] AS x CALL { RETURN 1 AS y } RETURN y",
		"CALL { UNWIND [1] AS x RETURN x } RETURN x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}
