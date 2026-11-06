package cypher

// #514 family, validator side: forms Neo4j rejects but the Nornic validator
// accepted — the NOT IN operator, trailing/leading commas in list literals,
// adjacent string literals (no doubled-quote escape in Cypher), and a
// dangling UNWIND with no following clause.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func strictnessExec(t *testing.T, name string) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "strict_"+name))
}

// TestNotInAndListLiteralMalformationsRejected owns the reject/accept lifecycle
// for the NOT IN operator form and for list-literal comma placement. Each entry
// keeps its own namespace and seeded node property, then runs every malformed
// query through the shared syntax-error assertion and every control query as a
// plain success.
func TestNotInAndListLiteralMalformationsRejected(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		namespace string
		seed      string
		rejected  []string
		accepted  []string
	}{
		{
			name:      "not in operator",
			namespace: "notin",
			seed:      "CREATE (:T {id: 1})",
			rejected: []string{
				"MATCH (n:T) WHERE n.id NOT IN [1] RETURN count(n) AS c",
				"MATCH (n:T) WHERE NOT  IN [1] RETURN count(n) AS c",
				"RETURN 1 NOT IN [1] AS x",
			},
			// The valid negation keeps working, and quoted/comment text never matches.
			accepted: []string{
				"MATCH (n:T) WHERE NOT n.id IN [1] RETURN count(n) AS c",
				"MATCH (n:T) WHERE NOT (n.id IN [1]) RETURN count(n) AS c",
				"RETURN 'NOT IN' AS s",
				"RETURN 1 AS x // NOT IN here is a comment",
				"RETURN 'a' + 'b' AS s",
			},
		},
		{
			name:      "list literal comma placement",
			namespace: "listcomma",
			seed:      "CREATE (:T {v: 1})",
			rejected: []string{
				"RETURN [1, 2,] AS l",
				"UNWIND [1, 2,] AS x RETURN x",
				"MATCH (n:T) WHERE n.v IN [1, 2,] RETURN count(n) AS c",
				"RETURN [, 1] AS l",
				"RETURN [[1],] AS l",
			},
			accepted: []string{
				"RETURN [1, 2] AS l",
				"RETURN [[1], [2]] AS l",
				"RETURN {a: 1, b: 2} AS m",
				"RETURN '[,]' AS s",
				"MATCH (n:T) WHERE n.v IN [1, 2] RETURN count(n) AS c",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := strictnessExec(t, tt.namespace)
			_, err := exec.Execute(ctx, tt.seed, nil)
			require.NoError(t, err, tt.seed)
			for _, query := range tt.rejected {
				_, err := exec.Execute(ctx, query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
			}
			for _, query := range tt.accepted {
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
		})
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
		requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
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
		requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
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
