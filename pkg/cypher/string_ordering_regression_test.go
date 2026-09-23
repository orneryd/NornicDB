package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStringFunctionsUseUnicodeCodePoints(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "unicode-string-functions")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:BugT {s: 'привет'})`, nil)
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
		want  interface{}
	}{
		{name: "literal Cyrillic size", query: `RETURN size('привет') AS value`, want: int64(6)},
		{name: "stored Cyrillic size", query: `MATCH (n:BugT) RETURN size(n.s) AS value`, want: int64(6)},
		{name: "CJK size", query: `RETURN size('東京') AS value`, want: int64(2)},
		{name: "accented Latin size", query: `RETURN size('café') AS value`, want: int64(4)},
		{name: "emoji size", query: `RETURN size('🙂🙃') AS value`, want: int64(2)},
		{name: "substring", query: `RETURN substring('привет', 1, 3) AS value`, want: "рив"},
		{name: "left", query: `RETURN left('привет', 2) AS value`, want: "пр"},
		{name: "right", query: `RETURN right('привет', 2) AS value`, want: "ет"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tt.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tt.want}}, result.Rows)
		})
	}

	for _, query := range []string{
		`RETURN 'привет'[1] AS value`,
		`RETURN '東京'[-1] AS value`,
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err)
		var semanticError *SemanticError
		require.True(t, errors.As(err, &semanticError))
		require.Equal(t, "Neo.ClientError.Statement.TypeError", semanticError.Code)
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}
}

func TestOrderByComputedReturnExpression(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "computed-return-ordering")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, query := range []string{
		`CREATE (:BugT {k: 0, s: 'привет'})`,
		`CREATE (:BugT {k: 3, s: 'abc'})`,
		`CREATE (:BugT {k: 4, s: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'})`,
		`CREATE (:BugT {k: 5, s: 'bbbbbbb'})`,
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	for _, orderExpr := range []string{"n", "size(n.s)"} {
		result, err := exec.Execute(ctx,
			`MATCH (n:BugT) WHERE n.s IS NOT NULL RETURN size(n.s) AS n ORDER BY `+orderExpr+` DESC LIMIT 3`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(50)}, {int64(7)}, {int64(6)}}, result.Rows)
	}

	result, err := exec.Execute(ctx, `
		MATCH (n:BugT)
		RETURN size(n.s) AS length, n.k AS key
		ORDER BY length ASC, key DESC SKIP 1 LIMIT 2
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6), int64(0)}, {int64(7), int64(5)}}, result.Rows)
}

func TestOrderByComputedReturnExpressionOnRelationshipRows(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "computed-relationship-ordering")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:Source {name: 'source'})
		CREATE (a)-[:LINKS_TO]->(:Target {s: '東京'})
		CREATE (a)-[:LINKS_TO]->(:Target {s: 'привет'})
	`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (:Source)-[r:LINKS_TO]->(n:Target)
		RETURN size(n.s) AS length, type(r) AS relationship
		ORDER BY length DESC
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6), "LINKS_TO"}, {int64(2), "LINKS_TO"}}, result.Rows)
}
