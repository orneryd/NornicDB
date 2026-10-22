package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallScopeRuntimeProgramIsolation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-program"))
	ctx := context.Background()
	type observation struct {
		query string
		want  [][]any
		got   *ExecuteResult
		err   error
	}
	observations := make(chan observation, 24)
	var workers sync.WaitGroup
	for index := 0; index < 12; index++ {
		for _, wildcard := range []bool{false, true} {
			workers.Add(1)
			go func(index int, wildcard bool) {
				defer workers.Done()
				value := fmt.Sprintf("literal-%d", index)
				body := fmt.Sprintf("WITH a RETURN '%s' AS s UNION ALL WITH a RETURN '%s-right' AS s", value, value)
				want := [][]any{{value}, {value + "-right"}}
				if wildcard {
					body = fmt.Sprintf("WITH a,'%s' AS s RETURN *", value)
					want = [][]any{{value}}
				}
				query := "WITH $outer AS a CALL (a) { " + body + " } RETURN s"
				got, err := exec.Execute(ctx, query, map[string]any{"outer": int64(index + 1)})
				observations <- observation{query: query, want: want, got: got, err: err}
			}(index, wildcard)
		}
	}
	workers.Wait()
	close(observations)
	for result := range observations {
		require.NoError(t, result.err, result.query)
		require.Equal(t, []string{"s"}, result.got.Columns)
		require.ElementsMatch(t, result.want, result.got.Rows, result.query)
	}

	for index, parameter := range []string{"first", "second", "third"} {
		query := "CALL () { RETURN $" + parameter + " AS s } RETURN s"
		result, err := exec.Execute(ctx, query, map[string]any{parameter: int64(index + 10)})
		require.NoError(t, err, query)
		require.Equal(t, [][]any{{int64(index + 10)}}, result.Rows)
	}
	for index := 0; index < 3; index++ {
		value := fmt.Sprintf("column-%d", index)
		for _, nested := range []bool{false, true} {
			body := "RETURN '" + value + "'"
			if nested {
				body = "CALL () { " + body + " } RETURN *"
			}
			query := "CALL () { " + body + " } RETURN *"
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, []string{"'" + value + "'"}, result.Columns)
			require.Equal(t, [][]any{{value}}, result.Rows)
		}
	}
	result, err := exec.Execute(ctx, "CALL () { UNWIND [1,1] AS v RETURN DISTINCT * } RETURN v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}}, result.Rows)
	valid := exec.executePipeline(ctx, "CALL () { RETURN 1 AS v LIMIT 2-1 } RETURN v")
	require.NoError(t, valid.err)
	require.True(t, valid.handled())
	require.Equal(t, [][]any{{int64(1)}}, valid.result.Rows)
	invalid := exec.executePipeline(ctx, "CALL () { RETURN 1 AS v LIMIT 2-3 } RETURN v")
	require.Error(t, invalid.err)
	code, _ := nornicerrors.Neo4jStatus(invalid.err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}

// A legacy import selects ordinary locals and explicitly named ancestors.
// The unlisted-import extension applies to the caller's current locals, not
// to ancestors that WITH * cannot see in a nested scalar/CALL query scope.
func TestCallScopeRuntimeLegacyImportBoundary(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "legacy-import-boundary"))
	ctx := context.Background()
	for _, query := range []string{
		"WITH 1 AS i, 2 AS z CALL { WITH i RETURN i+z AS j } RETURN j",
		"WITH 1 AS i, 2 AS z CALL { WITH i WITH DISTINCT i RETURN i+z AS j } RETURN j",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]any{{int64(3)}}, result.Rows, query)
	}
	query := "WITH 1 AS a CALL (a) { RETURN COLLECT { CALL { WITH a RETURN a AS innerValue } RETURN innerValue AS item } AS value } RETURN value"
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{[]any{int64(1)}}}, result.Rows)
	invalid := []string{
		"WITH 1 AS i, 2 AS z CALL { WITH * WITH i RETURN z AS j } RETURN j",
	}
	for _, kind := range []string{"COLLECT", "COUNT", "EXISTS"} {
		invalid = append(invalid,
			fmt.Sprintf("WITH 1 AS a CALL (a) { RETURN %s { CALL { WITH * RETURN a AS innerValue } RETURN innerValue AS item } AS value } RETURN value", kind),
			fmt.Sprintf("WITH 1 AS a CALL (a) { CALL { WITH * RETURN %s { RETURN a AS innerValue } AS innerValue } RETURN innerValue AS value } RETURN value", kind))
	}
	for _, query := range invalid {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}

func TestCallScopeRuntimeScalarProgramIsolation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scalar-program"))
	ctx := context.Background()
	for _, prefix := range []string{"", "CYPHER 25 "} {
		for _, scope := range []string{"WITH $input AS a", "WITH $input AS a, 9 AS extra"} {
			for _, value := range []any{int64(7), "different value", nil, int64(19)} {
				query := prefix + scope + " RETURN COLLECT { RETURN a AS item } AS value"
				for repeat := 0; repeat < 3; repeat++ {
					result, err := exec.Execute(ctx, query, map[string]any{"input": value})
					require.NoError(t, err, query)
					require.Equal(t, [][]any{{[]any{value}}}, result.Rows, query)
				}
			}
		}
	}

	// The same compiled predicate is shared across changing schemas, modes
	// and row values. Compare it with the text evaluator, including in parallel.
	for _, expression := range []string{"EXISTS { UNWIND a AS item RETURN item }", "NOT EXISTS { UNWIND a AS item RETURN item }"} {
		program := compileRowPredicateProgram(expression)
		type observation struct {
			want, got bool
		}
		observations := make(chan observation, 24)
		var workers sync.WaitGroup
		for index := 0; index < 24; index++ {
			workers.Add(1)
			go func(index int) {
				defer workers.Done()
				statement := ctx
				if index%2 != 0 {
					statement = withCypherVersion(ctx, "CYPHER 25 RETURN 1")
				}
				row := map[string]any{"a": []any{int64(index)}}
				if index%3 == 0 {
					row["a"] = []any{}
				}
				if index%4 == 0 {
					row["extra"] = int64(index)
				}
				want := exec.evaluateRowPredicateText(statement, expression, row)
				for repeat := 0; repeat < 3; repeat++ {
					observations <- observation{want: want, got: program.holds(exec, statement, row)}
				}
			}(index)
		}
		go func() {
			workers.Wait()
			close(observations)
		}()
		for result := range observations {
			require.Equal(t, result.want, result.got, expression)
		}
	}
}

func TestCallScopeRuntimeCanonicalBufferBoundaries(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical-buffer"))
	for _, width := range []int{0, 120} {
		padding := make([]string, width)
		for index := range padding {
			padding[index] = fmt.Sprintf(", 'padding-%d' AS pad%d", index, index)
		}
		for _, value := range []string{"first value", "second value", "first value"} {
			query := "CALL () { RETURN '" + value + "' AS s" + strings.Join(padding, "") + " } RETURN s"
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]any{{value}}, result.Rows)
		}
	}
}
