package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPerRowRound4LazySubscripts(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"UNWIND [0,0] AS x RETURN x=0 OR [1/x=1][0] AS v", [][]interface{}{{true}, {true}}},
		{"UNWIND [0,0] AS x RETURN x=0 OR [true][1/x] AS v", [][]interface{}{{true}, {true}}},
		{"UNWIND [0,0] AS x RETURN x<>0 AND [true][1/x] AS v", [][]interface{}{{false}, {false}}},
		{"UNWIND [0,0] AS x RETURN x=0 OR 1 IN [1/x] AS v", [][]interface{}{{true}, {true}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4reg"))
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
			})
		}
	}
}

func TestPerRowRound4MapLiteralSubscript(t *testing.T) {
	for _, query := range []string{"RETURN trim({a:'b'}['a']) AS v", "RETURN {a:'b'}['a'] AS v", "RETURN trim(({a:'b'})['a']) AS v"} {
		t.Run(query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4map"))
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"b"}}, result.Rows)
		})
	}
}

func TestPerRowRound4LazyRange(t *testing.T) {
	for _, text := range []bool{false, true} {
		rowValueProgramsDisabled.Store(text)
		exec, _ := perRowTestExecutor(t)
		result, err := exec.Execute(context.Background(), "UNWIND [0,0] AS x RETURN coalesce(1,range(1,1/x)) AS v", nil)
		rowValueProgramsDisabled.Store(false)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {int64(1)}}, result.Rows)
	}
}

func TestPerRowRound4ContextIndexErrors(t *testing.T) {
	for _, tc := range []struct{ query, message string }{
		{"CREATE (n:N {k:1.0}) RETURN trim(['a','b'][n.k]) AS v", "Cannot access a list using an non-integer number index, got Double(1.000000e+00)"},
		{"CREATE (n:N {k:1.5}) RETURN trim(['a','b'][n.k]) AS v", "Cannot access a list using an non-integer number index, got Double(1.500000e+00)"},
		{"CREATE (n:N {k:true}) RETURN trim(['a','b'][n.k]) AS v", "Cannot access a list 'List{String(\"a\"), String(\"b\")}' using a non-number index, got Boolean('true'): Expected Boolean('true') to be a org.neo4j.values.storable.NumberValue, but it was a org.neo4j.values.storable.BooleanValue$1"},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4index"))
				_, err := exec.Execute(context.Background(), tc.query, nil)
				requireRound3Neo4jError(t, err, tc.message)
			})
		}
	}
	// Reaching the context owner itself must produce an error, not unresolved.
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	_, resolved, err := exec.evaluateCypherSubscript("list", "key", func(expr string) (interface{}, bool, error) {
		if expr == "list" {
			return []interface{}{"a", "b"}, true, nil
		}
		return float64(1.5), true, nil
	})
	require.Error(t, err)
	require.False(t, resolved)
}
