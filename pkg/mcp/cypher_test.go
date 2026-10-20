package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestCypherResultBoundsPreserveNativeValues(t *testing.T) {
	stats := &cypher.QueryStats{NodesCreated: 2}
	metadata := map[string]interface{}{"receipt": map[string]interface{}{"outcome": "committed"}, "method": "native"}
	native := &cypher.ExecuteResult{Columns: []string{"evidence"}, Rows: [][]interface{}{{map[string]interface{}{"id": "selected", "scores": []interface{}{0.5}, "passages": []string{"full text"}}}, {"omitted"}}, Stats: stats, Metadata: metadata}
	result, err := newCypherResultBudget(context.Background(), 1).finish(context.Background(), native)
	require.NoError(t, err)
	want, err := json.Marshal(native.Rows[:1])
	require.NoError(t, err)
	got, err := json.Marshal(result.Rows)
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got))
	require.Same(t, stats, result.Stats)
	require.Equal(t, metadata, result.Metadata)
	require.Equal(t, []string{"max_rows"}, result.Truncation.Reasons)
	require.Len(t, native.Rows, 2, "output bounds must not mutate a cached native result")

	result, err = newCypherResultBudget(context.Background(), 1).finish(context.Background(), &cypher.ExecuteResult{})
	require.NoError(t, err)
	require.Empty(t, result.Columns)
	require.Empty(t, result.Rows)
	require.False(t, result.Truncation.Truncated)

	// JSON escaping counts toward the byte bound; a single large row is not
	// silently cut inside a caller-selected value.
	native = &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{"small"}, {strings.Repeat("\"", cypherMaxBytes)}}}
	result, err = newCypherResultBudget(context.Background(), 100).finish(context.Background(), native)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"small"}}, result.Rows)
	require.Equal(t, []string{"max_bytes"}, result.Truncation.Reasons)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), cypherMaxBytes)
}

func TestCypherResultSerializationAndCancellationErrors(t *testing.T) {
	for _, native := range []*cypher.ExecuteResult{
		{Metadata: map[string]interface{}{"too_large": strings.Repeat("x", cypherMaxBytes)}},
		{Metadata: map[string]interface{}{"invalid": make(chan int)}},
		{Rows: [][]interface{}{{make(chan int)}}},
	} {
		result, err := newCypherResultBudget(context.Background(), 100).finish(context.Background(), native)
		require.Error(t, err)
		require.Nil(t, result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newCypherResultBudget(ctx, 1).finish(ctx, &cypher.ExecuteResult{Rows: [][]interface{}{{1}}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCypherArgumentValidation(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := context.Background()
	for _, args := range []map[string]interface{}{
		{}, {"query": " "}, {"query": 1}, {"query": "RETURN 1", "unknown": true},
		{"query": "RETURN 1", "database": 1}, {"query": "RETURN 1", "parameters": nil},
		{"query": "RETURN 1", "max_rows": "1"}, {"query": "RETURN 1", "max_rows": int64(-1)},
		{"query": "RETURN 1", "max_rows": 1001}, {"query": "RETURN 1", "max_rows": 1001.0},
	} {
		_, err := server.handleCypher(ctx, args)
		require.Error(t, err)
	}
	_, err := server.handleCypher(ctx, map[string]interface{}{"query": "RETURN 1"})
	require.ErrorContains(t, err, "executor unavailable")
}
