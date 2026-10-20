package cypher

import (
	"context"
	"fmt"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR73PlainUnwindDemand(t *testing.T) {
	query := "UNWIND $items AS reviewR73Plain RETURN reviewR73Plain AS v"
	input := newSemanticStatementInput(query, nil)
	require.Nil(t, input.syntax(), "parameter UNWIND has no local-binding operand")
	clauses, ok := input.pipeline(true)
	require.True(t, ok)
	for _, c := range clauses {
		require.True(t, c.noLocalScopes)
		require.Nil(t, c.syntax)
	}
	result, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(context.Background(), query, map[string]interface{}{"items": []interface{}{int64(1), int64(2), int64(3)}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
}
func TestReviewR73ParameterEmpty(t *testing.T) {
	query := "WITH $p AS x UNWIND [] AS stop CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v"
	result, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(context.Background(), query, map[string]interface{}{"p": int64(1)})
	t.Logf("query=%q result=%v err=%v", query, result, err)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}
func BenchmarkReviewR73PlainUnwind(b *testing.B) {
	exec := NewStorageExecutor(newTestMemoryEngine(b))
	params := map[string]interface{}{"items": []interface{}{int64(1), int64(2), int64(3)}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("reviewPlain%d", i)
		if _, err := exec.Execute(context.Background(), "UNWIND $items AS "+name+" RETURN "+name+" AS v", params); err != nil {
			b.Fatal(err)
		}
	}
}
