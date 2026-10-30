package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A doubled quote inside a string literal is a SyntaxError however often a
// statement of the same shape with one string ran before: the syntax cache
// shares a verdict only between statements whose literals differ, and a
// doubled quote is two adjacent literals, not an escaped quote (#907).
func TestDoubledQuoteIsRejectedAfterASingleString(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quotes"))
	for _, pair := range [][2]string{
		{"RETURN 'it' AS v", "RETURN 'it''s' AS v"},
		{"WITH 'a' AS v RETURN v", "WITH 'it''s' AS v RETURN v"},
		{`RETURN "it" AS v`, `RETURN "it""s" AS v`},
	} {
		for round := 0; round < 2; round++ {
			_, err := exec.Execute(ctx, pair[0], nil)
			require.NoError(t, err, pair[0])
			_, err = exec.Execute(ctx, pair[1], nil)
			require.Error(t, err, pair[1])
			require.Contains(t, err.Error(), "SyntaxError", pair[1])
		}
	}
}
