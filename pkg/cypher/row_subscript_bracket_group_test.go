package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSubscriptValidationReadsBracketGroups: the subscript check reads the
// bracket group that closes an item, so a list comprehension or literal that
// is the whole item is not taken for a subscript of its inner list (which
// evaluated text such as `1,2,3] WHERE x > 1 | x * 2`), while a real
// subscript is still checked.
func TestSubscriptValidationReadsBracketGroups(t *testing.T) {
	for expression, want := range map[string]int{
		"[x IN [1,2,3] | x]":    0,
		"l[0]":                  1,
		"[x IN [1,2,3] | x][0]": 18,
		"m['a[b]']":             1,
		"n.list[1..2]":          6,
		"[1, [2]]":              0,
		"x]":                    -1,
		"'[a]'":                 -1,
	} {
		require.Equal(t, want, closingBracketGroupStart(expression), expression)
	}

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subscript"))
	ctx := context.Background()
	for statement, want := range map[string][][]interface{}{
		"RETURN [x IN [1,2,3] WHERE x > 1 | x * 2] AS r":     {{[]interface{}{int64(4), int64(6)}}},
		"UNWIND [1] AS one RETURN [x IN [1,2] | x * 2] AS r": {{[]interface{}{int64(2), int64(4)}}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
	_, err := exec.Execute(ctx, "WITH [1, 2] AS l RETURN l['a'] AS r", nil)
	require.Error(t, err)
}
