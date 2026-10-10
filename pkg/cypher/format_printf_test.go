package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NornicDB's printf extension, format(template, values…): every verb prints
// one value, and a template that doesn't fit its values is an error, not
// Go's %!… text in the result.
func TestFormatPrintfExtension(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "format_printf"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN format('%s-%d', 'a', 7) AS v":            "a-7",
		"RETURN format('%5.2f|%t', 1, true) AS v":        " 1.00|true",
		"RETURN format('%s %v', [1, 'b'], {k: 1}) AS v":  "[1, 'b'] {k: 1}",
		"RETURN format('%x %X %q', 255, 'hi', 'q') AS v": `ff 6869 "q"`,
		"RETURN format('100%% %s', 1.5) AS v":            "100% 1.5",
		"RETURN format('plain') AS v":                    "plain",
		"RETURN format('%-3d|%+d', 7, 5) AS v":           "7  |+5",
		"RETURN format('%s', null) AS v":                 nil,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for query, code := range map[string]string{
		"RETURN format('x', 'yyyy') AS v":            "Neo.ClientError.Statement.ArgumentError",
		"RETURN format('%s %s', 'a') AS v":           "Neo.ClientError.Statement.ArgumentError",
		"RETURN format('%d', 'a') AS v":              "Neo.ClientError.Statement.TypeError",
		"RETURN format('%f', 'a') AS v":              "Neo.ClientError.Statement.TypeError",
		"RETURN format('%t', 1) AS v":                "Neo.ClientError.Statement.TypeError",
		"RETURN format('%z', 1) AS v":                "Neo.ClientError.Statement.ArgumentError",
		"RETURN format('%*d', 1, 2) AS v":            "Neo.ClientError.Statement.ArgumentError",
		"RETURN format('50%', 1) AS v":               "Neo.ClientError.Statement.ArgumentError",
		"UNWIND ['x'] AS t RETURN format(t, 1) AS v": "Neo.ClientError.Statement.ArgumentError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
		require.NotContains(t, err.Error(), "%!", query)
	}
}
