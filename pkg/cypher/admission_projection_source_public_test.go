package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementProjectionSourceViewsPublic(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	queries := []struct {
		text  string
		value interface{}
	}{
		{"RETURN head([null]):A AS v", nil},
		{"RETURN [allReduce(acc = 0, x IN [1] | acc + x, acc > 0)] AS v", []interface{}{true}},
		{"WITH {k: 7} AS n RETURN (n {.k}).k AS v", int64(7)},
	}
	for _, query := range queries {
		t.Run(query.text, func(t *testing.T) {
			for repeat := 0; repeat < 2; repeat++ {
				result, err := exec.Execute(context.Background(), query.text, nil)
				require.NoError(t, err)
				require.Equal(t, []string{"v"}, result.Columns)
				require.Equal(t, [][]interface{}{{query.value}}, result.Rows)
			}
		})
	}
}
