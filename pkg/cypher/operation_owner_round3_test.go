package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exact values and error messages are checked against Neo4j 5.26.30.
func TestOperationOwnerRound3SubscriptErrors(t *testing.T) {
	for _, query := range []string{
		"UNWIND [0,0] AS x RETURN trim(null[1/x]) AS v",
		"UNWIND [0,0] AS x RETURN keys(null[1/x]) AS v",
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r3owners"))
				_, err := exec.Execute(context.Background(), query, nil)
				requireRound3Neo4jError(t, err, "/ by zero")
			})
		}
	}
}

func TestOperationOwnerRound3Values(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"RETURN trim(null[0]) AS v", [][]interface{}{{nil}}},
		{"RETURN trim(['a','b'][-1]) AS v", [][]interface{}{{"b"}}},
		{"UNWIND ['é🙂',[1,2],null] AS x RETURN size(x) AS v", [][]interface{}{{int64(2)}, {int64(2)}, {nil}}},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r3values"))
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
			})
		}
	}
}
