package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An operator inside a CASE … END is the CASE's, not the expression's: the
// row evaluator splits =~, IN, NOT IN, STARTS WITH, ENDS WITH and CONTAINS
// outside CASE blocks, in values and in predicates.
func TestOperatorInsideCaseIsTheCases(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_split"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN 'x' + CASE WHEN 'ab' =~ '.*b' THEN 'y' ELSE '' END AS v":                           "xy",
		"WITH 'ab' AS s RETURN 'x' + CASE WHEN s =~ '.*b' THEN 'y' ELSE '' END AS v":               "xy",
		"WITH 'ab' AS s RETURN 'x' + CASE WHEN s IN ['ab'] THEN 'y' ELSE '' END AS v":              "xy",
		"WITH 'ab' AS s RETURN 'x' + CASE WHEN NOT s IN ['c'] THEN 'y' ELSE '' END AS v":           "xy",
		"WITH 'ab' AS s RETURN 'x' + CASE WHEN s STARTS WITH 'a' THEN 'y' ELSE '' END AS v":        "xy",
		"WITH 'ab' AS s RETURN 'x' + CASE WHEN s ENDS WITH 'b' THEN 'y' ELSE '' END AS v":          "xy",
		"WITH 'ab' AS s RETURN 'x' + CASE WHEN s CONTAINS 'b' THEN 'y' ELSE '' END AS v":           "xy",
		"WITH 'ab' AS s RETURN CASE WHEN s =~ 'a.*' THEN 'y' ELSE 'n' END + 'x' AS v":              "yx",
		"WITH 'ab' AS s WITH s WHERE CASE WHEN s =~ '.*b' THEN true ELSE false END RETURN s AS v":  "ab",
		"WITH 'ab' AS s WITH s WHERE CASE WHEN s IN ['ab'] THEN true ELSE false END RETURN s AS v": "ab",
		"WITH 'ab' AS s WITH s WHERE CASE WHEN s CONTAINS 'a' THEN 'k' END = 'k' RETURN s AS v":    "ab",
		"WITH 'ab' AS s WITH s WHERE CASE WHEN s STARTS WITH 'a' THEN 1 END + 1 = 2 RETURN s AS v": "ab",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}
