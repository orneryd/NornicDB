package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParameterPassthroughWholeStatementProof(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  bool
	}{
		{"RETURN $p AS value", true},
		{"RETURN $`quoted parameter` AS value, $p AS other", true},
		{"RETURN DISTINCT $p AS value", false},
		{"RETURN $p AS value ORDER BY value", false},
		{"RETURN $p AS value LIMIT $limit", false},
		{"RETURN $p AS value UNION ALL RETURN toUpper($p) AS value", false},
		{"WITH $p AS value RETURN value", false},
		{"CALL { RETURN $p AS value } RETURN value", false},
		{"RETURN ($p) AS value", false},
		{"RETURN $p + 1 AS value", false},
		{"RETURN $p.key AS value", false},
		{"RETURN [$p] AS value", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, tc.want, returnProjectionPlanFor(t.Context(), tc.query).parameterPassthrough)
		})
	}
}

func TestParameterPassthroughPublicRebinding(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := norStaticFreshExecutor(t)
		exec.cache = nil
		params := map[string]any{}
		// The same cached text crosses dialects and repeatedly changes types.
		for _, prefix := range []string{"CYPHER 5 ", "CYPHER 25 ", "CYPHER 5 "} {
			for _, value := range []any{int64(7), "text", true, nil, []any{int64(2)}, map[string]any{"k": "v"}, float64(1.5)} {
				params["quoted parameter"] = value
				result, err := exec.Execute(t.Context(), prefix+"RETURN $`quoted parameter` AS value", params)
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, result.Columns)
				require.Equal(t, [][]any{{value}}, result.Rows)
			}
			delete(params, "quoted parameter")
			_, err := exec.Execute(t.Context(), prefix+"RETURN $`quoted parameter` AS value", params)
			require.Error(t, err)
			require.Contains(t, err.Error(), "quoted parameter")
			// A second UNION branch must retain its type-sensitive admission.
			_, err = exec.Execute(t.Context(), prefix+"RETURN $p AS value UNION ALL RETURN toUpper($p) AS value", map[string]any{"p": int64(7)})
			require.Error(t, err)
			require.Contains(t, err.Error(), "Type mismatch")
		}
	})
}
