package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// <> of two constant numbers compares an integer with a float as floats in a
// Cypher 25 statement (Neo4j 2026.09), as = already does, so
// 9007199254740993 <> 9007199254740992.0 is false; a Cypher 5 statement
// compares it exactly (Neo4j 5.26). Values from variables compare exactly
// in both.
func TestCypher25ConstantInequalityFolds(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_constant_inequality"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CiN)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query             string
		cypher25, cypher5 interface{}
	}{
		{"RETURN 9007199254740993 <> 9007199254740992.0 AS v", false, true},
		{"RETURN 9007199254740992.0 <> 9007199254740993 AS v", false, true},
		{"RETURN -9007199254740993 <> -9007199254740992.0 AS v", false, true},
		{"RETURN 9007199254740993 <> 9007199254740992.0 <> true AS v", false, true},
		{"RETURN CASE WHEN 9007199254740993 <> 9007199254740992.0 THEN 1 ELSE 0 END AS v", int64(0), int64(1)},
		{"RETURN [x IN [1] WHERE 9007199254740993 <> 9007199254740992.0] AS v", []interface{}{}, []interface{}{int64(1)}},
		{"UNWIND [1] AS x WITH x WHERE 9007199254740993 <> 9007199254740992.0 RETURN count(*) AS v", int64(0), int64(1)},
		{"MATCH (n:CiN) WHERE 9007199254740993 <> 9007199254740992.0 RETURN count(*) AS v", int64(0), int64(1)},
		{"MATCH (n:CiN) WITH count(n) AS c RETURN 9007199254740993 <> 9007199254740992.0 AS v", false, true},
		{"WITH 9007199254740993 AS i RETURN i <> 9007199254740992.0 AS v", true, true},
		{"RETURN 9007199254740993 <= 9007199254740992.0 AS v", false, false},
	} {
		for prefix, want := range map[string]interface{}{"CYPHER 25 ": tc.cypher25, "": tc.cypher5} {
			result, err := exec.Execute(ctx, prefix+tc.query, nil)
			require.NoError(t, err, prefix+tc.query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, prefix+tc.query)
		}
	}
}
