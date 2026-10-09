package cypher

import (
	"context"
	"math"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A NaN property in a MERGE pattern can't identify a node or relationship:
// Neo4j fails the statement with SemanticError and writes nothing. A NaN in a
// list, or one set by ON CREATE / ON MATCH SET, is a value like any other
// (Neo4j 5.26.30, #907).
func TestMergeNaNPropertyMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_nan"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R {w: 1}]->(:Q {id: 2})", nil)
	require.NoError(t, err)
	count := func() int64 {
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		return result.Rows[0][0].(int64)
	}
	for _, query := range []string{
		"MERGE (m:W {p: 0.0 / 0.0}) RETURN 1 AS v",
		"MERGE (m:W {p: 1, q: 0.0 / 0.0}) RETURN 1 AS v",
		"MERGE (m {id: 0.0 / 0.0}) RETURN 1 AS v",
		"MERGE ()-[r:T {w: 0.0 / 0.0}]->() RETURN 1 AS v",
		"MATCH (a:Q {id: 1}), (b:Q {id: 2}) MERGE (a)-[r:R {w: 0.0 / 0.0}]->(b) RETURN 1 AS v",
		"MERGE (a:W {p: 0.0 / 0.0})-[:T]->(b:W) RETURN 1 AS v",
		"UNWIND [1, 0.0 / 0.0] AS k MERGE (m:W {k: k}) RETURN 1 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			before := count()
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SemanticError", code)
			require.Equal(t, before, count())
		})
	}
	for _, query := range []string{
		"MERGE (m:W {p: [0.0 / 0.0]}) RETURN m.p[0] AS v",
		"MERGE (m:W {k: 2}) ON CREATE SET m.p = 0.0 / 0.0 RETURN m.p AS v",
		"MERGE (m:Q {id: 1}) ON MATCH SET m.x = 0.0 / 0.0 RETURN m.x AS v",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.True(t, math.IsNaN(result.Rows[0][0].(float64)))
		})
	}
	// null is rejected the same way.
	_, err = exec.Execute(ctx, "WITH null AS k MERGE (m:W {k: k}) RETURN 1 AS v", nil)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SemanticError", code)
}

func TestIsNaNValue(t *testing.T) {
	require.True(t, isNaNValue(math.NaN()))
	require.True(t, isNaNValue(float32(math.NaN())))
	require.False(t, isNaNValue(1.5))
	require.False(t, isNaNValue(float32(1)))
	require.False(t, isNaNValue("NaN"))
	require.False(t, isNaNValue(nil))
}
