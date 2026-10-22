package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallScopeProjectionProvenance(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "projection-provenance"))
	_, err := exec.Execute(ctx, "CREATE (:ScopeTarget)", nil)
	require.NoError(t, err)
	for _, tail := range []string{"ORDER BY b", "WHERE b = 1", "WHERE EXISTS { RETURN b AS value }"} {
		t.Run(tail, func(t *testing.T) {
			prefix := "WITH 7 AS b CALL { MATCH (n:ScopeTarget) WITH n, 1 AS b " + tail
			kept, err := exec.Execute(ctx, prefix+" RETURN b AS value } RETURN b,value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{int64(7), int64(1)}}, kept.Rows)
			_, err = exec.Execute(ctx, prefix+" WITH n RETURN b AS value } RETURN b,value", nil)
			require.Error(t, err, "a local alias must not become an implicit global because its tail reads it")
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		})
	}
	for _, literal := range []string{"true", "false", "null"} {
		t.Run(literal, func(t *testing.T) {
			query := "WITH 7 AS `" + literal + "` CALL (`" + literal + "`) { RETURN " + literal + " AS value } RETURN value"
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			var want any
			if literal != "null" {
				want = literal == "true"
			}
			require.Equal(t, [][]any{{want}}, result.Rows)
			query = "WITH 7 AS `" + literal + "` CALL (`" + literal + "`) { RETURN `" + literal + "` AS value } RETURN value"
			result, err = exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{int64(7)}}, result.Rows)
		})
	}
}

func TestCallScopeIncompleteNestedProof(t *testing.T) {
	for _, query := range []string{
		"RETURN COUNT { CALL db.labels() YIELD * RETURN 1 AS value } AS value",
		"WITH 1 AS a WHERE EXISTS { CALL db.labels() YIELD * RETURN 1 AS value } RETURN a",
	} {
		require.True(t, pipelineReadsBinding(pipelineDependencyClauses(query), "outer"), query)
	}
}
