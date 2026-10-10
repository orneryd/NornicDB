package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A number written right after a colon ({a:0.01}, minSimilarity:0.01) is
// checked like any numeric literal: it was skipped and the check resumed in
// its middle, so 0.01 failed as "01" and {a:00} passed. Recorded on Neo4j
// 5.26.30.
func TestNumericLiteralAfterColon(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_literal_after_colon"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN {a:0.01}.a AS v":             0.01,
		"RETURN {a:1.05}.a AS v":             1.05,
		"RETURN {a:0.5}.a AS v":              0.5,
		"RETURN {a:0}.a AS v":                int64(0),
		"WITH {a:0.01} AS m RETURN m.a AS v": 0.01,
		"RETURN {a:-0.01}.a AS v":            -0.01,
		"RETURN {a:.01}.a AS v":              0.01,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	_, err := exec.Execute(ctx, "RETURN {a:00} AS v", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
	_, err = exec.Execute(ctx, "CALL db.retrieve({query: 'source', limit: 10, minSimilarity:0.01}) YIELD node RETURN node", nil)
	require.NoError(t, err)
}
