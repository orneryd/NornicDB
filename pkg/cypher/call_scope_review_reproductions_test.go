package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These are the two independently reproduced review findings. The expected
// rejection is Neo4j 5.26.30's scalar scope rule and UNION's column contract,
// including NornicDB's documented implicit-import extension for bare CALL.
func TestCallScopeReviewReproductions(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call-scope-review"))
	_, err := exec.Execute(ctx, "CREATE (:Target {n:1})", nil)
	require.NoError(t, err)
	for _, call := range []string{"CALL (a) { ", "CALL (*) { "} {
		for _, prefix := range []string{"", "WITH * "} {
			query := "UNWIND [1,2] AS a " + call + prefix + "RETURN COLLECT { RETURN * } AS v } RETURN a,v ORDER BY a"
			t.Run(query, func(t *testing.T) {
				_, err := exec.Execute(ctx, query, nil)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			})
		}
	}
	for _, body := range []string{
		"MATCH (b:Target) RETURN * UNION MATCH (b:Target) RETURN b",
		"MATCH (b:Target) RETURN * UNION ALL MATCH (b:Target) RETURN b",
		"MATCH (b:Target) RETURN * UNION MATCH (b:Target) RETURN b AS b",
	} {
		query := "UNWIND [1,2] AS a CALL { " + body + " } RETURN a,b"
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		})
	}
}
