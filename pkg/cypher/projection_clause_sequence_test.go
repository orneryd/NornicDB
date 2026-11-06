package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestProjectionClauseSyntaxRejectedLikeNeo4j owns the reject/accept lifecycle
// for malformed projection aliases and for empty projections or dangling
// modifiers. Each entry keeps its own namespace, then runs every malformed
// query through the shared syntax-error assertion and every control query as a
// plain success.
func TestProjectionClauseSyntaxRejectedLikeNeo4j(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		namespace string
		rejected  []string
		accepted  []string
	}{
		{
			name:      "malformed projection aliases",
			namespace: "alias_reject",
			rejected: []string{
				"RETURN 1 AS x RETURN 2 AS y",
				"RETURN 1 AS x y",
				"RETURN 1 AS x\r\nRETURN 2 AS y",
			},
			accepted: []string{
				"RETURN 1 AS x",
				"RETURN 1 AS `x y`",
				"RETURN 1 AS x, 2 AS y",
			},
		},
		{
			name:      "empty projection and dangling modifiers",
			namespace: "modifier_reject",
			rejected: []string{
				"RETURN",
				"RETURN 1 ORDER BY",
				"RETURN 1 SKIP",
				"RETURN 1 LIMIT",
				"WITH 1 AS x RETURN",
				"MATCH (n) RETURN n ORDER BY",
				"MATCH (n) RETURN n SKIP",
				"RETURN DISTINCT",
				"WITH 1 AS",
				"MATCH (n) WHERE = 1 RETURN n",
				"MATCH () WHERE RETURN n",
				"WITH 1 AS x WHERE RETURN x",
				"MATCH (n) WHERE n.x IN RETURN n",
				"CALL { RETURN 1 AS x } RETURN",
			},
			accepted: []string{
				"RETURN 1 ORDER BY 1",
				"RETURN 1 SKIP 0",
				"RETURN 1 LIMIT 1",
				"WITH 1 AS x RETURN *",
				"WITH 1 AS x RETURN x",
				"MATCH (n) WHERE n.x = 1 RETURN n",
				"MATCH (n) WHERE EXISTS { (m)--(n) } RETURN n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), tt.namespace))
			for _, query := range tt.rejected {
				_, err := exec.Execute(ctx, query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
			}
			for _, query := range tt.accepted {
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
		})
	}
}
