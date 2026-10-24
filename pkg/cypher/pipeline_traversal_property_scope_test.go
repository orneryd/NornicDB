package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineTraversalSourcePropertyScope(t *testing.T) {
	patterns := []string{
		"(a:ScopeStart {k: [11,17][i]})-[r:Scope]->(:ScopeEnd)",
		"(:ScopeStart)-[r:Scope {k: [11,17][i]}]->(:ScopeEnd)",
		"(:ScopeStart)-[r:Scope]->(:ScopeEnd {k: [11,17][i]})",
	}
	for _, explicit := range []bool{false, true} {
		for _, pattern := range patterns {
			t.Run(pattern+map[bool]string{false: "/autocommit", true: "/transaction"}[explicit], func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "traversal_scope"))
				ctx := withExpressionFailureSlot(context.Background())
				input := func(func(pipelineRow) bool) bool {
					t.Fatal("operator selection must not consume input")
					return false
				}
				_, supported, err := exec.pipelineTraversalSource(ctx, input, "MATCH "+pattern, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, readTail: []string{"RETURN r.k"}})
				require.NoError(t, err)
				require.False(t, supported)
				require.NoError(t, getExpressionFailure(ctx), "admission must not evaluate an unbound row expression")

				_, err = exec.Execute(ctx, "CREATE (:ScopeStart {k:11})-[:Scope {k:11}]->(:ScopeEnd {k:11}), (:ScopeStart {k:17})-[:Scope {k:17}]->(:ScopeEnd {k:17})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				for repeat := 0; repeat < 2; repeat++ {
					result, err := exec.Execute(ctx, "UNWIND [0,1] AS i MATCH "+pattern+" RETURN r.k AS k ORDER BY k", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(11)}, {int64(17)}}, result.Rows)
				}
				if explicit {
					_, err = exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}
