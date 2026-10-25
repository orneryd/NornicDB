package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). A compatibility text-plan cache is not an
// admission source. Poisoning it must not change the constructor's importing
// WITH decision; both parsers exercise every mode through Execute.
func TestCanonicalBoundProjectionCacheCannotChangeAdmission(t *testing.T) {
	const clause = "WITH __held_input AS __held_alias"
	returnProjectionPlans.Lock()
	old, present := returnProjectionPlans.plans[clause]
	returnProjectionPlans.plans[clause] = &returnProjectionPlan{}
	returnProjectionPlans.Unlock()
	t.Cleanup(func() {
		returnProjectionPlans.Lock()
		defer returnProjectionPlans.Unlock()
		if present {
			returnProjectionPlans.plans[clause] = old
		} else {
			delete(returnProjectionPlans.plans, clause)
		}
	})

	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "held_projection"))
	const query = "WITH 11 AS __held_input CALL { " + clause + " RETURN __held_alias AS out } RETURN out"
	for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
		_, err := executor.Execute(context.Background(), prefix+query, nil)
		require.Error(t, err, prefix+query)
		code, detail := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, prefix)
		require.Equal(t, "InvalidVariableImport", detail, prefix)
		require.Contains(t, err.Error(), "Importing WITH must contain only simple references to outside variables", prefix)
	}
}
