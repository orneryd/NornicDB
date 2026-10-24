package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These public EXPLAIN budgets are the exact-base paired benchmark counts.
// Projecting the shared access program must not restore the temporary planning
// and per-field rendering allocations removed by the integrated correction.
func TestRelationshipAccessExplainAllocationBudget(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "explain_access_budget"))
	e.cache = nil
	ctx := context.Background()
	for _, query := range []string{"CREATE (:Key)-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := e.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	params := map[string]interface{}{"x": "x"}
	for _, tc := range []struct {
		name, query string
		budget      float64
	}{
		{"match", "EXPLAIN WITH $x AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", 122},
		{"expression", "EXPLAIN WITH $x AS k RETURN COLLECT { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p }", 123},
		{"dynamic_token", "EXPLAIN WITH $x AS k CREATE (n:$(head(COLLECT { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p }))) RETURN labels(n)", 106},
		{"all_token", "EXPLAIN CREATE (n:$all(head(COLLECT { MATCH ()-[r:T]->() WHERE r.p='x' RETURN r.p }))) RETURN labels(n)", 85},
		{"anchored", "EXPLAIN MATCH (a:Key) RETURN COUNT { MATCH (a)-[r:T]->() WHERE r.p='x' }", 138},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(100, func() {
				result, err := e.Execute(ctx, tc.query, params)
				if err != nil || result == nil || result.Metadata["plan"] == nil {
					t.Fatalf("EXPLAIN: %v, %v", result, err)
				}
			})
			t.Logf("%.0f allocations, exact-base budget %.0f", allocations, tc.budget)
			require.LessOrEqual(t, allocations, tc.budget)
		})
	}
}
