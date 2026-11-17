package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProfileRelationshipPropertyLookupObservesAllOperators(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			"CREATE CONSTRAINT relation_identity FOR ()-[r:PROFILE_LINK]-() REQUIRE r.id IS UNIQUE",
			"CREATE (a:ProfileEndpoint {id:'a'}), (b:ProfileEndpoint {id:'b'}), (a)-[:PROFILE_LINK {id:'target'}]->(b)")
		result, err := exec.Execute(ctx, "PROFILE MATCH (a)-[r:PROFILE_LINK {id:$id}]->(b) RETURN r.id, a.id, b.id", map[string]any{"id": "target"})
		require.NoError(t, err)
		require.Equal(t, [][]any{{"target", "a", "b"}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		var visit func(*PlanOperator)
		visit = func(operator *PlanOperator) {
			require.True(t, operator.DBHitsObserved, "operator %s must report actual reads", operator.OperatorType)
			require.True(t, operator.RowsObserved, "operator %s must report actual rows", operator.OperatorType)
			for _, child := range operator.Children {
				visit(child)
			}
		}
		visit(plan.Root)
	})
}
