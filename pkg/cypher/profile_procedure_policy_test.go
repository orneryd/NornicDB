package cypher

// The token-listing procedures read the graph themselves and report that read
// through the canonical access owner, so a PROFILE statement states the
// population each one actually read instead of calling it unknown. Unreviewed
// procedures stay opaque, and so does a replacement of a reviewed built-in
// registered through the exported API: a name carries no observation trust.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// tokenListingExecutor returns an executor over two :Person nodes joined by one
// :KNOWS relationship. Two entities are read, one label and one type are
// listed, so no record of the population can be satisfied by the row count.
func tokenListingExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec, _ := newProfileRuntimeTestExecutor(t,
		"CREATE (:Person {name: 'ada'})-[:KNOWS]->(:Person {name: 'bob'})")
	return exec
}

// tokenListingProfile runs one token listing under PROFILE through the public
// entry point and holds its access record to the population it read: what the
// statement did is observed, and the record counts entities, not the tokens the
// listing derives from them.
func tokenListingProfile(t *testing.T, exec *StorageExecutor, query, kind string, population int64) (*ExecuteResult, *ExecutionPlan) {
	t.Helper()
	result, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	plan := executionPlanFromResult(t, result)
	require.True(t, plan.TotalDBHitsObserved, "a reviewed registration reports its own reads: %s", query)
	require.Empty(t, plan.Unobserved, query)
	access := planOperatorOfType(plan.Root, kind)
	require.NotNil(t, access, "the population read is reported as a plan operator: %s", query)
	require.True(t, access.RowsObserved, query)
	require.Equal(t, population, access.ActualRows, "the record counts the entities read, not the tokens listed: %s", query)
	require.True(t, access.DBHitsObserved, query)
	require.Equal(t, population, access.DBHits, query)
	return result, plan
}

func TestProfileTokenListingProceduresReportThePopulationTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := tokenListingExecutor(t)

		labels, labelPlan := tokenListingProfile(t, exec,
			"PROFILE CALL db.labels() YIELD label RETURN label", profileKindAllNodesScan, 2)
		require.Equal(t, [][]interface{}{{"Person"}}, labels.Rows)
		require.EqualValues(t, 3, labelPlan.TotalDBHits)
		labelOrder := planOperatorOfType(labelPlan.Root, "SchemaTokenOrder")
		require.NotNil(t, labelOrder)
		require.EqualValues(t, 1, labelOrder.ActualRows)
		require.EqualValues(t, 1, labelOrder.DBHits)

		types, typePlan := tokenListingProfile(t, exec,
			"PROFILE CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
			profileKindAllRelationshipsScan, 1)
		require.Equal(t, [][]interface{}{{"KNOWS"}}, types.Rows)
		require.EqualValues(t, 2, typePlan.TotalDBHits)
		typeOrder := planOperatorOfType(typePlan.Root, "SchemaTokenOrder")
		require.NotNil(t, typeOrder)
		require.EqualValues(t, 1, typeOrder.ActualRows)
		require.EqualValues(t, 1, typeOrder.DBHits)

		// db.propertyKeys reads both populations, and neither record is its rows.
		keys, keyPlan := tokenListingProfile(t, exec,
			"PROFILE CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", profileKindAllNodesScan, 2)
		require.Contains(t, keys.Rows, []interface{}{"name"})
		relationships := planOperatorOfType(keyPlan.Root, profileKindAllRelationshipsScan)
		require.NotNil(t, relationships, "the relationship population is recorded too")
		require.True(t, relationships.RowsObserved)
		require.Equal(t, int64(1), relationships.ActualRows)
		require.EqualValues(t, 3, keyPlan.TotalDBHits, "both population reads are the statement's hits")
	})
}

// An empty graph is an observed zero, not a missing measurement: the listing
// reports the population it read, so its hit total stays certified.
func TestProfileTokenListingProcedureOnAnEmptyGraphReportsAnObservedZero(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		result, plan := tokenListingProfile(t, exec,
			"PROFILE CALL db.labels() YIELD label RETURN label", profileKindAllNodesScan, 0)
		require.Empty(t, result.Rows)
		require.EqualValues(t, 1, plan.TotalDBHits, "empty graph has zero entities but still acquires schema token ordering")
		order := planOperatorOfType(plan.Root, "SchemaTokenOrder")
		require.NotNil(t, order)
		require.Zero(t, order.ActualRows)
		require.EqualValues(t, 1, order.DBHits)
	})
}
