package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func relationshipPlanOperator(op *PlanOperator, kind string) *PlanOperator {
	if op == nil {
		return nil
	}
	if op.OperatorType == kind {
		return op
	}
	for _, child := range op.Children {
		if found := relationshipPlanOperator(child, kind); found != nil {
			return found
		}
	}
	return nil
}

// Input scope belongs to the normal Cypher owners. Plans must describe the
// seek actually selected through those owners, irrespective of how a key
// entered the row; scanning the query for UNWIND aliases cannot establish it.
func TestRelationshipPropertySeekScopePlans(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		record      bool
	}{
		{"parameter", "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p", false},
		{"with_parameter", "WITH $x AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", false},
		{"with_literal", "WITH 'x' AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", false},
		{"with_renamed", "WITH $x AS initial WITH initial AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", false},
		{"with_star", "WITH $x AS k WITH * MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", false},
		{"with_map", "WITH {value:$x} AS k MATCH ()-[r:T]->() WHERE r.p=k.value RETURN r.p", false},
		{"unwind", "UNWIND [$x] AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", false},
		{"unwind_with", "UNWIND [$x] AS initial WITH initial AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", false},
		{"call_scoped_import", "WITH $x AS k CALL (k) { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p AS value } RETURN value", false},
		{"call_with_import", "WITH $x AS k CALL { WITH k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p AS value } RETURN value", false},
		{"call_star_import", "WITH $x AS k CALL (*) { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p AS value } RETURN value", false},
		{"incoming_node", "MATCH (a:Key) WITH a MATCH ()-[r:T]->() WHERE r.p=a.p RETURN r.p", false},
		{"pipeline_record", "MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope_plan"))
			for _, query := range []string{
				"CREATE (:Key {p:'x'})-[:T {p:'x'}]->()",
				"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)",
			} {
				_, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
			}
			if tc.record {
				exec.fabricRecordBindings = map[string]interface{}{"k": "x"}
			}
			params := map[string]interface{}{"x": "x"}
			ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
			result, err := exec.Execute(ctx, tc.query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"x"}}, result.Rows)
			profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
			executed := profile.seeks["r\x00rel_p"]
			require.NotNil(t, executed, "public query did not execute a seek")
			require.Equal(t, int64(1), executed.lookups)
			require.Equal(t, int64(1), executed.candidates)
			require.Equal(t, int64(1), executed.rows)
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				t.Run(mode, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), mode+" "+tc.query, params)
					require.NoError(t, err)
					plan := result.Metadata["plan"].(*ExecutionPlan)
					seek := relationshipPlanOperator(plan.Root, "RelationshipIndexSeek")
					require.NotNil(t, seek, "plan differs from executed operator: %s", planStringFromResult(t, result))
					require.Equal(t, "rel_p", seek.Arguments["index"])
					require.Nil(t, relationshipPlanOperator(seek, "Expand"), "a pure seek must not report a scan")
					if mode == "PROFILE" {
						require.Equal(t, [][]interface{}{{"x"}}, result.Rows)
						require.Equal(t, executed.lookups, seek.Arguments["indexLookups"])
						require.Equal(t, executed.candidates, seek.Arguments["relationshipCandidates"])
						require.Equal(t, executed.rows, seek.ActualRows)
						require.Equal(t, int64(2), seek.DBHits)
						require.Equal(t, plan.TotalDBHits, plan.Root.DBHits)
					}
				})
			}
		})
	}
}

func TestRelationshipPropertySeekContextBindingPlan(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "context_plan"))
	for _, query := range []string{"CREATE ()-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	exec.fabricRecordBindings = map[string]interface{}{"k": "x"}
	ctx := withValueBindings(context.Background(), map[string]interface{}{"k": "x"})
	for _, mode := range []string{"EXPLAIN", "PROFILE"} {
		result, err := exec.Execute(ctx, mode+" MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p", nil)
		require.NoError(t, err)
		plan := result.Metadata["plan"].(*ExecutionPlan)
		require.NotNil(t, relationshipPlanOperator(plan.Root, "RelationshipIndexSeek"))
		if mode == "PROFILE" {
			require.Equal(t, [][]interface{}{{"x"}}, result.Rows)
		}
	}
}

func TestRelationshipPropertySeekPlanScanControls(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scan_plan"))
	for _, query := range []string{"CREATE ()-[:T {p:'x', same:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, query := range []string{
		"WITH 'x' AS k MATCH ()-[r:T]->() WHERE r.p IS NOT NULL RETURN r.p",
		"MATCH ()-[r:T]->() WHERE r.p=r.same RETURN r.p",
		"MATCH ()-[s:T]->() WHERE s.p IS NOT NULL WITH s MATCH ()-[r:T]->() WHERE r.p='x' RETURN r.p",
	} {
		for _, mode := range []string{"EXPLAIN", "PROFILE"} {
			result, err := exec.Execute(context.Background(), mode+" "+query, nil)
			require.NoError(t, err)
			plan := result.Metadata["plan"].(*ExecutionPlan)
			require.Nil(t, relationshipPlanOperator(plan.Root, "RelationshipIndexSeek"), query)
			require.NotNil(t, relationshipPlanOperator(plan.Root, "Expand"), query)
		}
	}
}
