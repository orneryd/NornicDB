package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Embedded USE selects another namespace without a database manager. Plan
// annotation must use that same selection, including when a body reads no graph.
func TestRelationshipAccessEmbeddedGraphSelectionPublic(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "embedded_source"))
	exec.cache = nil
	ctx := context.Background()
	for _, query := range []string{
		"CREATE ()-[:S]->()",
		"USE embedded_target CREATE (:A)-[:T {p:'x'}]->(:B)",
		"USE embedded_target CREATE RANGE INDEX embedded_p FOR ()-[r:T]-() ON (r.p)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	for _, tc := range []struct {
		name, query string
		columns     []string
		rows        [][]interface{}
		seek        bool
	}{
		{"empty_body", "CALL { USE embedded_target UNWIND [] AS before RETURN * } RETURN *", []string{"before"}, [][]interface{}{}, false},
		{"value_body", "CALL { USE embedded_target UNWIND [1] AS before RETURN before } RETURN *", []string{"before"}, [][]interface{}{{int64(1)}}, false},
		{"relationship_body", "CALL { USE embedded_target MATCH ()-[r:T {p:'x'}]->() RETURN r.p AS p } RETURN p", []string{"p"}, [][]interface{}{{"x"}}, true},
		{"leading_use", "USE embedded_target MATCH ()-[r:T {p:'x'}]->() RETURN r.p AS p", []string{"p"}, [][]interface{}{{"x"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
				result, err := exec.Execute(ctx, prefix+tc.query, nil)
				require.NoError(t, err, prefix+tc.query)
				require.Equal(t, tc.columns, result.Columns)
				if prefix != "EXPLAIN " {
					require.Equal(t, tc.rows, result.Rows)
				}
				if prefix != "" {
					plan := result.Metadata["plan"].(*ExecutionPlan)
					require.Equal(t, tc.seek, relationshipPlanOperator(plan.Root, "RelationshipIndexSeek") != nil, prefix+tc.query)
				}
			}
		})
	}
	t.Run("removed_nested_source", func(t *testing.T) {
		const query = "MATCH ()-[s:S]->() CALL { USE embedded_target MATCH ()-[r:T {p:'x'}]->() RETURN r.p AS p } RETURN p"
		original := relationshipProgramFor(query)
		removed := *original
		removed.byID = make(map[string]*relationshipAccessTemplate)
		for id, template := range original.byID {
			if template.syntax.relType != "T" {
				removed.byID[id] = template
			}
		}
		require.NotEmpty(t, removed.byID, "retain the outer source while removing only the nested relationship site")
		selected := context.WithValue(ctx, relationshipAccessProgramKey{}, &removed)
		for _, prefix := range []string{"PROFILE ", "EXPLAIN "} {
			result, err := exec.Execute(selected, prefix+query, nil)
			require.NoError(t, err, prefix+query)
			if prefix == "PROFILE " {
				require.Equal(t, [][]interface{}{{"x"}}, result.Rows)
			}
			plan := result.Metadata["plan"].(*ExecutionPlan)
			require.Nil(t, relationshipPlanOperator(plan.Root, "RelationshipIndexSeek"), "a fragment's cached spelling cannot authorize its removed source")
		}
	})
}
