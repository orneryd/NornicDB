package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSplitUnwindCompoundMutationStages_AdditionalRejections(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "with_clause_not_param",
			query: "MERGE (a:Node {id: row.id}) WITH rows AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "different_param_name",
			query: "MERGE (a:Node {id: row.id}) WITH $items AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "missing_first_as",
			query: "MERGE (a:Node {id: row.id}) WITH $rows rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "missing_unwind_keyword",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows WITH rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "alias_mismatch",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND other AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "missing_second_as",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "stage_var_mismatch",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows AS item MERGE (b:Node {id: row.id})",
		},
		{
			name:  "empty_stage_before_with",
			query: "WITH $rows AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "empty_last_stage",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows AS row",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages, ok := splitUnwindCompoundMutationStages(tt.query, "rows", "row")
			require.False(t, ok)
			require.Nil(t, stages)
		})
	}
}

func TestUnwindLookupSetHasUniqueAnchor_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_lookup_unique_anchor_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT uniq_user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE", nil)
	require.NoError(t, err)

	schema := store.GetSchema()
	require.NotNil(t, schema)

	require.True(t, unwindLookupSetHasUniqueAnchor(schema, unwindMergeChainLookupPlan{
		labels:           []string{"User"},
		matchAssignments: []unwindSimpleSetAssignment{{prop: "id", expr: "row.id"}},
	}))

	require.False(t, unwindLookupSetHasUniqueAnchor(schema, unwindMergeChainLookupPlan{
		labels:           []string{"User"},
		matchAssignments: []unwindSimpleSetAssignment{{prop: "name", expr: "row.name"}},
	}))

	require.False(t, unwindLookupSetHasUniqueAnchor(schema, unwindMergeChainLookupPlan{
		anyLabel:         true,
		labels:           []string{"User", "Admin"},
		matchAssignments: []unwindSimpleSetAssignment{{prop: "id", expr: "row.id"}},
	}))
}

func TestParseUnwindMergeChainPattern_AdditionalRejections(t *testing.T) {
	plan := parseUnwindMergeChainPattern(t.Context(), "MERGE (n:Node {id: row.id}) CALL db.labels()")
	require.False(t, plan.supported)

	plan = parseUnwindMergeChainPattern(t.Context(), "MERGE (a:Node {id: row.id}) MERGE (a)-[:REL]->(b)")
	require.False(t, plan.supported)

	// Relationship SET requires a relationship variable.
	plan = parseUnwindMergeChainPattern(t.Context(), "MERGE (a:Node {id: row.a}) MERGE (b:Node {id: row.b}) MERGE (a)-[:REL]->(b) SET r.k = row.k")
	require.False(t, plan.supported)

	// Supported mixed lookup + merge + rel chain.
	plan = parseUnwindMergeChainPattern(t.Context(), `
MATCH (u:User {id: row.user_id})
MERGE (p:Post {id: row.post_id})
SET p.title = row.title
MERGE (u)-[r:AUTHORED]->(p)
SET r.source = row.source
`)
	require.True(t, plan.supported)
	require.GreaterOrEqual(t, len(plan.steps), 3)
}
