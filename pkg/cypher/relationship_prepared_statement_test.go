package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An admitted prepared statement owns its immutable source independently of
// eviction/replacement in the syntax interning cache. Explicit caller source
// overrides remain covered by TestRelationshipAccessProgramIsSharedSource.
func TestPreparedRelationshipSourceSurvivesSyntaxCacheEviction(t *testing.T) {
	for _, mode := range []string{"PROFILE", "EXPLAIN"} {
		t.Run(mode, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prepared_source"))
			for _, query := range []string{
				"CREATE ()-[:PreparedOwnerSeek {p:'x'}]->(), ()-[:PreparedOwnerSeek {p:'y'}]->()",
				"CREATE RANGE INDEX prepared_owner_p FOR ()-[r:PreparedOwnerSeek]-() ON (r.p)",
			} {
				_, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
			}
			query := "MATCH ()-[r:PreparedOwnerSeek]->() WHERE r.p=$p RETURN r.p"
			source := relationshipProgramFor(query)
			require.NotEmpty(t, source.byID)
			t.Cleanup(func() { relationshipAccessPrograms.put(query, source) })
			first, err := exec.Execute(context.Background(), mode+" "+query, map[string]interface{}{"p": "x", "nonce": 0})
			require.NoError(t, err)
			require.NotNil(t, relationshipPlanOperator(first.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek"))
			// Replace the interning entry rather than mutate the admitted immutable
			// object. Warm plans must retain their original source and access sites.
			relationshipAccessPrograms.put(query, &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}})
			second, err := exec.Execute(context.Background(), mode+" "+query, map[string]interface{}{"p": "y", "nonce": 1})
			require.NoError(t, err)
			require.NotNil(t, relationshipPlanOperator(second.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek"))
			if mode == "PROFILE" {
				require.Equal(t, [][]interface{}{{"y"}}, second.Rows)
			} else {
				require.Empty(t, second.Rows)
			}
		})
	}
}
