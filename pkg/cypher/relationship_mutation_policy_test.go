package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// The common shape owner must preserve CREATE's direction requirement while
// retaining MERGE's supported undirected relationship construction publicly.
func TestRelationshipMutationPublicPurposePolicy(t *testing.T) {
	for _, tc := range []struct {
		query, detail string
		edges         int64
	}{
		{"CREATE ()-[:LINK]->()", "", 1},
		{"MERGE ()-[:LINK]-()", "", 1},
		{"CREATE ()-[:LINK]-()", "RequiresDirectedRelationship", 0},
		{"CREATE ()<-[:LINK]->()", "RequiresDirectedRelationship", 0},
		{"CREATE ()-[:FIRST|SECOND]->()", "NoSingleRelationshipType", 0},
		{"MERGE ()-[:FIRST|SECOND]-()", "NoSingleRelationshipType", 0},
	} {
		t.Run(tc.query, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "mutation-policy")
			executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			_, err := executor.Execute(t.Context(), tc.query, nil)
			if tc.detail == "" {
				require.NoError(t, err)
			} else {
				requireSemanticDetail(t, err, tc.detail)
			}
			count, err := store.EdgeCount()
			require.NoError(t, err)
			require.Equal(t, tc.edges, count)
			if tc.detail != "" {
				count, err := store.NodeCount()
				require.NoError(t, err)
				require.Zero(t, count, "rejected mutation must not create endpoints")
			}
		})
	}
}
