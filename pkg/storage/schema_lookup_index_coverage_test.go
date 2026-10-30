package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLookupIndexNameTakenByAnyIndexKind: a lookup index can't take the
// name of an index of any kind.
func TestLookupIndexNameTakenByAnyIndexKind(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.DropIndex(DefaultNodeLookupIndexName))
	require.NoError(t, sm.AddCompositeIndex("composite_idx", "Person", []string{"a", "b"}))
	require.NoError(t, sm.AddFulltextIndex("fulltext_idx", []string{"Person"}, []string{"bio"}))
	require.NoError(t, sm.AddVectorIndex("vector_idx", "Person", "embedding", 3, "cosine"))
	require.NoError(t, sm.AddRangeIndex("range_idx", "Person", "age"))
	for _, name := range []string{"composite_idx", "fulltext_idx", "vector_idx", "range_idx", DefaultRelationshipLookupIndexName} {
		err := sm.AddLookupIndex(name, ConstraintEntityNode)
		require.Error(t, err, name)
		require.Contains(t, err.Error(), name)
		_, ok := sm.LookupIndexName(ConstraintEntityNode)
		require.False(t, ok, name)
	}
	require.NoError(t, sm.AddLookupIndex("free_name", ConstraintEntityNode))
	name, ok := sm.LookupIndexName(ConstraintEntityNode)
	require.True(t, ok)
	require.Equal(t, "free_name", name)
}
