package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipUniquePutCancellationPreservesExistingPosting(t *testing.T) {
	for _, arity := range []int{1, 2, 32} {
		t.Run(fmt.Sprint(arity), func(t *testing.T) {
			idx := &relationshipUniqueIndex{key: relationshipUniqueIndexKey{namespace: "atomic", relType: "LINK"}, keys: make(map[EdgeID][]interface{}), index: benchmarkCompositeIndex(arity)}
			idx.properties = append([]string(nil), idx.index.Properties...)
			old := &Edge{ID: "atomic:edge", Type: "LINK", Properties: benchmarkCompositeProperties(idx.properties, 1)}
			next := &Edge{ID: old.ID, Type: old.Type, Properties: benchmarkCompositeProperties(idx.properties, 2)}
			require.NoError(t, idx.put(t.Context(), old))
			count := &normalizationPollCounter{Context: t.Context()}
			require.NoError(t, idx.put(count, old))
			require.Greater(t, count.polls, 1)
			before := schemaIndexPostings(idx.index)
			keys := append([]interface{}(nil), idx.keys[old.ID]...)
			for _, cancelAt := range []int64{1, int64(count.polls - 1)} {
				err := idx.put(newIndexPollCancelContext(t, cancelAt), next)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, before, schemaIndexPostings(idx.index), "all postings and metadata must survive rejected replacement")
				require.Equal(t, keys, idx.keys[old.ID])
			}
			require.NoError(t, idx.put(t.Context(), next))
			oldValues, ok, err := relationshipConstraintValueKeys(t.Context(), idx.properties, old.Properties, false)
			require.NoError(t, err)
			require.True(t, ok)
			ids, err := idx.index.LookupFull(t.Context(), oldValues...)
			require.NoError(t, err)
			require.Empty(t, ids)
			newValues, ok, err := relationshipConstraintValueKeys(t.Context(), idx.properties, next.Properties, false)
			require.NoError(t, err)
			require.True(t, ok)
			ids, err = idx.index.LookupFull(t.Context(), newValues...)
			require.NoError(t, err)
			require.Equal(t, []NodeID{NodeID(next.ID)}, ids)
		})
	}
}
