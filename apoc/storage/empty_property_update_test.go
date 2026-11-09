package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExplicitUpdatesInitializeAbsentPropertyBagsAndOwnValues(t *testing.T) {
	store := NewInMemoryStorage()
	a, err := store.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	b, err := store.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	rel, err := store.CreateRelationship(t.Context(), a.ID, b.ID, "LINK", nil)
	require.NoError(t, err)
	for _, entity := range []string{"node", "relationship"} {
		t.Run(entity, func(t *testing.T) {
			values := map[string]interface{}{"nested": []string{"accepted"}}
			if entity == "node" {
				require.NoError(t, store.UpdateNode(t.Context(), a.ID, values))
				values["nested"].([]string)[0] = "caller changed"
				got, err := store.GetNode(t.Context(), a.ID)
				require.NoError(t, err)
				require.Equal(t, []string{"accepted"}, got.Properties["nested"])
			} else {
				require.NoError(t, store.UpdateRelationship(t.Context(), rel.ID, values))
				values["nested"].([]string)[0] = "caller changed"
				got, err := store.GetRelationship(t.Context(), rel.ID)
				require.NoError(t, err)
				require.Equal(t, []string{"accepted"}, got.Properties["nested"])
			}
		})
	}
}
