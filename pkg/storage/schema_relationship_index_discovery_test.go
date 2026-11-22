package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipEqualityIndexPropertiesUsesCatalogAndPrefersConstraints(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddRangeIndexForEntity("ordinary", "T", []string{"id"}, ConstraintEntityRelationship))
	require.NoError(t, sm.AddRangeIndexForEntity("lexical", "T", []string{"a"}, ConstraintEntityRelationship))
	require.NoError(t, sm.AddRangeIndexForEntity("incomplete", "T", []string{"id", "missing"}, ConstraintEntityRelationship))
	require.NoError(t, sm.AddRangeIndexForEntity("node", "T", []string{"node"}, ConstraintEntityNode))
	require.NoError(t, sm.AddRangeIndexForEntity("other_type", "Other", []string{"other"}, ConstraintEntityRelationship))
	require.NoError(t, sm.AddTypedIndexForEntity(IndexKindText, "text", "T", []string{"text"}, ConstraintEntityRelationship))
	values := map[string]interface{}{"id": "x", "a": 1, "b": 2, "node": 1, "other": 1, "text": "x"}
	read := func() []string {
		properties, found, err := sm.RelationshipEqualityIndexProperties(t.Context(), "T", values)
		require.NoError(t, err)
		require.True(t, found)
		return properties
	}
	require.Equal(t, []string{"a"}, read(), "shortest complete ordinary keys use lexical property-order ties")
	require.NoError(t, sm.AddConstraint(Constraint{Name: "owned", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"id", "b"}}))
	selected := read()
	require.Equal(t, []string{"id", "b"}, selected, "a complete constraint-owned key retains priority over shorter ordinary keys")
	selected[0] = "caller mutation"
	require.Equal(t, []string{"id", "b"}, read(), "returned properties cannot mutate catalog state")
	require.NoError(t, sm.DropConstraint("owned"))
	require.Equal(t, []string{"a"}, read())
	require.NoError(t, sm.DropIndex("lexical"))
	require.Equal(t, []string{"id"}, read())
	require.NoError(t, sm.DropIndex("ordinary"))
	properties, found, err := sm.RelationshipEqualityIndexProperties(t.Context(), "T", values)
	require.NoError(t, err)
	require.False(t, found, "other entity/type/kind and incomplete keys cannot replace the dropped index")
	require.Nil(t, properties)
}

func TestRelationshipEqualityIndexPropertiesRestoredDefinitionAndCancellation(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.ReplaceFromDefinition(t.Context(), &SchemaDefinition{RangeIndexes: []SchemaRangeIndexDef{{Name: "restored", Label: "T", Property: "id", EntityType: ConstraintEntityRelationship}}}))
	properties, found, err := sm.RelationshipEqualityIndexProperties(t.Context(), "T", map[string]interface{}{"id": "x"})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"id"}, properties)
	properties, found, err = sm.RelationshipEqualityIndexProperties(t.Context(), "T", map[string]interface{}{"id": nil})
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, properties)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	properties, found, err = sm.RelationshipEqualityIndexProperties(ctx, "T", map[string]interface{}{"id": "x"})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, found)
	require.Nil(t, properties)
}
