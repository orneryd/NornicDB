package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Lookup indexes persist: a dropped one stays dropped, and a schema written
// before they were persisted reads back with the defaults.
func TestLookupIndexesPersist(t *testing.T) {
	sm := NewSchemaManager()
	name, ok := sm.LookupIndexName(ConstraintEntityNode)
	require.True(t, ok)
	require.Equal(t, DefaultNodeLookupIndexName, name)
	require.NoError(t, sm.DropIndex(DefaultRelationshipLookupIndexName))

	def := sm.ExportDefinition()
	require.NotNil(t, def.LookupIndexes)
	restored := NewSchemaManager()
	require.NoError(t, restored.ReplaceFromDefinition(def))
	_, ok = restored.LookupIndexName(ConstraintEntityRelationship)
	require.False(t, ok, "a dropped lookup index stays dropped")
	_, ok = restored.LookupIndexName(ConstraintEntityNode)
	require.True(t, ok)

	old := &SchemaDefinition{Version: schemaDefinitionVersion}
	require.NoError(t, restored.ReplaceFromDefinition(old))
	for _, entityType := range []ConstraintEntityType{ConstraintEntityNode, ConstraintEntityRelationship} {
		_, ok = restored.LookupIndexName(entityType)
		require.True(t, ok, "an older schema has the default %s lookup index", entityType)
	}

	require.Error(t, restored.AddLookupIndex("x", ConstraintEntityNode))
	require.NoError(t, restored.DropIndex(DefaultNodeLookupIndexName))
	require.NoError(t, restored.AddLookupIndex("", ConstraintEntityNode))
	name, _ = restored.LookupIndexName(ConstraintEntityNode)
	require.Equal(t, DefaultNodeLookupIndexName, name)

	failing := NewSchemaManager()
	failing.persist = func(*SchemaDefinition) error { return errPersistForTest }
	require.ErrorIs(t, failing.DropIndex(DefaultNodeLookupIndexName), errPersistForTest)
	_, ok = failing.LookupIndexName(ConstraintEntityNode)
	require.True(t, ok, "a failed persist keeps the index")
	failing.persist = nil
	require.NoError(t, failing.DropIndex(DefaultNodeLookupIndexName))
	failing.persist = func(*SchemaDefinition) error { return errPersistForTest }
	require.ErrorIs(t, failing.AddLookupIndex("", ConstraintEntityNode), errPersistForTest)
	_, ok = failing.LookupIndexName(ConstraintEntityNode)
	require.False(t, ok, "a failed persist doesn't add the index")
}

var errPersistForTest = errors.New("persist failed")

// nonLookupIndexes is indexes without the token lookup indexes every schema
// starts with, for tests about the other index kinds.
func nonLookupIndexes(indexes []interface{}) []interface{} {
	out := make([]interface{}, 0, len(indexes))
	for _, idx := range indexes {
		if m, ok := idx.(map[string]interface{}); ok && m["type"] == "LOOKUP" {
			continue
		}
		out = append(out, idx)
	}
	return out
}
