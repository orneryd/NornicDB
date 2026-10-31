package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFulltextIndexRejectsUnknownEntityKindWithoutPublication(t *testing.T) {
	schema := NewSchemaManager()
	persisted := 0
	schema.SetPersister(func(*SchemaDefinition) error { persisted++; return nil })
	require.ErrorIs(t, schema.AddFulltextIndex("invalid", []string{"Label"}, []string{"name"}, ConstraintEntityType("invalid")), ErrInvalidData)
	require.Empty(t, schema.ExportDefinition().FulltextIndexes)
	require.Zero(t, persisted)
}
