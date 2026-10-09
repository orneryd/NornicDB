package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Deleting a whole namespace ("db:") is how DROP DATABASE removes a database;
// it must take the namespace's schema with it. A narrower prefix inside a
// namespace ("db:sub:") only removes data and keeps the schema.
func TestDeleteByPrefix_SchemaLifecycle(t *testing.T) {
	t.Run("whole namespace drops schema in memory and on disk", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		defer engine.Close()

		old := engine.GetSchemaForNamespace("tenant")
		require.NoError(t, old.AddUniqueConstraint("u", "Person", "id"))
		require.NoError(t, engine.GetSchemaForNamespace("other").AddUniqueConstraint("u", "Person", "id"))

		_, _, err = engine.DeleteByPrefix("tenant:")
		require.NoError(t, err)

		fresh := engine.GetSchemaForNamespace("tenant")
		require.NotSame(t, old, fresh)
		require.Empty(t, fresh.GetAllConstraints())
		require.Len(t, engine.GetSchemaForNamespace("other").GetAllConstraints(), 1)

		// A stale reference to the dropped schema must not write it back.
		require.NoError(t, old.AddUniqueConstraint("late", "Person", "name"))
		require.NoError(t, engine.loadPersistedSchemas())
		require.Empty(t, engine.GetSchemaForNamespace("tenant").GetAllConstraints())
		require.Len(t, engine.GetSchemaForNamespace("other").GetAllConstraints(), 1)
	})

	t.Run("sub-prefix keeps schema", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		defer engine.Close()

		require.NoError(t, engine.GetSchemaForNamespace("tenant").AddUniqueConstraint("u", "Person", "id"))
		_, _, err = engine.DeleteByPrefix("tenant:sub:")
		require.NoError(t, err)
		require.Len(t, engine.GetSchemaForNamespace("tenant").GetAllConstraints(), 1)
	})
}
