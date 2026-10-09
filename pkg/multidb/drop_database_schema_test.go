package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BUG: DROP DATABASE removed a database's nodes and edges but kept its schema.
// Recreating a database with the same name brought back every constraint and
// contract of the dropped one (and they were reloaded again after a restart).
func TestBug_DropDatabaseClearsSchema(t *testing.T) {
	addSchema := func(t *testing.T, manager *DatabaseManager) {
		t.Helper()
		store, err := manager.GetStorage("tenant_a")
		require.NoError(t, err)
		require.NoError(t, store.GetSchema().AddUniqueConstraint("person_id_unique", "Person", "id"))
		require.NoError(t, store.GetSchema().AddPropertyTypeConstraint("person_age_type", "Person", "age", storage.PropertyTypeInteger))
		_, err = store.CreateNode(&storage.Node{ID: "p1", Labels: []string{"Person"}, Properties: map[string]any{"id": "p1", "age": int64(3)}})
		require.NoError(t, err)
	}
	requireEmptySchema := func(t *testing.T, store storage.Engine) {
		t.Helper()
		require.Empty(t, store.GetSchema().GetAllConstraints())
		require.Empty(t, store.GetSchema().GetAllPropertyTypeConstraints())
		// The dropped rules no longer apply: a duplicate id and a non-integer age both write.
		_, err := store.CreateNode(&storage.Node{ID: "x1", Labels: []string{"Person"}, Properties: map[string]any{"id": "p1", "age": "old"}})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: "x2", Labels: []string{"Person"}, Properties: map[string]any{"id": "p1"}})
		require.NoError(t, err)
	}

	t.Run("drop then recreate", func(t *testing.T) {
		inner := storage.NewMemoryEngine()
		defer inner.Close()
		manager, err := NewDatabaseManager(inner, nil)
		require.NoError(t, err)
		defer manager.Close()

		require.NoError(t, manager.CreateDatabase("tenant_a"))
		require.NoError(t, manager.CreateDatabase("tenant_b"))
		addSchema(t, manager)
		other, err := manager.GetStorage("tenant_b")
		require.NoError(t, err)
		require.NoError(t, other.GetSchema().AddUniqueConstraint("keep_me", "Person", "id"))

		require.NoError(t, manager.DropDatabase("tenant_a"))
		require.NoError(t, manager.CreateDatabase("tenant_a"))
		store, err := manager.GetStorage("tenant_a")
		require.NoError(t, err)
		requireEmptySchema(t, store)

		require.Len(t, other.GetSchema().GetAllConstraints(), 1, "other databases keep their schema")
	})

	t.Run("dropped schema is not reloaded after restart", func(t *testing.T) {
		dir := t.TempDir()
		inner, err := storage.NewBadgerEngine(dir)
		require.NoError(t, err)
		manager, err := NewDatabaseManager(inner, nil)
		require.NoError(t, err)
		require.NoError(t, manager.CreateDatabase("tenant_a"))
		addSchema(t, manager)
		require.NoError(t, manager.DropDatabase("tenant_a"))
		require.NoError(t, manager.Close())
		require.NoError(t, inner.Close())

		reopened, err := storage.NewBadgerEngine(dir)
		require.NoError(t, err)
		defer reopened.Close()
		manager, err = NewDatabaseManager(reopened, nil)
		require.NoError(t, err)
		defer manager.Close()
		require.NoError(t, manager.CreateDatabase("tenant_a"))
		store, err := manager.GetStorage("tenant_a")
		require.NoError(t, err)
		requireEmptySchema(t, store)
	})
}
