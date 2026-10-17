package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func newContainedSchema(t *testing.T, engine *BadgerEngine) (*SchemaManager, *ContainedIndex) {
	t.Helper()
	sm := engine.GetSchemaForNamespace("db")
	require.NoError(t, sm.AddContainedIndex("identity_keys", "Identity", []string{"name_key", "alias_keys"}))
	idx, ok := sm.GetContainedIndex("identity_keys")
	require.True(t, ok)
	require.False(t, idx.Filled(), "a new index waits for its backfill")
	require.NoError(t, sm.BackfillContainedIndex("identity_keys", nil))
	require.True(t, idx.Filled())
	return sm, idx
}

// Node writes keep a contained-key index current through the public write
// paths, direct and in explicit transactions: creates file their keys,
// updates move them, a removed label and a delete take them out, and other
// labels never enter.
func TestContainedIndexMaintainedOnWrite(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			engine := newTestEngine(t)
			_, idx := newContainedSchema(t, engine)
			write := func(inTx func(*BadgerTransaction) error, direct func() error) {
				t.Helper()
				require.NoError(t, writeMaybeInTransaction(engine, explicit, inTx, direct))
			}
			create := func(node *Node) {
				write(func(tx *BadgerTransaction) error { _, err := tx.CreateNode(node); return err },
					func() error { _, err := engine.CreateNode(node); return err })
			}
			update := func(node *Node) {
				write(func(tx *BadgerTransaction) error { return tx.UpdateNode(node) }, func() error { return engine.UpdateNode(node) })
			}
			create(&Node{ID: "db:a", Labels: []string{"Identity"}, Properties: map[string]any{"name_key": "acme", "alias_keys": []any{"acme corp", "ac"}}})
			create(&Node{ID: "db:b", Labels: []string{"Identity"}, Properties: map[string]any{"name_key": "globex"}})
			create(&Node{ID: "db:c", Labels: []string{"Doc"}, Properties: map[string]any{"name_key": "acme"}})

			text := "the acme corp office, not globex"
			require.Equal(t, []ContainedMatch{{"db:a", "ac"}, {"db:a", "acme"}, {"db:a", "acme corp"}, {"db:b", "globex"}}, idx.Lookup(text))

			update(&Node{ID: "db:a", Labels: []string{"Identity"}, Properties: map[string]any{"name_key": "acme", "alias_keys": []any{"initech"}}})
			require.Equal(t, []ContainedMatch{{"db:a", "acme"}, {"db:b", "globex"}}, idx.Lookup(text))

			update(&Node{ID: "db:a", Labels: []string{"Other"}, Properties: map[string]any{"name_key": "acme"}})
			require.Equal(t, []ContainedMatch{{"db:b", "globex"}}, idx.Lookup(text))

			write(func(tx *BadgerTransaction) error { return tx.DeleteNode("db:b") }, func() error { return engine.DeleteNode("db:b") })
			require.Empty(t, idx.Lookup(text))

			update(&Node{ID: "db:c", Labels: []string{"Doc", "Identity"}, Properties: map[string]any{"name_key": "acme"}})
			require.Equal(t, []NodeID{"db:c"}, idx.LookupNodes(text))
		})
	}
}

// The index is a schema object like the others: listed by GetIndexes and
// counted, persisted in the definition, dropped by name.
func TestContainedIndexSchemaDefinition(t *testing.T) {
	engine := newTestEngine(t)
	sm, _ := newContainedSchema(t, engine)

	var listed map[string]interface{}
	for _, raw := range sm.GetIndexes() {
		if entry, ok := raw.(map[string]interface{}); ok && entry["name"] == "identity_keys" {
			listed = entry
		}
	}
	require.Equal(t, map[string]interface{}{
		"name": "identity_keys", "type": "CONTAINED", "labels": []string{"Identity"}, "properties": []string{"name_key", "alias_keys"},
	}, listed)
	indexes, _ := sm.SchemaObjectCounts()
	require.Equal(t, 1, indexes)

	def := sm.ExportDefinition()
	require.Equal(t, []SchemaContainedIndexDef{{Name: "identity_keys", Label: "Identity", Properties: []string{"name_key", "alias_keys"}}}, def.ContainedIndexes)
	restored := NewSchemaManager()
	require.NoError(t, restored.ReplaceFromDefinition(def))
	loaded, ok := restored.GetContainedIndex("identity_keys")
	require.True(t, ok)
	require.False(t, loaded.Filled(), "a loaded index is filled by the open-time rebuild")
	require.Equal(t, def, restored.ExportDefinition())

	require.NoError(t, sm.DropIndex("identity_keys"))
	_, ok = sm.GetContainedIndex("identity_keys")
	require.False(t, ok)
	require.Empty(t, sm.ExportDefinition().ContainedIndexes)
}

// Reopening a database rebuilds the index from the stored nodes.
func TestContainedIndexRebuiltAtOpen(t *testing.T) {
	dir := t.TempDir()
	open := func() *BadgerEngine {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
		require.NoError(t, err)
		return engine
	}
	engine := open()
	newContainedSchema(t, engine)
	_, err := engine.CreateNode(&Node{ID: "db:a", Labels: []string{"Identity"}, Properties: map[string]any{"name_key": "acme", "alias_keys": []any{"ac"}}})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	engine = open()
	t.Cleanup(func() { _ = engine.Close() })
	idx, ok := engine.GetSchemaForNamespace("db").GetContainedIndex("identity_keys")
	require.True(t, ok)
	require.True(t, idx.Filled())
	require.Equal(t, []ContainedMatch{{"db:a", "ac"}, {"db:a", "acme"}}, idx.Lookup("acme"))
}
