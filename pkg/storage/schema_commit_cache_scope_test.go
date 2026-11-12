package storage

// Schema-only publications must preserve entity caches on both persistence
// routes while still publishing the new definition (NOR-407).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaTransactionCommitKeepsWarmCaches(t *testing.T) {
	engine := warmCacheFixture(t)
	before := snapshotWarmCaches(engine)

	schema := engine.GetSchemaForNamespace("caches")
	require.NotNil(t, schema)
	tx, err := schema.BeginSchemaTransaction(t.Context(), engine.BeginTransaction)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("caches"))
	view, err := tx.Schema(OrdinarySchemaChanges)
	require.NoError(t, err)
	require.NoError(t, view.AddPropertyIndex("nor407_keep_v", "Keep", []string{"v"}))
	require.NoError(t, tx.StageSchemaChanges())
	require.NoError(t, tx.Commit())

	requireWarmCachesUnchanged(t, before, snapshotWarmCaches(engine))

	// The commit published the definition, so "kept the caches" is not the
	// same as "wrote nothing".
	stored, ok := engine.GetSchemaForNamespace("caches").GetCompositeIndex("nor407_keep_v")
	require.True(t, ok, "the schema transaction published no index definition")
	require.Equal(t, []string{"v"}, stored.Properties)
}

func TestSchemaDefinitionPersistKeepsWarmCaches(t *testing.T) {
	engine := warmCacheFixture(t)
	before := snapshotWarmCaches(engine)

	schema := engine.GetSchemaForNamespace("caches")
	require.NotNil(t, schema)
	require.NoError(t, schema.AddPropertyIndex("nor407_live_keep_v", "Keep", []string{"v"}))

	requireWarmCachesUnchanged(t, before, snapshotWarmCaches(engine))

	stored, ok := engine.GetSchemaForNamespace("caches").GetCompositeIndex("nor407_live_keep_v")
	require.True(t, ok, "the live schema manager published no index definition")
	require.Equal(t, []string{"v"}, stored.Properties)
}
