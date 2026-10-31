package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationV1ToV2_ClosedDB_ErrorBranches(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	t.Run("collectBatch scan failure", func(t *testing.T) {
		_, err := engine.collectBatch(prefixNode, 10, nodeFormatTokenizedV1)
		require.Error(t, err)
	})

	t.Run("migrate nodes pass surfaces collect error", func(t *testing.T) {
		_, err := migrateV1ToV2Bodies[nodeV1ToV2Body](engine, "nodes")
		require.Error(t, err)
	})

	t.Run("migrate edges pass surfaces collect error", func(t *testing.T) {
		_, err := migrateV1ToV2Bodies[edgeV1ToV2Body](engine, "edges")
		require.Error(t, err)
	})

	t.Run("top-level migrate wraps node-pass failure", func(t *testing.T) {
		err := engine.migrateV1ToV2()
		require.Error(t, err)
		require.Contains(t, err.Error(), "rewriting nodes")
	})

	t.Run("rebuild secondary indexes drop prefix failure", func(t *testing.T) {
		_, err := engine.rebuildSecondaryIndexesForV2()
		require.Error(t, err)
	})

	t.Run("rebuild label index scan failure", func(t *testing.T) {
		err := engine.rebuildLabelIndexForV2(&v1ToV2IndexStats{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "scan nodes for index rebuild")
	})

	t.Run("rebuild edge indexes scan failure", func(t *testing.T) {
		err := engine.rebuildEdgeIndexesForV2(&v1ToV2IndexStats{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "scan edges for index rebuild")
	})
}

func TestMigrationV1ToV2_CompactAfterMigration_NilAndClosedDB(t *testing.T) {
	t.Run("nil db is no-op", func(t *testing.T) {
		engine := &BadgerEngine{}
		engine.compactAfterMigration()
	})

	t.Run("closed db hits flatten/gc error branches without panic", func(t *testing.T) {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
		if engine != nil {
			fixtureowner.Own(t, engine)
		}
		require.NoError(t, err)
		require.NoError(t, engine.Close())

		// Should not panic even when Badger returns errors.
		engine.compactAfterMigration()
	})
}
