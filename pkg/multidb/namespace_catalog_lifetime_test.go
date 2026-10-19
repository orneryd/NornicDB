package multidb

import (
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropRejectsOldCatalogAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	b, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	m, err := NewDatabaseManager(b, nil)
	require.NoError(t, err)
	require.NoError(t, m.CreateDatabase("catalog_old"))
	sys := storage.NewNamespacedEngine(b, "system")
	old, err := sys.GetNode(metadataNodeID)
	require.NoError(t, err)
	require.NoError(t, m.DropDatabase("catalog_old"))
	// Another database's already-captured full metadata write may arrive late.
	require.NoError(t, sys.UpdateNode(old))
	require.False(t, m.Exists("catalog_old"), "late metadata must not revive a retired lifetime")
	require.NoError(t, m.Close())
	b, err = storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	m, err = NewDatabaseManager(b, nil)
	require.NoError(t, err)
	defer m.Close()
	require.False(t, m.Exists("catalog_old"), "startup must retain the publication boundary")
	require.NoError(t, m.CreateDatabase("catalog_old"))
	newID, _ := m.DatabaseIdentity("catalog_old")
	require.NotEmpty(t, newID)
	require.True(t, m.Exists("catalog_old"))
}

func TestNamespaceDropCatalogRestoreRetainsBackupLifetime(t *testing.T) {
	b := storage.NewMemoryEngine()
	m, err := NewDatabaseManager(b, nil)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.CreateDatabase("catalog_restore"))
	before, _ := m.DatabaseIdentity("catalog_restore")
	backup := filepath.Join(t.TempDir(), "backup.bak")
	require.NoError(t, b.Backup(backup))
	require.NoError(t, m.DropDatabase("catalog_restore"))
	require.NoError(t, b.Restore(backup))
	actual, _ := m.DatabaseIdentity("catalog_restore")
	require.Equal(t, before, actual, "restore publishes the backup's catalog lifetime")
	require.NoError(t, m.DropDatabase("catalog_restore"))
	require.NoError(t, m.CreateDatabase("catalog_restore"))
	after, _ := m.DatabaseIdentity("catalog_restore")
	require.NotEqual(t, before, after)
}
