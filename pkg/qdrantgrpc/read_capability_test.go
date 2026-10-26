package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestServerFullSnapshotDiscoversDecoratedNamespaces(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	for _, namespace := range []string{"tenant", "other"} {
		_, err := storage.NewNamespacedEngine(base, namespace).CreateNode(&storage.Node{ID: "row"})
		require.NoError(t, err)
	}
	for name, engine := range map[string]storage.Engine{
		"direct": base,
		"T":      storage.NewTracedEngine(base),
		"WT":     storage.NewWALEngine(storage.NewTracedEngine(base), nil),
	} {
		t.Run(name, func(t *testing.T) {
			seen := make(map[string]bool)
			cfg := DefaultConfig()
			cfg.DatabaseAccessModeResolver = func([]string) auth.DatabaseAccessMode { return auth.FullDatabaseAccessMode }
			cfg.ResolvedAccessResolver = func(_ []string, database string) auth.ResolvedAccess {
				seen[database] = true
				return auth.ResolvedAccess{Read: true, Write: true}
			}
			// A generic collection store exercises storage-backed discovery rather
			// than the database-manager shortcut of databaseCollectionStore.
			collections := struct{ CollectionStore }{newTestCollectionStore(t)}
			server, err := NewServer(cfg, collections, engine, nil, nil)
			require.NoError(t, err)
			require.NoError(t, server.AllowAllDatabaseAccess(context.Background()))
			require.Equal(t, map[string]bool{"tenant": true, "other": true}, seen)
		})
	}
}
