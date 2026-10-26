package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReadMetadataCapabilitiesRetainNamespace(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	tenant := storage.NewNamespacedEngine(base, "tenant")
	other := storage.NewNamespacedEngine(base, "other")
	_, err := tenant.CreateNode(&storage.Node{ID: "row", Properties: map[string]interface{}{"known": int64(1)}})
	require.NoError(t, err)
	_, err = other.CreateNode(&storage.Node{ID: "row", Properties: map[string]interface{}{"foreignOnly": int64(1)}})
	require.NoError(t, err)
	for name, engine := range map[string]storage.Engine{
		"direct": base,
		"T":      storage.NewTracedEngine(base),
		"WT":     storage.NewWALEngine(storage.NewTracedEngine(base), nil),
	} {
		t.Run(name, func(t *testing.T) {
			view := storage.NewNamespacedEngine(engine, "tenant")
			require.True(t, view.PropertyKeyKnown("known"), "known keys must survive instrumentation")
			require.False(t, view.PropertyKeyKnown("foreignOnly"), "another namespace cannot supply its keys")
			require.False(t, view.PropertyKeyKnown("absent"))
		})
	}
	t.Run("wal_forwarder", func(t *testing.T) {
		wal := storage.NewWALEngine(storage.NewTracedEngine(base), nil)
		require.True(t, wal.PropertyKeyKnownInNamespace("tenant", "known"))
		require.False(t, wal.PropertyKeyKnownInNamespace("tenant", "foreignOnly"))
	})
}
