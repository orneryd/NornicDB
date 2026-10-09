package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The server's per-database wrapper forwards the property-key lookup (#907).
func TestSizeTrackingEngineForwardsPropertyKeyLookup(t *testing.T) {
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	tracked := &sizeTrackingEngine{Engine: storage.NewNamespacedEngine(inner, "db")}
	require.False(t, tracked.PropertyKeyKnown("k"))
	inner.NotePropertyKeysInNamespace("db", map[string]interface{}{"k": int64(1)})
	require.True(t, tracked.PropertyKeyKnown("k"))
	require.False(t, (&sizeTrackingEngine{Engine: inner}).PropertyKeyKnown("k"))
}
