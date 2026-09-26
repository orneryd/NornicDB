package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSizeTrackingEnginePassesLabelCounterAndNamespace: a database's storage
// (the size-tracking wrapper) passes the wrapped engine's label counter and
// namespace through, so the count-only MATCH fast path and the callers that
// read the database name see them (#683). Without them in the wrapped
// engine, the namespace is "" and the label is counted.
func TestSizeTrackingEnginePassesLabelCounterAndNamespace(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()
	require.NoError(t, mgr.CreateDatabase("tracked"))
	store, err := mgr.GetStorage("tracked")
	require.NoError(t, err)
	tracker, ok := store.(*sizeTrackingEngine)
	require.True(t, ok, "%T", store)
	require.Equal(t, "tracked", tracker.Namespace())

	for _, id := range []storage.NodeID{"n1", "n2", "n3"} {
		_, err := tracker.CreateNode(&storage.Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, tracker.DeleteNode("n2"))
	inner, ok := tracker.Engine.(interface{ NodeCountByLabel(string) (int64, error) })
	require.True(t, ok)
	want, err := inner.NodeCountByLabel("L")
	require.NoError(t, err)
	require.EqualValues(t, 2, want)
	count, err := tracker.NodeCountByLabel("L")
	require.NoError(t, err)
	require.Equal(t, want, count)

	// A wrapped engine with neither capability.
	plain := newSizeTrackingEngine(&sizeTrackingStreamingInner{Engine: tracker.Engine}, &DatabaseManager{}, "plain").(*sizeTrackingEngine)
	require.Equal(t, "", plain.Namespace())
	count, err = plain.NodeCountByLabel("L")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}
