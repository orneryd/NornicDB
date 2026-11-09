package search

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A refresh whose node has no pending reservation (a newer refresh, which
// read storage later, was applied) reads nothing. One superseded by a newer
// reservation still pending reads: discarding the newer one restores it.
func TestRetiredRefreshReadsNothing(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	engine := &storagefault.NodeReadSpy{Engine: store}
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "a", Properties: map[string]any{"content": "word"}})
	require.NoError(t, err)

	retired := svc.ReserveNodeMutation("a")
	require.NoError(t, svc.RefreshNode(t.Context(), "a"))
	before := engine.Full.Load()
	require.NoError(t, svc.RefreshNodeWithMutation(t.Context(), retired))
	require.Equal(t, before, engine.Full.Load(), "a retired refresh read storage")

	superseded := svc.ReserveNodeMutation("a")
	pending := svc.ReserveNodeMutation("a")
	before = engine.Full.Load()
	require.NoError(t, svc.RefreshNodeWithMutation(t.Context(), superseded))
	require.Equal(t, before+1, engine.Full.Load(), "a refresh with a newer pending one must still read")
	require.NoError(t, svc.RefreshNodeWithMutation(t.Context(), pending))
}
