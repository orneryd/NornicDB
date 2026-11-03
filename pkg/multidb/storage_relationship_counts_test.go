package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSizeTrackingNamespaceRelationshipCounts(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	manager, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	for _, namespace := range []string{"a", "b"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: namespace}))
		view, err := manager.GetStorage(namespace)
		require.NoError(t, err)
		_, err = view.CreateNode(t.Context(), &storage.Node{ID: "start", Labels: []string{"Start"}})
		require.NoError(t, err)
		_, err = view.CreateNode(t.Context(), &storage.Node{ID: "end", Labels: []string{"End"}})
		require.NoError(t, err)
		require.NoError(t, view.CreateEdge(t.Context(), &storage.Edge{ID: "edge", StartNode: "start", EndNode: "end", Type: "T"}))
	}
	view, err := manager.GetStorage("a")
	require.NoError(t, err)
	counter, ok := view.(storage.NamespaceEdgeTypeStatsProvider)
	require.True(t, ok)
	for _, namespace := range []string{"a", "b"} {
		n, err := counter.EdgeCountByTypeInNamespace(t.Context(), namespace, "T")
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		n, err = counter.EdgeCountByStartLabelInNamespace(t.Context(), namespace, "Start", "T")
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		n, err = counter.EdgeCountByEndLabelInNamespace(t.Context(), namespace, "End", "T")
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}
}
