package replication

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicatedEnginePreservesFulltextCommittedView(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	// No replication write is issued: this checks the reader/publication
	// capability of the local committed storage chain used by either role.
	engine := NewReplicatedEngine(base, nil, 0)
	view := storage.NewNamespacedEngine(engine, "tenant")
	require.True(t, storage.SameEventView(view, storage.NewNamespacedEngine(base, "tenant")))
	require.False(t, storage.SameEventView(view, storage.NewNamespacedEngine(base, "other")))
	require.True(t, storage.EventNotificationsSettled(view))
	require.True(t, storage.ReadPublicationSettled(view))
	_, err := base.CreateNode(t.Context(), &storage.Node{ID: "tenant:one", Properties: map[string]any{"text": "orchard"}})
	require.NoError(t, err)
	snapshot, err := storage.OpenEntityReadSnapshot(t.Context(), view, false)
	require.NoError(t, err)
	defer snapshot.Close()
	node, err := snapshot.ReadNode(t.Context(), "one")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("one"), node.ID)
	require.Equal(t, "orchard", node.Properties["text"])
}
