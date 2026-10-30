package replication

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A follower applies replicated creates through the engine's ordinary
// creates, never the executor's minted-ID path: a create over a hidden
// predecessor, under a UUID-shaped ID like any minted one, is rejected and
// leaves the predecessor in place.
func TestFollowerCreateMeetsHiddenPredecessor(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
			DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
		}))
		require.NoError(t, err)
		adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = adapter.Close(); _ = base.Close() })
		hiddenNode := storage.NodeID("test:" + uuid.NewString())
		hiddenEdge := storage.EdgeID("test:" + uuid.NewString())
		for _, node := range []*storage.Node{
			{ID: "test:a", Labels: []string{"A"}},
			{ID: "test:b", Labels: []string{"B"}},
			{ID: hiddenNode, Labels: []string{"Original"}},
		} {
			_, err := base.CreateNode(node)
			require.NoError(t, err)
		}
		require.NoError(t, base.CreateEdge(&storage.Edge{ID: hiddenEdge, StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"key": "old"}}))
		require.NoError(t, base.FlushWriteBehind(nil))
		require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, string(hiddenNode), true))
		require.NoError(t, base.SetManualSuppression(storage.StoredEntityEdgeKind, string(hiddenEdge), true))

		err = adapter.ApplyCommand(context.Background(), &Command{Type: CmdCreateNode, Data: runtimeFixtureOwner.Encode(t, storage.EncodeNodePayload, &storage.Node{ID: hiddenNode, Labels: []string{"Replicated"}})})
		require.ErrorIs(t, err, storage.ErrAlreadyExists, "write-behind=%v", buffered)
		err = adapter.ApplyCommand(context.Background(), &Command{Type: CmdCreateEdge, Data: runtimeFixtureOwner.Encode(t, storage.EncodeEdgePayload, &storage.Edge{ID: hiddenEdge, StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"key": "new"}})})
		var conflict *storage.HiddenEdgeIdentityConflictError
		require.True(t, errors.Is(err, storage.ErrAlreadyExists) || errors.As(err, &conflict), "write-behind=%v: %v", buffered, err)

		require.NoError(t, base.FlushWriteBehind(nil))
		node, err := base.GetStoredNode(hiddenNode)
		require.NoError(t, err)
		require.Equal(t, []string{"Original"}, node.Labels)
		edge, err := base.GetStoredEdge(hiddenEdge)
		require.NoError(t, err)
		require.Equal(t, "old", edge.Properties["key"])
	}
}
