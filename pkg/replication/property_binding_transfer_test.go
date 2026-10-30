package replication

import (
	"bytes"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicationPropertyBindingsUseNames(t *testing.T) {
	open := func() (*storage.BadgerEngine, *StorageAdapter) {
		b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
		require.NoError(t, err)
		a, err := NewStorageAdapterWithWAL(b, t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
		return b, a
	}
	source, leader := open()
	destination, follower := open()
	for _, ns := range []string{"binding", "second"} {
		_, err := destination.CreateNode(&storage.Node{ID: storage.NodeID(ns + ":sentinel"), Properties: map[string]any{"beta": "keep"}})
		require.NoError(t, err)
		for _, node := range []*storage.Node{
			{ID: storage.NodeID(ns + ":a"), Properties: map[string]any{"alpha": int64(1)}},
			{ID: storage.NodeID(ns + ":b"), Properties: map[string]any{"beta": int64(2)}},
		} {
			_, err = source.CreateNode(node)
			require.NoError(t, err)
		}
		require.NoError(t, source.CreateEdge(&storage.Edge{ID: storage.EdgeID(ns + ":r"), StartNode: storage.NodeID(ns + ":a"), EndNode: storage.NodeID(ns + ":b"), Type: "R", Properties: map[string]any{"alpha": int64(3)}}))
	}
	var snapshot bytes.Buffer
	require.NoError(t, leader.WriteSnapshot(&snapshot))
	require.NoError(t, follower.RestoreSnapshot(&snapshot))
	for _, ns := range []string{"binding", "second"} {
		want := map[string]any{"alpha": int64(11), "beta": []byte{1, 2}}
		node, err := source.GetNode(storage.NodeID(ns + ":a"))
		require.NoError(t, err)
		node.Properties = want
		require.NoError(t, follower.ApplyCommand(&Command{Type: CmdUpdateNode, Data: fixtureowner.Encode(t, storage.EncodeNodePayload, node)}))
		edge, err := source.GetEdge(storage.EdgeID(ns + ":r"))
		require.NoError(t, err)
		edge.Properties = want
		require.NoError(t, follower.ApplyCommand(&Command{Type: CmdUpdateEdge, Data: fixtureowner.Encode(t, storage.EncodeEdgePayload, edge)}))
		got, err := destination.GetNode(node.ID)
		require.NoError(t, err)
		require.Equal(t, want, got.Properties)
		gotEdge, err := destination.GetEdge(edge.ID)
		require.NoError(t, err)
		require.Equal(t, want, gotEdge.Properties)
		sentinel, err := destination.GetNode(storage.NodeID(ns + ":sentinel"))
		require.NoError(t, err)
		require.Equal(t, map[string]any{"beta": "keep"}, sentinel.Properties)
	}
}
