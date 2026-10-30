package replication

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR1655RawObservedRequestCannotEnterApplyWAL(t *testing.T) {
	for _, kind := range []CommandType{CmdUpdateNodeRequest, CmdUpdateEdgeRequest} {
		name := "node"
		if kind == CmdUpdateEdgeRequest {
			name = "edge"
		}
		t.Run(name, func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			for _, id := range []storage.NodeID{"test:a", "test:b"} {
				_, err := base.CreateNode(pr1655Node(id, map[string]any{"v": "before"}))
				require.NoError(t, err)
			}
			require.NoError(t, base.CreateEdge(pr1655Edge("test:r", "test:a", "test:b", map[string]any{"v": "before"})))
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, adapter.Close()) })
			var request any
			if kind == CmdUpdateNodeRequest {
				before, err := base.GetNode("test:a")
				require.NoError(t, err)
				request = storage.NodeUpdateRequest{Before: before, After: pr1655Node("test:a", map[string]any{"v": "rejected"})}
			} else {
				before, err := base.GetEdge("test:r")
				require.NoError(t, err)
				request = storage.EdgeUpdateRequest{Before: before, After: pr1655Edge("test:r", "test:a", "test:b", map[string]any{"v": "rejected"})}
			}
			payload, err := encodeEntityCommandPayload(kind, request)
			require.NoError(t, err)
			assert.ErrorIs(t, adapter.ApplyCommand(&Command{Type: kind, Data: payload, Timestamp: time.Now()}), storage.ErrInvalidData)
			require.NoError(t, adapter.FlushWAL())
			position, err := adapter.GetWALPosition()
			require.NoError(t, err)
			assert.Zero(t, position, "unnormalized client intent must never be replayable")
			entries, err := adapter.GetWALEntries(0, 10)
			require.NoError(t, err)
			assert.Empty(t, entries)
			node, err := base.GetNode("test:a")
			require.NoError(t, err)
			assert.Equal(t, "before", node.Properties["v"])
			edge, err := base.GetEdge("test:r")
			require.NoError(t, err)
			assert.Equal(t, "before", edge.Properties["v"])
		})
	}
}

func TestPR1655ObservedRequestsNormalizeBeforeReplicaApply(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	peer := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, base.Close()); require.NoError(t, peer.Close()) })
	for _, engine := range []storage.Engine{base, peer} {
		for _, id := range []storage.NodeID{"test:a", "test:b"} {
			_, err := engine.CreateNode(pr1655Node(id, map[string]any{"v": "before"}))
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(pr1655Edge("test:r", "test:a", "test:b", map[string]any{"v": "before"})))
	}
	localAdapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localAdapter.Close()) })
	peerAdapter, err := NewStorageAdapterWithWAL(peer, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, peerAdapter.Close()) })
	mirror := &acceptedMirrorStorage{Storage: localAdapter, peer: peerAdapter}
	replicator := NewStandaloneReplicator(DefaultConfig(), mirror)
	require.NoError(t, replicator.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, replicator.Shutdown()) })
	engine := NewReplicatedEngine(base, replicator, 5*time.Second)
	beforeNode, err := engine.GetNode("test:a")
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{Before: beforeNode, After: pr1655Node(beforeNode.ID, map[string]any{"v": "accepted"})}))
	beforeEdge, err := engine.GetEdge("test:r")
	require.NoError(t, err)
	require.NoError(t, engine.UpdateEdge(storage.EdgeUpdateRequest{Before: beforeEdge, After: pr1655Edge(beforeEdge.ID, beforeEdge.StartNode, beforeEdge.EndNode, map[string]any{"v": "accepted"})}))
	for _, adapter := range []*StorageAdapter{localAdapter, peerAdapter} {
		require.NoError(t, adapter.FlushWAL())
		entries, err := adapter.GetWALEntries(0, 10)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		for _, entry := range entries {
			require.Equal(t, CmdCommitTransaction, entry.Command.Type)
		}
	}
	for _, backend := range []storage.Engine{base, peer} {
		node, err := backend.GetNode("test:a")
		require.NoError(t, err)
		require.Equal(t, "accepted", node.Properties["v"])
		edge, err := backend.GetEdge("test:r")
		require.NoError(t, err)
		require.Equal(t, "accepted", edge.Properties["v"])
	}
}
