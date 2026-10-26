package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

// The substituted live dictionary is the specified negative control. Captured
// fact and full readers must resolve the persisted reverse entry in their own
// read view, rather than inheriting the deliberately wrong current mapping.
func TestStoredReadCapturedEndpointLookupControl(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, id := range []NodeID{"d7:start", "d7:end"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Captured"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "d7:edge", Type: "R", StartNode: "d7:start", EndNode: "d7:end", Properties: map[string]any{"key": "selected"}}))
	snapshot, err := b.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	original, err := ReadStoredMetadata(context.Background(), snapshot, StoredReadRequest{Edges: []EdgeID{"d7:edge"}})
	require.NoError(t, err)
	num := original.Edges[0].StartReference.NumericID
	shard := &b.idDict.nodes[nodeReverseShard(num)]
	shard.mu.Lock()
	saved := shard.reverse[num]
	shard.reverse[num] = "foreign:substituted"
	shard.mu.Unlock()
	defer func() { shard.mu.Lock(); shard.reverse[num] = saved; shard.mu.Unlock() }()
	for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
		t.Run(string(rune('0'+mode)), func(t *testing.T) {
			result, err := ReadStored(context.Background(), snapshot, StoredReadRequest{Mode: mode, Edges: []EdgeID{"d7:edge"}, EdgeProperties: []string{"key"}})
			require.NoError(t, err)
			require.Equal(t, NodeID("d7:start"), result.StoredFacts().Edges[0].StartNode)
			if mode == StoredFullMaterialization {
				require.Equal(t, NodeID("d7:start"), result.(StoredFullRead).Edges[0].StartNode)
			}
		})
	}
	// The live lookup really was changed; a regression to it fails the oracle.
	live, ok := b.idDict.lookupNodeIDByNum(num)
	require.True(t, ok)
	require.Equal(t, NodeID("foreign:substituted"), live)
}
