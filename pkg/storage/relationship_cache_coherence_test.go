package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The same monotonic record governs callbacks, fills, explicit payload drops
// and bounded resets. A reset must retain a newer *issued* timestamp even if
// the engine watermark has not yet caught up with that issuance.
func TestRelationshipCacheCoherenceRetainsRevocation(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	edge := &Edge{ID: "test:coherence", StartNode: "test:a", EndNode: "test:b", Type: "R"}
	func() {
		b.edgeCacheMu.Lock()
		defer b.edgeCacheMu.Unlock()
		b.revokeEdgeLocked(edge.ID, 100)
		b.putEdgeLocked(edge, 1, 99)
		require.Nil(t, b.edgeCache[edge.ID].Edge)
		b.putEdgeLocked(edge, 1, 100)
		require.NotNil(t, b.edgeCache[edge.ID].Edge)
		b.revokeEdgeLocked(edge.ID, 90)
		b.discardEdgePayloadLocked(edge.ID)
		require.Equal(t, uint64(100), b.edgeCache[edge.ID].coherence.writeTs)
		b.resetEdgeCacheLocked()
		require.Equal(t, uint64(100), b.edgeCoherence.floor)
		b.putEdgeLocked(edge, 1, 99)
		require.Nil(t, b.edgeCache[edge.ID].Edge)
	}()

	b.adjCacheMu.Lock()
	b.revokeAdjacencyLocked(edge.StartNode, true, 110)
	b.revokeAdjacencyLocked(edge.StartNode, true, 90)
	b.adjCacheMu.Unlock()
	b.adjCacheInvalidateForEdge(edge)
	require.Equal(t, uint64(110), b.outgoingAdjCache[edge.StartNode].coherence.writeTs)
	b.adjCacheStoreOutgoing(b.adjCacheGen.current(), edge.StartNode, []EdgeID{edge.ID}, 109)
	_, ok := b.adjCacheLoadOutgoing(edge.StartNode)
	require.False(t, ok)
	b.adjCacheInvalidateAll()
	require.Equal(t, uint64(110), b.adjCoherence.floor)
	b.adjCacheStoreOutgoing(b.adjCacheGen.current(), edge.StartNode, []EdgeID{edge.ID}, 109)
	_, ok = b.adjCacheLoadOutgoing(edge.StartNode)
	require.False(t, ok)
}

func TestRelationshipCacheClassifiedInventoryIgnoresUnrelatedPopulation(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	tx := &BadgerTransaction{pendingWrites: map[string][]byte{}}
	for i := 0; i < 128; i++ {
		edge := &Edge{ID: EdgeID(fmt.Sprintf("test:changed-%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "CHANGED"}
		tx.operations = append(tx.operations, Operation{Type: OpUpdateEdge, EdgeID: edge.ID, Edge: edge, OldEdge: edge})
		// An operation-covered raw value must not be decoded or rediscovered.
		tx.pendingWrites[string(edgeKey(edge.ID))] = []byte{1}
	}
	scope := transactionNodeCacheWrites(tx)
	empty, err := b.collectRelationshipCacheWrites(scope, nil)
	require.NoError(t, err)
	unrelated := make([]*Edge, 100000)
	for i := range unrelated {
		unrelated[i] = &Edge{ID: EdgeID(fmt.Sprintf("other:unrelated-%d", i)), Type: "OTHER"}
	}
	b.edgeTypeCache["OTHER"] = edgeListCacheEntry{edges: unrelated, readTs: 1}
	full, err := b.collectRelationshipCacheWrites(scope, nil)
	require.NoError(t, err)
	require.Equal(t, empty, full)
	require.Len(t, full.edges, 256)
	require.Len(t, full.bodies, 128)
	require.Empty(t, full.endpoints)
	require.Empty(t, full.types)
}

func TestRelationshipCacheEmptyScopeAllocationProfile(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for name, scope := range map[string]nodeCacheWriteScope{
		"metadata":         noNodeCacheWrites(),
		"single-node":      singleNodeCacheWrites("test:n"),
		"node-transaction": transactionNodeCacheWrites(&BadgerTransaction{operations: []Operation{{Type: OpUpdateNode, NodeID: "test:n"}}, pendingWrites: map[string][]byte{string(nodeKey("test:n")): nil}}),
		"node-replay":      bufferedNodeCacheWrites([]bufferedCommit{{ops: []Operation{{Type: OpUpdateNode, NodeID: "test:n"}}, writes: map[string][]byte{string(nodeKey("test:n")): nil}}}),
	} {
		t.Run(name, func(t *testing.T) {
			var prepared nodeCacheWriteScope
			n := testing.AllocsPerRun(1000, func() {
				var err error
				prepared, err = b.prepareRelationshipCacheWrites(scope, nil, false)
				if err != nil {
					panic(err)
				}
			})
			require.True(t, prepared.relationshipPrepared)
			require.Nil(t, prepared.relationshipKeys)
			require.Equal(t, float64(0), n, "unrelated node/metadata writes must not allocate a relationship inventory")
		})
	}
}
