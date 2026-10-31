package storage

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). Public create/delete churn must
// not retain one revocation record forever per formerly stored entity. A
// delayed old writer image still cannot replace the current public read.
func TestResidualCacheFloorRetention(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	const capacity = 8
	b.nodeCacheMaxEntries, b.edgeCacheMaxItems, b.adjCacheMaxNodes = capacity, capacity, capacity
	b.edgeTypeCacheMaxTypes, b.labelFirstCacheMax = capacity, capacity
	for _, id := range []NodeID{"test:floor-peer", "test:floor-peer-end", "test:floor-target"} {
		_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:floor-peer-edge", StartNode: "test:floor-peer", EndNode: "test:floor-peer-end", Type: "KEEP"}))
	_, err = b.GetEdgesBetween("test:floor-peer", "test:floor-peer-end")
	require.NoError(t, err)
	b.edgeCacheMu.RLock()
	peerEdge := b.edgeCache["test:floor-peer-edge"]
	b.edgeCacheMu.RUnlock()
	require.NotNil(t, peerEdge)
	prior, err := b.GetStoredNode("test:floor-target")
	require.NoError(t, err)
	oldSource := b.db.oracle.published.Load()
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: &Node{ID: prior.ID, Properties: map[string]any{"v": int64(2)}}}))
	_, err = b.GetStoredNode("test:floor-peer")
	require.NoError(t, err)
	b.nodeCacheMu.RLock()
	peer := b.nodeCache["test:floor-peer"]
	b.nodeCacheMu.RUnlock()
	require.NotNil(t, peer)
	for i := 0; i < 4*capacity; i++ {
		id := NodeID(fmt.Sprintf("test:floor-churn-%d", i))
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
		require.NoError(t, b.DeleteNode(id))
	}
	b.durableCaches.mu.Lock()
	retained := len(b.durableCaches.nodes) + len(b.durableCaches.edges) + len(b.durableCaches.adjacency) + len(b.durableCaches.types) + len(b.durableCaches.labels)
	b.durableCaches.mu.Unlock()
	// Model an earlier writer whose physical action finished but whose cache
	// publication was delayed. It carries its actual old physical source cut.
	b.cacheStoreNode(prior, oldSource)
	current, err := b.GetStoredNode(prior.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Properties["v"])
	b.nodeCacheMu.RLock()
	after := b.nodeCache["test:floor-peer"]
	b.nodeCacheMu.RUnlock()
	require.Same(t, peer, after, "retiring floor metadata must preserve an unrelated warm body")
	edges, err := b.GetOutgoingEdges("test:floor-peer")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	b.edgeCacheMu.RLock()
	afterEdge := b.edgeCache["test:floor-peer-edge"]
	b.edgeCacheMu.RUnlock()
	require.Same(t, peerEdge, afterEdge, "an indexed reader must reuse the unrelated warm relationship after retiring floor metadata")
	require.LessOrEqual(t, retained, capacity, "revocation metadata must be bounded by cache capacity, not historical writes")
}

// Author: #1548 implementation lane (Codex). New cache provenance state follows
// the existing Close owner, which releases retained caches after all work ends.
func TestResidualClosedCacheProvenance(t *testing.T) {
	dir := t.TempDir()
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	for _, id := range []NodeID{"test:close-a", "test:close-b"} {
		_, err = b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:close-e", StartNode: "test:close-a", EndNode: "test:close-b", Type: "CLOSE"}))
	// Settle construction/count work first: an incidental count-fold fence on
	// Close must not be the lifetime owner of the new cache provenance state.
	require.NoError(t, b.Close())
	b, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	_, err = b.GetOutgoingEdges("test:close-a")
	require.NoError(t, err)
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:close-a", Properties: map[string]any{"v": int64(2)}}}))
	require.NotEmpty(t, b.edgeCacheSources)
	require.NotEmpty(t, b.durableCaches.nodes)
	require.NoError(t, b.Close())
	require.Empty(t, b.edgeCacheSources, "closed engine must release relationship source identities with its body cache")
	require.Empty(t, b.durableCaches.nodes)
	require.Empty(t, b.durableCaches.edges)
	require.Empty(t, b.durableCaches.adjacency)
	require.Empty(t, b.durableCaches.types)
	require.Empty(t, b.durableCaches.labels)
}
