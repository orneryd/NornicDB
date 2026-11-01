package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingLabelScanEngine struct {
	Engine
	err error
}

func (engine failingLabelScanEngine) GetNodesByLabel(ctx context.Context, _ string) ([]*Node, error) {
	return nil, engine.err
}

type failingProjectedLabelEngine struct {
	Engine
	err error
}

func (engine failingProjectedLabelEngine) StreamNodesByLabelProjected(ctx context.Context, label string, _ []string, _ func(*Node) error) error {
	return engine.err
}

func TestProjectedLabelWrappers_PropagateBackingScanError(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	readErr := errors.New("projected label scan failed")
	failing := failingProjectedLabelEngine{Engine: backing, err: readErr}

	composite := NewCompositeEngine(map[string]Engine{"failed": failing}, nil, map[string]string{"failed": "read"})
	require.ErrorIs(t, composite.StreamNodesByLabelProjected(context.Background(), "Evidence", nil, func(*Node) error { return nil }), readErr)
}

func TestWALEngine_GetNodesByLabelPropagatesBackingError(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	readErr := errors.New("label scan failed")
	wal := NewWALEngine(failingLabelScanEngine{Engine: backing, err: readErr}, nil)
	nodes, err := wal.GetNodesByLabel(context.Background(), "Evidence")
	require.ErrorIs(t, err, readErr)
	require.Nil(t, nodes)
}

func TestBadgerEngine_GetNodeProjectedSkipsUnrequestedVectorProperty(t *testing.T) {
	engine := createTestBadgerEngine(t)
	embedding := make([]float64, 1024)
	for i := range embedding {
		embedding[i] = float64(i) / 1024
	}
	node := testNode("projected-node")
	node.Labels = []string{"Entity"}
	node.Properties = map[string]any{
		"uuid":           "entity-1",
		"group_id":       "episode-1",
		"name_embedding": embedding,
	}

	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	projected, err := engine.GetNodeProjected(node.ID, []string{"uuid", "group_id"})
	require.NoError(t, err)
	require.Equal(t, node.ID, projected.ID)
	require.Equal(t, []string{"Entity"}, projected.Labels)
	require.Equal(t, "entity-1", projected.Properties["uuid"])
	require.Equal(t, "episode-1", projected.Properties["group_id"])
	require.NotContains(t, projected.Properties, "name_embedding")

	full, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Contains(t, full.Properties, "name_embedding")
	require.Len(t, full.Properties["name_embedding"], 1024)
}

func TestBadgerEngine_GetNodeProjectedEmptyPropertyList(t *testing.T) {
	engine := createTestBadgerEngine(t)
	node := testNode("projected-empty")
	node.Properties = map[string]any{"uuid": "entity-1"}

	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	projected, err := engine.GetNodeProjected(node.ID, []string{})
	require.NoError(t, err)
	require.Equal(t, node.ID, projected.ID)
	require.Empty(t, projected.Properties)
}

// newLargeVectorEvidenceNode builds the embedding-heavy Evidence node these
// embedding-free reads exercise: two 10,000-element float32 chunks. Every call
// allocates its own vectors, so separate nodes never share backing memory.
func newLargeVectorEvidenceNode(id NodeID, assetID string) *Node {
	chunks := make([][]float32, 2)
	for i := range chunks {
		chunks[i] = make([]float32, 10_000)
	}
	return &Node{
		ID:              id,
		Labels:          []string{"Evidence"},
		Properties:      map[string]any{"asset_id": assetID},
		ChunkEmbeddings: chunks,
	}
}

func TestNamespacedEngine_GetNodeWithoutEmbeddingsSkipsSeparateVectors(t *testing.T) {
	engine := createTestBadgerEngine(t)
	tenant := NewNamespacedEngine(engine, "tenant")
	node := newLargeVectorEvidenceNode("large-vector-node", "asset-a")
	_, err := tenant.CreateNode(node)
	require.NoError(t, err)

	light, err := tenant.GetNodeWithoutEmbeddings(node.ID)
	require.NoError(t, err)
	require.Equal(t, node.ID, light.ID)
	require.Equal(t, "asset-a", light.Properties["asset_id"])
	require.Empty(t, light.ChunkEmbeddings)
	require.Empty(t, light.NamedEmbeddings)

	full, err := tenant.GetNode(node.ID)
	require.NoError(t, err)
	require.False(t, full.RuntimeMetadata().EmbeddingsStoredSeparately)
	require.Len(t, full.ChunkEmbeddings, 2)
	require.Len(t, full.ChunkEmbeddings[0], 10_000)
}

func TestNamespacedEngine_BatchGetNodesWithoutEmbeddingsSkipsSeparateVectors(t *testing.T) {
	engine := createTestBadgerEngine(t)
	tenant := NewNamespacedEngine(engine, "tenant")
	for _, id := range []NodeID{"large-vector-a", "large-vector-b"} {
		_, err := tenant.CreateNode(newLargeVectorEvidenceNode(id, string(id)+"-asset"))
		require.NoError(t, err)
	}

	light, err := tenant.BatchGetNodesWithoutEmbeddings(context.Background(), []NodeID{"large-vector-a", "missing", "large-vector-b"})
	require.NoError(t, err)
	require.Len(t, light, 2)
	require.Equal(t, NodeID("large-vector-a"), light["large-vector-a"].ID)
	require.Equal(t, "large-vector-a-asset", light["large-vector-a"].Properties["asset_id"])
	require.Empty(t, light["large-vector-a"].ChunkEmbeddings)
	require.Empty(t, light["large-vector-b"].ChunkEmbeddings)
}

func TestNamespacedWAL_StreamNodesByLabelProjected(t *testing.T) {
	engine := createTestBadgerEngine(t)
	wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	tenant := NewNamespacedEngine(NewWALEngine(engine, wal), "tenant")
	_, err = tenant.CreateNode(&Node{ID: "one", Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "found", "unused": "hidden"}})
	require.NoError(t, err)
	var found []*Node
	err = tenant.StreamNodesByLabelProjected(context.Background(), "Evidence", []string{"asset_id"}, func(node *Node) error {
		found = append(found, node)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.Equal(t, NodeID("one"), found[0].ID)
	require.Equal(t, map[string]any{"asset_id": "found"}, found[0].Properties)
	err = tenant.StreamNodesByLabelProjected(context.Background(), "Evidence", nil, func(*Node) error {
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
}

func TestProjectedLabelWrappers_UnsupportedBackendReturnsError(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	unsupported := struct{ Engine }{backing}

	visited := false
	wal, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	err = NewWALEngine(unsupported, wal).StreamNodesByLabelProjected(context.Background(), "Evidence", nil, func(*Node) error {
		visited = true
		return nil
	})
	require.ErrorIs(t, err, ErrNotImplemented)
	require.False(t, visited)
}

// TestBadgerEngine_GetNodeKernel_CacheStoreAsymmetry pins the shared
// getNodeByID kernel contract: GetNode caches decoded nodes, while
// GetNodeWithoutEmbeddings never stores — an embedding-free read must not
// poison the cache for later full reads — and both return copies.
func TestBadgerEngine_GetNodeKernel_CacheStoreAsymmetry(t *testing.T) {
	engine := createTestBadgerEngine(t)
	tenant := NewNamespacedEngine(engine, "tenant")
	node := &Node{
		ID:              "cache-asym",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"title": "T"},
		ChunkEmbeddings: [][]float32{make([]float32, 10_000)},
	}
	_, err := tenant.CreateNode(node)
	require.NoError(t, err)
	storedID := NodeID("tenant:cache-asym")

	// Evict the create-time cache entry so the first read is a genuine miss.
	engine.nodeCacheMu.Lock()
	delete(engine.nodeCache, storedID)
	engine.nodeCacheMu.Unlock()

	// Cold read without embeddings: must not populate the cache.
	beforeMisses := atomic.LoadInt64(&engine.cacheMisses)
	light, err := engine.GetNodeWithoutEmbeddings(storedID)
	require.NoError(t, err)
	require.Equal(t, "T", light.Properties["title"])
	require.Equal(t, beforeMisses+1, atomic.LoadInt64(&engine.cacheMisses))
	engine.nodeCacheMu.RLock()
	_, cached := engine.nodeCache[storedID]
	engine.nodeCacheMu.RUnlock()
	require.False(t, cached, "without-embeddings read must not store into the node cache")

	_, err = engine.GetNodeWithoutEmbeddings(storedID)
	require.NoError(t, err)
	require.Equal(t, beforeMisses+2, atomic.LoadInt64(&engine.cacheMisses), "without-embeddings read must not cache")

	// A full read caches; the next without-embeddings read is a cache hit
	// that copies without embeddings.
	full, err := engine.GetNode(storedID)
	require.NoError(t, err)
	require.NotEmpty(t, full.ChunkEmbeddings)
	beforeHits := atomic.LoadInt64(&engine.cacheHits)
	fromCache, err := engine.GetNodeWithoutEmbeddings(storedID)
	require.NoError(t, err)
	require.Equal(t, beforeHits+1, atomic.LoadInt64(&engine.cacheHits), "after a full read the without-embeddings read is a cache hit")
	require.Empty(t, fromCache.ChunkEmbeddings)
	require.Equal(t, "T", fromCache.Properties["title"])

	// Copies: mutating a returned node must not leak into the cache.
	light.Properties["title"] = "mutated"
	again, err := engine.GetNodeWithoutEmbeddings(storedID)
	require.NoError(t, err)
	require.Equal(t, "T", again.Properties["title"])
}

func TestNamespacedEngine_ProjectedStreamingIsolation(t *testing.T) {
	for _, test := range []struct {
		name, selector string
		id, otherID    NodeID
		otherAsset     string
		extra          bool
		read           func(*NamespacedEngine, context.Context, string, []string, func(*Node) error) error
	}{
		{"TestNamespacedEngine_StreamNodesByLabelProjected", "Evidence", "one", "two", "other", false, (*NamespacedEngine).StreamNodesByLabelProjected},
		{"TestNamespacedEngine_StreamNodesByPrefixProjected", "evidence", "evidence-one", "evidence-two", "other-tenant", true, (*NamespacedEngine).StreamNodesByPrefixProjected},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			tenantA, tenantB := NewNamespacedEngine(engine, "tenant_a"), NewNamespacedEngine(engine, "tenant_b")
			_, err := tenantA.CreateNode(&Node{ID: test.id, Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "wanted", "embedding": make([]float64, 1024)}})
			require.NoError(t, err)
			if test.extra {
				_, err = tenantA.CreateNode(&Node{ID: "other-one", Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "same-tenant-other-prefix"}})
				require.NoError(t, err)
			}
			_, err = tenantB.CreateNode(&Node{ID: test.otherID, Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": test.otherAsset}})
			require.NoError(t, err)
			var nodes []*Node
			err = test.read(tenantA, context.Background(), test.selector, []string{"asset_id"}, func(node *Node) error { nodes = append(nodes, node); return nil })
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.Equal(t, test.id, nodes[0].ID)
			require.Equal(t, "wanted", nodes[0].Properties["asset_id"])
			require.NotContains(t, nodes[0].Properties, "embedding")
		})
	}
}
