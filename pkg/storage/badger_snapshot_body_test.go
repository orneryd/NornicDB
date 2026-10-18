package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotOverlayVersionedBodyCaches(t *testing.T) {
	b := snapshotOverlayFixture(t)
	require.NoError(t, b.FlushWriteBehind())
	_, err := b.GetNode("test:a")
	require.NoError(t, err)
	_, err = b.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	edgeGen := b.edgeCacheGen.current()
	view, err := b.captureReadView(nil)
	require.NoError(t, err)
	defer view.Close()

	writer, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, writer.SetNamespace("test"))
	require.NoError(t, writer.SetImplicit(true))
	require.NoError(t, writer.UpdateNode(&Node{ID: "test:a", Labels: []string{"Live"}, Properties: map[string]any{"v": int64(4)}}))
	require.NoError(t, writer.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"v": int64(4)}}))
	require.NoError(t, writer.Commit())
	require.NoError(t, b.FlushWriteBehind())
	// Warm the newer bodies before asking the pinned view for its old records.
	current, err := b.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Properties["v"])
	edges, err := b.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(4), edges[0].Properties["v"])

	item, err := view.txn.Get(nodeKey("test:a"))
	require.NoError(t, err)
	old, err := b.readNodeItemInTxn(view.txn, item, "test:a", b.loadNodeEmbeddings)
	require.NoError(t, err)
	require.Equal(t, int64(2), old.Properties["v"])
	edge, err := b.readIndexedEdgeInTxn(view.txn, "test:r", edgeGen)
	require.NoError(t, err)
	require.Equal(t, int64(2), edge.Properties["v"])
	// The older read must not refill the current unversioned cache with old data.
	edges, err = b.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(4), edges[0].Properties["v"])
}

func TestSnapshotOverlayBodyCacheHydratesSidecarsAtReadPoint(t *testing.T) {
	b := snapshotOverlayFixture(t)
	require.NoError(t, b.FlushWriteBehind())
	node, err := b.GetNode("test:a")
	require.NoError(t, err)
	view, err := b.captureReadView(nil)
	require.NoError(t, err)
	defer view.Close()
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	node.EmbedMeta = map[string]any{"status": "new"}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
	current, err := b.GetNode("test:a")
	require.NoError(t, err)
	require.NotEmpty(t, current.ChunkEmbeddings)
	require.Equal(t, "new", current.EmbedMeta["status"])
	item, err := view.txn.Get(nodeKey("test:a"))
	require.NoError(t, err)
	old, err := b.readNodeItemInTxn(view.txn, item, "test:a", b.loadNodeEmbeddings)
	require.NoError(t, err)
	require.Empty(t, old.ChunkEmbeddings)
	require.Empty(t, old.EmbedMeta)
}

func TestSnapshotOverlayProjectionDoesNotHydrateCachedVectors(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	vector := make([]float32, 20_000)
	for i := range vector {
		vector[i] = float32(i) / 3
	}
	_, err = b.CreateNode(&Node{ID: "test:vector", Properties: map[string]any{"v": int64(1)}, ChunkEmbeddings: [][]float32{vector}})
	require.NoError(t, err)
	b.nodeCacheMu.Lock()
	delete(b.nodeCache, "test:vector")
	b.nodeCacheMu.Unlock()
	full, err := b.GetNode("test:vector")
	require.NoError(t, err)
	require.NotEmpty(t, full.ChunkEmbeddings)
	require.True(t, full.EmbeddingsStoredSeparately)
	projected, err := b.GetNodeProjected("test:vector", []string{"v"})
	require.NoError(t, err)
	require.Empty(t, projected.ChunkEmbeddings)
	require.Equal(t, int64(1), projected.Properties["v"])
}
