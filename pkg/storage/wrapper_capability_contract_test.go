package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWrapperCapabilityParity_ProjectedReadsAcrossStack verifies GetNodeProjected
// returns the same projected shape through every layer of the production stack,
// including the async overlay for staged writes.
func TestWrapperCapabilityParity_ProjectedReadsAcrossStack(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()
	walBacking, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	wal := NewWALEngine(badger, walBacking)
	async := NewAsyncEngine(wal, &AsyncEngineConfig{FlushInterval: time.Hour})
	// Closing the async engine closes the WAL file under it (#924).
	t.Cleanup(func() { _ = async.Close() })
	namespaced := NewNamespacedEngine(async, "ns")

	_, err = namespaced.CreateNode(&Node{
		ID:         "n1",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"a": int64(1), "b": "two", "c": 3.5},
	})
	require.NoError(t, err)
	require.NoError(t, async.Flush())

	projected, err := namespaced.GetNodeProjected("n1", []string{"a", "c"})
	require.NoError(t, err)
	require.NotNil(t, projected)
	require.Equal(t, NodeID("n1"), projected.ID)
	require.Equal(t, []string{"Doc"}, projected.Labels)
	require.Equal(t, map[string]any{"a": int64(1), "c": 3.5}, projected.Properties)

	// Same read through the async layer uses the prefixed ID.
	asyncProjected, err := async.GetNodeProjected("ns:n1", []string{"b"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"b": "two"}, asyncProjected.Properties)

	walProjected, err := wal.GetNodeProjected("ns:n1", []string{"a"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": int64(1)}, walProjected.Properties)

	badgerProjected, err := badger.GetNodeProjected("ns:n1", []string{"c"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"c": 3.5}, badgerProjected.Properties)

	// Overlay parity: a staged async update is visible through the namespaced
	// projected read before flush, and a staged delete reports ErrNotFound.
	latest, err := namespaced.GetNode("n1")
	require.NoError(t, err)
	latest.Properties["d"] = int64(4)
	require.NoError(t, namespaced.UpdateNode(latest))

	overlayRead, err := namespaced.GetNodeProjected("n1", []string{"d"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"d": int64(4)}, overlayRead.Properties)

	_, err = namespaced.CreateNode(&Node{ID: "n2", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, namespaced.DeleteNode("n2"))
	_, err = async.GetNodeProjected("ns:n2", nil)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestWrapperCapabilityParity_NamespacedMaintenanceForwards verifies the
// namespace-scoped counting, iteration, schema, namespace and embedding
// maintenance forwards behave like the inner engine with the namespace
// boundary applied.
func TestWrapperCapabilityParity_NamespacedMaintenanceForwards(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()

	nsA := NewNamespacedEngine(badger, "a")
	nsB := NewNamespacedEngine(badger, "b")

	for index := 0; index < 3; index++ {
		_, err := nsA.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("n-%d", index)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"seq": int64(index)},
		})
		require.NoError(t, err)
	}
	for index := 0; index < 2; index++ {
		_, err := nsB.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("m-%d", index)),
			Labels:     []string{"Other"},
			Properties: map[string]any{"seq": int64(index)},
		})
		require.NoError(t, err)
	}
	err = nsA.CreateEdge(&Edge{ID: "e-0", Type: "R", StartNode: "n-0", EndNode: "n-1"})
	require.NoError(t, err)

	t.Run("prefix counts are namespace scoped", func(t *testing.T) {
		nodes, err := nsA.NodeCountByPrefix("")
		require.NoError(t, err)
		require.Equal(t, int64(3), nodes)
		edges, err := nsA.EdgeCountByPrefix("")
		require.NoError(t, err)
		require.Equal(t, int64(1), edges)
		other, err := nsB.NodeCountByPrefix("")
		require.NoError(t, err)
		require.Equal(t, int64(2), other)
	})

	t.Run("iterate nodes strips prefix and filters namespace", func(t *testing.T) {
		var ids []NodeID
		require.NoError(t, nsA.IterateNodes(func(node *Node) bool {
			ids = append(ids, node.ID)
			return true
		}))
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		require.Equal(t, []NodeID{"n-0", "n-1", "n-2"}, ids)
	})

	t.Run("label count in namespace forwards", func(t *testing.T) {
		count, err := nsA.NodeCountByLabelInNamespace("a", "Doc")
		require.NoError(t, err)
		require.Equal(t, int64(3), count)
	})

	t.Run("namespace list and schema forwards", func(t *testing.T) {
		namespaces := nsA.ListNamespaces()
		require.Contains(t, namespaces, "a")
		require.Contains(t, namespaces, "b")
		require.NotNil(t, nsA.GetSchemaForNamespace("a"))
	})

	t.Run("pending embeddings count matches inner", func(t *testing.T) {
		require.Equal(t, badger.PendingEmbeddingsCount(), nsA.PendingEmbeddingsCount())
	})

	t.Run("embedding update rejects missing and persists existing", func(t *testing.T) {
		require.ErrorIs(t, nsA.UpdateNodeEmbeddingSidecar(&Node{ID: "missing", ChunkEmbeddings: [][]float32{{0.1}}}), ErrNotFound)

		node, err := nsA.GetNode("n-0")
		require.NoError(t, err)
		node.ChunkEmbeddings = [][]float32{{0.2, 0.3}}
		node.EmbedMeta = map[string]any{"embedded": true, "chunk_count": 1}
		require.NoError(t, nsA.UpdateNodeEmbeddingSidecar(node))

		readback, err := nsA.GetNode("n-0")
		require.NoError(t, err)
		require.NotEmpty(t, readback.ChunkEmbeddings)
		require.NotEmpty(t, readback.EmbedMeta)
	})

	t.Run("shutdown marker forwards", func(t *testing.T) {
		require.NoError(t, nsA.MarkCleanShutdown(context.Background()))
		consumed, err := nsA.ConsumeCleanShutdownMarker(context.Background())
		require.NoError(t, err)
		require.True(t, consumed)
	})
}

// TestWrapperCapabilityParity_EventCallbacksTranslateThroughWrappers verifies
// event registration works through WAL and Namespaced wrappers, that the
// namespaced view translates IDs, and that cross-namespace events are filtered.
func TestWrapperCapabilityParity_EventCallbacksTranslateThroughWrappers(t *testing.T) {
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer badger.Close()

	nsA := NewNamespacedEngine(badger, "a")
	nsB := NewNamespacedEngine(badger, "b")

	// The inner engine holds a single callback slot per event, so one view
	// registers at a time; the wrapper must still filter foreign namespaces.
	var createdA, deletedA []NodeID
	nsA.OnNodeCreated(func(node *Node) { createdA = append(createdA, node.ID) })
	nsA.OnNodeDeleted(func(id NodeID) { deletedA = append(deletedA, id) })

	// A node created through the other namespace's view must not fire this
	// view's callback.
	_, err = nsB.CreateNode(&Node{ID: "foreign", Labels: []string{"B"}})
	require.NoError(t, err)
	require.Empty(t, createdA)

	_, err = nsA.CreateNode(&Node{ID: "only-a", Labels: []string{"A"}})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"only-a"}, createdA)

	require.NoError(t, nsA.DeleteNode("only-a"))
	require.Equal(t, []NodeID{"only-a"}, deletedA)

	t.Run("WAL passes events through unchanged", func(t *testing.T) {
		walBacking, err := NewWAL(t.TempDir(), nil)
		require.NoError(t, err)
		// The parent test keeps using badger, so close only the WAL file (#924).
		t.Cleanup(func() { _ = walBacking.Close() })
		wal := NewWALEngine(badger, walBacking)
		var created []NodeID
		var edges []EdgeID
		wal.OnNodeCreated(func(node *Node) { created = append(created, node.ID) })
		wal.OnEdgeCreated(func(edge *Edge) { edges = append(edges, edge.ID) })
		_, err = wal.CreateNode(&Node{ID: "w:x", Labels: []string{"W"}})
		require.NoError(t, err)
		err = wal.CreateEdge(&Edge{ID: "w:e", Type: "T", StartNode: "w:x", EndNode: "w:x"})
		require.NoError(t, err)
		require.Equal(t, []NodeID{"w:x"}, created)
		require.Equal(t, []EdgeID{"w:e"}, edges)
	})
}
