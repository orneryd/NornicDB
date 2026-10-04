package storage

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestCompositeEngine(t *testing.T) (*CompositeEngine, *NamespacedEngine, *NamespacedEngine) {
	t.Helper()
	badger, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })

	nsA := NewNamespacedEngine(badger, "a")
	nsB := NewNamespacedEngine(badger, "b")
	composite := NewCompositeEngine(
		map[string]Engine{"a": nsA, "b": nsB},
		map[string]string{"a": "a", "b": "b"},
		map[string]string{"a": "read_write", "b": "read_write"},
	)
	return composite, nsA, nsB
}

func TestCompositeCapabilityParity_MVCCAndVisibilityReads(t *testing.T) {
	composite, nsA, nsB := newTestCompositeEngine(t)

	_, err := nsA.CreateNode(&Node{ID: "n-1", Labels: []string{"Doc"}, Properties: map[string]any{"p": int64(1)}})
	require.NoError(t, err)
	_, err = nsB.CreateNode(&Node{ID: "m-1", Labels: []string{"Doc"}, Properties: map[string]any{"p": int64(2), "private": "hidden"}})
	require.NoError(t, err)

	t.Run("latest visible routes by ID", func(t *testing.T) {
		node, err := composite.GetNodeLatestVisible("n-1")
		require.NoError(t, err)
		require.Equal(t, NodeID("n-1"), node.ID)
		node, err = composite.GetNodeLatestVisible("m-1")
		require.NoError(t, err)
		require.Equal(t, NodeID("m-1"), node.ID)
		_, err = composite.GetNodeLatestVisible("missing")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("heads route by ID and visible-at resolves", func(t *testing.T) {
		headA, err := composite.GetNodeCurrentHead("n-1")
		require.NoError(t, err)
		require.False(t, headA.Version.IsZero())

		node, err := composite.GetNodeVisibleAt("n-1", headA.Version)
		require.NoError(t, err)
		require.Equal(t, NodeID("n-1"), node.ID)

		_, err = composite.GetNodeCurrentHead("missing")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("label visible-at merges constituents", func(t *testing.T) {
		headA, err := composite.GetNodeCurrentHead("n-1")
		require.NoError(t, err)
		headB, err := composite.GetNodeCurrentHead("m-1")
		require.NoError(t, err)
		version := headA.Version
		if headB.Version.Compare(version) > 0 {
			version = headB.Version
		}
		nodes, err := composite.GetNodesByLabelVisibleAt("Doc", version)
		require.NoError(t, err)
		var ids []NodeID
		for _, node := range nodes {
			ids = append(ids, node.ID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		require.Equal(t, []NodeID{"m-1", "n-1"}, ids)
	})

	t.Run("label ID iteration dedupes across constituents", func(t *testing.T) {
		seen := make(map[NodeID]bool)
		require.NoError(t, composite.ForEachNodeIDByLabel("Doc", func(id NodeID) bool {
			seen[id] = true
			return true
		}))
		require.Len(t, seen, 2)
		require.True(t, seen["n-1"] && seen["m-1"])
	})

	t.Run("projected read routes by ID", func(t *testing.T) {
		node, err := composite.GetNodeProjected("n-1", []string{"p"})
		require.NoError(t, err)
		require.Equal(t, map[string]any{"p": int64(1)}, node.Properties)
		node, err = composite.GetNodeProjected("m-1", []string{"p"})
		require.NoError(t, err)
		require.Equal(t, NodeID("m-1"), node.ID)
		require.Equal(t, map[string]any{"p": int64(2)}, node.Properties)
	})
}

func TestCompositeCapabilityParity_AggregatesAndBroadcasts(t *testing.T) {
	composite, nsA, nsB := newTestCompositeEngine(t)

	_, err := nsA.CreateNode(&Node{ID: "n-1", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = nsB.CreateNode(&Node{ID: "m-1", Labels: []string{"Other"}})
	require.NoError(t, err)

	t.Run("graph mutation version sums constituents", func(t *testing.T) {
		version, supported := composite.GraphMutationVersion()
		require.True(t, supported)
		require.Greater(t, version, uint64(0))
	})

	t.Run("namespaces and schema route", func(t *testing.T) {
		namespaces := composite.ListNamespaces()
		require.Contains(t, namespaces, "a")
		require.Contains(t, namespaces, "b")
		require.NotNil(t, composite.GetSchemaForNamespace("a"))
		require.Nil(t, composite.GetSchemaForNamespace("missing"))
	})

	t.Run("pending embeddings count sums", func(t *testing.T) {
		require.Equal(t, nsA.PendingEmbeddingsCount()+nsB.PendingEmbeddingsCount(), composite.PendingEmbeddingsCount())
	})

	t.Run("shutdown marker broadcasts", func(t *testing.T) {
		require.NoError(t, composite.MarkCleanShutdown(context.Background()))
		consumed, err := composite.ConsumeCleanShutdownMarker(context.Background())
		require.NoError(t, err)
		require.True(t, consumed)
	})

	t.Run("lifecycle broadcasts without panic", func(t *testing.T) {
		composite.PauseLifecycle()
		composite.ResumeLifecycle()
		require.NotNil(t, composite.LifecycleStatus())
	})

	t.Run("iterate nodes merges constituents", func(t *testing.T) {
		count := 0
		require.NoError(t, composite.IterateNodes(func(*Node) bool {
			count++
			return true
		}))
		require.Equal(t, 2, count)
	})

	t.Run("embedding update routes and rejects missing", func(t *testing.T) {
		require.ErrorIs(t, composite.UpdateNodeEmbeddingSidecar(&Node{ID: "missing"}), ErrNotFound)

		node, err := composite.GetNode("n-1")
		require.NoError(t, err)
		node.ChunkEmbeddings = [][]float32{{0.1, 0.2}}
		node.EmbedMeta = map[string]any{"embedded": true, "chunk_count": 1}
		require.NoError(t, composite.UpdateNodeEmbeddingSidecar(node))

		readback, err := composite.GetNode("n-1")
		require.NoError(t, err)
		require.NotEmpty(t, readback.ChunkEmbeddings)
	})

	t.Run("events broadcast to constituents", func(t *testing.T) {
		var created []NodeID
		composite.OnNodeCreated(func(node *Node) { created = append(created, node.ID) })
		_, err := nsB.CreateNode(&Node{ID: "m-2", Labels: []string{"Other"}})
		require.NoError(t, err)
		require.Equal(t, []NodeID{"m-2"}, created)
	})
}

// Constituents often share one underlying engine (one Badger per host with
// several namespaces). Event registrations must deduplicate the shared engine
// and translate through every namespace view, so delivery does not depend on
// constituent iteration order and one namespace cannot shadow another.
func TestCompositeEvents_SharedEngineTranslatesEveryNamespace(t *testing.T) {
	composite, nsA, nsB := newTestCompositeEngine(t)

	var created []NodeID
	composite.OnNodeCreated(func(node *Node) { created = append(created, node.ID) })
	_, err := nsA.CreateNode(&Node{ID: "shared-a", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = nsB.CreateNode(&Node{ID: "shared-b", Labels: []string{"Other"}})
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"shared-a", "shared-b"}, created)

	var deleted []NodeID
	composite.OnNodeDeleted(func(nodeID NodeID) { deleted = append(deleted, nodeID) })
	require.NoError(t, nsB.DeleteNode("shared-b"))
	require.Equal(t, []NodeID{"shared-b"}, deleted)

	_, err = nsB.CreateNode(&Node{ID: "shared-b2", Labels: []string{"Other"}})
	require.NoError(t, err)
	_, err = nsB.CreateNode(&Node{ID: "shared-b3", Labels: []string{"Other"}})
	require.NoError(t, err)
	var createdEdges []EdgeID
	composite.OnEdgeCreated(func(edge *Edge) { createdEdges = append(createdEdges, edge.ID) })
	require.NoError(t, nsB.CreateEdge(&Edge{ID: "shared-e", StartNode: "shared-b2", EndNode: "shared-b3", Type: "R"}))
	require.Equal(t, []EdgeID{"shared-e"}, createdEdges)
}
