//go:build nornic_copycount

// NOR-160 lane (Claude), a read copies only what it delivers, 30 October 2026.
// Run with -tags nornic_copycount: the test build counts every structural copy.
package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// nor160CountRead counts the copies one read makes, by entity ID.
func nor160CountRead(read func()) (map[NodeID]int, map[EdgeID]int) {
	return CountStructuralCopies(read)
}

func nor160EdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	return ids
}

// nor160CacheEdgeBody keeps id's committed body cached at its record version,
// as a read that filled the cache would.
func nor160CacheEdgeBody(t *testing.T, engine *BadgerEngine, edge *Edge) {
	var version uint64
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeKey(edge.ID))
		if err != nil {
			return err
		}
		version = item.Version()
		return nil
	}))
	engine.cacheFillEdgeAtVersion(engine.edgeCacheGen.current(), edge, version, engine.db.oracle.published.Load())
}

// A warm adjacency list whose second body was evicted: the cached attempt
// stops at the miss and the physical read answers. The relationship read
// before the miss is copied once, by the read that delivers it.
func TestNOR160PartialAdjacencyCacheCopiesOnce(t *testing.T) {
	engine := nor160CommittedEdges(t)
	for i := 0; i < 2; i++ {
		_, err := engine.GetOutgoingEdges("test:a")
		require.NoError(t, err)
	}
	engine.cacheDeleteEdge("test:e2")
	_, warm := engine.adjCacheLoad("test:a", true)
	require.True(t, warm, "the adjacency list must stay cached")

	var edges []*Edge
	_, copies := nor160CountRead(func() { edges = nor160Edges(engine.GetOutgoingEdges("test:a")) })
	require.ElementsMatch(t, []EdgeID{"test:e1", "test:e2"}, nor160EdgeIDs(edges))
	want := nor160EdgeSourceWant(engine, nil)
	for _, id := range []EdgeID{"test:e1", "test:e2"} {
		require.Equal(t, want(id), copies[id], "%s", id)
	}
	require.Equal(t, 1, copies["test:e1"], "the cached e1 is copied once")
}

// Read-your-writes: the transaction's own update or delete replaces the
// committed relationship before anything is copied.
func TestNOR160TransactionOwnWritesCopyOnce(t *testing.T) {
	read := func(tx *BadgerTransaction) func() []*Edge {
		return func() []*Edge {
			return nor160Edges(tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"}))
		}
	}
	t.Run("update", func(t *testing.T) {
		engine := nor160CommittedEdges(t)
		tx := nor160Tx(t, engine)
		read(tx)()
		updated := nor160EdgeFixtureEdge("test:e1", "test:b")
		updated.Properties["key"] = "pending"
		require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: updated}))

		var edges []*Edge
		_, copies := nor160CountRead(func() { edges = read(tx)() })
		require.ElementsMatch(t, []EdgeID{"test:e1", "test:e2"}, nor160EdgeIDs(edges))
		for _, edge := range edges {
			if edge.ID == "test:e1" {
				require.Equal(t, "pending", edge.Properties["key"], "the transaction reads its own write")
			}
		}
		require.Equal(t, 1, copies["test:e1"], "only the pending e1 is copied")
		require.Equal(t, nor160EdgeSourceWant(engine, tx)("test:e2"), copies["test:e2"])
	})
	t.Run("delete", func(t *testing.T) {
		engine := nor160CommittedEdges(t)
		tx := nor160Tx(t, engine)
		read(tx)()
		require.NoError(t, tx.DeleteEdge("test:e2"))

		var edges []*Edge
		_, copies := nor160CountRead(func() { edges = read(tx)() })
		require.Equal(t, []EdgeID{"test:e1"}, nor160EdgeIDs(edges))
		require.Zero(t, copies["test:e2"], "a relationship the transaction deleted is never copied")
		require.Equal(t, nor160EdgeSourceWant(engine, tx)("test:e1"), copies["test:e1"])
	})
}

// The first equality lookup of a cacheable label returns from its new index:
// the matching node is copied once more than a node the scan read but the
// lookup does not return (the scan's own copies, if any, are the same for
// both), never for an unused fallback result as well.
func TestNOR160FirstEqualityLookupCopiesOnce(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for id, thing := range map[NodeID]string{"test:held": "t-1", "test:other": "t-2"} {
		node := &Node{ID: id, Labels: []string{"Held"}, Properties: nor160OwnershipProperties()}
		node.Properties["thing_id"] = thing
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	tx := nor160Tx(t, engine)

	var found []*Node
	var ok bool
	var err error
	copies, _ := nor160CountRead(func() {
		found, ok, err = tx.LookupNodesByEquality("Held", map[string]any{"thing_id": "t-1"})
	})
	require.NoError(t, err)
	require.True(t, ok, "the transaction answers the lookup")
	require.Len(t, found, 1)
	require.Equal(t, NodeID("test:held"), found[0].ID)
	require.Equal(t, copies["test:other"]+1, copies["test:held"], "the returned node is copied once for its result")
}

// A hidden node or relationship whose body is cached is rejected before it
// is copied, on point reads and on the indexed adjacency reader alike.
func TestNOR160HiddenCachedReadsAreNotCopied(t *testing.T) {
	t.Run("point", func(t *testing.T) {
		engine := nor160CommittedEdges(t)
		require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:b", true))
		_, err := engine.GetStoredNode("test:b")
		require.NoError(t, err)
		for name, read := range map[string]func(NodeID) (*Node, error){"GetNode": engine.GetNode, "GetQueryNode": engine.GetQueryNode} {
			copies, _ := nor160CountRead(func() { _, err = read("test:b") })
			require.ErrorIs(t, err, ErrNotFound, name)
			require.Zero(t, copies["test:b"], "%s copied a hidden node", name)
		}
	})
	t.Run("indexed adjacency", func(t *testing.T) {
		engine := nor160CommittedEdges(t)
		require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:e1", true))
		nor160CacheEdgeBody(t, engine, nor160EdgeFixtureEdge("test:e1", "test:b"))
		engine.adjCacheInvalidate("test:a")
		_, warm := engine.adjCacheLoad("test:a", true)
		require.False(t, warm, "the read must take the indexed reader")

		var edges []*Edge
		_, copies := nor160CountRead(func() { edges = nor160Edges(engine.GetOutgoingEdges("test:a")) })
		require.Equal(t, []EdgeID{"test:e2"}, nor160EdgeIDs(edges))
		require.Zero(t, copies["test:e1"], "a hidden cached relationship was copied")
	})
}

// The round 5 reproduction, counted on the read that takes the indexed
// reader: e1's body is cached at its record version, e2 is buffered, and a's
// adjacency list is not cached. Nothing warms the path before it is counted.
func TestNOR160IndexedAdjacencyFirstReadCopiesOnce(t *testing.T) {
	engine := nor160WriteBehindEdges(t)
	_, warm := engine.adjCacheLoad("test:a", true)
	require.False(t, warm, "the counted read must take the indexed reader")

	var edges []*Edge
	_, copies := nor160CountRead(func() { edges = nor160Edges(engine.GetOutgoingEdges("test:a")) })
	require.ElementsMatch(t, []EdgeID{"test:e1", "test:e2"}, nor160EdgeIDs(edges))
	want := nor160EdgeSourceWant(engine, nil, "test:e2")
	for _, id := range []EdgeID{"test:e1", "test:e2"} {
		require.Equal(t, want(id), copies[id], "%s", id)
	}
	require.Equal(t, 1, copies["test:e1"], "the cached e1 is copied once")
}
