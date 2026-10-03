package storage

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_DeleteByPrefix_DropsOnlyMatchingNamespace(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db2:n2", Labels: []string{"Person"}})
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(&Edge{ID: "db1:e1", StartNode: "db1:n1", EndNode: "db1:n1", Type: "KNOWS"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "db2:e2", StartNode: "db2:n2", EndNode: "db2:n2", Type: "KNOWS"}))

	// Warm caches to ensure DeleteByPrefix invalidates them.
	nodes, err := engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	edges, err := engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, edges, 2)

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)

	_, err = engine.GetNode("db1:n1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("db1:e1")
	require.ErrorIs(t, err, ErrNotFound)

	_, err = engine.GetNode("db2:n2")
	require.NoError(t, err)
	_, err = engine.GetEdge("db2:e2")
	require.NoError(t, err)

	// Label index must not return dropped nodes.
	nodes, err = engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("db2:n2"), nodes[0].ID)

	// Edge type cache must not return dropped edges.
	edges, err = engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, EdgeID("db2:e2"), edges[0].ID)
}

func TestBadgerEngine_DeleteByPrefix_RemovesNumericHistoryAndDictionary(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, EngineOptions: EngineOptions{
		RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
	}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(&Node{ID: "drop:n", Labels: []string{"Person"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	oldHead, err := engine.GetNodeCurrentHead("drop:n")
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&Node{ID: "drop:n", Labels: []string{"Person"}, Properties: map[string]any{"v": 2}}))
	require.NoError(t, engine.UpdateNode(&Node{ID: "drop:n", Labels: []string{"Person"}, Properties: map[string]any{"v": 3}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "drop:e", StartNode: "drop:n", EndNode: "drop:n", Type: "KNOWS"}))
	oldEdgeHead, err := engine.GetEdgeCurrentHead("drop:e")
	require.NoError(t, err)
	require.NoError(t, engine.UpdateEdge(&Edge{ID: "drop:e", StartNode: "drop:n", EndNode: "drop:n", Type: "KNOWS", Properties: map[string]any{"v": 2}}))
	require.NoError(t, engine.UpdateEdge(&Edge{ID: "drop:e", StartNode: "drop:n", EndNode: "drop:n", Type: "KNOWS", Properties: map[string]any{"v": 3}}))
	_, err = engine.CreateNode(&Node{ID: "keep:n", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append([]byte{prefixTemporalHead}, []byte("drop\x00Person\x00")...), []byte("drop:n"))
	}))
	var nodeNum, edgeNum uint64
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		for _, entry := range []struct {
			key []byte
			id  *uint64
		}{{nodeIDForwardKey("drop:n"), &nodeNum}, {edgeIDForwardKey("drop:e"), &edgeNum}} {
			item, err := txn.Get(entry.key)
			if err != nil {
				return err
			}
			if err := item.Value(func(value []byte) error { *entry.id = binary.BigEndian.Uint64(value); return nil }); err != nil {
				return err
			}
		}
		return nil
	}))
	pruned, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, pruned, int64(2))
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		for _, logical := range [][]byte{
			append([]byte{prefixMVCCNode}, encodeNumID(nodeNum)...),
			append([]byte{prefixMVCCEdge}, encodeNumID(edgeNum)...),
		} {
			_, err := txn.Get(mvccPruneFloorKey(logical))
			require.NoError(t, err)
		}
		return nil
	}))
	nodes, edges, err := engine.DeleteByPrefix("drop:")
	require.NoError(t, err)
	require.Equal(t, int64(1), nodes)
	require.Equal(t, int64(1), edges)
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(nodeIDForwardKey("drop:n"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(edgeIDForwardKey("drop:e"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(append([]byte{prefixTemporalHead}, []byte("drop\x00Person\x00")...))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		for _, kind := range []byte{prefixMVCCNode, prefixMVCCNodeHead, prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj} {
			prefix := append([]byte{kind}, encodeNumID(nodeNum)...)
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			it.Rewind()
			require.False(t, it.ValidForPrefix(prefix))
			it.Close()
		}
		for _, logical := range [][]byte{
			append([]byte{prefixMVCCNode}, encodeNumID(nodeNum)...),
			append([]byte{prefixMVCCEdge}, encodeNumID(edgeNum)...),
		} {
			_, err := txn.Get(mvccPruneFloorKey(logical))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
		}
		for _, kind := range []byte{prefixMVCCEdge, prefixMVCCEdgeHead} {
			prefix := append([]byte{kind}, encodeNumID(edgeNum)...)
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			it.Rewind()
			require.False(t, it.ValidForPrefix(prefix))
			it.Close()
		}
		for _, family := range badgerKeyFamilies {
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{family.prefix}))
			for it.Rewind(); it.ValidForPrefix([]byte{family.prefix}); it.Next() {
				require.False(t, namespaceOwnsBadgerKey(it.Item().Key(), []byte("drop:"), "drop", true,
					map[uint64]struct{}{nodeNum: {}}, map[uint64]struct{}{edgeNum: {}}))
			}
			it.Close()
		}
		return nil
	}))
	_, err = engine.CreateNode(&Node{ID: "drop:n", Properties: map[string]any{"v": 100}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "drop:e", StartNode: "drop:n", EndNode: "drop:n", Type: "KNOWS"}))
	_, err = engine.GetNodeVisibleAt("drop:n", oldHead.Version)
	require.ErrorIs(t, err, ErrNotVisibleAtSnapshot)
	_, err = engine.GetEdgeVisibleAt("drop:e", oldEdgeHead.Version)
	require.ErrorIs(t, err, ErrNotVisibleAtSnapshot)
	_, err = engine.GetNode("keep:n")
	require.NoError(t, err)
}

func TestBadgerEngine_DeleteByPrefix_UpdatesLabelCounts(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person", "Employee"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:n2", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db2:n1", Labels: []string{"Person"}})
	require.NoError(t, err)

	_, _, err = engine.DeleteByPrefix("db1:")
	require.NoError(t, err)

	count, err := engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = engine.NodeCountByLabelInNamespace("db1", "Employee")
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = engine.NodeCountByLabelInNamespace("db2", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	count, err = engine.NodeCountByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestBadgerEngine_DeleteByPrefix_UpdatesLabelCountsForPartialPrefix(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "db1:temp-1", Labels: []string{"Person", "Temporary"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:temp-2", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:keep", Labels: []string{"Person", "Permanent"}})
	require.NoError(t, err)

	nodesDeleted, _, err := engine.DeleteByPrefix("db1:temp-")
	require.NoError(t, err)
	require.Equal(t, int64(2), nodesDeleted)

	count, err := engine.NodeCountByLabelInNamespace("db1", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	count, err = engine.NodeCountByLabelInNamespace("db1", "Temporary")
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = engine.NodeCountByLabelInNamespace("db1", "Permanent")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestBadgerEngine_DeleteByPrefix_AdvancesRevisionForEdgeOnlyDelete(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "keep:n1"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "keep:n2"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "db1:e1", StartNode: "keep:n1", EndNode: "keep:n2", Type: "LINKS",
	}))

	versionBefore, supported := engine.GraphMutationVersionInNamespace("db1")
	require.True(t, supported)
	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("db1:")
	require.NoError(t, err)
	require.Zero(t, nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)
	versionAfter, supported := engine.GraphMutationVersionInNamespace("db1")
	require.True(t, supported)
	require.Greater(t, versionAfter, versionBefore)
}

func TestBadgerEngine_DeleteByPrefix_EdgeCases(t *testing.T) {
	t.Run("empty prefix rejected", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		_, _, err := engine.DeleteByPrefix("")
		require.ErrorContains(t, err, "prefix cannot be empty")
	})

	t.Run("missing namespace returns zero counts", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		_, err := engine.CreateNode(&Node{ID: "db1:n1", Labels: []string{"Person"}})
		require.NoError(t, err)

		nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("missing:")
		require.NoError(t, err)
		require.Zero(t, nodesDeleted)
		require.Zero(t, edgesDeleted)

		_, err = engine.GetNode("db1:n1")
		require.NoError(t, err)
	})

	t.Run("closed engine returns storage closed", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.Close())

		_, _, err := engine.DeleteByPrefix("db1:")
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestBadgerEngine_DeleteByPrefix_EmptyPrefix(t *testing.T) {
	engine := createTestBadgerEngine(t)

	_, _, err := engine.DeleteByPrefix("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "prefix cannot be empty")
}

func TestBadgerEngine_DeleteByPrefix_CleansIndexes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes with label and edge type indexes
	n1 := &Node{ID: NodeID(prefixTestID("dp-n1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	n2 := &Node{ID: NodeID(prefixTestID("dp-n2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	e := &Edge{ID: EdgeID(prefixTestID("dp-e1")), StartNode: n1.ID, EndNode: n2.ID, Type: "KNOWS", Properties: map[string]interface{}{}}
	require.NoError(t, engine.CreateEdge(e))

	// Verify data exists
	nodes, err := engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	// Delete by prefix — "test:" matches the test prefix
	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(2), nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)

	// Verify everything is gone
	nodes, err = engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 0)

	edges, err := engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, edges, 0)
}
