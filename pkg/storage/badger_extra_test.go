package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// BadgerEngine – AllNodes / AllEdges / BatchGetNodes
// ============================================================================

func TestBadgerEngine_AllNodes_Empty(t *testing.T) {
	b := createTestBadgerEngine(t)
	nodes, err := b.AllNodes()
	require.NoError(t, err)
	assert.Empty(t, nodes)
}

func TestBadgerEngine_AllNodes_WithData(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := testNode(prefixTestID("ban1"))
	_, err := b.CreateNode(n)
	require.NoError(t, err)

	nodes, err := b.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 1)
}

func TestBadgerEngine_AllEdges_Empty(t *testing.T) {
	b := createTestBadgerEngine(t)
	edges, err := b.AllEdges()
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestBadgerEngine_AllEdges_WithData(t *testing.T) {
	b := createTestBadgerEngine(t)
	n1 := testNode(prefixTestID("bae1"))
	n2 := testNode(prefixTestID("bae2"))
	_, err := b.CreateNode(n1)
	require.NoError(t, err)
	_, err = b.CreateNode(n2)
	require.NoError(t, err)

	e := &Edge{
		ID:         EdgeID(prefixTestID("bae-e1")),
		StartNode:  NodeID(prefixTestID("bae1")),
		EndNode:    NodeID(prefixTestID("bae2")),
		Type:       "KNOWS",
		Properties: map[string]interface{}{},
	}
	err = b.CreateEdge(e)
	require.NoError(t, err)

	edges, err := b.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestBadgerEngine_BatchGetNodes_Empty(t *testing.T) {
	b := createTestBadgerEngine(t)
	result, err := b.BatchGetNodes([]NodeID{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestBadgerEngine_BatchGetNodes_WithData(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := testNode(prefixTestID("bgn1"))
	id, err := b.CreateNode(n)
	require.NoError(t, err)

	result, err := b.BatchGetNodes([]NodeID{id})
	require.NoError(t, err)
	assert.Len(t, result, 1)
}

func TestBadgerEngine_BatchGetNodes_Missing(t *testing.T) {
	b := createTestBadgerEngine(t)
	result, err := b.BatchGetNodes([]NodeID{NodeID(prefixTestID("nonexist"))})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestBadgerEngine_InvalidatePendingEmbeddingsIndex_NoOp(t *testing.T) {
	b := createTestBadgerEngine(t)
	// Badger pending embeddings index is persistent; invalidation is a no-op.
	b.InvalidatePendingEmbeddingsIndex()
}

func TestBadgerEngine_QueryHelpers_Extra(t *testing.T) {
	t.Run("GetFirstNodeByLabel skips stale and corrupt indexed entries", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		missingID := NodeID(prefixTestID("aa-missing"))
		corruptID := NodeID(prefixTestID("ab-corrupt"))
		valid := &Node{ID: NodeID(prefixTestID("zz-valid")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "valid"}}
		_, err := b.CreateNode(valid)
		require.NoError(t, err)

		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			missingKey, err := b.labelIndexKeyString(txn, "Person", missingID)
			if err != nil {
				return err
			}
			if err := txn.Set(missingKey, []byte{}); err != nil {
				return err
			}
			corruptKey, err := b.labelIndexKeyString(txn, "Person", corruptID)
			if err != nil {
				return err
			}
			if err := txn.Set(corruptKey, []byte{}); err != nil {
				return err
			}
			return txn.Set(nodeKey(corruptID), []byte("not-a-node"))
		}))

		got, err := b.GetFirstNodeByLabel("Person")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, valid.ID, got.ID)
	})

	t.Run("ForEachNodeIDByLabel invalidates stale cache and supports early stop", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		first := &Node{ID: NodeID(prefixTestID("foreach-a")), Labels: []string{"Person"}}
		second := &Node{ID: NodeID(prefixTestID("foreach-b")), Labels: []string{"Person"}}
		_, err := b.CreateNode(first)
		require.NoError(t, err)
		_, err = b.CreateNode(second)
		require.NoError(t, err)

		staleID := NodeID(prefixTestID("foreach-stale"))
		b.labelCacheSetFirst("Person", staleID)

		var visited []NodeID
		err = b.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
			visited = append(visited, id)
			return true
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []NodeID{first.ID, second.ID}, visited)

		cachedID, ok := b.labelCacheGetFirst("Person")
		require.True(t, ok)
		assert.NotEqual(t, staleID, cachedID)

		visited = visited[:0]
		err = b.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
			visited = append(visited, id)
			return false
		})
		require.NoError(t, err)
		assert.Len(t, visited, 1)
	})

	t.Run("BatchGetNodes merges cache hits db hits and empty ids", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		cached := &Node{ID: NodeID(prefixTestID("batch-cached")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "cached"}}
		stored := &Node{ID: NodeID(prefixTestID("batch-stored")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "stored"}}
		b.cacheStoreNode(cached)
		_, err := b.CreateNode(stored)
		require.NoError(t, err)

		result, err := b.BatchGetNodes([]NodeID{"", cached.ID, stored.ID, NodeID(prefixTestID("batch-missing"))})
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "cached", result[cached.ID].Properties["name"])
		assert.Equal(t, "stored", result[stored.ID].Properties["name"])

		b.nodeCacheMu.RLock()
		_, cachedStored := b.nodeCache[stored.ID]
		b.nodeCacheMu.RUnlock()
		assert.True(t, cachedStored)
	})

	t.Run("BatchGetNodes all cache hits returns copies without DB read", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		n1 := &Node{ID: NodeID(prefixTestID("cache-only-1")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "one"}}
		n2 := &Node{ID: NodeID(prefixTestID("cache-only-2")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "two"}}
		b.cacheStoreNode(n1)
		b.cacheStoreNode(n2)

		result, err := b.BatchGetNodes([]NodeID{n1.ID, n2.ID})
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.NotSame(t, n1, result[n1.ID])
		require.NotSame(t, n2, result[n2.ID])
		assert.Equal(t, "one", result[n1.ID].Properties["name"])
		assert.Equal(t, "two", result[n2.ID].Properties["name"])
	})

	t.Run("BatchGetNodes skips corrupt payloads and missing keys", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		valid := &Node{ID: NodeID(prefixTestID("batch-valid")), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "valid"}}
		corruptID := NodeID(prefixTestID("batch-corrupt"))
		_, err := b.CreateNode(valid)
		require.NoError(t, err)
		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(nodeKey(corruptID), []byte("not-a-node"))
		}))

		result, err := b.BatchGetNodes([]NodeID{valid.ID, corruptID, NodeID(prefixTestID("batch-missing"))})
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, valid.ID, result[valid.ID].ID)
	})

	t.Run("BatchGetNodes returns error when view transaction fails", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		require.NoError(t, b.Close())
		_, err := b.BatchGetNodes([]NodeID{NodeID(prefixTestID("after-close"))})
		require.Error(t, err)
	})

	t.Run("edge query helpers validate ids and skip corrupt payloads", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		_, err := b.GetOutgoingEdges("")
		require.ErrorIs(t, err, ErrInvalidID)
		_, err = b.GetIncomingEdges("")
		require.ErrorIs(t, err, ErrInvalidID)

		start := testNode(prefixTestID("edge-start"))
		end := testNode(prefixTestID("edge-end"))
		_, err = b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)
		require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(prefixTestID("edge-good")), StartNode: start.ID, EndNode: end.ID, Type: "REL"}))

		corruptID := EdgeID(prefixTestID("edge-bad"))
		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			corruptOut, err := b.outgoingIndexKeyString(txn, start.ID, corruptID)
			if err != nil {
				return err
			}
			if err := txn.Set(corruptOut, []byte{}); err != nil {
				return err
			}
			corruptIn, err := b.incomingIndexKeyString(txn, end.ID, corruptID)
			if err != nil {
				return err
			}
			if err := txn.Set(corruptIn, []byte{}); err != nil {
				return err
			}
			corruptType, err := b.edgeTypeIndexKeyString(txn, "REL", corruptID)
			if err != nil {
				return err
			}
			if err := txn.Set(corruptType, []byte{}); err != nil {
				return err
			}
			return txn.Set(edgeKey(corruptID), []byte("not-an-edge"))
		}))

		outgoing, err := b.GetOutgoingEdges(start.ID)
		require.NoError(t, err)
		assert.Len(t, outgoing, 1)

		incoming, err := b.GetIncomingEdges(end.ID)
		require.NoError(t, err)
		assert.Len(t, incoming, 1)

		byType, err := b.GetEdgesByType("REL")
		require.NoError(t, err)
		assert.Len(t, byType, 1)
	})

	t.Run("UpdateEdge endpoint changes require existing nodes", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		start := testNode(prefixTestID("upd-start"))
		end := testNode(prefixTestID("upd-end"))
		_, err := b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)

		edge := &Edge{
			ID:         EdgeID(prefixTestID("upd-e1")),
			StartNode:  start.ID,
			EndNode:    end.ID,
			Type:       "REL",
			Properties: map[string]interface{}{},
		}
		require.NoError(t, b.CreateEdge(edge))

		edge.StartNode = NodeID(prefixTestID("missing-node"))
		err = b.UpdateEdge(edge)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("UpdateEdge returns decode error for corrupt stored edge", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		start := testNode(prefixTestID("upd2-start"))
		end := testNode(prefixTestID("upd2-end"))
		_, err := b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)
		edge := &Edge{
			ID:         EdgeID(prefixTestID("upd2-e1")),
			StartNode:  start.ID,
			EndNode:    end.ID,
			Type:       "REL",
			Properties: map[string]interface{}{},
		}
		require.NoError(t, b.CreateEdge(edge))
		require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey(edge.ID), []byte("not-an-edge"))
		}))
		err = b.UpdateEdge(edge)
		require.Error(t, err)
	})

	t.Run("DeleteEdge succeeds for typeless edge", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		start := testNode(prefixTestID("del-start"))
		end := testNode(prefixTestID("del-end"))
		_, err := b.CreateNode(start)
		require.NoError(t, err)
		_, err = b.CreateNode(end)
		require.NoError(t, err)

		edge := &Edge{
			ID:         EdgeID(prefixTestID("del-typeless")),
			StartNode:  start.ID,
			EndNode:    end.ID,
			Type:       "",
			Properties: map[string]interface{}{},
		}
		require.NoError(t, b.CreateEdge(edge))
		require.NoError(t, b.DeleteEdge(edge.ID))
		_, err = b.GetEdge(edge.ID)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// ============================================================================
// BadgerEngine – BulkCreate / BulkDelete
// ============================================================================

func TestBadgerEngine_BulkCreateNodes_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	nodes := []*Node{
		testNode(prefixTestID("bulk-bn1")),
		testNode(prefixTestID("bulk-bn2")),
	}
	err := b.BulkCreateNodes(nodes)
	require.NoError(t, err)

	all, err := b.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(all), 2)

	t.Run("stores oversized node embeddings as separate chunks", func(t *testing.T) {
		chunks := make([][]float32, 4)
		for c := range chunks {
			chunks[c] = make([]float32, 10000) // each chunk < badger value limit
			for i := range chunks[c] {
				chunks[c][i] = float32((c + i) % 7)
			}
		}
		node := &Node{
			ID:              NodeID(prefixTestID("bulk-oversized")),
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"name": "oversized"},
			ChunkEmbeddings: chunks,
		}
		err := b.BulkCreateNodes([]*Node{node})
		require.NoError(t, err)

		loaded, getErr := b.GetNode(node.ID)
		require.NoError(t, getErr)
		require.NotNil(t, loaded)
		require.Len(t, loaded.ChunkEmbeddings, len(chunks))
		require.Len(t, loaded.ChunkEmbeddings[0], len(chunks[0]))
	})

	t.Run("returns encode error for unsupported properties", func(t *testing.T) {
		node := &Node{
			ID:     NodeID(prefixTestID("bulk-invalid-prop")),
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"bad": make(chan int),
			},
		}
		err := b.BulkCreateNodes([]*Node{node})
		require.ErrorContains(t, err, "failed to encode node")
	})
}

func TestBadgerEngine_BulkCreateNodes_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkCreateNodes(nil))
}

func TestBadgerEngine_BulkCreateEdges_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n1 := testNode(prefixTestID("bce1"))
	n2 := testNode(prefixTestID("bce2"))
	_, _ = b.CreateNode(n1)
	_, _ = b.CreateNode(n2)

	edges := []*Edge{{
		ID: EdgeID(prefixTestID("bulk-be1")), StartNode: NodeID(prefixTestID("bce1")),
		EndNode: NodeID(prefixTestID("bce2")), Type: "LINK", Properties: map[string]interface{}{},
	}}
	err := b.BulkCreateEdges(edges)
	require.NoError(t, err)
}

func TestBadgerEngine_BulkCreateEdges_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkCreateEdges(nil))
}

func TestBadgerEngine_BulkDeleteNodes_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := testNode(prefixTestID("bdn1"))
	id, _ := b.CreateNode(n)
	err := b.BulkDeleteNodes([]NodeID{id})
	require.NoError(t, err)
}

func TestBadgerEngine_BulkDeleteNodes_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkDeleteNodes([]NodeID{}))
}

func TestBadgerEngine_BulkDeleteEdges_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n1 := testNode(prefixTestID("bde1"))
	n2 := testNode(prefixTestID("bde2"))
	_, _ = b.CreateNode(n1)
	_, _ = b.CreateNode(n2)
	e := &Edge{
		ID: EdgeID(prefixTestID("bde-e1")), StartNode: NodeID(prefixTestID("bde1")),
		EndNode: NodeID(prefixTestID("bde2")), Type: "KNOWS", Properties: map[string]interface{}{},
	}
	_ = b.CreateEdge(e)
	err := b.BulkDeleteEdges([]EdgeID{EdgeID(prefixTestID("bde-e1"))})
	require.NoError(t, err)
}

func TestBadgerEngine_BulkDeleteEdges_ExtraEmpty(t *testing.T) {
	b := createTestBadgerEngine(t)
	assert.NoError(t, b.BulkDeleteEdges([]EdgeID{}))
}

// ============================================================================
// SchemaManager – AddUniqueConstraint / AddPropertyTypeConstraint / CheckUniqueConstraint
// ============================================================================

func TestSchemaManager_AddUniqueConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()
	require.NotNil(t, sm)

	err := sm.AddUniqueConstraint("uc-person-email", "Person", "email")
	assert.NoError(t, err)
}

func TestSchemaManager_AddUniqueConstraint_Duplicate(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	err := sm.AddUniqueConstraint("uc-dup", "User", "username")
	assert.NoError(t, err)

	// Duplicate without IF NOT EXISTS should error
	err = sm.AddUniqueConstraint("uc-dup", "User", "username")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Duplicate with IF NOT EXISTS should succeed silently
	err = sm.AddUniqueConstraint("uc-dup", "User", "username", true)
	assert.NoError(t, err)
}

func TestSchemaManager_AddPropertyTypeConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	err := sm.AddPropertyTypeConstraint("ptc-age", "Person", "age", PropertyTypeInteger)
	assert.NoError(t, err)
}

func TestSchemaManager_CheckUniqueConstraint_NoConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	// No constraint registered for this label/property — should return no error
	err := sm.CheckUniqueConstraint("Ghost", "prop", "val", "")
	assert.NoError(t, err)
}

func TestSchemaManager_CheckUniqueConstraint_WithConstraint(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()

	_ = sm.AddUniqueConstraint("uc-check", "Item", "code")

	// No data yet → no conflict
	err := sm.CheckUniqueConstraint("Item", "code", "ABC123", "")
	assert.NoError(t, err)
}

func TestBadgerEngine_validateBulkNodeConstraints(t *testing.T) {
	t.Run("rejects unprefixed ids and batch constraint violations", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		schema := b.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddUniqueConstraint("user_email", "User", "email"))
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "user_key",
			Type:       ConstraintNodeKey,
			Label:      "User",
			Properties: []string{"tenant", "username"},
		}))
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "user_name_exists",
			Type:       ConstraintExists,
			Label:      "User",
			Properties: []string{"name"},
		}))

		err := b.validateBulkNodeConstraints([]*Node{{ID: "plain-id", Labels: []string{"User"}}})
		require.ErrorContains(t, err, "node ID must be prefixed with namespace")

		err = b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u1")), Labels: []string{"User"}, Properties: map[string]any{"email": "dup@example.com", "tenant": "t1", "username": "alice", "name": "Alice"}},
			{ID: NodeID(prefixTestID("u2")), Labels: []string{"User"}, Properties: map[string]any{"email": "dup@example.com", "tenant": "t2", "username": "bob", "name": "Bob"}},
		})
		require.Error(t, err)
		var violation *ConstraintViolationError
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, ConstraintUnique, violation.Type)

		err = b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u3")), Labels: []string{"User"}, Properties: map[string]any{"tenant": "t1", "name": "Alice"}},
		})
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, ConstraintNodeKey, violation.Type)

		err = b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u4")), Labels: []string{"User"}, Properties: map[string]any{"tenant": "t1", "username": "alice"}},
		})
		require.ErrorAs(t, err, &violation)
		assert.Equal(t, ConstraintExists, violation.Type)
	})

	t.Run("ignores unsupported unique arity and nil unique values", func(t *testing.T) {
		b := createTestBadgerEngine(t)
		schema := b.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "ignored_multi_unique",
			Type:       ConstraintUnique,
			Label:      "Multi",
			Properties: []string{"a", "b"},
		}))
		require.NoError(t, schema.AddUniqueConstraint("user_email", "User", "email"))

		err := b.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("m1")), Labels: []string{"Multi"}, Properties: map[string]any{"b": "only-second"}},
			{ID: NodeID(prefixTestID("u5")), Labels: []string{"User"}, Properties: map[string]any{"email": nil}},
			{ID: NodeID(prefixTestID("u6")), Labels: []string{"NoConstraint"}, Properties: map[string]any{"name": "ok"}},
		})
		require.NoError(t, err)
	})
}

// ============================================================================
// NodeConfig / NodeConfigStore
// ============================================================================

func TestNodeConfig_AddToPin(t *testing.T) {
	cfg := &NodeConfig{PinList: []string{}, DenyList: []string{}}
	cfg.AddToPin("target-node-1")
	cfg.AddToPin("target-node-2")
	assert.Len(t, cfg.PinList, 2)
}

func TestNodeConfig_AddToDeny(t *testing.T) {
	cfg := &NodeConfig{PinList: []string{}, DenyList: []string{}}
	cfg.AddToDeny("blocked-node-1")
	assert.Len(t, cfg.DenyList, 1)
}

func TestNodeConfigStore_AddToNodePinList(t *testing.T) {
	store := NewNodeConfigStore()
	store.AddToNodePinList("nornic:node-a", "nornic:node-b")
	cfg := store.Get("nornic:node-a")
	require.NotNil(t, cfg)
	assert.Contains(t, cfg.PinList, "nornic:node-b")
}

func TestNodeConfigStore_AddToNodeDenyList(t *testing.T) {
	store := NewNodeConfigStore()
	store.AddToNodeDenyList("nornic:node-x", "nornic:node-y")
	cfg := store.Get("nornic:node-x")
	require.NotNil(t, cfg)
	assert.Contains(t, cfg.DenyList, "nornic:node-y")
}

func TestBadgerEngine_ForEachNodeIDByLabel_EarlyStop(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create several nodes with the same label
	for i := 0; i < 5; i++ {
		n := &Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("each-n%d", i))),
			Labels:     []string{"Batch"},
			Properties: map[string]interface{}{},
		}
		_, err := engine.CreateNode(n)
		require.NoError(t, err)
	}

	// Visit and stop after 2
	var visited []NodeID
	err := engine.ForEachNodeIDByLabel("Batch", func(id NodeID) bool {
		visited = append(visited, id)
		return len(visited) < 2 // stop after 2
	})
	require.NoError(t, err)
	assert.Len(t, visited, 2)
}

func TestBadgerEngine_ForEachNodeIDByLabel_NilVisitor(t *testing.T) {
	engine := createTestBadgerEngine(t)
	err := engine.ForEachNodeIDByLabel("Person", nil)
	assert.NoError(t, err) // nil visitor is a no-op
}

func TestBadgerEngine_ForEachNodeIDByLabel_ClosedEngine(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	err = engine.ForEachNodeIDByLabel("Person", func(id NodeID) bool { return true })
	assert.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerEngine_HasLabelBatch(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := &Node{ID: NodeID(prefixTestID("hlb-n1")), Labels: []string{"Person"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: NodeID(prefixTestID("hlb-n2")), Labels: []string{"Dog"}, Properties: map[string]interface{}{}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	result, err := engine.HasLabelBatch([]NodeID{n1.ID, n2.ID, NodeID(prefixTestID("hlb-missing"))}, "Person")
	require.NoError(t, err)
	assert.True(t, result[n1.ID])
	assert.False(t, result[n2.ID])
	assert.False(t, result[NodeID(prefixTestID("hlb-missing"))])
}

func TestBadgerEngine_HasLabelBatch_Empty(t *testing.T) {
	engine := createTestBadgerEngine(t)
	result, err := engine.HasLabelBatch(nil, "Person")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestBadgerEngine_GetEdgesByType_EmptyType(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes and edge
	n1 := &Node{ID: NodeID(prefixTestID("ete-n1")), Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: NodeID(prefixTestID("ete-n2")), Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("ete-e1")), StartNode: n1.ID, EndNode: n2.ID, Type: "KNOWS", Properties: map[string]interface{}{}}))

	// Empty type returns all edges
	edges, err := engine.GetEdgesByType("")
	require.NoError(t, err)
	assert.Len(t, edges, 1)
}

func TestBadgerEngine_GetEdgesByType_CacheEviction(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := &Node{ID: NodeID(prefixTestID("ce-n1")), Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: NodeID(prefixTestID("ce-n2")), Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	// Create edges with many different types to trigger cache eviction
	for i := 0; i < 200; i++ {
		edgeType := fmt.Sprintf("TYPE_%d", i)
		e := &Edge{ID: EdgeID(prefixTestID(fmt.Sprintf("ce-e%d", i))), StartNode: n1.ID, EndNode: n2.ID, Type: edgeType, Properties: map[string]interface{}{}}
		require.NoError(t, engine.CreateEdge(e))
	}

	// Query them all to populate and overflow cache
	for i := 0; i < 200; i++ {
		edges, err := engine.GetEdgesByType(fmt.Sprintf("TYPE_%d", i))
		require.NoError(t, err)
		assert.Len(t, edges, 1)
	}
}

func TestBadgerEngine_GetOutgoingEdges_EmptyID(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.GetOutgoingEdges("")
	assert.ErrorIs(t, err, ErrInvalidID)
}

func TestBadgerEngine_GetIncomingEdges_EmptyID(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.GetIncomingEdges("")
	assert.ErrorIs(t, err, ErrInvalidID)
}

func TestBadgerEngine_BulkDeleteNodes_WithEdges(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("bdn-n1")
	n2 := testNode("bdn-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(testEdge("bdn-e1", "bdn-n1", "bdn-n2", "KNOWS")))

	// Bulk delete both nodes — should also cascade delete edges
	err = engine.BulkDeleteNodes([]NodeID{n1.ID, n2.ID})
	require.NoError(t, err)

	_, err = engine.GetNode(n1.ID)
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge(EdgeID(prefixTestID("bdn-e1")))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_BulkDeleteEdges_Multiple(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("bde-n1")
	n2 := testNode("bde-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	e1 := testEdge("bde-e1", "bde-n1", "bde-n2", "KNOWS")
	e2 := testEdge("bde-e2", "bde-n1", "bde-n2", "LIKES")
	require.NoError(t, engine.CreateEdge(e1))
	require.NoError(t, engine.CreateEdge(e2))

	// Delete both edges
	err = engine.BulkDeleteEdges([]EdgeID{e1.ID, e2.ID})
	require.NoError(t, err)

	_, err = engine.GetEdge(e1.ID)
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge(e2.ID)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_DeleteEdge_UnknownType(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("det-n1")
	n2 := testNode("det-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	e := testEdge("det-e1", "det-n1", "det-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(e))

	// Delete without knowing the type first — exercises the cacheOnEdgeDeleted path
	err = engine.DeleteEdge(e.ID)
	require.NoError(t, err)

	_, err = engine.GetEdge(e.ID)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_DeleteEdge_NotFound(t *testing.T) {
	engine := createTestBadgerEngine(t)
	err := engine.DeleteEdge(EdgeID(prefixTestID("nonexistent")))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_CreateEdge_DuplicateEdge(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("ced-n1")
	n2 := testNode("ced-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	e := testEdge("ced-e1", "ced-n1", "ced-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(e))

	err = engine.CreateEdge(e)
	assert.ErrorIs(t, err, ErrAlreadyExists)
}

func TestBadgerEngine_BulkCreateNodes_UniqueConstraintViolation(t *testing.T) {
	engine := createTestBadgerEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "unique_email",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"email"},
	}))

	nodes := []*Node{
		{ID: NodeID(prefixTestID("bcu-n1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"email": "dup@test.com"}},
		{ID: NodeID(prefixTestID("bcu-n2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"email": "dup@test.com"}},
	}
	err := engine.BulkCreateNodes(nodes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists in batch")
}

func TestBadgerEngine_BulkCreateNodes_NodeKeyConstraintViolation(t *testing.T) {
	engine := createTestBadgerEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "nodekey_person",
		Type:       ConstraintNodeKey,
		Label:      "Person",
		Properties: []string{"first", "last"},
	}))

	nodes := []*Node{
		{ID: NodeID(prefixTestID("bck-n1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"first": "John", "last": "Doe"}},
		{ID: NodeID(prefixTestID("bck-n2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"first": "John", "last": "Doe"}},
	}
	err := engine.BulkCreateNodes(nodes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists in batch")
}

func TestBadgerEngine_BulkCreateNodes_NodeKeyNullProperty(t *testing.T) {
	engine := createTestBadgerEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "nodekey_person2",
		Type:       ConstraintNodeKey,
		Label:      "Person",
		Properties: []string{"first", "last"},
	}))

	nodes := []*Node{
		{ID: NodeID(prefixTestID("bckn-n1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"first": "John"}}, // "last" is nil
	}
	err := engine.BulkCreateNodes(nodes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be null")
}

func TestBadgerEngine_BulkCreateNodes_ExistsConstraintViolation(t *testing.T) {
	engine := createTestBadgerEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "exists_name",
		Type:       ConstraintExists,
		Label:      "Person",
		Properties: []string{"name"},
	}))

	nodes := []*Node{
		{ID: NodeID(prefixTestID("bce-n1")), Labels: []string{"Person"}, Properties: map[string]interface{}{}}, // missing "name"
	}
	err := engine.BulkCreateNodes(nodes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
}

func TestBadgerEngine_BulkCreateNodes_ExistsConstraintNilProperties(t *testing.T) {
	engine := createTestBadgerEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "exists_name2",
		Type:       ConstraintExists,
		Label:      "Person",
		Properties: []string{"name"},
	}))

	nodes := []*Node{
		{ID: NodeID(prefixTestID("bcen-n1")), Labels: []string{"Person"}, Properties: nil},
	}
	err := engine.BulkCreateNodes(nodes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
}

func TestBadgerEngine_BulkCreateEdges_MissingNodes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create one node but not the other
	n1 := testNode("bcedge-n1")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)

	edges := []*Edge{
		{ID: EdgeID(prefixTestID("bcedge-e1")), StartNode: n1.ID, EndNode: NodeID(prefixTestID("missing")), Type: "KNOWS", Properties: map[string]interface{}{}},
	}
	err = engine.BulkCreateEdges(edges)
	assert.Error(t, err) // end node doesn't exist
}

// makeLargeChunkEmbeddings creates multiple embedding chunks that collectively
// exceed maxNodeSize (50KB) but individually stay under Badger's 65KB value limit.
func makeLargeChunkEmbeddings() [][]float32 {
	chunks := make([][]float32, 4)
	for c := 0; c < 4; c++ {
		chunk := make([]float32, 4000) // ~16KB each, total ~64KB > 50KB threshold
		for i := range chunk {
			chunk[i] = float32(c*4000+i) * 0.001
		}
		chunks[c] = chunk
	}
	return chunks
}

func TestBadgerEngine_CreateNode_LargeEmbedding_SeparateStorage(t *testing.T) {
	engine := createTestBadgerEngine(t)

	node := &Node{
		ID:              NodeID(prefixTestID("large-emb-n1")),
		Labels:          []string{"Document"},
		Properties:      map[string]interface{}{"title": "test"},
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
	}

	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	assert.Equal(t, "Document", got.Labels[0])
	require.Len(t, got.ChunkEmbeddings, 4)
	assert.Len(t, got.ChunkEmbeddings[0], 4000)
}

func TestBadgerEngine_UpdateNode_LargeEmbedding_SeparateStorage(t *testing.T) {
	engine := createTestBadgerEngine(t)

	node := &Node{
		ID:         NodeID(prefixTestID("large-emb-up-n1")),
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"title": "test"},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	// Update with large embeddings
	node.ChunkEmbeddings = makeLargeChunkEmbeddings()
	err = engine.UpdateNode(node)
	require.NoError(t, err)

	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 4)
	assert.Len(t, got.ChunkEmbeddings[0], 4000)
}

func TestTransaction_CreateNode_LargeEmbedding_SeparateStorage(t *testing.T) {
	engine := createTestBadgerEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	node := &Node{
		ID:              NodeID(prefixTestID("tx-large-n1")),
		Labels:          []string{"Document"},
		Properties:      map[string]interface{}{"title": "test"},
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
	}

	_, err = tx.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 4)
	assert.Len(t, got.ChunkEmbeddings[0], 4000)
}

func TestBadgerEngine_UpdateNodeEmbedding_LargeThenSmall(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create node with large embeddings (stored separately)
	node := &Node{
		ID:              NodeID(prefixTestID("emb-swap-n1")),
		Labels:          []string{"Document"},
		Properties:      map[string]interface{}{"title": "test"},
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	// Verify large embeddings stored
	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 4)

	// Now update with small embedding (should go inline, cleaning up separate storage)
	smallEmb := [][]float32{{0.1, 0.2, 0.3}}
	updateNode := &Node{
		ID:              node.ID,
		ChunkEmbeddings: smallEmb,
	}
	err = engine.UpdateNodeEmbedding(updateNode)
	require.NoError(t, err)

	got, err = engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1)
	assert.Len(t, got.ChunkEmbeddings[0], 3)
}

func TestBadgerEngine_UpdateNodeEmbedding_SmallToLarge(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create node with small embedding
	node := &Node{
		ID:              NodeID(prefixTestID("emb-grow-n1")),
		Labels:          []string{"Document"},
		Properties:      map[string]interface{}{"title": "test"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	// Update with large embeddings (triggers replaceSeparateEmbeddingChunks)
	updateNode := &Node{
		ID:              node.ID,
		ChunkEmbeddings: makeLargeChunkEmbeddings(),
	}
	err = engine.UpdateNodeEmbedding(updateNode)
	require.NoError(t, err)

	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 4)
	assert.Len(t, got.ChunkEmbeddings[0], 4000)
}

func TestBadgerEngine_UpdateNodeEmbedding_NotFound(t *testing.T) {
	engine := createTestBadgerEngine(t)

	err := engine.UpdateNodeEmbedding(&Node{
		ID:              NodeID(prefixTestID("nonexistent")),
		ChunkEmbeddings: [][]float32{{0.1}},
	})
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_UpdateNode_UpsertNewNode(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// UpdateNode on a non-existing node should upsert (insert)
	node := &Node{
		ID:         NodeID(prefixTestID("upsert-n1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	err := engine.UpdateNode(node)
	require.NoError(t, err)

	// Verify the node was created
	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	assert.Equal(t, "Alice", got.Properties["name"])

	// Verify label index was created
	nodes, err := engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	found := false
	for _, n := range nodes {
		if n.ID == node.ID {
			found = true
			break
		}
	}
	assert.True(t, found, "upserted node should appear in label index")
}

func TestBadgerEngine_UpdateNode_LabelChange(t *testing.T) {
	engine := createTestBadgerEngine(t)

	node := &Node{
		ID:         NodeID(prefixTestID("lc-n1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	// Update labels
	node.Labels = []string{"Employee"}
	err = engine.UpdateNode(node)
	require.NoError(t, err)

	// Old label should not find it
	persons, err := engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	for _, n := range persons {
		assert.NotEqual(t, node.ID, n.ID)
	}

	// New label should find it
	employees, err := engine.GetNodesByLabel("Employee")
	require.NoError(t, err)
	found := false
	for _, n := range employees {
		if n.ID == node.ID {
			found = true
		}
	}
	assert.True(t, found)
}

func TestBadgerEngine_UpdateNodeEmbedding_Validation(t *testing.T) {
	engine := createTestBadgerEngine(t)

	err := engine.UpdateNodeEmbedding(nil)
	assert.ErrorIs(t, err, ErrInvalidData)

	err = engine.UpdateNodeEmbedding(&Node{ID: ""})
	assert.ErrorIs(t, err, ErrInvalidID)
}

func TestBadgerEngine_BulkCreateNodes_LargeEmbedding(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Use multiple smaller chunks that total >50KB to trigger separate storage
	// but each chunk stays under Badger's 65KB value limit
	chunk1 := make([]float32, 4000) // ~16KB per chunk
	chunk2 := make([]float32, 4000)
	chunk3 := make([]float32, 4000)
	chunk4 := make([]float32, 4000)
	for i := range chunk1 {
		chunk1[i] = float32(i) * 0.001
		chunk2[i] = float32(i) * 0.002
		chunk3[i] = float32(i) * 0.003
		chunk4[i] = float32(i) * 0.004
	}

	nodes := []*Node{
		{
			ID:              NodeID(prefixTestID("bulk-large-n1")),
			Labels:          []string{"Document"},
			Properties:      map[string]interface{}{"title": "bulk1"},
			ChunkEmbeddings: [][]float32{chunk1, chunk2, chunk3, chunk4},
		},
	}
	err := engine.BulkCreateNodes(nodes)
	require.NoError(t, err)

	got, err := engine.GetNode(nodes[0].ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 4)
	assert.Len(t, got.ChunkEmbeddings[0], 4000)
}

func TestBadgerEngine_CreateEdge_MissingNodes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Start node doesn't exist
	e := testEdge("cem-e1", "cem-missing", "cem-n2", "KNOWS")
	err := engine.CreateEdge(e)
	assert.ErrorIs(t, err, ErrNotFound)

	// Create start node but end node doesn't exist
	n1 := testNode("cem-n1")
	_, err = engine.CreateNode(n1)
	require.NoError(t, err)

	e2 := testEdge("cem-e2", "cem-n1", "cem-missing2", "KNOWS")
	err = engine.CreateEdge(e2)
	assert.ErrorIs(t, err, ErrNotFound)
}
