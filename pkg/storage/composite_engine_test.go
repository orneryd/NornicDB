package storage

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type compositeStreamEngine struct {
	Engine
	streamNodeErr error
	streamEdgeErr error
	chunkErr      error
}

type compositeErrorEngine struct {
	*MemoryEngine
	getNodeErr         error
	getEdgeErr         error
	firstNodeErr       error
	returnNilFirstNode bool
	outgoingErr        error
	incomingErr        error
	betweenErr         error
	byTypeErr          error
	allNodesErr        error
	allEdgesErr        error
	batchGetErr        error
	nodeCountErr       error
	edgeCountErr       error
	closeErr           error
	createNodeErr      error
	createEdgeErr      error
	bulkCreateNodesErr error
	bulkCreateEdgesErr error
}

func (e *compositeErrorEngine) GetNode(ctx context.Context, id NodeID) (*Node, error) {
	if e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	return e.MemoryEngine.GetNode(ctx, id)
}

func (e *compositeErrorEngine) GetEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	if e.getEdgeErr != nil {
		return nil, e.getEdgeErr
	}
	return e.MemoryEngine.GetEdge(ctx, id)
}

func (e *compositeErrorEngine) GetFirstNodeByLabel(ctx context.Context, label string) (*Node, error) {
	if e.firstNodeErr != nil {
		return nil, e.firstNodeErr
	}
	if e.returnNilFirstNode {
		return nil, nil
	}
	return e.MemoryEngine.GetFirstNodeByLabel(ctx, label)
}

func (e *compositeErrorEngine) ReadAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	injected := e.outgoingErr
	if request.Direction == AdjacentIncoming {
		injected = e.incomingErr
	}
	if request.Direction == AdjacentBetween {
		injected = e.betweenErr
	}
	return fixtureowner.ReadWithFailure(ctx, injected, func(ctx context.Context) ([]*Edge, error) {
		return e.MemoryEngine.ReadAdjacentEdges(ctx, request)
	})
}

func (e *compositeErrorEngine) GetEdgesByType(ctx context.Context, edgeType string) ([]*Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.byTypeErr, func(ctx context.Context) ([]*Edge, error) {
		return e.MemoryEngine.GetEdgesByType(ctx, edgeType)
	})
}

func (e *compositeErrorEngine) AllNodes(ctx context.Context) ([]*Node, error) {
	return fixtureowner.ReadWithFailure(ctx, e.allNodesErr, func(ctx context.Context) ([]*Node, error) {
		return e.MemoryEngine.AllNodes(ctx)
	})
}

func (e *compositeErrorEngine) AllEdges(ctx context.Context) ([]*Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.allEdgesErr, func(ctx context.Context) ([]*Edge, error) {
		return e.MemoryEngine.AllEdges(ctx)
	})
}

func (e *compositeErrorEngine) BatchGetNodes(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	if e.batchGetErr != nil {
		return nil, e.batchGetErr
	}
	return e.MemoryEngine.BatchGetNodes(ctx, ids)
}

func (e *compositeErrorEngine) NodeCount(ctx context.Context) (int64, error) {
	if e.nodeCountErr != nil {
		return 0, e.nodeCountErr
	}
	return e.MemoryEngine.NodeCount(ctx)
}

func (e *compositeErrorEngine) EdgeCount(ctx context.Context) (int64, error) {
	if e.edgeCountErr != nil {
		return 0, e.edgeCountErr
	}
	return e.MemoryEngine.EdgeCount(ctx)
}

func (e *compositeErrorEngine) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	return e.MemoryEngine.Close()
}

func (e *compositeErrorEngine) CreateNode(ctx context.Context, node *Node) (NodeID, error) {
	if e.createNodeErr != nil {
		return "", e.createNodeErr
	}
	return e.MemoryEngine.CreateNode(ctx, node)
}

func (e *compositeErrorEngine) CreateEdge(ctx context.Context, edge *Edge) error {
	if e.createEdgeErr != nil {
		return e.createEdgeErr
	}
	return e.MemoryEngine.CreateEdge(ctx, edge)
}

func (e *compositeErrorEngine) BulkCreateNodes(ctx context.Context, nodes []*Node) error {
	if e.bulkCreateNodesErr != nil {
		return e.bulkCreateNodesErr
	}
	return e.MemoryEngine.BulkCreateNodes(ctx, nodes)
}

func (e *compositeErrorEngine) BulkCreateEdges(ctx context.Context, edges []*Edge) error {
	if e.bulkCreateEdgesErr != nil {
		return e.bulkCreateEdgesErr
	}
	return e.MemoryEngine.BulkCreateEdges(ctx, edges)
}

func (e *compositeStreamEngine) StreamNodes(_ context.Context, fn func(node *Node) error) error {
	if e.streamNodeErr != nil {
		return e.streamNodeErr
	}
	return streamFixtureItems(context.Background(), e.Engine.AllNodes, nil, fn)
}

func (e *compositeStreamEngine) StreamEdges(_ context.Context, fn func(edge *Edge) error) error {
	if e.streamEdgeErr != nil {
		return e.streamEdgeErr
	}
	return streamFixtureItems(context.Background(), e.Engine.AllEdges, nil, fn)
}

func (e *compositeStreamEngine) StreamNodeChunks(_ context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if e.chunkErr != nil {
		return e.chunkErr
	}
	nodes, err := e.Engine.AllNodes(context.Background())
	if err != nil {
		return err
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for i := 0; i < len(nodes); i += chunkSize {
		end := i + chunkSize
		if end > len(nodes) {
			end = len(nodes)
		}
		if err := fn(nodes[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func TestCompositeEngine_RoutingSetters(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)
	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2},
		map[string]string{"db1": "db1", "db2": "db2"},
		map[string]string{"db1": "read_write", "db2": "read_write"},
	)

	composite.SetLabelRouting("Person", []string{"db2"})
	composite.SetPropertyRouting("database_id", "tenant-a", "db1")
	composite.SetPropertyDefault("database_id", "db2")

	composite.mu.RLock()
	defer composite.mu.RUnlock()
	assert.Equal(t, []string{"db2"}, composite.labelRouting["person"])
	assert.Equal(t, "db1", composite.propertyRouting["database_id"]["tenant-a"])
	assert.Equal(t, "db2", composite.propertyDefaults["database_id"])
}

func TestCompositeEngine_StreamingAPIs(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)
	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2},
		map[string]string{"db1": "db1", "db2": "db2"},
		map[string]string{"db1": "read_write", "db2": "read_write"},
	)

	_, err := engine1.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = engine2.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"B"}})
	require.NoError(t, err)
	require.NoError(t, engine1.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n1")), Type: "LOOP"}))
	require.NoError(t, engine2.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e2")), StartNode: NodeID(prefixTestID("n2")), EndNode: NodeID(prefixTestID("n2")), Type: "LOOP"}))

	ctx := context.Background()
	var nodeCount int
	err = composite.StreamNodes(ctx, func(node *Node) error {
		nodeCount++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, nodeCount)

	var edgeCount int
	err = composite.StreamEdges(ctx, func(edge *Edge) error {
		edgeCount++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, edgeCount)

	var chunkCount int
	err = composite.StreamNodeChunks(ctx, 1, func(nodes []*Node) error {
		chunkCount += len(nodes)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, chunkCount)
}

func TestCompositeEngine_StreamingFallbackAndErrors(t *testing.T) {
	t.Run("streaming engines propagate wrapped errors", func(t *testing.T) {
		base1 := newTestMemoryEngine(t)
		base2 := newTestMemoryEngine(t)
		engine1 := &compositeStreamEngine{Engine: base1}
		engine2 := &compositeStreamEngine{Engine: base2}
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		_, err := base1.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("stream-n1")), Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = base1.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("stream-n2")), Labels: []string{"A"}})
		require.NoError(t, err)
		require.NoError(t, base1.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("stream-e1")), StartNode: NodeID(prefixTestID("stream-n1")), EndNode: NodeID(prefixTestID("stream-n2")), Type: "REL"}))

		errBoom := errors.New("node callback failed")
		err = composite.StreamNodes(context.Background(), func(node *Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("edge callback failed")
		err = composite.StreamEdges(context.Background(), func(edge *Edge) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("chunk callback failed")
		err = composite.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		engine1.streamNodeErr = errors.New("stream nodes failed")
		err = composite.StreamNodes(context.Background(), func(node *Node) error { return nil })
		require.ErrorContains(t, err, "error streaming from constituent 'db1'")

		engine1.streamNodeErr = nil
		engine1.streamEdgeErr = errors.New("stream edges failed")
		err = composite.StreamEdges(context.Background(), func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "error streaming from constituent 'db1'")

		engine1.streamEdgeErr = nil
		engine1.chunkErr = errors.New("stream chunks failed")
		err = composite.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return nil })
		require.ErrorContains(t, err, "error streaming from constituent 'db1'")
	})

	t.Run("fallback engines use allnodes and alledges", func(t *testing.T) {
		base1 := newTestMemoryEngine(t)
		base2 := newTestMemoryEngine(t)
		engine1 := &nonStreamingCountEngine{Engine: base1}
		engine2 := &nonStreamingCountEngine{Engine: base2}
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		_, err := base1.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("fallback-n1")), Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = base2.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("fallback-n2")), Labels: []string{"B"}})
		require.NoError(t, err)
		require.NoError(t, base1.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("fallback-e1")), StartNode: NodeID(prefixTestID("fallback-n1")), EndNode: NodeID(prefixTestID("fallback-n1")), Type: "REL"}))
		require.NoError(t, base2.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("fallback-e2")), StartNode: NodeID(prefixTestID("fallback-n2")), EndNode: NodeID(prefixTestID("fallback-n2")), Type: "REL"}))

		var nodeCount int
		err = composite.StreamNodes(context.Background(), func(node *Node) error {
			nodeCount++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, nodeCount)

		var edgeCount int
		err = composite.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeCount++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, edgeCount)

		var chunkCount int
		err = composite.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error {
			chunkCount += len(nodes)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, chunkCount)

		errBoom := errors.New("fallback node callback failed")
		err = composite.StreamNodes(context.Background(), func(node *Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("fallback edge callback failed")
		err = composite.StreamEdges(context.Background(), func(edge *Edge) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("fallback chunk callback failed")
		err = composite.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		engine1.allNodesErr = errors.New("all nodes failed")
		err = composite.StreamNodes(context.Background(), func(node *Node) error { return nil })
		require.ErrorContains(t, err, "error querying constituent 'db1'")

		engine1.allNodesErr = nil
		engine1.allEdgesErr = errors.New("all edges failed")
		err = composite.StreamEdges(context.Background(), func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "error querying constituent 'db1'")

		engine1.allEdgesErr = nil
		engine1.allNodesErr = errors.New("chunk nodes failed")
		err = composite.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return nil })
		require.ErrorContains(t, err, "error querying constituent 'db1'")
	})
}

func TestCompositeEngine_ReadWriteSelectorsAndDeleteByPrefix(t *testing.T) {
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)
	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2, "db3": nil},
		map[string]string{"db1": "db1", "db2": "db2", "db3": "db3"},
		map[string]string{"db1": "read", "db2": "read_write", "db3": "read_write"},
	)

	reads := composite.getConstituentsForRead()
	assert.ElementsMatch(t, []string{"db1", "db2"}, reads)

	writes := composite.getConstituentsForWrite()
	assert.ElementsMatch(t, []string{"db2"}, writes)

	_, _, err := composite.DeleteByPrefix("tenant:")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported on composite")
}

func TestCompositeEngine_CreateNode(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create node
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "database_id": "db1"},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		retrieved, err := engine.GetNode(t.Context(), node.ID)
		if err == nil {
			assert.Equal(t, node.ID, retrieved.ID)
			assert.Equal(t, node.Labels, retrieved.Labels)
			found = true
			break
		}
	}
	assert.True(t, found, "Node should be created in one of the constituents")
}

func TestCompositeEngine_GetNode(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes in each constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine2.CreateNode(t.Context(), node2)

	// Get node from first constituent
	retrieved, err := composite.GetNode(t.Context(), node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved.ID)

	// Get node from second constituent
	retrieved, err = composite.GetNode(t.Context(), node2.ID)
	require.NoError(t, err)
	assert.Equal(t, node2.ID, retrieved.ID)

	// Get non-existent node
	_, err = composite.GetNode(t.Context(), NodeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_CreateEdge(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)

	// Create edge
	edge := &Edge{
		ID:         EdgeID(prefixTestID("edge1")),
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": 2020},
	}
	err := composite.CreateEdge(t.Context(), edge)
	require.NoError(t, err)

	// Verify edge exists in first constituent
	retrieved, err := engine1.GetEdge(t.Context(), edge.ID)
	require.NoError(t, err)
	assert.Equal(t, edge.ID, retrieved.ID)
	assert.Equal(t, edge.StartNode, retrieved.StartNode)
	assert.Equal(t, edge.EndNode, retrieved.EndNode)
}

func TestCompositeEngine_CreateEdge_ValidationPaths(t *testing.T) {
	t.Run("rejects nil edge and no writable constituents", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine},
			map[string]string{"db1": "db1"},
			map[string]string{"db1": "read"},
		)

		err := composite.CreateEdge(t.Context(), nil)
		require.ErrorContains(t, err, "edge cannot be nil")

		err = composite.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("edge-no-write")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2")), Type: "REL"})
		require.ErrorContains(t, err, "no writable constituents available")
	})

	t.Run("returns not found when endpoints do not exist anywhere", func(t *testing.T) {
		engine1 := newTestMemoryEngine(t)
		engine2 := newTestMemoryEngine(t)
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		err := composite.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("missing-edge")),
			StartNode: NodeID(prefixTestID("missing-1")),
			EndNode:   NodeID(prefixTestID("missing-2")),
			Type:      "REL",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start node not found in any constituent")
	})

	t.Run("uses transaction state when both nodes map to same namespaced constituent", func(t *testing.T) {
		base1 := newTestMemoryEngine(t)
		base2 := newTestMemoryEngine(t)
		engine1 := NewNamespacedEngine(base1, "db1")
		engine2 := NewNamespacedEngine(base2, "db2")
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		_, err := engine1.CreateNode(t.Context(), &Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = engine1.CreateNode(t.Context(), &Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)

		composite.mu.Lock()
		composite.nodeToConstituent["db1:n1"] = "db1"
		composite.nodeToConstituent["db1:n2"] = "db1"
		composite.mu.Unlock()

		err = composite.CreateEdge(t.Context(), &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})
		require.NoError(t, err)

		edge, err := engine1.GetEdge(t.Context(), "e1")
		require.NoError(t, err)
		assert.Equal(t, EdgeID("e1"), edge.ID)
	})

	t.Run("returns read-only constituent error when start node only exists there", func(t *testing.T) {
		base1 := newTestMemoryEngine(t)
		base2 := newTestMemoryEngine(t)
		engine1 := NewNamespacedEngine(base1, "db1")
		engine2 := NewNamespacedEngine(base2, "db2")
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read", "db2": "read_write"},
		)

		_, err := engine1.CreateNode(t.Context(), &Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)

		err = composite.CreateEdge(t.Context(), &Edge{ID: "e-readonly", StartNode: "n1", EndNode: "missing", Type: "KNOWS"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start node found in read-only constituent 'db1'")
	})
}

func TestCompositeEngine_BulkCreateNodes(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create multiple nodes
	nodes := []*Node{
		{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"database_id": "db1"}},
		{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"database_id": "db1"}},
		{ID: NodeID(prefixTestID("node3")), Labels: []string{"Company"}, Properties: map[string]interface{}{"database_id": "db2"}},
	}
	err := composite.BulkCreateNodes(t.Context(), nodes)
	require.NoError(t, err)

	// Verify all nodes exist in one of the constituents
	for _, node := range nodes {
		found := false
		for _, engine := range []Engine{engine1, engine2} {
			_, err := engine.GetNode(t.Context(), node.ID)
			if err == nil {
				found = true
				break
			}
		}
		assert.True(t, found, "Node %s should exist in one of the constituents", node.ID)
	}
}

func compositePairFixture(t *testing.T) (*CompositeEngine, *MemoryEngine, *MemoryEngine) {
	t.Helper()
	first, second := newTestMemoryEngine(t), newTestMemoryEngine(t)
	return NewCompositeEngine(map[string]Engine{"db1": first, "db2": second},
		map[string]string{"db1": "db1", "db2": "db2"},
		map[string]string{"db1": "read_write", "db2": "read_write"}), first, second
}

func TestCompositeEngine_GetSchema(t *testing.T) {
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes with labels to populate schema
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Company"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Product"}}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine2.CreateNode(t.Context(), node3)
	engine2.CreateNode(t.Context(), node4)

	// Get merged schema
	mergedSchema := composite.GetSchema()

	// Verify schema exists (labels will be populated from nodes)
	assert.NotNil(t, mergedSchema)
}

func TestCompositeEngine_ReadOnlyConstituent(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)

	// Create composite engine with one read-only constituent
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read",       // Read-only
		"db2": "read_write", // Writable
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Can read from read-only constituent
	retrieved, err := composite.GetNode(t.Context(), node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved.ID)

	// Cannot write to read-only constituent - should route to writable one
	newNode := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	_, err = composite.CreateNode(t.Context(), newNode)
	require.NoError(t, err)

	// New node should be in writable constituent
	_, err = engine2.GetNode(t.Context(), newNode.ID)
	assert.NoError(t, err)
}

func TestCompositeEngine_NodeAccess(t *testing.T) {
	for _, mode := range []string{"get from multiple constituents", "update"} {
		t.Run(mode, func(t *testing.T) {
			engine1 := newTestMemoryEngine(t)
			engine2 := newTestMemoryEngine(t)
			node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
			_, err := engine1.CreateNode(t.Context(), node1)
			require.NoError(t, err)
			var node2 *Node
			if mode == "get from multiple constituents" {
				node2 = &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
				_, err := engine2.CreateNode(t.Context(), node2)
				require.NoError(t, err)
			}
			composite := NewCompositeEngine(
				map[string]Engine{"db1": engine1, "db2": engine2},
				map[string]string{"db1": "db1", "db2": "db2"},
				map[string]string{"db1": "read_write", "db2": "read_write"})
			if mode == "update" {
				node1.Properties["age"] = 30
				require.NoError(t, composite.UpdateNode(t.Context(), NodeUpdateRequest{After: node1}))
			}
			retrieved, err := composite.GetNode(t.Context(), node1.ID)
			require.NoError(t, err)
			if mode == "update" {
				assert.EqualValues(t, 30, retrieved.Properties["age"])
				return
			}
			assert.Equal(t, node1.ID, retrieved.ID)
			assert.Equal(t, "Alice", retrieved.Properties["name"])
			retrieved, err = composite.GetNode(t.Context(), node2.ID)
			require.NoError(t, err)
			assert.Equal(t, node2.ID, retrieved.ID)
			assert.Equal(t, "Bob", retrieved.Properties["name"])
		})
	}
}

func TestCompositeEngine_DeleteNode(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)

	// Delete node
	err := composite.DeleteNode(t.Context(), node1.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = composite.GetNode(t.Context(), node1.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetEdge(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)

	// Get edge
	retrieved, err := composite.GetEdge(t.Context(), edge1.ID)
	require.NoError(t, err)
	assert.Equal(t, edge1.ID, retrieved.ID)
	assert.Equal(t, edge1.StartNode, retrieved.StartNode)
	assert.Equal(t, edge1.EndNode, retrieved.EndNode)
}

func TestCompositeEngine_UpdateEdge(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)

	// Update edge
	edge1.Properties = map[string]interface{}{"since": 2020}
	err := composite.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge1})
	require.NoError(t, err)

	// Verify update
	retrieved, err := composite.GetEdge(t.Context(), edge1.ID)
	require.NoError(t, err)
	// Gob may decode integers as different types (int, int8, int16, int32, int64, uint16, etc.)
	// Normalize to int for comparison
	var sinceVal int
	switch v := retrieved.Properties["since"].(type) {
	case int:
		sinceVal = v
	case int8:
		sinceVal = int(v)
	case int16:
		sinceVal = int(v)
	case int32:
		sinceVal = int(v)
	case int64:
		sinceVal = int(v)
	case uint8:
		sinceVal = int(v)
	case uint16:
		sinceVal = int(v)
	case uint32:
		sinceVal = int(v)
	case uint64:
		sinceVal = int(v)
	default:
		t.Fatalf("unexpected type for 'since': %T", v)
	}
	assert.Equal(t, 2020, sinceVal)
}

func TestCompositeEngine_DeleteEdge(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)

	// Delete edge
	err := composite.DeleteEdge(t.Context(), edge1.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = composite.GetEdge(t.Context(), edge1.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

// TestCompositeEngine_NodeReadsAggregateConstituents covers the label-filtered and
// unfiltered node reads. Both cases share this two-constituent fixture and the same
// membership assertions; the filter stays in the case's read.
func TestCompositeEngine_NodeReadsAggregateConstituents(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(context.Context, *CompositeEngine) ([]*Node, error)
	}{
		{
			name: "TestCompositeEngine_GetNodesByLabel",
			read: func(ctx context.Context, composite *CompositeEngine) ([]*Node, error) {
				return composite.GetNodesByLabel(ctx, "Person")
			},
		},
		{
			name: "TestCompositeEngine_AllNodes",
			read: func(ctx context.Context, composite *CompositeEngine) ([]*Node, error) {
				return composite.AllNodes(ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Create constituent engines
			composite, engine1, engine2 := compositePairFixture(t)

			// Create nodes in different constituents
			node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
			node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
			engine1.CreateNode(t.Context(), node1)
			engine2.CreateNode(t.Context(), node2)

			nodes, err := tc.read(context.Background(), composite)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(nodes), 2)

			// Verify both nodes are in results
			nodeMap := make(map[NodeID]bool)
			for _, node := range nodes {
				nodeMap[node.ID] = true
			}
			assert.True(t, nodeMap[node1.ID])
			assert.True(t, nodeMap[node2.ID])
		})
	}
}

func TestCompositeEngine_GetFirstNodeByLabel(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes with same label in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine2.CreateNode(t.Context(), node2)

	// Get first node by label
	node, err := composite.GetFirstNodeByLabel(t.Context(), "Person")
	require.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "Person", node.Labels[0])
}

func TestCompositeEngine_GetOutgoingEdges(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node1.ID, EndNode: node3.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)
	engine2.CreateNode(t.Context(), node3)
	engine2.CreateEdge(t.Context(), edge2)

	// Get outgoing edges
	edges, err := composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: node1.ID})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

// TestCompositeEngine_AdjacencyReadsReturnConstituentEdges covers the incoming and
// between-nodes edge reads over one shared single-constituent population.
func TestCompositeEngine_AdjacencyReadsReturnConstituentEdges(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(context.Context, *CompositeEngine, NodeID, NodeID) ([]*Edge, error)
	}{
		{
			name: "TestCompositeEngine_GetIncomingEdges",
			read: func(ctx context.Context, composite *CompositeEngine, node1, node2 NodeID) ([]*Edge, error) {
				return composite.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: node2})
			},
		},
		{
			name: "TestCompositeEngine_GetEdgesBetween",
			read: func(ctx context.Context, composite *CompositeEngine, node1, node2 NodeID) ([]*Edge, error) {
				return composite.ReadAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentBetween, Node: node1, Other: node2})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Create constituent engines
			composite, engine1, _ := compositePairFixture(t)

			// Create nodes and edge in db1 only
			node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
			node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
			edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
			engine1.CreateNode(t.Context(), node1)
			engine1.CreateNode(t.Context(), node2)
			engine1.CreateEdge(t.Context(), edge1)

			edges, err := tc.read(context.Background(), composite, node1.ID, node2.ID)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(edges), 1)
			assert.Equal(t, edge1.ID, edges[0].ID)
		})
	}
}

func TestCompositeEngine_GetEdgeBetween(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes and edge
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)

	// Get edge between nodes
	edge, err := composite.GetEdgeBetween(t.Context(), node1.ID, node2.ID, "KNOWS")
	require.NoError(t, err)
	require.NotNil(t, edge)
	assert.Equal(t, edge1.ID, edge.ID)
}

// TestCompositeEngine_EdgeReadsAggregateConstituents covers the type-filtered and
// unfiltered edge reads. Both cases share this two-constituent fixture and the same
// membership assertions; the type filter stays in the case's read.
func TestCompositeEngine_EdgeReadsAggregateConstituents(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(context.Context, *CompositeEngine) ([]*Edge, error)
	}{
		{
			name: "TestCompositeEngine_GetEdgesByType",
			read: func(ctx context.Context, composite *CompositeEngine) ([]*Edge, error) {
				return composite.GetEdgesByType(ctx, "KNOWS")
			},
		},
		{
			name: "TestCompositeEngine_AllEdges",
			read: func(ctx context.Context, composite *CompositeEngine) ([]*Edge, error) {
				return composite.AllEdges(ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Create constituent engines
			composite, engine1, engine2 := compositePairFixture(t)

			// Create nodes and edges with the same type in different constituents
			node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
			node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
			node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
			node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
			edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
			edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
			engine1.CreateNode(t.Context(), node1)
			engine1.CreateNode(t.Context(), node2)
			engine1.CreateEdge(t.Context(), edge1)
			engine2.CreateNode(t.Context(), node3)
			engine2.CreateNode(t.Context(), node4)
			engine2.CreateEdge(t.Context(), edge2)

			edges, err := tc.read(context.Background(), composite)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(edges), 2)

			// Verify both edges are in results
			edgeMap := make(map[EdgeID]bool)
			for _, edge := range edges {
				edgeMap[edge.ID] = true
			}
			assert.True(t, edgeMap[edge1.ID])
			assert.True(t, edgeMap[edge2.ID])
		})
	}
}

func TestCompositeEngine_GetInDegree(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes and edges (both edges point to node2)
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)
	// For edge2, node3 is in engine2 but node2 is in engine1
	// We need to create node2 in engine2 as well, or create edge2 in engine1
	// Let's create edge2 in engine1 since node2 is there
	engine1.CreateNode(t.Context(), node3)
	engine1.CreateEdge(t.Context(), edge2)

	// Get in-degree (should count edges from both constituents)
	degree, err := composite.GetInDegree(t.Context(), node2.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, degree, 2)
}

func TestCompositeEngine_GetOutDegree(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes and edges
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node1.ID, EndNode: node3.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)
	engine2.CreateNode(t.Context(), node3)
	engine2.CreateEdge(t.Context(), edge2)

	// Get out-degree (should count edges from both constituents)
	degree, err := composite.GetOutDegree(t.Context(), node1.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, degree, 1)
}

func TestCompositeEngine_BulkCreateEdges(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes - put node1 and node2 in engine1, node3 and node4 in engine2
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine2.CreateNode(t.Context(), node3)
	engine2.CreateNode(t.Context(), node4)

	// Create edges - edge1 within engine1, edge2 within engine2
	edges := []*Edge{
		{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"},
		{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"},
	}

	err := composite.BulkCreateEdges(t.Context(), edges)
	require.NoError(t, err)

	// Verify edges exist
	_, err = composite.GetEdge(t.Context(), edges[0].ID)
	require.NoError(t, err)
	_, err = composite.GetEdge(t.Context(), edges[1].ID)
	require.NoError(t, err)
}

func TestCompositeEngine_BulkDeleteNodes(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine2.CreateNode(t.Context(), node2)

	// Delete nodes
	err := composite.BulkDeleteNodes(t.Context(), []NodeID{node1.ID, node2.ID})
	require.NoError(t, err)

	// Verify nodes are deleted
	_, err = composite.GetNode(t.Context(), node1.ID)
	assert.Error(t, err)
	_, err = composite.GetNode(t.Context(), node2.ID)
	assert.Error(t, err)
}

func TestCompositeEngine_BulkDeleteEdges(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)
	engine2.CreateNode(t.Context(), node3)
	engine2.CreateNode(t.Context(), node4)
	engine2.CreateEdge(t.Context(), edge2)

	// Delete edges
	err := composite.BulkDeleteEdges(t.Context(), []EdgeID{edge1.ID, edge2.ID})
	require.NoError(t, err)

	// Verify edges are deleted
	_, err = composite.GetEdge(t.Context(), edge1.ID)
	assert.Error(t, err)
	_, err = composite.GetEdge(t.Context(), edge2.ID)
	assert.Error(t, err)
}

func TestCompositeEngine_routeWrite_PropertyBased(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Test routing with database_id property
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": "db1"},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		_, err := engine.GetNode(t.Context(), node.ID)
		if err == nil {
			found = true
			break
		}
	}
	assert.True(t, found, "Node should be created in one of the constituents")
}

func TestCompositeEngine_routeWrite_LabelBased(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Test routing with labels only (no properties)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_Int64(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": int64(123)},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_Int(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": 456},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_NegativeHash(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": int64(-123)},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_NegativeInt(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": -456},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_LabelBased_NegativeHash(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Label-only routing is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Z"}, // Single char that might produce negative hash
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_NoLabelsNoProperties(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// No explicit routing target must fail for multi-writable composites.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{},
		Properties: nil,
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertiesWithoutDatabaseID(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// No explicit routing target must fail for multi-writable composites.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := composite.CreateNode(t.Context(), node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_hashValue(t *testing.T) {
	assert.Equal(t, hashString("abc"), hashValue("abc"))
	assert.Equal(t, 42, hashValue(int64(42)))
	assert.Equal(t, 42, hashValue(int64(-42)))
	assert.Equal(t, 7, hashValue(7))
	assert.Equal(t, 7, hashValue(-7))
	assert.Equal(t, 9, hashValue(int32(9)))
	assert.Equal(t, 9, hashValue(int32(-9)))
	assert.Equal(t, hashString("true"), hashValue(true))
}

func TestCompositeEngine_routeWrite_DirectBranches(t *testing.T) {
	composite := NewCompositeEngine(
		map[string]Engine{"db1": newTestMemoryEngine(t), "db2": newTestMemoryEngine(t)},
		map[string]string{"db1": "primary", "db2": "analytics"},
		map[string]string{"db1": "read_write", "db2": "read_write"},
	)
	composite.SetLabelRouting("Person", []string{"db2"})
	composite.SetPropertyRouting("tenant", "t1", "db1")
	composite.SetPropertyDefault("tenant", "db2")

	assert.Equal(t, "", composite.routeWrite("create", nil, nil, nil))
	assert.Equal(t, "db1", composite.routeWrite("create", nil, map[string]interface{}{"database_id": "db1"}, []string{"db1", "db2"}))
	assert.Equal(t, "db2", composite.routeWrite("create", nil, map[string]interface{}{"database_id": "analytics"}, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", []string{"Person"}, nil, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", []string{"db1"}, nil, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", nil, map[string]interface{}{"tenant": "t1"}, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", nil, map[string]interface{}{"tenant": "unknown"}, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", nil, map[string]interface{}{"database_id": int32(3)}, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", []string{"Other"}, nil, []string{"db1", "db2"}))
	assert.Equal(t, "", composite.routeWrite("create", nil, map[string]interface{}{"other": "value"}, []string{"db1", "db2"}))
}

func TestCompositeEngine_CreateNode_NoWritableConstituents(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create composite engine with read-only constituents
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read", // Read-only
		"db2": "read", // Read-only
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to create node (should fail - no writable constituents)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(t.Context(), node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no writable constituents")
}

func TestCompositeEngine_UpdateNode_NotFound(t *testing.T) {
	composite, _, _ := compositePairFixture(t)

	// Try to update non-existent node
	node := &Node{ID: NodeID(prefixTestID("nonexistent")), Labels: []string{"Person"}}
	err := composite.UpdateNode(t.Context(), NodeUpdateRequest{After: node})
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_DeleteNode_NotFound(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Try to delete non-existent node
	err := composite.DeleteNode(t.Context(), NodeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetEdge_NotFound(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Try to get non-existent edge
	_, err := composite.GetEdge(t.Context(), EdgeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_UpdateEdge_NotFound(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Try to update non-existent edge
	edge := &Edge{ID: EdgeID(prefixTestID("nonexistent")), StartNode: NodeID(prefixTestID("node1")), EndNode: NodeID(prefixTestID("node2")), Type: "KNOWS"}
	err := composite.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge})
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_DeleteEdge_NotFound(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Try to delete non-existent edge
	err := composite.DeleteEdge(t.Context(), EdgeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetFirstNodeByLabel_NotFound(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Try to get first node with non-existent label
	// GetFirstNodeByLabel should return nil, ErrNotFound when not found
	node, err := composite.GetFirstNodeByLabel(t.Context(), "NonExistent")
	// CompositeEngine should return ErrNotFound after checking all constituents
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
	assert.Nil(t, node)
}

func TestCompositeEngine_GetEdgeBetween_NotFound(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Try to get edge between non-existent nodes
	edge, err := composite.GetEdgeBetween(t.Context(), NodeID(prefixTestID("node1")), NodeID(prefixTestID("node2")), "KNOWS")
	require.NoError(t, err)
	assert.Nil(t, edge)
}

func TestCompositeEngine_BulkCreateEdges_Unrouted(t *testing.T) {
	// Create constituent engines
	composite, engine1, _ := compositePairFixture(t)

	// Create nodes in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)

	// Create edge with non-existent start node (should route to first writable)
	// But edge creation will fail because nodes don't exist
	edge := &Edge{
		ID:        EdgeID(prefixTestID("edge1")),
		StartNode: NodeID(prefixTestID("nonexistent")),
		EndNode:   NodeID(prefixTestID("nonexistent2")),
		Type:      "KNOWS",
	}

	err := composite.BulkCreateEdges(t.Context(), []*Edge{edge})
	// Will fail because nodes don't exist, but tests the unrouted path
	assert.Error(t, err)
}

func TestCompositeEngine_BatchGetNodes(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine2.CreateNode(t.Context(), node2)

	// Batch get nodes
	nodes, err := composite.BatchGetNodes(context.Background(), []NodeID{node1.ID, node2.ID, NodeID(prefixTestID("nonexistent"))})
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))
	assert.NotNil(t, nodes[node1.ID])
	assert.NotNil(t, nodes[node2.ID])
	assert.Nil(t, nodes[NodeID(prefixTestID("nonexistent"))])
}

func TestCompositeEngine_Close(t *testing.T) {
	// Create constituent engines
	composite, _, _ := compositePairFixture(t)

	// Close composite engine
	err := composite.Close()
	require.NoError(t, err)
}

func TestCompositeEngine_NodeCount(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(t.Context(), node1)
	engine2.CreateNode(t.Context(), node2)

	// Get node count (should sum from all constituents)
	count, err := composite.NodeCount(t.Context())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestCompositeEngine_EdgeCount(t *testing.T) {
	// Create constituent engines
	composite, engine1, engine2 := compositePairFixture(t)

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(t.Context(), node1)
	engine1.CreateNode(t.Context(), node2)
	engine1.CreateEdge(t.Context(), edge1)
	engine2.CreateNode(t.Context(), node3)
	engine2.CreateNode(t.Context(), node4)
	engine2.CreateEdge(t.Context(), edge2)

	// Get edge count (should sum from all constituents)
	count, err := composite.EdgeCount(t.Context())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestCompositeEngine_ErrorPaths(t *testing.T) {
	t.Run("query helpers propagate constituent errors and continue nil-first-node results", func(t *testing.T) {
		boom := errors.New("boom")
		errEngine := &compositeErrorEngine{MemoryEngine: newTestMemoryEngine(t)}
		okEngine := newTestMemoryEngine(t)
		t.Cleanup(func() { _ = errEngine.MemoryEngine.Close() })

		okNode := &Node{ID: NodeID(prefixTestID("ok-node")), Labels: []string{"Person"}}
		_, err := okEngine.CreateNode(t.Context(), okNode)
		require.NoError(t, err)

		errorOnly := NewCompositeEngine(
			map[string]Engine{"err": errEngine},
			map[string]string{"err": "err"},
			map[string]string{"err": "read_write"},
		)

		errEngine.getNodeErr = boom
		_, err = errorOnly.GetNode(t.Context(), okNode.ID)
		require.ErrorIs(t, err, boom)
		err = errorOnly.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: okNode.ID, Labels: []string{"Person"}}})
		require.ErrorIs(t, err, boom)
		err = errorOnly.DeleteNode(t.Context(), okNode.ID)
		require.ErrorIs(t, err, boom)
		errEngine.getNodeErr = nil

		errEngine.getEdgeErr = boom
		_, err = errorOnly.GetEdge(t.Context(), EdgeID(prefixTestID("missing-edge")))
		require.ErrorIs(t, err, boom)
		err = errorOnly.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: EdgeID(prefixTestID("missing-edge")), StartNode: okNode.ID, EndNode: okNode.ID, Type: "REL"}})
		require.ErrorIs(t, err, boom)
		err = errorOnly.DeleteEdge(t.Context(), EdgeID(prefixTestID("missing-edge")))
		require.ErrorIs(t, err, boom)
		errEngine.getEdgeErr = nil

		errEngine.firstNodeErr = boom
		_, err = errorOnly.GetFirstNodeByLabel(t.Context(), "Person")
		require.ErrorIs(t, err, boom)
		errEngine.firstNodeErr = nil

		composite := NewCompositeEngine(
			map[string]Engine{"err": errEngine, "ok": okEngine},
			map[string]string{"err": "err", "ok": "ok"},
			map[string]string{"err": "read_write", "ok": "read_write"},
		)
		errEngine.returnNilFirstNode = true
		got, err := composite.GetFirstNodeByLabel(t.Context(), "Person")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, okNode.ID, got.ID)
		errEngine.returnNilFirstNode = false

		errEngine.outgoingErr = boom
		_, err = composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: okNode.ID})
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.outgoingErr = nil

		errEngine.incomingErr = boom
		_, err = composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: okNode.ID})
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.incomingErr = nil

		errEngine.betweenErr = boom
		_, err = composite.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: okNode.ID, Other: okNode.ID})
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.betweenErr = nil

		errEngine.byTypeErr = boom
		_, err = composite.GetEdgesByType(context.Background(), "REL")
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.byTypeErr = nil

		errEngine.allNodesErr = boom
		_, err = composite.AllNodes(context.Background())
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.allNodesErr = nil

		errEngine.allEdgesErr = boom
		_, err = composite.AllEdges(context.Background())
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.allEdgesErr = nil

		errEngine.batchGetErr = boom
		_, err = composite.BatchGetNodes(context.Background(), []NodeID{okNode.ID})
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.batchGetErr = nil

		errEngine.nodeCountErr = boom
		_, err = composite.NodeCount(t.Context())
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.nodeCountErr = nil

		errEngine.edgeCountErr = boom
		_, err = composite.EdgeCount(t.Context())
		require.ErrorContains(t, err, "constituent 'err'")
	})

	t.Run("write helpers propagate routed constituent and close errors", func(t *testing.T) {
		boom := errors.New("boom")
		errEngine := &compositeErrorEngine{MemoryEngine: newTestMemoryEngine(t)}
		okEngine := newTestMemoryEngine(t)
		t.Cleanup(func() { _ = errEngine.MemoryEngine.Close() })

		composite := NewCompositeEngine(
			map[string]Engine{"err": errEngine, "ok": okEngine},
			map[string]string{"err": "err", "ok": "ok"},
			map[string]string{"err": "read_write", "ok": "read_write"},
		)
		composite.SetLabelRouting("ErrLabel", []string{"err"})

		errEngine.createNodeErr = boom
		_, err := composite.CreateNode(t.Context(), &Node{
			ID:         NodeID(prefixTestID("route-node")),
			Labels:     []string{"ErrLabel"},
			Properties: map[string]interface{}{"database_id": "err"},
		})
		require.ErrorIs(t, err, boom)
		errEngine.createNodeErr = nil

		start := &Node{ID: NodeID(prefixTestID("start")), Labels: []string{"ErrLabel"}}
		end := &Node{ID: NodeID(prefixTestID("end")), Labels: []string{"ErrLabel"}}
		_, err = errEngine.CreateNode(t.Context(), start)
		require.NoError(t, err)
		_, err = errEngine.CreateNode(t.Context(), end)
		require.NoError(t, err)

		errEngine.createEdgeErr = boom
		err = composite.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("route-edge")), StartNode: start.ID, EndNode: end.ID, Type: "REL"})
		require.ErrorIs(t, err, boom)
		errEngine.createEdgeErr = nil

		errEngine.bulkCreateNodesErr = boom
		err = composite.BulkCreateNodes(t.Context(), []*Node{{
			ID:         NodeID(prefixTestID("bulk-node")),
			Labels:     []string{"ErrLabel"},
			Properties: map[string]interface{}{"database_id": "err"},
		}})
		require.ErrorContains(t, err, "failed to create nodes in constituent 'err'")
		errEngine.bulkCreateNodesErr = nil

		errEngine.bulkCreateEdgesErr = boom
		err = composite.BulkCreateEdges(t.Context(), []*Edge{{ID: EdgeID(prefixTestID("bulk-edge")), StartNode: start.ID, EndNode: end.ID, Type: "REL"}})
		require.ErrorContains(t, err, "failed to create edges in constituent 'err'")

		errEngine.closeErr = errors.New("close failed")
		err = composite.Close()
		require.ErrorContains(t, err, "error closing constituent 'err'")
	})
}
