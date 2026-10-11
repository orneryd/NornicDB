package storage

import (
	"context"
	"errors"
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

func (e *compositeErrorEngine) GetNode(id NodeID) (*Node, error) {
	if e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	return e.MemoryEngine.GetNode(id)
}

func (e *compositeErrorEngine) GetEdge(id EdgeID) (*Edge, error) {
	if e.getEdgeErr != nil {
		return nil, e.getEdgeErr
	}
	return e.MemoryEngine.GetEdge(id)
}

func (e *compositeErrorEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	if e.firstNodeErr != nil {
		return nil, e.firstNodeErr
	}
	if e.returnNilFirstNode {
		return nil, nil
	}
	return e.MemoryEngine.GetFirstNodeByLabel(label)
}

func (e *compositeErrorEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.outgoingErr != nil {
		return nil, e.outgoingErr
	}
	return e.MemoryEngine.GetOutgoingEdges(nodeID)
}

func (e *compositeErrorEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.incomingErr != nil {
		return nil, e.incomingErr
	}
	return e.MemoryEngine.GetIncomingEdges(nodeID)
}

func (e *compositeErrorEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	if e.betweenErr != nil {
		return nil, e.betweenErr
	}
	return e.MemoryEngine.GetEdgesBetween(startID, endID)
}

func (e *compositeErrorEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	if e.byTypeErr != nil {
		return nil, e.byTypeErr
	}
	return e.MemoryEngine.GetEdgesByType(edgeType)
}

func (e *compositeErrorEngine) AllNodes() ([]*Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.MemoryEngine.AllNodes()
}

func (e *compositeErrorEngine) AllEdges() ([]*Edge, error) {
	if e.allEdgesErr != nil {
		return nil, e.allEdgesErr
	}
	return e.MemoryEngine.AllEdges()
}

func (e *compositeErrorEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if e.batchGetErr != nil {
		return nil, e.batchGetErr
	}
	return e.MemoryEngine.BatchGetNodes(ids)
}

func (e *compositeErrorEngine) NodeCount() (int64, error) {
	if e.nodeCountErr != nil {
		return 0, e.nodeCountErr
	}
	return e.MemoryEngine.NodeCount()
}

func (e *compositeErrorEngine) EdgeCount() (int64, error) {
	if e.edgeCountErr != nil {
		return 0, e.edgeCountErr
	}
	return e.MemoryEngine.EdgeCount()
}

func (e *compositeErrorEngine) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	return e.MemoryEngine.Close()
}

func (e *compositeErrorEngine) CreateNode(node *Node) (NodeID, error) {
	if e.createNodeErr != nil {
		return "", e.createNodeErr
	}
	return e.MemoryEngine.CreateNode(node)
}

func (e *compositeErrorEngine) CreateEdge(edge *Edge) error {
	if e.createEdgeErr != nil {
		return e.createEdgeErr
	}
	return e.MemoryEngine.CreateEdge(edge)
}

func (e *compositeErrorEngine) BulkCreateNodes(nodes []*Node) error {
	if e.bulkCreateNodesErr != nil {
		return e.bulkCreateNodesErr
	}
	return e.MemoryEngine.BulkCreateNodes(nodes)
}

func (e *compositeErrorEngine) BulkCreateEdges(edges []*Edge) error {
	if e.bulkCreateEdgesErr != nil {
		return e.bulkCreateEdgesErr
	}
	return e.MemoryEngine.BulkCreateEdges(edges)
}

func (e *compositeStreamEngine) StreamNodes(_ context.Context, fn func(node *Node) error) error {
	if e.streamNodeErr != nil {
		return e.streamNodeErr
	}
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *compositeStreamEngine) StreamEdges(_ context.Context, fn func(edge *Edge) error) error {
	if e.streamEdgeErr != nil {
		return e.streamEdgeErr
	}
	edges, err := e.Engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *compositeStreamEngine) StreamNodeChunks(_ context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if e.chunkErr != nil {
		return e.chunkErr
	}
	nodes, err := e.Engine.AllNodes()
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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()
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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()
	composite := NewCompositeEngine(
		map[string]Engine{"db1": engine1, "db2": engine2},
		map[string]string{"db1": "db1", "db2": "db2"},
		map[string]string{"db1": "read_write", "db2": "read_write"},
	)

	_, err := engine1.CreateNode(&Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"A"}})
	require.NoError(t, err)
	_, err = engine2.CreateNode(&Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"B"}})
	require.NoError(t, err)
	require.NoError(t, engine1.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n1")), Type: "LOOP"}))
	require.NoError(t, engine2.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e2")), StartNode: NodeID(prefixTestID("n2")), EndNode: NodeID(prefixTestID("n2")), Type: "LOOP"}))

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
		base1 := NewMemoryEngine()
		base2 := NewMemoryEngine()
		t.Cleanup(func() { _ = base1.Close() })
		t.Cleanup(func() { _ = base2.Close() })
		engine1 := &compositeStreamEngine{Engine: base1}
		engine2 := &compositeStreamEngine{Engine: base2}
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		_, err := base1.CreateNode(&Node{ID: NodeID(prefixTestID("stream-n1")), Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = base1.CreateNode(&Node{ID: NodeID(prefixTestID("stream-n2")), Labels: []string{"A"}})
		require.NoError(t, err)
		require.NoError(t, base1.CreateEdge(&Edge{ID: EdgeID(prefixTestID("stream-e1")), StartNode: NodeID(prefixTestID("stream-n1")), EndNode: NodeID(prefixTestID("stream-n2")), Type: "REL"}))

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
		base1 := NewMemoryEngine()
		base2 := NewMemoryEngine()
		t.Cleanup(func() { _ = base1.Close() })
		t.Cleanup(func() { _ = base2.Close() })
		engine1 := &nonStreamingCountEngine{Engine: base1}
		engine2 := &nonStreamingCountEngine{Engine: base2}
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		_, err := base1.CreateNode(&Node{ID: NodeID(prefixTestID("fallback-n1")), Labels: []string{"A"}})
		require.NoError(t, err)
		_, err = base2.CreateNode(&Node{ID: NodeID(prefixTestID("fallback-n2")), Labels: []string{"B"}})
		require.NoError(t, err)
		require.NoError(t, base1.CreateEdge(&Edge{ID: EdgeID(prefixTestID("fallback-e1")), StartNode: NodeID(prefixTestID("fallback-n1")), EndNode: NodeID(prefixTestID("fallback-n1")), Type: "REL"}))
		require.NoError(t, base2.CreateEdge(&Edge{ID: EdgeID(prefixTestID("fallback-e2")), StartNode: NodeID(prefixTestID("fallback-n2")), EndNode: NodeID(prefixTestID("fallback-n2")), Type: "REL"}))

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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()
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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Create node
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "database_id": "db1"},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		retrieved, err := engine.GetNode(node.ID)
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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in each constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get node from first constituent
	retrieved, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved.ID)

	// Get node from second constituent
	retrieved, err = composite.GetNode(node2.ID)
	require.NoError(t, err)
	assert.Equal(t, node2.ID, retrieved.ID)

	// Get non-existent node
	_, err = composite.GetNode(NodeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_CreateEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Create edge
	edge := &Edge{
		ID:         EdgeID(prefixTestID("edge1")),
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": 2020},
	}
	err := composite.CreateEdge(edge)
	require.NoError(t, err)

	// Verify edge exists in first constituent
	retrieved, err := engine1.GetEdge(edge.ID)
	require.NoError(t, err)
	assert.Equal(t, edge.ID, retrieved.ID)
	assert.Equal(t, edge.StartNode, retrieved.StartNode)
	assert.Equal(t, edge.EndNode, retrieved.EndNode)
}

func TestCompositeEngine_CreateEdge_ValidationPaths(t *testing.T) {
	t.Run("rejects nil edge and no writable constituents", func(t *testing.T) {
		engine := NewMemoryEngine()
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine},
			map[string]string{"db1": "db1"},
			map[string]string{"db1": "read"},
		)

		err := composite.CreateEdge(nil)
		require.ErrorContains(t, err, "edge cannot be nil")

		err = composite.CreateEdge(&Edge{ID: EdgeID(prefixTestID("edge-no-write")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2")), Type: "REL"})
		require.ErrorContains(t, err, "no writable constituents available")
	})

	t.Run("returns not found when endpoints do not exist anywhere", func(t *testing.T) {
		engine1 := NewMemoryEngine()
		engine2 := NewMemoryEngine()
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		err := composite.CreateEdge(&Edge{
			ID:        EdgeID(prefixTestID("missing-edge")),
			StartNode: NodeID(prefixTestID("missing-1")),
			EndNode:   NodeID(prefixTestID("missing-2")),
			Type:      "REL",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start node not found in any constituent")
	})

	t.Run("uses transaction state when both nodes map to same namespaced constituent", func(t *testing.T) {
		base1 := NewMemoryEngine()
		base2 := NewMemoryEngine()
		t.Cleanup(func() { _ = base1.Close() })
		t.Cleanup(func() { _ = base2.Close() })
		engine1 := NewNamespacedEngine(base1, "db1")
		engine2 := NewNamespacedEngine(base2, "db2")
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read_write", "db2": "read_write"},
		)

		_, err := engine1.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = engine1.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)

		composite.mu.Lock()
		composite.nodeToConstituent["db1:n1"] = "db1"
		composite.nodeToConstituent["db1:n2"] = "db1"
		composite.mu.Unlock()

		err = composite.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})
		require.NoError(t, err)

		edge, err := engine1.GetEdge("e1")
		require.NoError(t, err)
		assert.Equal(t, EdgeID("e1"), edge.ID)
	})

	t.Run("returns read-only constituent error when start node only exists there", func(t *testing.T) {
		base1 := NewMemoryEngine()
		base2 := NewMemoryEngine()
		t.Cleanup(func() { _ = base1.Close() })
		t.Cleanup(func() { _ = base2.Close() })
		engine1 := NewNamespacedEngine(base1, "db1")
		engine2 := NewNamespacedEngine(base2, "db2")
		composite := NewCompositeEngine(
			map[string]Engine{"db1": engine1, "db2": engine2},
			map[string]string{"db1": "db1", "db2": "db2"},
			map[string]string{"db1": "read", "db2": "read_write"},
		)

		_, err := engine1.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)

		err = composite.CreateEdge(&Edge{ID: "e-readonly", StartNode: "n1", EndNode: "missing", Type: "KNOWS"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start node found in read-only constituent 'db1'")
	})
}

func TestCompositeEngine_BulkCreateNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Create multiple nodes
	nodes := []*Node{
		{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"database_id": "db1"}},
		{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"database_id": "db1"}},
		{ID: NodeID(prefixTestID("node3")), Labels: []string{"Company"}, Properties: map[string]interface{}{"database_id": "db2"}},
	}
	err := composite.BulkCreateNodes(nodes)
	require.NoError(t, err)

	// Verify all nodes exist in one of the constituents
	for _, node := range nodes {
		found := false
		for _, engine := range []Engine{engine1, engine2} {
			_, err := engine.GetNode(node.ID)
			if err == nil {
				found = true
				break
			}
		}
		assert.True(t, found, "Node %s should exist in one of the constituents", node.ID)
	}
}

func TestCompositeEngine_GetSchema(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes with labels to populate schema
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Company"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Product"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get merged schema
	mergedSchema := composite.GetSchema()

	// Verify schema exists (labels will be populated from nodes)
	assert.NotNil(t, mergedSchema)
}

func TestCompositeEngine_ReadOnlyConstituent(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)

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
	retrieved, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved.ID)

	// Cannot write to read-only constituent - should route to writable one
	newNode := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	_, err = composite.CreateNode(newNode)
	require.NoError(t, err)

	// New node should be in writable constituent
	_, err = engine2.GetNode(newNode.ID)
	assert.NoError(t, err)
}

func TestCompositeEngine_GetNodeFromMultipleConstituents(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in each constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get nodes from both constituents
	retrieved1, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved1.ID)
	assert.Equal(t, "Alice", retrieved1.Properties["name"])

	retrieved2, err := composite.GetNode(node2.ID)
	require.NoError(t, err)
	assert.Equal(t, node2.ID, retrieved2.ID)
	assert.Equal(t, "Bob", retrieved2.Properties["name"])
}

func TestCompositeEngine_UpdateNode(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	engine1.CreateNode(node1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Update node
	node1.Properties["age"] = 30
	err := composite.UpdateNode(node1)
	require.NoError(t, err)

	// Verify update
	retrieved, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, 30, retrieved.Properties["age"])
}

func TestCompositeEngine_DeleteNode(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Delete node
	err := composite.DeleteNode(node1.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = composite.GetNode(node1.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edge
	retrieved, err := composite.GetEdge(edge1.ID)
	require.NoError(t, err)
	assert.Equal(t, edge1.ID, retrieved.ID)
	assert.Equal(t, edge1.StartNode, retrieved.StartNode)
	assert.Equal(t, edge1.EndNode, retrieved.EndNode)
}

func TestCompositeEngine_UpdateEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Update edge
	edge1.Properties = map[string]interface{}{"since": 2020}
	err := composite.UpdateEdge(edge1)
	require.NoError(t, err)

	// Verify update
	retrieved, err := composite.GetEdge(edge1.ID)
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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Delete edge
	err := composite.DeleteEdge(edge1.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = composite.GetEdge(edge1.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetNodesByLabel(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes with same label in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get nodes by label
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2)

	// Verify both nodes are in results
	nodeMap := make(map[NodeID]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}
	assert.True(t, nodeMap[node1.ID])
	assert.True(t, nodeMap[node2.ID])
}

func TestCompositeEngine_GetFirstNodeByLabel(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes with same label in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get first node by label
	node, err := composite.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "Person", node.Labels[0])
}

func TestCompositeEngine_GetOutgoingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node1.ID, EndNode: node3.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get outgoing edges
	edges, err := composite.GetOutgoingEdges(node1.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestCompositeEngine_GetIncomingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get incoming edges
	edges, err := composite.GetIncomingEdges(node2.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_GetEdgesBetween(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edges between nodes
	edges, err := composite.GetEdgesBetween(node1.ID, node2.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_GetEdgeBetween(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edge between nodes
	edge := composite.GetEdgeBetween(node1.ID, node2.ID, "KNOWS")
	require.NotNil(t, edge)
	assert.Equal(t, edge1.ID, edge.ID)
}

func TestCompositeEngine_GetEdgesByType(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges with same type in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edges by type
	edges, err := composite.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 2)

	// Verify both edges are in results
	edgeMap := make(map[EdgeID]bool)
	for _, edge := range edges {
		edgeMap[edge.ID] = true
	}
	assert.True(t, edgeMap[edge1.ID])
	assert.True(t, edgeMap[edge2.ID])
}

func TestCompositeEngine_AllNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get all nodes
	nodes, err := composite.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2)

	// Verify both nodes are in results
	nodeMap := make(map[NodeID]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}
	assert.True(t, nodeMap[node1.ID])
	assert.True(t, nodeMap[node2.ID])
}

func TestCompositeEngine_AllEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get all edges
	edges, err := composite.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 2)

	// Verify both edges are in results
	edgeMap := make(map[EdgeID]bool)
	for _, edge := range edges {
		edgeMap[edge.ID] = true
	}
	assert.True(t, edgeMap[edge1.ID])
	assert.True(t, edgeMap[edge2.ID])
}

func TestCompositeEngine_GetAllNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get all nodes (non-error version)
	nodes := composite.GetAllNodes()
	assert.GreaterOrEqual(t, len(nodes), 2)

	// Verify both nodes are in results
	nodeMap := make(map[NodeID]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}
	assert.True(t, nodeMap[node1.ID])
	assert.True(t, nodeMap[node2.ID])
}

func TestCompositeEngine_GetInDegree(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges (both edges point to node2)
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	// For edge2, node3 is in engine2 but node2 is in engine1
	// We need to create node2 in engine2 as well, or create edge2 in engine1
	// Let's create edge2 in engine1 since node2 is there
	engine1.CreateNode(node3)
	engine1.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get in-degree (should count edges from both constituents)
	degree := composite.GetInDegree(node2.ID)
	assert.GreaterOrEqual(t, degree, 2)
}

func TestCompositeEngine_GetOutDegree(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node1.ID, EndNode: node3.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get out-degree (should count edges from both constituents)
	degree := composite.GetOutDegree(node1.ID)
	assert.GreaterOrEqual(t, degree, 1)
}

func TestCompositeEngine_BulkCreateEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes - put node1 and node2 in engine1, node3 and node4 in engine2
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Create edges - edge1 within engine1, edge2 within engine2
	edges := []*Edge{
		{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"},
		{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"},
	}

	err := composite.BulkCreateEdges(edges)
	require.NoError(t, err)

	// Verify edges exist
	_, err = composite.GetEdge(edges[0].ID)
	require.NoError(t, err)
	_, err = composite.GetEdge(edges[1].ID)
	require.NoError(t, err)
}

func TestCompositeEngine_BulkDeleteNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Delete nodes
	err := composite.BulkDeleteNodes([]NodeID{node1.ID, node2.ID})
	require.NoError(t, err)

	// Verify nodes are deleted
	_, err = composite.GetNode(node1.ID)
	assert.Error(t, err)
	_, err = composite.GetNode(node2.ID)
	assert.Error(t, err)
}

func TestCompositeEngine_BulkDeleteEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Delete edges
	err := composite.BulkDeleteEdges([]EdgeID{edge1.ID, edge2.ID})
	require.NoError(t, err)

	// Verify edges are deleted
	_, err = composite.GetEdge(edge1.ID)
	assert.Error(t, err)
	_, err = composite.GetEdge(edge2.ID)
	assert.Error(t, err)
}

func TestCompositeEngine_routeWrite_PropertyBased(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Test routing with database_id property
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": "db1"},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		_, err := engine.GetNode(node.ID)
		if err == nil {
			found = true
			break
		}
	}
	assert.True(t, found, "Node should be created in one of the constituents")
}

func TestCompositeEngine_routeWrite_LabelBased(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Test routing with labels only (no properties)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_Int64(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": int64(123)},
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_Int(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": 456},
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_NegativeHash(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": int64(-123)},
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertyBased_NegativeInt(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Non-string database_id is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"database_id": -456},
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_LabelBased_NegativeHash(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Label-only routing is ambiguous and must be rejected.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Z"}, // Single char that might produce negative hash
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_NoLabelsNoProperties(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// No explicit routing target must fail for multi-writable composites.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{},
		Properties: nil,
	}
	_, err := composite.CreateNode(node)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous composite write target")
}

func TestCompositeEngine_routeWrite_PropertiesWithoutDatabaseID(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// No explicit routing target must fail for multi-writable composites.
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := composite.CreateNode(node)
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
		map[string]Engine{"db1": NewMemoryEngine(), "db2": NewMemoryEngine()},
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
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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
	_, err := composite.CreateNode(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no writable constituents")
}

func TestCompositeEngine_UpdateNode_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to update non-existent node
	node := &Node{ID: NodeID(prefixTestID("nonexistent")), Labels: []string{"Person"}}
	err := composite.UpdateNode(node)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_DeleteNode_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to delete non-existent node
	err := composite.DeleteNode(NodeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetEdge_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to get non-existent edge
	_, err := composite.GetEdge(EdgeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_UpdateEdge_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to update non-existent edge
	edge := &Edge{ID: EdgeID(prefixTestID("nonexistent")), StartNode: NodeID(prefixTestID("node1")), EndNode: NodeID(prefixTestID("node2")), Type: "KNOWS"}
	err := composite.UpdateEdge(edge)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_DeleteEdge_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to delete non-existent edge
	err := composite.DeleteEdge(EdgeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetFirstNodeByLabel_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to get first node with non-existent label
	// GetFirstNodeByLabel should return nil, ErrNotFound when not found
	node, err := composite.GetFirstNodeByLabel("NonExistent")
	// CompositeEngine should return ErrNotFound after checking all constituents
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
	assert.Nil(t, node)
}

func TestCompositeEngine_GetEdgeBetween_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to get edge between non-existent nodes
	edge := composite.GetEdgeBetween(NodeID(prefixTestID("node1")), NodeID(prefixTestID("node2")), "KNOWS")
	assert.Nil(t, edge)
}

func TestCompositeEngine_BulkCreateEdges_Unrouted(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Create edge with non-existent start node (should route to first writable)
	// But edge creation will fail because nodes don't exist
	edge := &Edge{
		ID:        EdgeID(prefixTestID("edge1")),
		StartNode: NodeID(prefixTestID("nonexistent")),
		EndNode:   NodeID(prefixTestID("nonexistent2")),
		Type:      "KNOWS",
	}

	err := composite.BulkCreateEdges([]*Edge{edge})
	// Will fail because nodes don't exist, but tests the unrouted path
	assert.Error(t, err)
}

func TestCompositeEngine_BatchGetNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Batch get nodes
	nodes, err := composite.BatchGetNodes([]NodeID{node1.ID, node2.ID, NodeID(prefixTestID("nonexistent"))})
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))
	assert.NotNil(t, nodes[node1.ID])
	assert.NotNil(t, nodes[node2.ID])
	assert.Nil(t, nodes[NodeID(prefixTestID("nonexistent"))])
}

func TestCompositeEngine_Close(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Close composite engine
	err := composite.Close()
	require.NoError(t, err)
}

func TestCompositeEngine_NodeCount(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get node count (should sum from all constituents)
	count, err := composite.NodeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestCompositeEngine_EdgeCount(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edge count (should sum from all constituents)
	count, err := composite.EdgeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestCompositeEngine_ErrorPaths(t *testing.T) {
	t.Run("query helpers propagate constituent errors and continue nil-first-node results", func(t *testing.T) {
		boom := errors.New("boom")
		errEngine := &compositeErrorEngine{MemoryEngine: NewMemoryEngine()}
		okEngine := NewMemoryEngine()
		t.Cleanup(func() { _ = errEngine.MemoryEngine.Close() })
		t.Cleanup(func() { _ = okEngine.Close() })

		okNode := &Node{ID: NodeID(prefixTestID("ok-node")), Labels: []string{"Person"}}
		_, err := okEngine.CreateNode(okNode)
		require.NoError(t, err)

		errorOnly := NewCompositeEngine(
			map[string]Engine{"err": errEngine},
			map[string]string{"err": "err"},
			map[string]string{"err": "read_write"},
		)

		errEngine.getNodeErr = boom
		_, err = errorOnly.GetNode(okNode.ID)
		require.ErrorIs(t, err, boom)
		err = errorOnly.UpdateNode(&Node{ID: okNode.ID, Labels: []string{"Person"}})
		require.ErrorIs(t, err, boom)
		err = errorOnly.DeleteNode(okNode.ID)
		require.ErrorIs(t, err, boom)
		errEngine.getNodeErr = nil

		errEngine.getEdgeErr = boom
		_, err = errorOnly.GetEdge(EdgeID(prefixTestID("missing-edge")))
		require.ErrorIs(t, err, boom)
		err = errorOnly.UpdateEdge(&Edge{ID: EdgeID(prefixTestID("missing-edge")), StartNode: okNode.ID, EndNode: okNode.ID, Type: "REL"})
		require.ErrorIs(t, err, boom)
		err = errorOnly.DeleteEdge(EdgeID(prefixTestID("missing-edge")))
		require.ErrorIs(t, err, boom)
		errEngine.getEdgeErr = nil

		errEngine.firstNodeErr = boom
		_, err = errorOnly.GetFirstNodeByLabel("Person")
		require.ErrorIs(t, err, boom)
		errEngine.firstNodeErr = nil

		composite := NewCompositeEngine(
			map[string]Engine{"err": errEngine, "ok": okEngine},
			map[string]string{"err": "err", "ok": "ok"},
			map[string]string{"err": "read_write", "ok": "read_write"},
		)
		errEngine.returnNilFirstNode = true
		got, err := composite.GetFirstNodeByLabel("Person")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, okNode.ID, got.ID)
		errEngine.returnNilFirstNode = false

		errEngine.outgoingErr = boom
		_, err = composite.GetOutgoingEdges(okNode.ID)
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.outgoingErr = nil

		errEngine.incomingErr = boom
		_, err = composite.GetIncomingEdges(okNode.ID)
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.incomingErr = nil

		errEngine.betweenErr = boom
		_, err = composite.GetEdgesBetween(okNode.ID, okNode.ID)
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.betweenErr = nil

		errEngine.byTypeErr = boom
		_, err = composite.GetEdgesByType("REL")
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.byTypeErr = nil

		errEngine.allNodesErr = boom
		_, err = composite.AllNodes()
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.allNodesErr = nil

		errEngine.allEdgesErr = boom
		_, err = composite.AllEdges()
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.allEdgesErr = nil

		errEngine.batchGetErr = boom
		_, err = composite.BatchGetNodes([]NodeID{okNode.ID})
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.batchGetErr = nil

		errEngine.nodeCountErr = boom
		_, err = composite.NodeCount()
		require.ErrorContains(t, err, "constituent 'err'")
		errEngine.nodeCountErr = nil

		errEngine.edgeCountErr = boom
		_, err = composite.EdgeCount()
		require.ErrorContains(t, err, "constituent 'err'")
	})

	t.Run("write helpers propagate routed constituent and close errors", func(t *testing.T) {
		boom := errors.New("boom")
		errEngine := &compositeErrorEngine{MemoryEngine: NewMemoryEngine()}
		okEngine := NewMemoryEngine()
		t.Cleanup(func() { _ = errEngine.MemoryEngine.Close() })
		t.Cleanup(func() { _ = okEngine.Close() })

		composite := NewCompositeEngine(
			map[string]Engine{"err": errEngine, "ok": okEngine},
			map[string]string{"err": "err", "ok": "ok"},
			map[string]string{"err": "read_write", "ok": "read_write"},
		)
		composite.SetLabelRouting("ErrLabel", []string{"err"})

		errEngine.createNodeErr = boom
		_, err := composite.CreateNode(&Node{
			ID:         NodeID(prefixTestID("route-node")),
			Labels:     []string{"ErrLabel"},
			Properties: map[string]interface{}{"database_id": "err"},
		})
		require.ErrorIs(t, err, boom)
		errEngine.createNodeErr = nil

		start := &Node{ID: NodeID(prefixTestID("start")), Labels: []string{"ErrLabel"}}
		end := &Node{ID: NodeID(prefixTestID("end")), Labels: []string{"ErrLabel"}}
		_, err = errEngine.CreateNode(start)
		require.NoError(t, err)
		_, err = errEngine.CreateNode(end)
		require.NoError(t, err)

		errEngine.createEdgeErr = boom
		err = composite.CreateEdge(&Edge{ID: EdgeID(prefixTestID("route-edge")), StartNode: start.ID, EndNode: end.ID, Type: "REL"})
		require.ErrorIs(t, err, boom)
		errEngine.createEdgeErr = nil

		errEngine.bulkCreateNodesErr = boom
		err = composite.BulkCreateNodes([]*Node{{
			ID:         NodeID(prefixTestID("bulk-node")),
			Labels:     []string{"ErrLabel"},
			Properties: map[string]interface{}{"database_id": "err"},
		}})
		require.ErrorContains(t, err, "failed to create nodes in constituent 'err'")
		errEngine.bulkCreateNodesErr = nil

		errEngine.bulkCreateEdgesErr = boom
		err = composite.BulkCreateEdges([]*Edge{{ID: EdgeID(prefixTestID("bulk-edge")), StartNode: start.ID, EndNode: end.ID, Type: "REL"}})
		require.ErrorContains(t, err, "failed to create edges in constituent 'err'")

		errEngine.closeErr = errors.New("close failed")
		err = composite.Close()
		require.ErrorContains(t, err, "error closing constituent 'err'")
	})
}
