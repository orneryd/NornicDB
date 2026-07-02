package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type namespacedHelperEngine struct {
	*MemoryEngine
	streamNodeErr error
	streamEdgeErr error
	chunkErr      error
	findQueue     []*Node
	refreshCount  int
	addedIDs      []NodeID
	markedIDs     []NodeID
	missingIDs    map[NodeID]bool
}

type namespacedPrefixStatsEngine struct {
	Engine
	nodeCount int64
	edgeCount int64
	nodeErr   error
	edgeErr   error
}

type namespacedLabelStatsEngine struct {
	Engine
	count     int64
	err       error
	namespace string
	label     string
}

type namespacedStreamingOnlyEngine struct {
	Engine
	streamNodeErr error
	streamEdgeErr error
}

type namespacedPrefixStreamingEngine struct {
	Engine
	prefixCalls int
	lastPrefix  string
}

type namespacedSchemaProviderEngine struct {
	Engine
	schema *SchemaManager
}

type namespacedSchemaFallbackEngine struct {
	Engine
	schema *SchemaManager
}

type namespacedMVCCVisibleEngine struct {
	*MemoryEngine
	nodes              []*Node
	outgoing           []*Edge
	incoming           []*Edge
	edgesByType        []*Edge
	edgesBetween       []*Edge
	outgoingErr        error
	incomingErr        error
	lastOutgoingNodeID NodeID
	lastIncomingNodeID NodeID
}

func (e *namespacedHelperEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	if e.streamNodeErr != nil {
		return e.streamNodeErr
	}
	nodes, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedHelperEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	if e.streamNodeErr != nil {
		return e.streamNodeErr
	}
	nodes, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !strings.HasPrefix(string(node.ID), prefix) {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedHelperEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if e.streamEdgeErr != nil {
		return e.streamEdgeErr
	}
	edges, err := e.MemoryEngine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedHelperEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if e.chunkErr != nil {
		return e.chunkErr
	}
	nodes, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return err
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	for i := 0; i < len(nodes); i += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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

func (e *namespacedHelperEngine) FindNodeNeedingEmbedding() *Node {
	if len(e.findQueue) == 0 {
		return nil
	}
	node := CopyNode(e.findQueue[0])
	e.findQueue = e.findQueue[1:]
	return node
}

func (e *namespacedHelperEngine) RefreshPendingEmbeddingsIndex() int {
	return e.refreshCount
}

func (e *namespacedHelperEngine) AddToPendingEmbeddings(nodeID NodeID) {
	e.addedIDs = append(e.addedIDs, nodeID)
}

func (e *namespacedHelperEngine) MarkNodeEmbedded(nodeID NodeID) {
	e.markedIDs = append(e.markedIDs, nodeID)
}

func (e *namespacedHelperEngine) GetNode(nodeID NodeID) (*Node, error) {
	if e.missingIDs != nil && e.missingIDs[nodeID] {
		return nil, ErrNotFound
	}
	return e.MemoryEngine.GetNode(nodeID)
}

func (e *namespacedPrefixStatsEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if e.nodeErr != nil {
		return 0, e.nodeErr
	}
	return e.nodeCount, nil
}

func (e *namespacedPrefixStatsEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if e.edgeErr != nil {
		return 0, e.edgeErr
	}
	return e.edgeCount, nil
}

func (e *namespacedLabelStatsEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	e.namespace = namespace
	e.label = label
	if e.err != nil {
		return 0, e.err
	}
	return e.count, nil
}

func (e *namespacedStreamingOnlyEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	if e.streamNodeErr != nil {
		return e.streamNodeErr
	}
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedStreamingOnlyEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if e.streamEdgeErr != nil {
		return e.streamEdgeErr
	}
	edges, err := e.Engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedStreamingOnlyEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	for i := 0; i < len(nodes); i += chunkSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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

func (e *namespacedMVCCVisibleEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	return e.nodes, nil
}

func (e *namespacedMVCCVisibleEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	e.lastOutgoingNodeID = nodeID
	if e.outgoingErr != nil {
		return nil, e.outgoingErr
	}
	return e.outgoing, nil
}

func (e *namespacedMVCCVisibleEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	e.lastIncomingNodeID = nodeID
	if e.incomingErr != nil {
		return nil, e.incomingErr
	}
	return e.incoming, nil
}

func (e *namespacedMVCCVisibleEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	return e.edgesByType, nil
}

func (e *namespacedMVCCVisibleEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	return e.edgesBetween, nil
}

func (e *namespacedPrefixStreamingEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedPrefixStreamingEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	edges, err := e.Engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedPrefixStreamingEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
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

func (e *namespacedPrefixStreamingEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	e.prefixCalls++
	e.lastPrefix = prefix
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !strings.HasPrefix(string(node.ID), prefix) {
			continue
		}
		if err := fn(node); err != nil {
			return err
		}
	}
	return nil
}

func (e *namespacedSchemaProviderEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	return e.schema
}

func (e *namespacedSchemaFallbackEngine) GetSchema() *SchemaManager {
	return e.schema
}

func TestNamespacedEngine_BasicOperations(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	// Create namespaced engine for tenant_a
	tenantA := NewNamespacedEngine(inner, "tenant_a")
	assert.Equal(t, "tenant_a", tenantA.Namespace())

	// Create a node (NamespacedEngine receives unprefixed IDs)
	node := &Node{
		ID:     NodeID("node-1"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
		},
	}
	_, err := tenantA.CreateNode(node)
	require.NoError(t, err)

	// Get the node back (NamespacedEngine receives unprefixed IDs)
	retrieved, err := tenantA.GetNode(NodeID("node-1"))
	require.NoError(t, err)
	assert.Equal(t, "node-1", string(retrieved.ID))
	assert.Equal(t, "Alice", retrieved.Properties["name"])

	// Verify the underlying storage has the prefixed ID
	prefixedNode, err := inner.GetNode(NodeID("tenant_a:node-1"))
	require.NoError(t, err)
	assert.Equal(t, "tenant_a:node-1", string(prefixedNode.ID))
}

func TestNamespacedEngine_StreamNodes_UsesPrefixStreamingWhenAvailable(t *testing.T) {
	base := NewMemoryEngine()
	defer base.Close()

	_, err := base.CreateNode(&Node{ID: NodeID("tenant_a:n1"), Labels: []string{"L"}, Properties: map[string]any{"name": "a1"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: NodeID("tenant_b:n2"), Labels: []string{"L"}, Properties: map[string]any{"name": "b1"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: NodeID("tenant_a:n3"), Labels: []string{"L"}, Properties: map[string]any{"name": "a2"}})
	require.NoError(t, err)

	prefixInner := &namespacedPrefixStreamingEngine{Engine: base}
	tenantA := NewNamespacedEngine(prefixInner, "tenant_a")

	var seen []NodeID
	err = tenantA.StreamNodes(context.Background(), func(node *Node) error {
		seen = append(seen, node.ID)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, 1, prefixInner.prefixCalls, "should use prefix streaming path exactly once")
	require.Equal(t, "tenant_a:", prefixInner.lastPrefix)
	require.ElementsMatch(t, []NodeID{"n1", "n3"}, seen, "should only return unprefixed tenant_a nodes")
}

func TestNamespacedEngine_Isolation(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")
	tenantB := NewNamespacedEngine(inner, "tenant_b")

	// Create nodes in different tenants (NamespacedEngine receives unprefixed IDs)
	nodeA := &Node{
		ID:         NodeID("node-1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"tenant": "a"},
	}
	_, err := tenantA.CreateNode(nodeA)
	require.NoError(t, err)

	nodeB := &Node{
		ID:         NodeID("node-1"), // Same ID, different tenant
		Labels:     []string{"Person"},
		Properties: map[string]any{"tenant": "b"},
	}
	_, err = tenantB.CreateNode(nodeB)
	require.NoError(t, err)

	// Each tenant should only see their own nodes
	nodesA, err := tenantA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodesA, 1)
	assert.Equal(t, "a", nodesA[0].Properties["tenant"])

	nodesB, err := tenantB.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodesB, 1)
	assert.Equal(t, "b", nodesB[0].Properties["tenant"])
}

func TestNamespacedEngine_Edges(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Create two nodes (NamespacedEngine receives unprefixed IDs)
	node1 := &Node{ID: NodeID("n1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("n2"), Labels: []string{"Person"}}
	_, err := tenantA.CreateNode(node1)
	require.NoError(t, err)
	_, err = tenantA.CreateNode(node2)
	require.NoError(t, err)

	// Create edge (NamespacedEngine receives unprefixed IDs)
	edge := &Edge{
		ID:        EdgeID("e1"),
		StartNode: NodeID("n1"),
		EndNode:   NodeID("n2"),
		Type:      "KNOWS",
		Properties: map[string]any{
			"since": "2020",
		},
	}
	err = tenantA.CreateEdge(edge)
	require.NoError(t, err)

	// Get edge back (NamespacedEngine receives unprefixed IDs)
	retrieved, err := tenantA.GetEdge(EdgeID("e1"))
	require.NoError(t, err)
	assert.Equal(t, "n1", string(retrieved.StartNode))
	assert.Equal(t, "n2", string(retrieved.EndNode))
	assert.Equal(t, "KNOWS", retrieved.Type)

	// Get outgoing edges (NamespacedEngine receives unprefixed IDs)
	outgoing, err := tenantA.GetOutgoingEdges(NodeID("n1"))
	require.NoError(t, err)
	assert.Len(t, outgoing, 1)
	assert.Equal(t, "e1", string(outgoing[0].ID))
}

func TestNamespacedEngine_QueryOperations(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")
	tenantB := NewNamespacedEngine(inner, "tenant_b")

	// Create nodes with same label in different tenants
	for i := 0; i < 3; i++ {
		node := &Node{
			ID:         NodeID("node-" + string(rune('a'+i))),
			Labels:     []string{"Person"},
			Properties: map[string]any{"id": i},
		}
		_, err := tenantA.CreateNode(node)
		require.NoError(t, err)
	}

	for i := 0; i < 2; i++ {
		node := &Node{
			ID:         NodeID("node-" + string(rune('x'+i))),
			Labels:     []string{"Person"},
			Properties: map[string]any{"id": i},
		}
		_, err := tenantB.CreateNode(node)
		require.NoError(t, err)
	}

	// Query by label - should only see tenant's nodes
	nodesA, err := tenantA.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, nodesA, 3)

	nodesB, err := tenantB.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, nodesB, 2)
}

func TestNamespacedEngine_DeleteByPrefix(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Create some nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID("node-" + string(rune('0'+i))),
			Labels: []string{"Test"},
		}
		_, err := tenantA.CreateNode(node)
		require.NoError(t, err)
	}

	// DeleteByPrefix should not be supported on NamespacedEngine
	// (should be called on underlying engine)
	_, _, err := tenantA.DeleteByPrefix("tenant_a:")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported on NamespacedEngine")
}

func TestNamespacedEngine_Stats(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Create nodes and edges (NamespacedEngine receives unprefixed IDs)
	node1 := &Node{ID: NodeID("n1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("n2"), Labels: []string{"Person"}}
	_, err := tenantA.CreateNode(node1)
	require.NoError(t, err)
	_, err = tenantA.CreateNode(node2)
	require.NoError(t, err)

	edge := &Edge{
		ID:        EdgeID("e1"),
		StartNode: NodeID("n1"),
		EndNode:   NodeID("n2"),
		Type:      "KNOWS",
	}
	err = tenantA.CreateEdge(edge)
	require.NoError(t, err)

	// Check counts
	nodeCount, err := tenantA.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(2), nodeCount)

	edgeCount, err := tenantA.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), edgeCount)
}

func TestNamespacedEngine_BulkOperations(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Bulk create nodes (NamespacedEngine receives unprefixed IDs)
	nodes := []*Node{
		{ID: NodeID("n1"), Labels: []string{"Person"}},
		{ID: NodeID("n2"), Labels: []string{"Person"}},
		{ID: NodeID("n3"), Labels: []string{"Person"}},
	}
	err := tenantA.BulkCreateNodes(nodes)
	require.NoError(t, err)

	// Verify all created
	allNodes, err := tenantA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, allNodes, 3)

	// Bulk delete (NamespacedEngine receives unprefixed IDs)
	err = tenantA.BulkDeleteNodes([]NodeID{NodeID("n1"), NodeID("n2")})
	require.NoError(t, err)

	// Verify deleted
	allNodes, err = tenantA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, allNodes, 1)
	assert.Equal(t, "n3", string(allNodes[0].ID))

	t.Run("bulk create rejects nil node and edge entries", func(t *testing.T) {
		err := tenantA.BulkCreateNodes([]*Node{nil})
		require.ErrorIs(t, err, ErrInvalidData)

		err = tenantA.BulkCreateEdges([]*Edge{nil})
		require.ErrorIs(t, err, ErrInvalidData)
	})
}

func TestNamespacedEngine_Close(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Close should not close underlying engine
	err := tenantA.Close()
	require.NoError(t, err)

	// Underlying engine should still work (direct access to inner engine needs prefixed IDs)
	node := &Node{ID: NodeID("test:test"), Labels: []string{"Test"}}
	_, err = inner.CreateNode(node)
	require.NoError(t, err)
}

func TestNamespacedEngine_StreamingAPIs(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")
	tenantB := NewNamespacedEngine(inner, "tenant_b")

	_, err := tenantA.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "a1"}})
	require.NoError(t, err)
	_, err = tenantA.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"name": "a2"}})
	require.NoError(t, err)
	_, err = tenantB.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "b1"}})
	require.NoError(t, err)

	err = tenantA.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"})
	require.NoError(t, err)
	err = tenantB.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "KNOWS"})
	require.NoError(t, err)

	var nodeIDs []NodeID
	err = tenantA.StreamNodes(context.Background(), func(node *Node) error {
		nodeIDs = append(nodeIDs, node.ID)
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []NodeID{"n1", "n2"}, nodeIDs)

	var edgeIDs []EdgeID
	err = tenantA.StreamEdges(context.Background(), func(edge *Edge) error {
		edgeIDs = append(edgeIDs, edge.ID)
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []EdgeID{"e1"}, edgeIDs)

	var chunkedCount int
	err = tenantA.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error {
		chunkedCount += len(nodes)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, chunkedCount)
}

func TestNamespacedEngine_EmbeddingWrappersAndLastWriteTime(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()
	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Wrapper methods should prefix transparently and not panic.
	tenantA.AddToPendingEmbeddings("n1")
	tenantA.MarkNodeEmbedded("n1")

	removed := tenantA.RefreshPendingEmbeddingsIndex()
	assert.GreaterOrEqual(t, removed, 0)

	_ = tenantA.FindNodeNeedingEmbedding()

	// Namespaced LastWriteTime intentionally returns zero to avoid cross-db false positives.
	assert.Equal(t, time.Time{}, tenantA.LastWriteTime())
}

func TestNamespacedEngine_QueryDelegateMethods(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()
	tenantA := NewNamespacedEngine(inner, "tenant_a")

	_, err := tenantA.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "A"}})
	require.NoError(t, err)
	_, err = tenantA.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"name": "B"}})
	require.NoError(t, err)
	err = tenantA.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]any{}})
	require.NoError(t, err)

	first, err := tenantA.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	assert.NotNil(t, first)

	incoming, err := tenantA.GetIncomingEdges("n2")
	require.NoError(t, err)
	assert.Len(t, incoming, 1)

	byType, err := tenantA.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	assert.Len(t, byType, 1)

	edge := tenantA.GetEdgeBetween("n1", "n2", "KNOWS")
	assert.NotNil(t, edge)

	all := tenantA.GetAllNodes()
	assert.Len(t, all, 2)
	assert.GreaterOrEqual(t, tenantA.GetOutDegree("n1"), 1)
	assert.GreaterOrEqual(t, tenantA.GetInDegree("n2"), 1)

	batch, err := tenantA.BatchGetNodes([]NodeID{"n1", "n2"})
	require.NoError(t, err)
	assert.Len(t, batch, 2)
}

func TestNamespacedEngine_DirectStreamingAndEmbeddingHelpers(t *testing.T) {
	t.Run("direct streaming filters namespace and propagates errors", func(t *testing.T) {
		inner := &namespacedHelperEngine{MemoryEngine: NewMemoryEngine()}
		t.Cleanup(func() { _ = inner.Close() })
		tenantA := NewNamespacedEngine(inner, "tenant_a")
		tenantB := NewNamespacedEngine(inner, "tenant_b")

		_, err := tenantA.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "a1"}})
		require.NoError(t, err)
		_, err = tenantA.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"name": "a2"}})
		require.NoError(t, err)
		_, err = tenantB.CreateNode(&Node{ID: "n9", Labels: []string{"Person"}, Properties: map[string]any{"name": "b1"}})
		require.NoError(t, err)
		require.NoError(t, tenantA.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}))
		require.NoError(t, tenantB.CreateEdge(&Edge{ID: "e9", StartNode: "n9", EndNode: "n9", Type: "KNOWS"}))

		var nodeIDs []NodeID
		err = tenantA.StreamNodes(context.Background(), func(node *Node) error {
			nodeIDs = append(nodeIDs, node.ID)
			return nil
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []NodeID{"n1", "n2"}, nodeIDs)

		var edgeIDs []EdgeID
		err = tenantA.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeIDs = append(edgeIDs, edge.ID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []EdgeID{"e1"}, edgeIDs)

		var chunked [][]NodeID
		err = tenantA.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error {
			var ids []NodeID
			for _, node := range nodes {
				ids = append(ids, node.ID)
			}
			chunked = append(chunked, ids)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, chunked, 1)
		assert.ElementsMatch(t, []NodeID{"n1", "n2"}, chunked[0])

		errBoom := errors.New("node callback failed")
		err = tenantA.StreamNodes(context.Background(), func(node *Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("edge callback failed")
		err = tenantA.StreamEdges(context.Background(), func(edge *Edge) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("chunk callback failed")
		err = tenantA.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		inner.streamNodeErr = errors.New("stream nodes failed")
		err = tenantA.StreamNodes(context.Background(), func(node *Node) error { return nil })
		require.ErrorContains(t, err, "stream nodes failed")

		inner.streamNodeErr = nil
		inner.streamEdgeErr = errors.New("stream edges failed")
		err = tenantA.StreamEdges(context.Background(), func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "stream edges failed")

		inner.streamEdgeErr = nil
		inner.chunkErr = errors.New("stream chunks failed")
		err = tenantA.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return nil })
		require.ErrorContains(t, err, "stream chunks failed")
	})

	t.Run("find node needing embedding skips foreign and stale nodes", func(t *testing.T) {
		inner := &namespacedHelperEngine{
			MemoryEngine: NewMemoryEngine(),
			findQueue: []*Node{
				{ID: "tenant_b:foreign", Labels: []string{"Doc"}, Properties: map[string]any{"text": "skip me"}},
				{ID: "tenant_a:stale", Labels: []string{"Doc"}, Properties: map[string]any{"text": "stale"}},
				{ID: "tenant_a:valid", Labels: []string{"Doc"}, Properties: map[string]any{"text": "keep"}},
			},
			missingIDs: map[NodeID]bool{"tenant_a:stale": true},
		}
		t.Cleanup(func() { _ = inner.Close() })
		tenantA := NewNamespacedEngine(inner, "tenant_a")

		_, err := inner.CreateNode(&Node{ID: "tenant_a:valid", Labels: []string{"Doc"}, Properties: map[string]any{"text": "keep"}})
		require.NoError(t, err)

		found := tenantA.FindNodeNeedingEmbedding()
		require.NotNil(t, found)
		assert.Equal(t, NodeID("valid"), found.ID)
		assert.Equal(t, []NodeID{"tenant_a:stale"}, inner.markedIDs)
	})

	t.Run("refresh pending embeddings prefixes add and remove operations", func(t *testing.T) {
		inner := &namespacedHelperEngine{
			MemoryEngine: NewMemoryEngine(),
			refreshCount: 2,
		}
		t.Cleanup(func() { _ = inner.Close() })
		tenantA := NewNamespacedEngine(inner, "tenant_a")

		_, err := tenantA.CreateNode(&Node{ID: "needs", Labels: []string{"Doc"}, Properties: map[string]any{"text": "embed me"}})
		require.NoError(t, err)
		_, err = tenantA.CreateNode(&Node{
			ID:              "done",
			Labels:          []string{"Doc"},
			Properties:      map[string]any{"text": "embedded"},
			ChunkEmbeddings: [][]float32{{1, 2}},
		})
		require.NoError(t, err)
		_, err = tenantA.CreateNode(&Node{ID: "internal", Labels: []string{"_Meta"}, Properties: map[string]any{"text": "skip"}})
		require.NoError(t, err)

		removed := tenantA.RefreshPendingEmbeddingsIndex()
		assert.Equal(t, 3, removed)
		assert.Equal(t, []NodeID{"tenant_a:needs"}, inner.addedIDs)
		assert.Equal(t, []NodeID{"tenant_a:done"}, inner.markedIDs)
	})

	t.Run("streaming fallbacks work without streaming inner engine", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		inner := &nonStreamingCountEngine{Engine: base}
		tenantA := NewNamespacedEngine(inner, "tenant_a")
		tenantB := NewNamespacedEngine(inner, "tenant_b")

		_, err := tenantA.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantA.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantB.CreateNode(&Node{ID: "n9", Labels: []string{"Person"}})
		require.NoError(t, err)
		require.NoError(t, tenantA.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}))
		require.NoError(t, tenantB.CreateEdge(&Edge{ID: "e9", StartNode: "n9", EndNode: "n9", Type: "KNOWS"}))

		var nodeIDs []NodeID
		err = tenantA.StreamNodes(context.Background(), func(node *Node) error {
			nodeIDs = append(nodeIDs, node.ID)
			return nil
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []NodeID{"n1", "n2"}, nodeIDs)

		var edgeIDs []EdgeID
		err = tenantA.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeIDs = append(edgeIDs, edge.ID)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, []EdgeID{"e1"}, edgeIDs)

		var chunkedCount int
		err = tenantA.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error {
			chunkedCount += len(nodes)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, chunkedCount)

		errBoom := errors.New("fallback node callback failed")
		err = tenantA.StreamNodes(context.Background(), func(node *Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("fallback edge callback failed")
		err = tenantA.StreamEdges(context.Background(), func(edge *Edge) error { return errBoom })
		require.ErrorIs(t, err, errBoom)

		errBoom = errors.New("fallback chunk callback failed")
		err = tenantA.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error { return errBoom })
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("node count by label covers stats streaming and all-nodes fallbacks", func(t *testing.T) {
		statsInner := &namespacedLabelStatsEngine{Engine: NewMemoryEngine(), count: 9}
		t.Cleanup(func() { _ = statsInner.Close() })
		statsTenant := NewNamespacedEngine(statsInner, "tenant_a")
		count, err := statsTenant.NodeCountByLabel("Person")
		require.NoError(t, err)
		assert.EqualValues(t, 9, count)
		assert.Equal(t, "tenant_a", statsInner.namespace)
		assert.Equal(t, "Person", statsInner.label)

		wantStatsErr := errors.New("stats count failed")
		statsInner.err = wantStatsErr
		_, err = statsTenant.NodeCountByLabel("Person")
		require.ErrorIs(t, err, wantStatsErr)

		streamBase := NewMemoryEngine()
		t.Cleanup(func() { _ = streamBase.Close() })
		streamInner := &namespacedStreamingOnlyEngine{Engine: streamBase}
		streamTenantA := NewNamespacedEngine(streamInner, "tenant_a")
		streamTenantB := NewNamespacedEngine(streamInner, "tenant_b")
		_, err = streamTenantA.CreateNode(&Node{ID: "one", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = streamTenantA.CreateNode(&Node{ID: "two", Labels: []string{"person"}})
		require.NoError(t, err)
		_, err = streamTenantB.CreateNode(&Node{ID: "three", Labels: []string{"Person"}})
		require.NoError(t, err)
		count, err = streamTenantA.NodeCountByLabel("PERSON")
		require.NoError(t, err)
		assert.EqualValues(t, 2, count)

		wantStreamErr := errors.New("stream count failed")
		streamInner.streamNodeErr = wantStreamErr
		_, err = streamTenantA.NodeCountByLabel("Person")
		require.ErrorIs(t, err, wantStreamErr)

		allNodesBase := NewMemoryEngine()
		t.Cleanup(func() { _ = allNodesBase.Close() })
		allNodesInner := &nonStreamingCountEngine{Engine: allNodesBase}
		allNodesTenantA := NewNamespacedEngine(allNodesInner, "tenant_a")
		allNodesTenantB := NewNamespacedEngine(allNodesInner, "tenant_b")
		_, err = allNodesTenantA.CreateNode(&Node{ID: "one", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = allNodesTenantB.CreateNode(&Node{ID: "two", Labels: []string{"Person"}})
		require.NoError(t, err)
		count, err = allNodesTenantA.NodeCountByLabel("person")
		require.NoError(t, err)
		assert.EqualValues(t, 1, count)

		wantAllNodesErr := errors.New("all nodes failed")
		allNodesInner.allNodesErr = wantAllNodesErr
		_, err = allNodesTenantA.NodeCountByLabel("Person")
		require.ErrorIs(t, err, wantAllNodesErr)
	})

	t.Run("find node needing embedding falls back to namespace scan after many foreign nodes", func(t *testing.T) {
		foreignQueue := make([]*Node, 10000)
		for i := range foreignQueue {
			foreignQueue[i] = &Node{
				ID:         NodeID("tenant_b:foreign"),
				Labels:     []string{"Doc"},
				Properties: map[string]any{"text": "skip"},
			}
		}
		inner := &namespacedHelperEngine{
			MemoryEngine: NewMemoryEngine(),
			findQueue:    foreignQueue,
		}
		t.Cleanup(func() { _ = inner.Close() })
		tenantA := NewNamespacedEngine(inner, "tenant_a")

		_, err := tenantA.CreateNode(&Node{
			ID:         "scan-me",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"text": "embed me"},
		})
		require.NoError(t, err)

		found := tenantA.FindNodeNeedingEmbedding()
		require.NotNil(t, found)
		assert.Equal(t, NodeID("scan-me"), found.ID)
		assert.Equal(t, []NodeID{"tenant_a:scan-me"}, inner.addedIDs)
	})
}

func TestNamespacedEngine_CountAndLookupHelpers(t *testing.T) {
	t.Run("node and edge count use prefix stats and propagate errors", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &namespacedPrefixStatsEngine{
			Engine:    base,
			nodeCount: 7,
			edgeCount: 3,
		}
		tenantA := NewNamespacedEngine(engine, "tenant_a")

		nodeCount, err := tenantA.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(7), nodeCount)

		edgeCount, err := tenantA.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(3), edgeCount)

		engine.nodeErr = errors.New("node count failed")
		_, err = tenantA.NodeCount()
		require.ErrorContains(t, err, "node count failed")

		engine.edgeErr = errors.New("edge count failed")
		_, err = tenantA.EdgeCount()
		require.ErrorContains(t, err, "edge count failed")
	})

	t.Run("node and edge count use streaming and all fallbacks", func(t *testing.T) {
		streamingBase := NewMemoryEngine()
		t.Cleanup(func() { _ = streamingBase.Close() })
		streamingInner := &namespacedStreamingOnlyEngine{Engine: streamingBase}
		tenantA := NewNamespacedEngine(streamingInner, "tenant_a")
		tenantB := NewNamespacedEngine(streamingInner, "tenant_b")

		_, err := tenantA.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantA.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantB.CreateNode(&Node{ID: "n9", Labels: []string{"Person"}})
		require.NoError(t, err)
		require.NoError(t, tenantA.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}))
		require.NoError(t, tenantB.CreateEdge(&Edge{ID: "e9", StartNode: "n9", EndNode: "n9", Type: "KNOWS"}))

		nodeCount, err := tenantA.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(2), nodeCount)

		edgeCount, err := tenantA.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(1), edgeCount)

		streamingInner.streamNodeErr = errors.New("stream nodes failed")
		_, err = tenantA.NodeCount()
		require.ErrorContains(t, err, "stream nodes failed")

		streamingInner.streamNodeErr = nil
		streamingInner.streamEdgeErr = errors.New("stream edges failed")
		_, err = tenantA.EdgeCount()
		require.ErrorContains(t, err, "stream edges failed")

		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		fallbackInner := &nonStreamingCountEngine{Engine: base}
		tenantAFallback := NewNamespacedEngine(fallbackInner, "tenant_a")
		tenantBFallback := NewNamespacedEngine(fallbackInner, "tenant_b")
		_, err = tenantAFallback.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantBFallback.CreateNode(&Node{ID: "n9", Labels: []string{"Person"}})
		require.NoError(t, err)
		require.NoError(t, tenantAFallback.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "KNOWS"}))
		require.NoError(t, tenantBFallback.CreateEdge(&Edge{ID: "e9", StartNode: "n9", EndNode: "n9", Type: "KNOWS"}))

		nodeCount, err = tenantAFallback.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(1), nodeCount)

		edgeCount, err = tenantAFallback.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(1), edgeCount)

		fallbackInner.allNodesErr = errors.New("all nodes failed")
		_, err = tenantAFallback.NodeCount()
		require.ErrorContains(t, err, "all nodes failed")

		fallbackInner.allNodesErr = nil
		fallbackInner.allEdgesErr = errors.New("all edges failed")
		_, err = tenantAFallback.EdgeCount()
		require.ErrorContains(t, err, "all edges failed")
	})

	t.Run("get first falls back when inner first node is foreign", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		_, err := base.CreateNode(&Node{ID: "tenant_a:first", Labels: []string{"Person"}, Properties: map[string]any{"name": "tenant-a"}})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "tenant_b:first", Labels: []string{"Person"}, Properties: map[string]any{"name": "tenant-b"}})
		require.NoError(t, err)

		engine := &firstLabelEngine{
			Engine: base,
			node:   &Node{ID: "tenant_b:first", Labels: []string{"Person"}, Properties: map[string]any{"name": "tenant-b"}},
		}
		tenantA := NewNamespacedEngine(engine, "tenant_a")

		first, err := tenantA.GetFirstNodeByLabel("Person")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, NodeID("first"), first.ID)

		engine.err = errors.New("first label failed")
		first, err = tenantA.GetFirstNodeByLabel("Person")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, NodeID("first"), first.ID)
	})

	t.Run("get first returns not found when namespace has no matches", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &firstLabelEngine{
			Engine: base,
			node:   &Node{ID: "tenant_b:first", Labels: []string{"Person"}},
			err:    errors.New("first label failed"),
		}
		tenantA := NewNamespacedEngine(engine, "tenant_a")

		first, err := tenantA.GetFirstNodeByLabel("Person")
		require.ErrorIs(t, err, ErrNotFound)
		assert.Nil(t, first)
	})

	t.Run("for-each uses lookup and fallback paths", func(t *testing.T) {
		engine := &lookupEngine{
			Engine: NewMemoryEngine(),
			ids:    []NodeID{"tenant_b:skip", "tenant_a:one", "tenant_a:two"},
		}
		t.Cleanup(func() {
			if closer, ok := engine.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})
		tenantA := NewNamespacedEngine(engine, "tenant_a")

		var seen []NodeID
		err := tenantA.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
			seen = append(seen, id)
			return id != "one"
		})
		require.NoError(t, err)
		assert.Equal(t, []NodeID{"one"}, seen)

		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		fallback := NewNamespacedEngine(&nonStreamingCountEngine{Engine: base}, "tenant_a")
		other := NewNamespacedEngine(&nonStreamingCountEngine{Engine: base}, "tenant_b")
		_, err = fallback.CreateNode(&Node{ID: "one", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = other.CreateNode(&Node{ID: "two", Labels: []string{"Person"}})
		require.NoError(t, err)

		seen = nil
		err = fallback.ForEachNodeIDByLabel("Person", func(id NodeID) bool {
			seen = append(seen, id)
			return true
		})
		require.NoError(t, err)
		assert.Equal(t, []NodeID{"one"}, seen)

		require.NoError(t, fallback.ForEachNodeIDByLabel("Person", nil))
	})

	t.Run("get edge between filters foreign edges and schema uses provider", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		tenantA := NewNamespacedEngine(base, "tenant_a")
		tenantB := NewNamespacedEngine(base, "tenant_b")

		_, err := tenantA.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantA.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = tenantB.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		require.NoError(t, tenantA.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}))
		require.NoError(t, tenantB.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "KNOWS"}))

		edge := tenantA.GetEdgeBetween("n1", "n2", "KNOWS")
		require.NotNil(t, edge)
		assert.Equal(t, EdgeID("e1"), edge.ID)

		assert.Nil(t, tenantA.GetEdgeBetween("n1", "n1", "KNOWS"))

		provider := &namespacedSchemaProviderEngine{Engine: base, schema: NewSchemaManager()}
		assert.Same(t, provider.schema, NewNamespacedEngine(provider, "tenant_a").GetSchema())
		fallbackSchema := NewSchemaManager()
		fallback := &namespacedSchemaFallbackEngine{Engine: base, schema: fallbackSchema}
		assert.Same(t, fallbackSchema, NewNamespacedEngine(fallback, "tenant_a").GetSchema())
	})
}

func TestNamespacedEngine_OptionalDelegateFallbackBranches(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	inner := &nonStreamingCountEngine{Engine: base}
	ns := NewNamespacedEngine(inner, "tenant_a")

	logger := ns.namespaceLog()
	require.NotNil(t, logger)
	require.Same(t, logger, ns.namespaceLog())

	node, err := ns.GetTemporalNodeAsOf("Doc", "id", "n1", "from", "to", time.Now())
	require.NoError(t, err)
	require.Nil(t, node)

	current, err := ns.IsCurrentTemporalNode(&Node{ID: "n1", Labels: []string{"Doc"}}, time.Now())
	require.NoError(t, err)
	require.True(t, current)

	_, err = ns.GetNodeVisibleAt("n1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetEdgeVisibleAt("e1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetNodesByLabelVisibleAt("Doc", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetOutgoingEdgesVisibleAt("n1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetIncomingEdgesVisibleAt("n1", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetEdgesByTypeVisibleAt("REL", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetEdgesBetweenVisibleAt("n1", "n2", MVCCVersion{})
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetNodeCurrentHead("n1")
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = ns.GetEdgeCurrentHead("e1")
	require.ErrorIs(t, err, ErrNotImplemented)

	release := ns.RegisterSnapshotReader(SnapshotReaderInfo{ReaderID: "reader"})
	require.NotNil(t, release)
	release()
	require.Equal(t, map[string]interface{}{"enabled": false}, ns.LifecycleStatus())
	require.NoError(t, ns.TriggerPruneNow(context.Background()))
	ns.PauseLifecycle()
	ns.ResumeLifecycle()
	require.NoError(t, ns.SetLifecycleSchedule(time.Second))
	require.Nil(t, ns.TopLifecycleDebtKeys(5))

	latestNode, err := ns.GetNodeLatestVisible("missing")
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, latestNode)
	latestEdge, err := ns.GetEdgeLatestVisible("missing")
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, latestEdge)
}

func TestNamespacedEngine_MVCCIndexedVisibilityFiltersAndStripsNamespace(t *testing.T) {
	inner := &namespacedMVCCVisibleEngine{
		MemoryEngine: NewMemoryEngine(),
		nodes: []*Node{
			{ID: "tenant_a:n1", Labels: []string{"Doc"}, Properties: map[string]any{"name": "kept"}},
			{ID: "tenant_b:n2", Labels: []string{"Doc"}, Properties: map[string]any{"name": "filtered"}},
		},
		outgoing: []*Edge{
			{ID: "tenant_a:e1", Type: "REL", StartNode: "tenant_a:n1", EndNode: "tenant_a:n3"},
			{ID: "tenant_b:e2", Type: "REL", StartNode: "tenant_b:n1", EndNode: "tenant_b:n3"},
		},
		incoming: []*Edge{
			{ID: "tenant_a:e3", Type: "REL", StartNode: "tenant_a:n4", EndNode: "tenant_a:n1"},
			{ID: "tenant_b:e4", Type: "REL", StartNode: "tenant_b:n4", EndNode: "tenant_b:n1"},
		},
	}
	t.Cleanup(func() { _ = inner.Close() })

	ns := NewNamespacedEngine(inner, "tenant_a")

	nodes, err := ns.GetNodesByLabelVisibleAt("Doc", MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, NodeID("n1"), nodes[0].ID)

	outgoing, err := ns.GetOutgoingEdgesVisibleAt("n1", MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, NodeID("tenant_a:n1"), inner.lastOutgoingNodeID)
	require.Len(t, outgoing, 1)
	require.Equal(t, EdgeID("e1"), outgoing[0].ID)
	require.Equal(t, NodeID("n1"), outgoing[0].StartNode)
	require.Equal(t, NodeID("n3"), outgoing[0].EndNode)

	incoming, err := ns.GetIncomingEdgesVisibleAt("n1", MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, NodeID("tenant_a:n1"), inner.lastIncomingNodeID)
	require.Len(t, incoming, 1)
	require.Equal(t, EdgeID("e3"), incoming[0].ID)
	require.Equal(t, NodeID("n4"), incoming[0].StartNode)
	require.Equal(t, NodeID("n1"), incoming[0].EndNode)
}

func TestNamespacedEngine_MVCCIndexedVisibilityPropagatesProviderErrors(t *testing.T) {
	inner := &namespacedMVCCVisibleEngine{
		MemoryEngine: NewMemoryEngine(),
		outgoingErr:  errors.New("outgoing boom"),
		incomingErr:  errors.New("incoming boom"),
	}
	t.Cleanup(func() { _ = inner.Close() })
	ns := NewNamespacedEngine(inner, "tenant_a")

	_, err := ns.GetOutgoingEdgesVisibleAt("n1", MVCCVersion{})
	require.EqualError(t, err, "outgoing boom")
	_, err = ns.GetIncomingEdgesVisibleAt("n1", MVCCVersion{})
	require.EqualError(t, err, "incoming boom")
}

func TestNamespacedEngine_UserConversionHelpers(t *testing.T) {
	memory := NewNamespacedEngine(NewMemoryEngine(), "tenant_a")
	assert.Nil(t, memory.toUserNode(nil))
	assert.Nil(t, memory.toUserEdge(nil))

	shallowNode := memory.toUserNode(&Node{ID: "tenant_a:n1", Labels: []string{"Doc"}, Properties: map[string]any{"name": "one"}})
	require.NotNil(t, shallowNode)
	assert.Equal(t, NodeID("n1"), shallowNode.ID)

	shallow := memory.toUserEdge(&Edge{
		ID:        "tenant_a:e1",
		StartNode: "tenant_a:n1",
		EndNode:   "tenant_a:n2",
		Type:      "KNOWS",
	})
	require.NotNil(t, shallow)
	assert.Equal(t, EdgeID("e1"), shallow.ID)
	assert.Equal(t, NodeID("n1"), shallow.StartNode)
	assert.Equal(t, NodeID("n2"), shallow.EndNode)

	asyncInner := NewAsyncEngine(NewMemoryEngine(), &AsyncEngineConfig{FlushInterval: time.Hour})
	defer asyncInner.Close()
	deep := NewNamespacedEngine(asyncInner, "tenant_a").toUserEdge(&Edge{
		ID:        "tenant_a:e2",
		StartNode: "tenant_a:n3",
		EndNode:   "tenant_a:n4",
		Type:      "KNOWS",
	})
	require.NotNil(t, deep)
	assert.Equal(t, EdgeID("e2"), deep.ID)
	assert.Equal(t, NodeID("n3"), deep.StartNode)
	assert.Equal(t, NodeID("n4"), deep.EndNode)

	deepNode := NewNamespacedEngine(asyncInner, "tenant_a").toUserNode(&Node{ID: "tenant_a:n2", Labels: []string{"Doc"}})
	require.NotNil(t, deepNode)
	assert.Equal(t, NodeID("n2"), deepNode.ID)
}

func TestNamespacedEngine_UnprefixFallback(t *testing.T) {
	inner, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer inner.Close()

	ns := NewNamespacedEngine(inner, "mydb")
	assert.Equal(t, NodeID("mydb:n1"), ns.prefixNodeID("mydb:n1"))
	assert.Equal(t, EdgeID("mydb:e1"), ns.prefixEdgeID("mydb:e1"))

	// IDs that don't match the namespace should be returned as-is
	assert.Equal(t, NodeID("otherdb:n1"), ns.unprefixNodeID("otherdb:n1"))
	assert.Equal(t, EdgeID("otherdb:e1"), ns.unprefixEdgeID("otherdb:e1"))

	// IDs that match should be stripped
	assert.Equal(t, NodeID("n1"), ns.unprefixNodeID("mydb:n1"))
	assert.Equal(t, EdgeID("e1"), ns.unprefixEdgeID("mydb:e1"))
}

func TestNamespacedEngine_GetEdgeBetween_NotFound(t *testing.T) {
	inner, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer inner.Close()

	ns := NewNamespacedEngine(inner, "mydb")

	// No edge exists between non-existent nodes
	edge := ns.GetEdgeBetween("n1", "n2", "KNOWS")
	assert.Nil(t, edge)
}

func TestNamespacedEngine_ForEachNodeIDByLabel_EarlyStop(t *testing.T) {
	inner, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer inner.Close()

	ns := NewNamespacedEngine(inner, "mydb")

	// Create several nodes
	for i := 0; i < 5; i++ {
		_, err := ns.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("n%d", i)),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		})
		require.NoError(t, err)
	}

	// Early stop after 2
	var visited []NodeID
	err = ns.ForEachNodeIDByLabel("Item", func(id NodeID) bool {
		visited = append(visited, id)
		return len(visited) < 2
	})
	require.NoError(t, err)
	assert.Len(t, visited, 2)
}

func TestNamespacedEngine_GetEdgesByType(t *testing.T) {
	inner, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer inner.Close()

	ns := NewNamespacedEngine(inner, "mydb")

	n1 := &Node{ID: "n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: "n2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	_, err = ns.CreateNode(n1)
	require.NoError(t, err)
	_, err = ns.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]interface{}{}}))

	edges, err := ns.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	assert.Len(t, edges, 1)
	// Should return unprefixed IDs
	assert.Equal(t, EdgeID("e1"), edges[0].ID)
}
