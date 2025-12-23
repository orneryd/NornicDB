// Package storage provides namespaced storage engine wrapper for multi-database support.
//
// NamespacedEngine wraps any storage.Engine with automatic key prefixing for database isolation.
// This enables multiple logical databases (tenants) to share a single physical storage backend
// while maintaining complete data isolation.
//
// Key Design:
//   - All node and edge IDs are prefixed with the namespace: "tenant_a:123" instead of "123"
//   - Queries only see data in the current namespace
//   - DROP DATABASE = delete all keys with namespace prefix
//
// Thread Safety:
//
//	Delegates to underlying engine's thread safety guarantees.
//
// Example:
//
//	inner := storage.NewBadgerEngine("./data")
//	tenantA := storage.NewNamespacedEngine(inner, "tenant_a")
//
//	// Creates node with ID "tenant_a:123" in BadgerDB
//	tenantA.CreateNode(&Node{ID: "123", Labels: []string{"Person"}})
//
//	// Only sees nodes with "tenant_a:" prefix
//	nodes, _ := tenantA.AllNodes()
package storage

import (
	"context"
	"fmt"
	"strings"
)

// NamespacedEngine wraps a storage engine with database namespace isolation.
// All node and edge IDs are automatically prefixed with the namespace.
//
// This provides logical database separation within a single physical storage:
//   - Keys are prefixed: "tenant_a:node:123" instead of "node:123"
//   - Queries only see data in the current namespace
//   - DROP DATABASE = delete all keys with prefix
//
// Thread-safe: delegates to underlying engine's thread safety.
type NamespacedEngine struct {
	inner     Engine
	namespace string
	separator string // Default ":"
}

// NewNamespacedEngine creates a namespaced view of the storage engine.
//
// Parameters:
//   - inner: The underlying storage engine (shared across all namespaces)
//   - namespace: The database name (e.g., "tenant_a", "nornic")
//
// The namespace is used as a key prefix for all operations.
func NewNamespacedEngine(inner Engine, namespace string) *NamespacedEngine {
	return &NamespacedEngine{
		inner:     inner,
		namespace: namespace,
		separator: ":",
	}
}

// Namespace returns the current database namespace.
func (n *NamespacedEngine) Namespace() string {
	return n.namespace
}

// GetInnerEngine returns the underlying storage engine (unwraps the namespace).
// This is used by DatabaseManager to create NamespacedEngines for other databases.
func (n *NamespacedEngine) GetInnerEngine() Engine {
	return n.inner
}

// prefixNodeID adds namespace prefix to a node ID.
// "123" → "tenant_a:123"
func (n *NamespacedEngine) prefixNodeID(id NodeID) NodeID {
	return NodeID(n.namespace + n.separator + string(id))
}

// unprefixNodeID removes namespace prefix from a node ID.
// "tenant_a:123" → "123"
func (n *NamespacedEngine) unprefixNodeID(id NodeID) NodeID {
	prefix := n.namespace + n.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return NodeID(s[len(prefix):])
	}
	return id
}

// prefixEdgeID adds namespace prefix to an edge ID.
func (n *NamespacedEngine) prefixEdgeID(id EdgeID) EdgeID {
	return EdgeID(n.namespace + n.separator + string(id))
}

// unprefixEdgeID removes namespace prefix from an edge ID.
func (n *NamespacedEngine) unprefixEdgeID(id EdgeID) EdgeID {
	prefix := n.namespace + n.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return EdgeID(s[len(prefix):])
	}
	return id
}

// hasNodePrefix checks if an ID belongs to this namespace.
func (n *NamespacedEngine) hasNodePrefix(id NodeID) bool {
	return strings.HasPrefix(string(id), n.namespace+n.separator)
}

func (n *NamespacedEngine) hasEdgePrefix(id EdgeID) bool {
	return strings.HasPrefix(string(id), n.namespace+n.separator)
}

// requiresDeepCopy returns true when the inner engine may return cached pointers
// that must not be mutated (e.g., AsyncEngine caches nodes/edges).
func (n *NamespacedEngine) requiresDeepCopy() bool {
	engine := n.inner
	// Unwrap WAL if present
	if walEngine, ok := engine.(*WALEngine); ok {
		engine = walEngine.GetEngine()
	}
	// AsyncEngine returns cached objects; always deep copy for safety.
	if _, ok := engine.(*AsyncEngine); ok {
		return true
	}
	return false
}

func (n *NamespacedEngine) toUserNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	if !n.requiresDeepCopy() {
		// Shallow copy is safe for engines that do not expose cached pointers
		out := *node
		out.ID = n.unprefixNodeID(out.ID)
		return &out
	}
	out := CopyNode(node)
	out.ID = n.unprefixNodeID(out.ID)
	return out
}

func (n *NamespacedEngine) toUserEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	if !n.requiresDeepCopy() {
		out := *edge
		out.ID = n.unprefixEdgeID(out.ID)
		out.StartNode = n.unprefixNodeID(out.StartNode)
		out.EndNode = n.unprefixNodeID(out.EndNode)
		return &out
	}
	out := CopyEdge(edge)
	out.ID = n.unprefixEdgeID(out.ID)
	out.StartNode = n.unprefixNodeID(out.StartNode)
	out.EndNode = n.unprefixNodeID(out.EndNode)
	return out
}

// ============================================================================
// Node Operations
// ============================================================================

func (n *NamespacedEngine) CreateNode(node *Node) (NodeID, error) {
	// Create a copy with namespaced ID
	namespacedID := n.prefixNodeID(node.ID)
	namespacedNode := &Node{
		ID:              namespacedID,
		Labels:          node.Labels,
		Properties:      node.Properties,
		CreatedAt:       node.CreatedAt,
		UpdatedAt:       node.UpdatedAt,
		DecayScore:      node.DecayScore,
		LastAccessed:    node.LastAccessed,
		AccessCount:     node.AccessCount,
		ChunkEmbeddings: node.ChunkEmbeddings,
	}
	actualID, err := n.inner.CreateNode(namespacedNode)
	if err != nil {
		return "", err
	}
	// Return unprefixed ID to user (user-facing API)
	// But internally, we track the prefixed ID in nodeToConstituent
	return n.unprefixNodeID(actualID), nil
}

func (n *NamespacedEngine) GetNode(id NodeID) (*Node, error) {
	// Always prefix the ID (user-facing API always receives unprefixed IDs)
	namespacedID := n.prefixNodeID(id)
	node, err := n.inner.GetNode(namespacedID)
	if err != nil {
		return nil, err
	}

	// IMPORTANT: Never mutate nodes returned from the inner engine in-place.
	// AsyncEngine (and other engines) may return pointers to cached objects that
	// are later flushed; mutating IDs would corrupt cache state and cause data loss.
	return n.toUserNode(node), nil
}

func (n *NamespacedEngine) UpdateNode(node *Node) error {
	// Always prefix the ID (user-facing API always receives unprefixed IDs)
	namespacedID := n.prefixNodeID(node.ID)
	namespacedNode := &Node{
		ID:              namespacedID,
		Labels:          node.Labels,
		Properties:      node.Properties,
		CreatedAt:       node.CreatedAt,
		UpdatedAt:       node.UpdatedAt,
		DecayScore:      node.DecayScore,
		LastAccessed:    node.LastAccessed,
		AccessCount:     node.AccessCount,
		ChunkEmbeddings: node.ChunkEmbeddings,
	}
	return n.inner.UpdateNode(namespacedNode)
}

func (n *NamespacedEngine) DeleteNode(id NodeID) error {
	prefixedID := n.prefixNodeID(id)

	// Delete the node (this will remove it from pending embeddings index)
	err := n.inner.DeleteNode(prefixedID)

	// Remove from pending embeddings index (with namespace prefix)
	// ALL node IDs in the index must be prefixed
	if mgr, ok := n.inner.(interface{ MarkNodeEmbedded(NodeID) }); ok {
		mgr.MarkNodeEmbedded(prefixedID)
	}

	return err
}

// ============================================================================
// Edge Operations
// ============================================================================

func (n *NamespacedEngine) CreateEdge(edge *Edge) error {
	// Always prefix node IDs (user-facing API always receives unprefixed IDs)
	startNodeID := n.prefixNodeID(edge.StartNode)
	endNodeID := n.prefixNodeID(edge.EndNode)

	namespacedEdge := &Edge{
		ID:            n.prefixEdgeID(edge.ID),
		Type:          edge.Type,
		StartNode:     startNodeID,
		EndNode:       endNodeID,
		Properties:    edge.Properties,
		CreatedAt:     edge.CreatedAt,
		UpdatedAt:     edge.UpdatedAt,
		Confidence:    edge.Confidence,
		AutoGenerated: edge.AutoGenerated,
	}
	return n.inner.CreateEdge(namespacedEdge)
}

func (n *NamespacedEngine) GetEdge(id EdgeID) (*Edge, error) {
	namespacedID := n.prefixEdgeID(id)
	edge, err := n.inner.GetEdge(namespacedID)
	if err != nil {
		return nil, err
	}

	// IMPORTANT: Never mutate edges returned from the inner engine in-place.
	return n.toUserEdge(edge), nil
}

func (n *NamespacedEngine) UpdateEdge(edge *Edge) error {
	// Always prefix IDs (user-facing API always receives unprefixed IDs)
	startNodeID := n.prefixNodeID(edge.StartNode)
	endNodeID := n.prefixNodeID(edge.EndNode)
	edgeID := n.prefixEdgeID(edge.ID)
	namespacedEdge := &Edge{
		ID:            edgeID,
		Type:          edge.Type,
		StartNode:     startNodeID,
		EndNode:       endNodeID,
		Properties:    edge.Properties,
		CreatedAt:     edge.CreatedAt,
		UpdatedAt:     edge.UpdatedAt,
		Confidence:    edge.Confidence,
		AutoGenerated: edge.AutoGenerated,
	}
	return n.inner.UpdateEdge(namespacedEdge)
}

func (n *NamespacedEngine) DeleteEdge(id EdgeID) error {
	return n.inner.DeleteEdge(n.prefixEdgeID(id))
}

// ============================================================================
// Query Operations - Filter to namespace
// ============================================================================

func (n *NamespacedEngine) GetNodesByLabel(label string) ([]*Node, error) {
	// Get all nodes with label, then filter to our namespace
	allNodes, err := n.inner.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}

	var filtered []*Node
	for _, node := range allNodes {
		if n.hasNodePrefix(node.ID) {
			filtered = append(filtered, n.toUserNode(node))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	// Fast path: delegate to inner engine's first-node lookup, then filter namespace.
	if node, err := n.inner.GetFirstNodeByLabel(label); err == nil && node != nil {
		if n.hasNodePrefix(node.ID) {
			return n.toUserNode(node), nil
		}
	}

	// Fallback: iterate all and return first in this namespace
	nodes, err := n.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, ErrNotFound
	}
	return nodes[0], nil
}

func (n *NamespacedEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	// Always prefix the ID (user-facing API always receives unprefixed IDs)
	namespacedID := n.prefixNodeID(nodeID)
	edges, err := n.inner.GetOutgoingEdges(namespacedID)
	if err != nil {
		return nil, err
	}

	// Filter to edges in our namespace and unprefix
	var filtered []*Edge
	for _, edge := range edges {
		if n.hasEdgePrefix(edge.ID) {
			filtered = append(filtered, n.toUserEdge(edge))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	// Always prefix the ID (user-facing API always receives unprefixed IDs)
	namespacedID := n.prefixNodeID(nodeID)
	edges, err := n.inner.GetIncomingEdges(namespacedID)
	if err != nil {
		return nil, err
	}

	// Filter to edges in our namespace and unprefix
	var filtered []*Edge
	for _, edge := range edges {
		if n.hasEdgePrefix(edge.ID) {
			filtered = append(filtered, n.toUserEdge(edge))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	// Always prefix IDs (user-facing API always receives unprefixed IDs)
	startNamespacedID := n.prefixNodeID(startID)
	endNamespacedID := n.prefixNodeID(endID)
	edges, err := n.inner.GetEdgesBetween(startNamespacedID, endNamespacedID)
	if err != nil {
		return nil, err
	}

	// Filter to edges in our namespace and unprefix
	var filtered []*Edge
	for _, edge := range edges {
		if n.hasEdgePrefix(edge.ID) {
			filtered = append(filtered, n.toUserEdge(edge))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	// Always prefix IDs (user-facing API always receives unprefixed IDs)
	startNamespacedID := n.prefixNodeID(startID)
	endNamespacedID := n.prefixNodeID(endID)
	edge := n.inner.GetEdgeBetween(startNamespacedID, endNamespacedID, edgeType)
	if edge == nil {
		return nil
	}
	if !n.hasEdgePrefix(edge.ID) {
		return nil
	}
	return n.toUserEdge(edge)
}

func (n *NamespacedEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	allEdges, err := n.inner.GetEdgesByType(edgeType)
	if err != nil {
		return nil, err
	}

	var filtered []*Edge
	for _, edge := range allEdges {
		if n.hasEdgePrefix(edge.ID) {
			filtered = append(filtered, n.toUserEdge(edge))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) AllNodes() ([]*Node, error) {
	allNodes, err := n.inner.AllNodes()
	if err != nil {
		return nil, err
	}

	var filtered []*Node
	for _, node := range allNodes {
		if n.hasNodePrefix(node.ID) {
			filtered = append(filtered, n.toUserNode(node))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) AllEdges() ([]*Edge, error) {
	allEdges, err := n.inner.AllEdges()
	if err != nil {
		return nil, err
	}

	var filtered []*Edge
	for _, edge := range allEdges {
		if n.hasEdgePrefix(edge.ID) {
			filtered = append(filtered, n.toUserEdge(edge))
		}
	}
	return filtered, nil
}

func (n *NamespacedEngine) GetAllNodes() []*Node {
	allNodes := n.inner.GetAllNodes()
	var filtered []*Node
	for _, node := range allNodes {
		if n.hasNodePrefix(node.ID) {
			filtered = append(filtered, n.toUserNode(node))
		}
	}
	return filtered
}

// ============================================================================
// Degree Operations
// ============================================================================

func (n *NamespacedEngine) GetInDegree(nodeID NodeID) int {
	return n.inner.GetInDegree(n.prefixNodeID(nodeID))
}

func (n *NamespacedEngine) GetOutDegree(nodeID NodeID) int {
	return n.inner.GetOutDegree(n.prefixNodeID(nodeID))
}

// ============================================================================
// Schema Operations
// ============================================================================

func (n *NamespacedEngine) GetSchema() *SchemaManager {
	// Schema is shared across namespaces (labels/types are global concepts)
	// But we could namespace this in the future if needed
	return n.inner.GetSchema()
}

// ============================================================================
// Bulk Operations
// ============================================================================

func (n *NamespacedEngine) BulkCreateNodes(nodes []*Node) error {
	namespacedNodes := make([]*Node, len(nodes))
	for i, node := range nodes {
		namespacedNode := *node
		namespacedNode.ID = n.prefixNodeID(node.ID)
		namespacedNodes[i] = &namespacedNode
	}
	return n.inner.BulkCreateNodes(namespacedNodes)
}

func (n *NamespacedEngine) BulkCreateEdges(edges []*Edge) error {
	namespacedEdges := make([]*Edge, len(edges))
	for i, edge := range edges {
		namespacedEdge := *edge
		namespacedEdge.ID = n.prefixEdgeID(edge.ID)
		namespacedEdge.StartNode = n.prefixNodeID(edge.StartNode)
		namespacedEdge.EndNode = n.prefixNodeID(edge.EndNode)
		namespacedEdges[i] = &namespacedEdge
	}
	return n.inner.BulkCreateEdges(namespacedEdges)
}

func (n *NamespacedEngine) BulkDeleteNodes(ids []NodeID) error {
	namespacedIDs := make([]NodeID, len(ids))
	for i, id := range ids {
		namespacedIDs[i] = n.prefixNodeID(id)
	}
	return n.inner.BulkDeleteNodes(namespacedIDs)
}

func (n *NamespacedEngine) BulkDeleteEdges(ids []EdgeID) error {
	namespacedIDs := make([]EdgeID, len(ids))
	for i, id := range ids {
		namespacedIDs[i] = n.prefixEdgeID(id)
	}
	return n.inner.BulkDeleteEdges(namespacedIDs)
}

// ============================================================================
// Batch Operations
// ============================================================================

func (n *NamespacedEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	namespacedIDs := make([]NodeID, len(ids))
	for i, id := range ids {
		namespacedIDs[i] = n.prefixNodeID(id)
	}

	result, err := n.inner.BatchGetNodes(namespacedIDs)
	if err != nil {
		return nil, err
	}

	// Unprefix all returned nodes (user-facing API)
	unprefixed := make(map[NodeID]*Node, len(result))
	for namespacedID, node := range result {
		unprefixedID := n.unprefixNodeID(namespacedID)
		out := CopyNode(node)
		out.ID = unprefixedID
		unprefixed[unprefixedID] = out
	}
	return unprefixed, nil
}

// ============================================================================
// Lifecycle
// ============================================================================

func (n *NamespacedEngine) Close() error {
	// Don't close the inner engine - it's shared across namespaces
	// The DatabaseManager will handle closing the underlying engine
	return nil
}

// ============================================================================
// Stats
// ============================================================================

func (n *NamespacedEngine) NodeCount() (int64, error) {
	// CRITICAL: If inner engine has its own NodeCount() (e.g., AsyncEngine with pending nodes),
	// we must use it to get accurate counts that include pending operations.
	// However, the inner engine's NodeCount() counts ALL nodes (all namespaces),
	// so we need to filter by namespace prefix.

	// Try streaming first (most efficient for large databases)
	if streamer, ok := n.inner.(StreamingEngine); ok {
		var count int64
		err := streamer.StreamNodes(context.Background(), func(node *Node) error {
			if n.hasNodePrefix(node.ID) {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		return count, nil
	}

	// Fallback: Count nodes by loading them (works for all engines, includes pending in AsyncEngine)
	// This ensures AsyncEngine's pending nodes are included via AllNodes()
	nodes, err := n.AllNodes()
	if err != nil {
		return 0, err
	}
	return int64(len(nodes)), nil
}

func (n *NamespacedEngine) EdgeCount() (int64, error) {
	// Optimized: Use streaming to count without loading all edges into memory
	var count int64
	if streamer, ok := n.inner.(StreamingEngine); ok {
		// Use streaming for efficient counting
		err := streamer.StreamEdges(context.Background(), func(edge *Edge) error {
			if n.hasEdgePrefix(edge.ID) {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		return count, nil
	}

	// Fallback: Count edges by loading them (less efficient but works for all engines)
	edges, err := n.AllEdges()
	if err != nil {
		return 0, err
	}
	return int64(len(edges)), nil
}

// ============================================================================
// Streaming Support (if underlying engine supports it)
// ============================================================================

// StreamNodes streams nodes in the namespace.
func (n *NamespacedEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	if streamer, ok := n.inner.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, func(node *Node) error {
			if n.hasNodePrefix(node.ID) {
				return fn(n.toUserNode(node))
			}
			return nil // Skip nodes not in our namespace
		})
	}
	// Fallback to AllNodes
	nodes, err := n.AllNodes()
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

// StreamEdges streams edges in the namespace.
func (n *NamespacedEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if streamer, ok := n.inner.(StreamingEngine); ok {
		return streamer.StreamEdges(ctx, func(edge *Edge) error {
			if n.hasEdgePrefix(edge.ID) {
				return fn(n.toUserEdge(edge))
			}
			return nil // Skip edges not in our namespace
		})
	}
	// Fallback to AllEdges
	edges, err := n.AllEdges()
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

// StreamNodeChunks streams nodes in chunks.
func (n *NamespacedEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if streamer, ok := n.inner.(StreamingEngine); ok {
		return streamer.StreamNodeChunks(ctx, chunkSize, func(nodes []*Node) error {
			var filtered []*Node
			for _, node := range nodes {
				if n.hasNodePrefix(node.ID) {
					filtered = append(filtered, n.toUserNode(node))
				}
			}
			if len(filtered) > 0 {
				return fn(filtered)
			}
			return nil
		})
	}
	// Fallback
	nodes, err := n.AllNodes()
	if err != nil {
		return err
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

// DeleteByPrefix is not supported for NamespacedEngine.
// Use the underlying engine's DeleteByPrefix with the namespace prefix instead.
func (n *NamespacedEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// NamespacedEngine doesn't support DeleteByPrefix directly.
	// The DatabaseManager should call DeleteByPrefix on the underlying engine
	// with the full namespace prefix (e.g., "tenant_a:").
	return 0, 0, fmt.Errorf("DeleteByPrefix not supported on NamespacedEngine - use underlying engine with namespace prefix")
}

// FindNodeNeedingEmbedding finds a node that needs embedding, but only from this namespace.
// It only looks for nodes with the current namespace prefix - all IDs must be prefixed.
func (n *NamespacedEngine) FindNodeNeedingEmbedding() *Node {
	// Get underlying engine's finder
	finder, ok := n.inner.(interface{ FindNodeNeedingEmbedding() *Node })
	if !ok {
		return nil
	}

	// Get underlying engine's marker for skipping nodes
	marker, hasMarker := n.inner.(interface{ MarkNodeEmbedded(NodeID) })

	// Keep trying until we find a node in our namespace or run out of nodes
	maxAttempts := 10000 // Prevent infinite loop (but allow scanning many nodes)
	skippedCount := 0
	for i := 0; i < maxAttempts; i++ {
		node := finder.FindNodeNeedingEmbedding()
		if node == nil {
			// No more nodes need embedding
			if skippedCount > 0 {
				fmt.Printf("🧹 Skipped %d nodes from other namespaces while searching\n", skippedCount)
			}
			return nil
		}

		// ALL node IDs must be prefixed - if not, it's a bug or old data
		// Check if this node belongs to our namespace
		if n.hasNodePrefix(node.ID) {
			// Verify the node actually exists before returning it
			// This prevents returning nodes that were deleted but still in the index
			_, err := n.inner.GetNode(node.ID)
			if err != nil {
				// Node doesn't exist - mark as embedded to remove from index and try next
				if hasMarker {
					marker.MarkNodeEmbedded(node.ID)
				}
				skippedCount++
				continue
			}

			// Return a user-facing copy (never mutate inner engine objects in-place).
			out := n.toUserNode(node)
			if skippedCount > 0 && skippedCount%100 == 0 {
				fmt.Printf("🧹 Skipped %d nodes from other namespaces while searching\n", skippedCount)
			}
			return out
		}

		// Node is from a different namespace (has a different prefix)
		// Don't mark it as embedded - it belongs to another namespace
		// Just skip it and try the next one
		skippedCount++
	}

	// Too many attempts - likely many nodes from other namespaces
	if skippedCount > 0 {
		fmt.Printf("⚠️  FindNodeNeedingEmbedding: gave up after %d attempts, skipped %d nodes from other namespaces\n", maxAttempts, skippedCount)
	}
	return nil
}

// RefreshPendingEmbeddingsIndex refreshes the pending embeddings index,
// but only for nodes in this namespace.
// Also cleans up stale entries from other namespaces in the underlying index.
func (n *NamespacedEngine) RefreshPendingEmbeddingsIndex() int {
	// First, call the underlying engine's RefreshPendingEmbeddingsIndex to clean up
	// stale entries from ALL namespaces (including deleted nodes, nodes with embeddings, etc.)
	// This is important because the underlying index contains entries from all namespaces
	underlyingRemoved := 0
	if underlyingMgr, ok := n.inner.(interface {
		RefreshPendingEmbeddingsIndex() int
	}); ok {
		// This will clean up stale entries from all namespaces
		underlyingRemoved = underlyingMgr.RefreshPendingEmbeddingsIndex()
	}

	// Get all nodes in this namespace
	nodes, err := n.AllNodes()
	if err != nil {
		return underlyingRemoved
	}

	added := 0
	removed := 0

	// Get underlying engine's pending index manager
	mgr, ok := n.inner.(interface {
		AddToPendingEmbeddings(NodeID)
		MarkNodeEmbedded(NodeID)
		GetNode(NodeID) (*Node, error)
	})
	if !ok {
		return underlyingRemoved
	}

	// Check each node in our namespace
	for _, node := range nodes {
		// Skip internal nodes
		skip := false
		for _, label := range node.Labels {
			if len(label) > 0 && label[0] == '_' {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		// Check if node needs embedding
		if (len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) && NodeNeedsEmbedding(node) {
			// Add to pending index (with namespace prefix)
			prefixedID := n.prefixNodeID(node.ID)
			mgr.AddToPendingEmbeddings(prefixedID)
			added++
		} else if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			// Node has embedding - remove from pending index if present
			prefixedID := n.prefixNodeID(node.ID)
			mgr.MarkNodeEmbedded(prefixedID)
			removed++
		}
	}

	totalRemoved := underlyingRemoved + removed
	if added > 0 || totalRemoved > 0 {
		fmt.Printf("📊 Pending embeddings index refreshed for namespace '%s': added %d nodes, removed %d stale entries (including %d from underlying cleanup)\n", n.namespace, added, totalRemoved, underlyingRemoved)
	}

	return totalRemoved
}

// MarkNodeEmbedded marks a node as embedded (removes from pending index).
// The node ID should be unprefixed (without namespace).
func (n *NamespacedEngine) MarkNodeEmbedded(nodeID NodeID) {
	if mgr, ok := n.inner.(interface{ MarkNodeEmbedded(NodeID) }); ok {
		// Add namespace prefix before marking
		prefixedID := n.prefixNodeID(nodeID)
		mgr.MarkNodeEmbedded(prefixedID)
	}
}

// Ensure NamespacedEngine implements Engine interface
var _ Engine = (*NamespacedEngine)(nil)

// Ensure NamespacedEngine implements StreamingEngine if inner does
var _ StreamingEngine = (*NamespacedEngine)(nil)
