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
	"io"
	"log/slog"
	"strings"
	"time"
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

	// log is the structured *slog.Logger for namespace-scoped emissions.
	// Resolved lazily: if the inner engine implements StructuredLogger
	// (currently BadgerEngine via .Logger()), the namespace's logger is
	// derived from it with namespace=<n.namespace>. Otherwise a discard
	// logger is installed at first access.
	log *slog.Logger
}

// StructuredLogger is implemented by storage engines that expose their
// structured *slog.Logger so wrappers (NamespacedEngine, WALEngine) can
// derive a child logger without constructor plumbing.
type StructuredLogger interface {
	Logger() *slog.Logger
}

// Logger returns the BadgerEngine's structured logger (D-01 accessor).
// Used by NamespacedEngine and other wrappers to derive child loggers
// without an additional ctor parameter. Returns a discard logger if no
// logger was supplied at construction.
func (b *BadgerEngine) Logger() *slog.Logger {
	if b == nil || b.log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return b.log
}

// namespaceLog returns a lazily-initialized namespace-scoped child logger.
// Single allocation per NamespacedEngine for the lifetime of the wrapper.
func (n *NamespacedEngine) namespaceLog() *slog.Logger {
	if n.log != nil {
		return n.log
	}
	var base *slog.Logger
	if sl, ok := n.inner.(StructuredLogger); ok {
		base = sl.Logger()
	}
	if base == nil {
		base = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	n.log = base.With("subsystem", "namespaced", "namespace", n.namespace)
	return n.log
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
	if n.hasNodePrefix(id) {
		return id
	}
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
	if n.hasEdgePrefix(id) {
		return id
	}
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

// toUserNode returns a node with the namespace prefix stripped from its ID.
//
// The Edge/Node Properties map is shared with whatever the inner engine
// returned (often a cache pointer). All current callers in the cypher
// executor and storage layer treat Get*/AllNodes results as read-only:
// mutations go through UpdateNode/UpdateEdge with fresh structs. If a
// future caller needs to mutate, it must call CopyNode/CopyEdge first.
//
// Profiling: NamespacedEngine.toUserEdge → CopyEdge was 30% of the warm
// shortestPath benchmark. The deep copy was duplicating the Properties
// map for every cache-hit edge — pure waste because nothing in the
// traversal path touches the map.
func (n *NamespacedEngine) toUserNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	out := *node
	out.ID = n.unprefixNodeID(out.ID)
	return &out
}

func (n *NamespacedEngine) toUserEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	out := *edge
	out.ID = n.unprefixEdgeID(out.ID)
	out.StartNode = n.unprefixNodeID(out.StartNode)
	out.EndNode = n.unprefixNodeID(out.EndNode)
	return &out
}

// ============================================================================
// Node Operations
// ============================================================================

func (n *NamespacedEngine) CreateNode(node *Node) (NodeID, error) {
	// Create a copy with namespaced ID
	namespacedID := n.prefixNodeID(node.ID)
	namespacedNode := copyNode(node)
	namespacedNode.ID = namespacedID
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
	namespacedNode := copyNode(node)
	namespacedNode.ID = namespacedID
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

// ForEachNodeIDByLabel streams node IDs for a label, filtered to the namespace.
// Stops early when visit returns false.
func (n *NamespacedEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}

	if lookup, ok := n.inner.(LabelNodeIDLookupEngine); ok {
		prefix := n.namespace + n.separator
		return lookup.ForEachNodeIDByLabel(label, func(id NodeID) bool {
			if !strings.HasPrefix(string(id), prefix) {
				return true
			}
			return visit(n.unprefixNodeID(id))
		})
	}

	nodes, err := n.GetNodesByLabel(label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !visit(node.ID) {
			return nil
		}
	}
	return nil
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

// GetAdjacentEdges fetches both directions through a single inner call when
// the inner engine supports the AdjacentEdgesEngine capability. ID
// translation and namespace filtering mirror the per-direction methods.
func (n *NamespacedEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	namespacedID := n.prefixNodeID(nodeID)
	var outgoing, incoming []*Edge
	if inner, ok := n.inner.(AdjacentEdgesEngine); ok {
		out, in, err := inner.GetAdjacentEdges(namespacedID)
		if err != nil {
			return nil, nil, err
		}
		outgoing, incoming = out, in
	} else {
		out, err := n.inner.GetOutgoingEdges(namespacedID)
		if err != nil {
			return nil, nil, err
		}
		in, err := n.inner.GetIncomingEdges(namespacedID)
		if err != nil {
			return nil, nil, err
		}
		outgoing, incoming = out, in
	}

	var filteredOut, filteredIn []*Edge
	for _, edge := range outgoing {
		if n.hasEdgePrefix(edge.ID) {
			filteredOut = append(filteredOut, n.toUserEdge(edge))
		}
	}
	for _, edge := range incoming {
		if n.hasEdgePrefix(edge.ID) {
			filteredIn = append(filteredIn, n.toUserEdge(edge))
		}
	}
	return filteredOut, filteredIn, nil
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

// GetNodesByLabelVisibleAt resolves snapshot-visible label queries within the namespace.
func (n *NamespacedEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	provider, ok := n.inner.(MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, ErrNotImplemented
	}
	allNodes, err := provider.GetNodesByLabelVisibleAt(label, version)
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

// GetEdgesByTypeVisibleAt resolves snapshot-visible edge-type queries within the namespace.
func (n *NamespacedEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	provider, ok := n.inner.(MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, ErrNotImplemented
	}
	allEdges, err := provider.GetEdgesByTypeVisibleAt(edgeType, version)
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

// GetEdgesBetweenVisibleAt resolves snapshot-visible topology queries within the namespace.
func (n *NamespacedEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	provider, ok := n.inner.(MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, ErrNotImplemented
	}
	edges, err := provider.GetEdgesBetweenVisibleAt(n.prefixNodeID(startID), n.prefixNodeID(endID), version)
	if err != nil {
		return nil, err
	}
	filtered := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
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
	// Prefer per-namespace schema when supported by the underlying engine chain.
	// This matches Neo4j behavior (each database has its own schema).
	if p, ok := n.inner.(NamespaceSchemaProvider); ok {
		return p.GetSchemaForNamespace(n.namespace)
	}
	return n.inner.GetSchema()
}

// GetTemporalNodeAsOf performs an efficient temporal lookup when the inner engine supports it.
func (n *NamespacedEngine) GetTemporalNodeAsOf(label, keyProp string, keyValue interface{}, validFromProp, validToProp string, asOf time.Time) (*Node, error) {
	provider, ok := n.inner.(NamespaceTemporalLookupProvider)
	if !ok {
		return nil, nil
	}
	node, err := provider.GetTemporalNodeAsOfInNamespace(n.namespace, label, keyProp, keyValue, validFromProp, validToProp, asOf)
	if err != nil || node == nil {
		return node, err
	}
	return n.toUserNode(node), nil
}

// IsCurrentTemporalNode reports whether node is the current/live temporal version.
func (n *NamespacedEngine) IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error) {
	provider, ok := n.inner.(NamespaceTemporalCurrentNodeProvider)
	if !ok {
		return true, nil
	}
	return provider.IsCurrentTemporalNodeInNamespace(n.namespace, node, asOf)
}

// GetNodeLatestVisible resolves the latest visible node within the namespace.
func (n *NamespacedEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	provider, ok := n.inner.(MVCCVisibilityEngine)
	if !ok {
		return n.GetNode(id)
	}
	node, err := provider.GetNodeLatestVisible(n.prefixNodeID(id))
	if err != nil || node == nil {
		return node, err
	}
	return n.toUserNode(node), nil
}

// GetNodeVisibleAt resolves a snapshot-visible node within the namespace.
func (n *NamespacedEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	provider, ok := n.inner.(MVCCVisibilityEngine)
	if !ok {
		return nil, ErrNotImplemented
	}
	node, err := provider.GetNodeVisibleAt(n.prefixNodeID(id), version)
	if err != nil || node == nil {
		return node, err
	}
	return n.toUserNode(node), nil
}

// GetEdgeLatestVisible resolves the latest visible edge within the namespace.
func (n *NamespacedEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	provider, ok := n.inner.(MVCCVisibilityEngine)
	if !ok {
		return n.GetEdge(id)
	}
	edge, err := provider.GetEdgeLatestVisible(n.prefixEdgeID(id))
	if err != nil || edge == nil {
		return edge, err
	}
	return n.toUserEdge(edge), nil
}

// GetEdgeVisibleAt resolves a snapshot-visible edge within the namespace.
func (n *NamespacedEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	provider, ok := n.inner.(MVCCVisibilityEngine)
	if !ok {
		return nil, ErrNotImplemented
	}
	edge, err := provider.GetEdgeVisibleAt(n.prefixEdgeID(id), version)
	if err != nil || edge == nil {
		return edge, err
	}
	return n.toUserEdge(edge), nil
}

// GetNodeCurrentHead resolves node head metadata within the namespace.
func (n *NamespacedEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	provider, ok := n.inner.(MVCCHeadEngine)
	if !ok {
		return MVCCHead{}, ErrNotImplemented
	}
	return provider.GetNodeCurrentHead(n.prefixNodeID(id))
}

// GetEdgeCurrentHead resolves edge head metadata within the namespace.
func (n *NamespacedEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	provider, ok := n.inner.(MVCCHeadEngine)
	if !ok {
		return MVCCHead{}, ErrNotImplemented
	}
	return provider.GetEdgeCurrentHead(n.prefixEdgeID(id))
}

// RegisterSnapshotReader registers a reader scoped to the namespace when supported.
func (n *NamespacedEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	provider, ok := n.inner.(MVCCLifecycleEngine)
	if !ok {
		return func() {}
	}
	copyInfo := info
	if copyInfo.Namespace == "" {
		copyInfo.Namespace = n.namespace
	}
	return provider.RegisterSnapshotReader(copyInfo)
}

// LifecycleStatus delegates lifecycle status when supported.
func (n *NamespacedEngine) LifecycleStatus() map[string]interface{} {
	provider, ok := n.inner.(MVCCLifecycleEngine)
	if !ok {
		return map[string]interface{}{"enabled": false}
	}
	status := provider.LifecycleStatus()
	status["namespace"] = n.namespace
	return status
}

// TriggerPruneNow delegates lifecycle prune-now when supported.
func (n *NamespacedEngine) TriggerPruneNow(ctx context.Context) error {
	provider, ok := n.inner.(MVCCLifecycleEngine)
	if !ok {
		return nil
	}
	return provider.TriggerPruneNow(ctx)
}

// PauseLifecycle delegates lifecycle pause when supported.
func (n *NamespacedEngine) PauseLifecycle() {
	provider, ok := n.inner.(MVCCLifecycleEngine)
	if ok {
		provider.PauseLifecycle()
	}
}

// ResumeLifecycle delegates lifecycle resume when supported.
func (n *NamespacedEngine) ResumeLifecycle() {
	provider, ok := n.inner.(MVCCLifecycleEngine)
	if ok {
		provider.ResumeLifecycle()
	}
}

// SetLifecycleSchedule delegates lifecycle cadence updates when supported.
func (n *NamespacedEngine) SetLifecycleSchedule(interval time.Duration) error {
	provider, ok := n.inner.(MVCCLifecycleScheduleEngine)
	if !ok {
		return nil
	}
	return provider.SetLifecycleSchedule(interval)
}

// TopLifecycleDebtKeys delegates lifecycle debt inspection when supported.
func (n *NamespacedEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	provider, ok := n.inner.(MVCCLifecycleDebtEngine)
	if !ok {
		return nil
	}
	return provider.TopLifecycleDebtKeys(limit)
}

// ============================================================================
// Bulk Operations
// ============================================================================

func (n *NamespacedEngine) BulkCreateNodes(nodes []*Node) error {
	namespacedNodes := make([]*Node, len(nodes))
	for i, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		namespacedNode := *node
		namespacedNode.ID = n.prefixNodeID(node.ID)
		namespacedNodes[i] = &namespacedNode
	}
	return n.inner.BulkCreateNodes(namespacedNodes)
}

func (n *NamespacedEngine) BulkCreateEdges(edges []*Edge) error {
	namespacedEdges := make([]*Edge, len(edges))
	for i, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
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
	// Prefer a prefix-scoped count from the inner engine to keep COUNT fast
	// in multi-database deployments.
	if stats, ok := n.inner.(PrefixStatsEngine); ok {
		return stats.NodeCountByPrefix(n.namespace + n.separator)
	}

	// Fallback: streaming scan (correct but slower; should be rare in production).
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

	// Last resort: load and filter.
	nodes, err := n.AllNodes()
	if err != nil {
		return 0, err
	}
	return int64(len(nodes)), nil
}

func (n *NamespacedEngine) EdgeCount() (int64, error) {
	// Prefer a prefix-scoped count from the inner engine.
	if stats, ok := n.inner.(PrefixStatsEngine); ok {
		return stats.EdgeCountByPrefix(n.namespace + n.separator)
	}

	// Fallback: streaming scan (correct but slower; should be rare in production).
	var count int64
	if streamer, ok := n.inner.(StreamingEngine); ok {
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

	// Last resort: load and filter.
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
	if prefixStreamer, ok := n.inner.(PrefixStreamingEngine); ok {
		return prefixStreamer.StreamNodesByPrefix(ctx, n.namespace+n.separator, func(node *Node) error {
			return fn(n.toUserNode(node))
		})
	}

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
	// CRITICAL: If the inner engine is AsyncEngine (or wrapped with WALEngine), we need to
	// check its cache first because nodes in the cache might not be in the underlying
	// engine's pending index yet. Use the AsyncEngine's FindNodeNeedingEmbedding which
	// already checks the cache, but we need to filter by namespace.
	innerEngine := n.inner
	// Unwrap WALEngine if present
	if walEngine, ok := innerEngine.(*WALEngine); ok {
		innerEngine = walEngine.GetEngine()
	}

	// If inner engine is AsyncEngine, it will check its cache first via its FindNodeNeedingEmbedding
	// But we still need to filter by namespace, so we'll do that in the loop below

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
			// No more nodes need embedding in the underlying engine.
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
				n.namespaceLog().Debug("skipped nodes from other namespaces while searching",
					"skipped", skippedCount,
				)
			}
			return out
		}

		// Node is from a different namespace (has a different prefix)
		// Don't mark it as embedded - it belongs to another namespace
		// Just skip it and try the next one
		skippedCount++
	}

	// If we got here, we hit maxAttempts (usually because the underlying pending index
	// is dominated by other namespaces). Fall back to scanning only this namespace.

	// Fallback: scan all nodes in this namespace
	nodes, err := n.AllNodes()
	if err != nil {
		return nil
	}

	// Find first node that needs embedding
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
			// Found one! Add it to the pending index if not already there
			if mgr, ok := n.inner.(interface{ AddToPendingEmbeddings(NodeID) }); ok {
				prefixedID := n.prefixNodeID(node.ID)
				mgr.AddToPendingEmbeddings(prefixedID)
			}
			return node
		}
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
	nsLog := n.namespaceLog()
	if added > 0 || totalRemoved > 0 {
		nsLog.Info("pending embeddings index refreshed",
			"subsystem", "embeddings_index",
			"added", added,
			"removed_stale", totalRemoved,
			"underlying_removed", underlyingRemoved,
		)
	} else {
		// Log even if no changes, to help debug why nodes aren't being found.
		nsLog.Debug("pending embeddings index refreshed: no changes",
			"subsystem", "embeddings_index",
			"scanned", len(nodes),
		)
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

// AddToPendingEmbeddings adds a node back to the pending embeddings index (e.g. after a failed embed so it can be retried).
func (n *NamespacedEngine) AddToPendingEmbeddings(nodeID NodeID) {
	if mgr, ok := n.inner.(interface{ AddToPendingEmbeddings(NodeID) }); ok {
		prefixedID := n.prefixNodeID(nodeID)
		mgr.AddToPendingEmbeddings(prefixedID)
	}
}

// RecordMaterializedAccess records a result-materialization access against the
// fully qualified entity ID in the underlying engine.
func (n *NamespacedEngine) RecordMaterializedAccess(entityID string) {
	if recorder, ok := n.inner.(interface{ RecordMaterializedAccess(string) }); ok {
		prefixed := entityID
		if !strings.Contains(entityID, n.separator) {
			prefixed = n.namespace + n.separator + entityID
		}
		recorder.RecordMaterializedAccess(prefixed)
	}
}

// LastWriteTime returns the last known write time from the underlying engine, if available.
func (n *NamespacedEngine) LastWriteTime() time.Time {
	// Intentionally return zero here because a global LastWriteTime from the
	// underlying engine is not namespace-scoped and can cause false rebuilds
	// across databases.
	return time.Time{}
}

// Ensure NamespacedEngine implements Engine interface
var _ Engine = (*NamespacedEngine)(nil)

// Ensure NamespacedEngine implements StreamingEngine if inner does
var _ StreamingEngine = (*NamespacedEngine)(nil)
