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
	// scope is the namespace's ID prefix, namespace + ":", fixed at
	// construction so prefix checks and count reads need not build it.
	scope string

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
		scope:     namespace + ":",
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

// PrefixNodeID adds namespace prefix to a node ID.
// "123" → "tenant_a:123"
func (n *NamespacedEngine) PrefixNodeID(id NodeID) NodeID {
	return NodeID(qualifyEntityID(n.scope, string(id)))
}

// PrefixEdgeID adds namespace prefix to an edge ID.
func (n *NamespacedEngine) PrefixEdgeID(id EdgeID) EdgeID {
	return EdgeID(qualifyEntityID(n.scope, string(id)))
}

// nodeScope is this namespace's node ID prefix, the scope of its label reads
// (ScopedLabelNodeReader).
func (n *NamespacedEngine) nodeScope() string {
	return n.scope
}

// hasNodePrefix checks if an ID belongs to this namespace.
func (n *NamespacedEngine) hasNodePrefix(id NodeID) bool {
	return strings.HasPrefix(string(id), n.scope)
}

func (n *NamespacedEngine) hasEdgePrefix(id EdgeID) bool {
	return strings.HasPrefix(string(id), n.scope)
}

// nodeUserView binds an owned body or an authoritative batch result key.
// Its zero value preserves the ordinary shallow borrowed-body view.
type nodeUserView struct {
	owned, keyed bool
	id           NodeID
}

// toUserNode returns a node with the namespace prefix stripped from its ID.
//
// Every inner engine read is already caller-owned (NOR-160), so an owned view
// rewrites the identity in place and any other view takes a shallow struct
// copy; the Properties map is never copied again here. (Profiling: a deep
// copy in toUserEdge was 30% of the warm shortestPath benchmark.)

func (n *NamespacedEngine) toUserNode(node *Node, view nodeUserView) *Node {
	if node == nil {
		return nil
	}
	out := node
	if !view.owned {
		copy := *node
		out = &copy
	}
	if view.keyed {
		out.ID = view.id
	}
	out.ID = NodeID(strings.TrimPrefix(string(out.ID), n.scope))
	{
		metadata := out.RuntimeMetadata()
		metadata.Database = n.namespace
		out.SetRuntimeMetadata(metadata)
	}
	return out
}

func (n *NamespacedEngine) toUserEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	out := *edge
	out.ID = EdgeID(strings.TrimPrefix(string(out.ID), n.scope))
	out.Database = n.namespace
	out.StartNode = NodeID(strings.TrimPrefix(string(out.StartNode), n.scope))
	out.EndNode = NodeID(strings.TrimPrefix(string(out.EndNode), n.scope))
	return &out
}

// ============================================================================
// Node Operations
// ============================================================================

func (n *NamespacedEngine) CreateNode(node *Node) (NodeID, error) {
	namespacedNode, err := n.NodeWriteInput(node)
	if err != nil {
		return "", err
	}
	actualID, err := n.inner.CreateNode(namespacedNode)
	if err != nil {
		return "", err
	}
	// Return unprefixed ID to user (user-facing API)
	// But internally, we track the prefixed ID in nodeToConstituent
	return NodeID(strings.TrimPrefix(string(actualID), n.scope)), nil
}

func (n *NamespacedEngine) GetNode(id NodeID) (*Node, error) {
	return namespacedPointRead[Engine, NodeID, *Node](n, id, Engine.GetNode, n.PrefixNodeID, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, nil, false, MVCCVersion{})
}

// RelationshipEndpointVisible forwards with the namespace prefix applied.
func (n *NamespacedEngine) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	return relationshipEndpointVisibility(n.inner, n.PrefixNodeID(id))
}

func (n *NamespacedEngine) GetNodeProjected(id NodeID, properties []string) (*Node, error) {
	projected, ok := n.inner.(ProjectedNodeReader)
	if !ok || properties == nil {
		node, err := n.GetNode(id)
		if err != nil || node == nil || properties == nil {
			return node, err
		}
		// GetNode returned a caller-owned node: select in place.
		return shapeNode(node, properties, false, true), nil
	}

	return materializedRead(func() (*Node, error) { return projected.GetNodeProjected(n.PrefixNodeID(id), properties) }, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, true)
}

// GetQueryNode forwards the query-row node read (QueryNodeReader) with the
// namespace prefix applied, else reads the full node.
func (n *NamespacedEngine) GetQueryNode(id NodeID) (*Node, error) {
	return namespacedPointRead[QueryNodeReader, NodeID, *Node](n, id, QueryNodeReader.GetQueryNode, n.PrefixNodeID, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, n.GetNode, false, MVCCVersion{})
}

func (n *NamespacedEngine) GetNodeWithoutEmbeddings(id NodeID) (*Node, error) {
	reader, ok := n.inner.(NodeWithoutEmbeddingsReader)
	if !ok {
		node, err := n.GetNode(id)
		if err != nil || node == nil {
			return node, err
		}
		node.ChunkEmbeddings = nil
		node.NamedEmbeddings = nil
		return node, nil
	}
	return materializedRead(func() (*Node, error) { return reader.GetNodeWithoutEmbeddings(n.PrefixNodeID(id)) }, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, true)
}

func (n *NamespacedEngine) BatchGetNodesWithoutEmbeddings(ids []NodeID) (map[NodeID]*Node, error) {
	if len(ids) == 0 {
		return make(map[NodeID]*Node), nil
	}
	return n.readNodeBatch(ids, true)
}

func (n *NamespacedEngine) BatchGetNodesWithoutEmbeddingsSupported() bool {
	return batchNodeWithoutEmbeddingsSupported(n.inner)
}

// StreamNodesByLabelProjected iterates projected label matches in this namespace.
func (n *NamespacedEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return n.streamNamespaceNodesProjected(label, properties, visit, false)
}

func (n *NamespacedEngine) UpdateNode(node *Node) error {
	input, err := n.NodeWriteInput(node)
	if err != nil {
		return err
	}
	return n.inner.UpdateNode(input)
}

func (n *NamespacedEngine) DeleteNode(id NodeID) error {
	prefixedID := n.PrefixNodeID(id)

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
	input, err := n.EdgeWriteInput(edge)
	if err != nil {
		return err
	}
	return n.inner.CreateEdge(input)
}

func (n *NamespacedEngine) GetEdge(id EdgeID) (*Edge, error) {
	return namespacedPointRead[Engine, EdgeID, *Edge](n, id, Engine.GetEdge, n.PrefixEdgeID, n.toUserEdge, nil, false, MVCCVersion{})
}

func (n *NamespacedEngine) UpdateEdge(edge *Edge) error {
	input, err := n.EdgeWriteInput(edge)
	if err != nil {
		return err
	}
	return n.inner.UpdateEdge(input)
}

func (n *NamespacedEngine) DeleteEdge(id EdgeID) error {
	return n.inner.DeleteEdge(n.PrefixEdgeID(id))
}

// ============================================================================
// Query Operations - Filter to namespace
// ============================================================================

func (n *NamespacedEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return materializedRead(func() ([]*Node, error) { return getNodesByLabelInScope(n.inner, n.nodeScope(), label) }, func(nodes []*Node) []*Node { return n.toUserNodesInNamespace(nodes, true) }, false)
}

func (n *NamespacedEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	// Fast path: delegate to inner engine's first-node lookup, then filter namespace.
	if node, err := getFirstNodeByLabelInScope(n.inner, n.nodeScope(), label); err == nil && node != nil {
		if n.hasNodePrefix(node.ID) {
			return n.toUserNode(node, nodeUserView{}), nil
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

	return forEachNodeIDByLabelInScope(n.inner, n.nodeScope(), label, func(id NodeID) bool {
		if !n.hasNodePrefix(id) {
			return true
		}
		return visit(NodeID(strings.TrimPrefix(string(id), n.scope)))
	})
}

func (n *NamespacedEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	out, _, err := readAdjacency(n, adjacencyOutgoing, nodeID, "", nil, visibleRead)
	return out, err
}

// OutgoingEdgeHeaders forwards with the namespace applied, as
// GetOutgoingEdges does.
func (n *NamespacedEngine) OutgoingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return n.edgeHeaders(nodeID, true)
}

// IncomingEdgeHeaders is the incoming-side OutgoingEdgeHeaders.
func (n *NamespacedEngine) IncomingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return n.edgeHeaders(nodeID, false)
}

func (n *NamespacedEngine) edgeHeaders(nodeID NodeID, outgoing bool) ([]*Edge, bool, error) {
	reader, ok := n.inner.(EdgeHeaderReader)
	if !ok {
		return nil, false, nil
	}
	var edges []*Edge
	var answered bool
	var err error
	if outgoing {
		edges, answered, err = reader.OutgoingEdgeHeaders(n.PrefixNodeID(nodeID))
	} else {
		edges, answered, err = reader.IncomingEdgeHeaders(n.PrefixNodeID(nodeID))
	}
	if !answered || err != nil {
		return nil, answered, err
	}
	return n.toUserEdgesInNamespace(edges, false, true), true, nil
}

func (n *NamespacedEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	_, in, err := readAdjacency(n, adjacencyIncoming, nodeID, "", nil, visibleRead)
	return in, err
}

// GetAdjacentEdges fetches both directions through a single inner call when
// the inner engine supports the AdjacentEdgesEngine capability. ID
// translation and namespace filtering mirror the per-direction methods.
func (n *NamespacedEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	return readAdjacency(n, adjacencyBoth, nodeID, "", nil, visibleRead)
}

func (n *NamespacedEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	out, _, err := readAdjacency(n, adjacencyBetween, startID, endID, nil, visibleRead)
	return out, err
}

func (n *NamespacedEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	// Always prefix IDs (user-facing API always receives unprefixed IDs)
	startNamespacedID := n.PrefixNodeID(startID)
	endNamespacedID := n.PrefixNodeID(endID)
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
	return materializedRead(func() ([]*Edge, error) { return n.inner.GetEdgesByType(edgeType) }, func(edges []*Edge) []*Edge { return n.toUserEdgesInNamespace(edges, false, false) }, false)
}

// GetNodesByLabelVisibleAt resolves snapshot-visible label queries within the namespace.
func (n *NamespacedEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	return materializedRead(func() ([]*Node, error) {
		return getNodesByLabelVisibleAtInScope(n.inner, n.nodeScope(), label, version)
	}, func(nodes []*Node) []*Node { return n.toUserNodesInNamespace(nodes, false) }, false)
}

// GetOutgoingEdgesVisibleAt resolves snapshot-visible outgoing adjacency queries within the namespace.
func (n *NamespacedEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	out, _, err := readAdjacency(n, adjacencyOutgoing, nodeID, "", &version, visibleRead)
	return out, err
}

// GetIncomingEdgesVisibleAt resolves snapshot-visible incoming adjacency queries within the namespace.
func (n *NamespacedEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	_, in, err := readAdjacency(n, adjacencyIncoming, nodeID, "", &version, visibleRead)
	return in, err
}

// GetEdgesByTypeVisibleAt resolves snapshot-visible edge-type queries within the namespace.
func (n *NamespacedEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	provider, ok := n.inner.(MVCCIndexedVisibilityEngine)
	if !ok {
		return nil, ErrNotImplemented
	}
	return materializedRead(func() ([]*Edge, error) { return provider.GetEdgesByTypeVisibleAt(edgeType, version) }, func(edges []*Edge) []*Edge { return n.toUserEdgesInNamespace(edges, false, false) }, false)
}

// GetEdgesBetweenVisibleAt resolves snapshot-visible topology queries within the namespace.
func (n *NamespacedEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	out, _, err := readAdjacency(n, adjacencyBetween, startID, endID, &version, visibleRead)
	return out, err
}

func (n *NamespacedEngine) AllNodes() ([]*Node, error) {
	return materializedRead(func() ([]*Node, error) { return n.inner.AllNodes() }, func(values []*Node) []*Node { return n.toUserNodesInNamespace(values, false) }, false)
}

func (n *NamespacedEngine) AllEdges() ([]*Edge, error) {
	return materializedRead(func() ([]*Edge, error) { return n.inner.AllEdges() }, func(values []*Edge) []*Edge { return n.toUserEdgesInNamespace(values, false, false) }, false)
}

func (n *NamespacedEngine) GetAllNodes() []*Node {
	return n.toUserNodesInNamespace(n.inner.GetAllNodes(), false)
}

// ============================================================================
// Degree Operations
// ============================================================================

func (n *NamespacedEngine) GetInDegree(nodeID NodeID) int {
	return n.inner.GetInDegree(n.PrefixNodeID(nodeID))
}

func (n *NamespacedEngine) GetOutDegree(nodeID NodeID) int {
	return n.inner.GetOutDegree(n.PrefixNodeID(nodeID))
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
	return n.toUserNode(node, nodeUserView{}), nil
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
	return namespacedPointRead[MVCCVisibilityEngine, NodeID, *Node](n, id, MVCCVisibilityEngine.GetNodeLatestVisible, n.PrefixNodeID, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, n.GetNode, true, MVCCVersion{})
}

// GetNodeVisibleAt resolves a snapshot-visible node within the namespace.
func (n *NamespacedEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	return namespacedPointRead[MVCCVisibilityEngine, NodeID, *Node](n, id, MVCCVisibilityEngine.GetNodeVisibleAt, n.PrefixNodeID, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, nil, true, version)
}

// GetEdgeLatestVisible resolves the latest visible edge within the namespace.
func (n *NamespacedEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	return namespacedPointRead[MVCCVisibilityEngine, EdgeID, *Edge](n, id, MVCCVisibilityEngine.GetEdgeLatestVisible, n.PrefixEdgeID, n.toUserEdge, n.GetEdge, true, MVCCVersion{})
}

// GetEdgeVisibleAt resolves a snapshot-visible edge within the namespace.
func (n *NamespacedEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	return namespacedPointRead[MVCCVisibilityEngine, EdgeID, *Edge](n, id, MVCCVisibilityEngine.GetEdgeVisibleAt, n.PrefixEdgeID, n.toUserEdge, nil, true, version)
}

// GetNodeLatestEffective resolves the latest-effective node within the
// namespace, delegating to the wrapped engine when it supports the
// MVCCLatestEffectiveEngine capability.
func (n *NamespacedEngine) GetNodeLatestEffective(id NodeID) (*Node, error) {
	return namespacedPointRead[MVCCLatestEffectiveEngine, NodeID, *Node](n, id, MVCCLatestEffectiveEngine.GetNodeLatestEffective, n.PrefixNodeID, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, n.GetNode, true, MVCCVersion{})
}

// GetEdgeLatestEffective resolves the latest-effective edge within the
// namespace, delegating to the wrapped engine when it supports the
// MVCCLatestEffectiveEngine capability.
func (n *NamespacedEngine) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	return namespacedPointRead[MVCCLatestEffectiveEngine, EdgeID, *Edge](n, id, MVCCLatestEffectiveEngine.GetEdgeLatestEffective, n.PrefixEdgeID, n.toUserEdge, n.GetEdge, true, MVCCVersion{})
}

// GetNodeCurrentHead resolves node head metadata within the namespace.
func (n *NamespacedEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	return namespacedPointRead[MVCCHeadEngine, NodeID, MVCCHead](n, id, MVCCHeadEngine.GetNodeCurrentHead, n.PrefixNodeID, nil, nil, true, MVCCVersion{})
}

// GetEdgeCurrentHead resolves edge head metadata within the namespace.
func (n *NamespacedEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	return namespacedPointRead[MVCCHeadEngine, EdgeID, MVCCHead](n, id, MVCCHeadEngine.GetEdgeCurrentHead, n.PrefixEdgeID, nil, nil, true, MVCCVersion{})
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
	var labels []string
	for i, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		namespacedNode := *node
		namespacedNode.ID = n.PrefixNodeID(node.ID)
		namespacedNodes[i] = &namespacedNode
		labels = append(labels, node.Labels...)
	}
	if err := n.GetSchema().RegisterTokens(labels, nil); err != nil {
		return err
	}
	return n.inner.BulkCreateNodes(namespacedNodes)
}

func (n *NamespacedEngine) BulkCreateEdges(edges []*Edge) error {
	if err := n.GetSchema().RegisterEdgeTokens(edges); err != nil {
		return err
	}
	namespacedEdges := make([]*Edge, len(edges))
	for i, edge := range edges {
		namespacedEdge := *edge
		namespacedEdge.ID = n.PrefixEdgeID(edge.ID)
		namespacedEdge.StartNode = n.PrefixNodeID(edge.StartNode)
		namespacedEdge.EndNode = n.PrefixNodeID(edge.EndNode)
		namespacedEdges[i] = &namespacedEdge
	}
	return n.inner.BulkCreateEdges(namespacedEdges)
}

func (n *NamespacedEngine) BulkDeleteNodes(ids []NodeID) error {
	return n.inner.BulkDeleteNodes(materializedIDs(ids, func(id NodeID) NodeID { return n.PrefixNodeID(id) }))
}

func (n *NamespacedEngine) BulkDeleteEdges(ids []EdgeID) error {
	return n.inner.BulkDeleteEdges(materializedIDs(ids, func(id EdgeID) EdgeID { return n.PrefixEdgeID(id) }))
}

// ============================================================================
// Batch Operations
// ============================================================================

func (n *NamespacedEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return n.readNodeBatch(ids, false)
}

// toUserNodeBatch binds stored result keys to the one node view owner;
// materializedKeyed owns the batch materialization walk. Inner batch reads
// are caller-owned, so identity is rewritten in place, never copied again.
func (n *NamespacedEngine) toUserNodeBatch(rows map[NodeID]*Node, withoutEmbeddings bool) map[NodeID]*Node {
	return materializedKeyed(rows, func(key NodeID, node *Node) (NodeID, *Node) {
		if withoutEmbeddings && node == nil {
			return "", nil
		}
		out := n.toUserNode(node, nodeUserView{owned: true, keyed: true, id: key})
		return out.ID, out
	})
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
		return stats.NodeCountByPrefix(n.scope)
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

func (n *NamespacedEngine) NodeCountByLabel(label string) (int64, error) {
	if stats, ok := n.inner.(NamespaceLabelStatsProvider); ok {
		return stats.NodeCountByLabelInNamespace(n.namespace, label)
	}

	var count int64
	if streamer, ok := n.inner.(StreamingEngine); ok {
		err := streamer.StreamNodes(context.Background(), func(node *Node) error {
			if !n.hasNodePrefix(node.ID) {
				return nil
			}
			for _, existingLabel := range node.Labels {
				if existingLabel == label {
					count++
					break
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		return count, nil
	}

	nodes, err := n.AllNodes()
	if err != nil {
		return 0, err
	}
	for _, node := range nodes {
		for _, existingLabel := range node.Labels {
			if existingLabel == label {
				count++
				break
			}
		}
	}
	return count, nil
}

func (n *NamespacedEngine) EdgeCount() (int64, error) {
	// Prefer a prefix-scoped count from the inner engine.
	if stats, ok := n.inner.(PrefixStatsEngine); ok {
		return stats.EdgeCountByPrefix(n.scope)
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

// EdgeCountByType returns the namespace-scoped count of edges of the given
// type. It is answered by the inner engine's persisted per-type counters
// (issue #638), preserving the database namespace through decorators.
func (n *NamespacedEngine) EdgeCountByType(edgeType string) (int64, error) {
	return n.EdgeCountByTypeInNamespace(n.namespace, edgeType)
}

// EdgeCountByStartLabel returns the namespace-scoped count of edges of the
// given type whose physical start endpoint carries the label (issue #638).
func (n *NamespacedEngine) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return n.EdgeCountByStartLabelInNamespace(n.namespace, label, edgeType)
}

// EdgeCountByEndLabel returns the namespace-scoped count of edges of the
// given type whose physical end endpoint carries the label (issue #638).
func (n *NamespacedEngine) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return n.EdgeCountByEndLabelInNamespace(n.namespace, label, edgeType)
}

// ============================================================================
// Streaming Support (if underlying engine supports it)
// ============================================================================

// StreamNodes streams nodes in the namespace.
// StreamNodesWithOptions streams this namespace's nodes with the options-driven
// kernel. The caller-supplied prefix is namespace-relative; the physical scan
// carries the namespace prefix so results stay inside this view. The kernel is
// part of the Engine contract, so the delegation is direct.
func (n *NamespacedEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(node *Node) error) error {
	if fn == nil {
		return ErrInvalidData
	}
	physicalOpts := opts
	physicalOpts.Prefix = n.scope + opts.Prefix
	return n.inner.StreamNodesWithOptions(ctx, physicalOpts, func(node *Node) error {
		return fn(n.toUserNode(node, nodeUserView{}))
	})
}

func (n *NamespacedEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	return n.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}, fn)
}

// StreamNodesWithoutEmbeddings streams complete lightweight nodes in this
// namespace while preserving user-visible IDs.
func (n *NamespacedEngine) StreamNodesWithoutEmbeddings(ctx context.Context, fn func(node *Node) error) error {
	return n.StreamNodesWithOptions(ctx, StreamNodesOptions{}, fn)
}

// StreamNodesByPrefix streams nodes in the namespace whose user-visible IDs
// start with prefix.
func (n *NamespacedEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	return n.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, WithEmbeddings: true, ApplyDecayFilter: true}, fn)
}

// StreamNodesByPrefixProjected streams nodes in the namespace whose
// user-visible IDs start with prefix while decoding only requested properties
// when the wrapped engine supports projected prefix scans.
func (n *NamespacedEngine) StreamNodesByPrefixProjected(ctx context.Context, prefix string, properties []string, fn func(node *Node) error) error {
	if properties == nil {
		return n.StreamNodesByPrefix(ctx, prefix, fn)
	}
	return n.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, Projection: properties}, fn)
}

// StreamNodesByPrefixWithoutEmbeddings streams lightweight nodes in this
// namespace and returns user-visible IDs.
func (n *NamespacedEngine) StreamNodesByPrefixWithoutEmbeddings(ctx context.Context, prefix string, fn func(node *Node) error) error {
	return n.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, Projection: []string{}, StripEmbeddings: true}, fn)
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
			filtered := n.toUserNodesInNamespace(nodes, false)
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
		// Cap each chunk so appending to one cannot overwrite the next.
		if err := fn(nodes[i:end:end]); err != nil {
			return err
		}
	}
	return nil
}

// DeleteByPrefix is not supported for NamespacedEngine.
// Use the underlying engine's DeleteByPrefix with the namespace prefix instead.
func (n *NamespacedEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// Scope the deletion to this namespace by translating the user prefix into
	// the stored namespace-prefixed form, then delegate.
	return n.inner.DeleteByPrefix(n.scope + prefix)
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
		innerEngine = walEngine.GetInnerEngine()
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
			out := n.toUserNode(node, nodeUserView{})
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
				prefixedID := n.PrefixNodeID(node.ID)
				mgr.AddToPendingEmbeddings(prefixedID)
			}
			return node
		}
	}

	return nil
}

// RefreshPendingEmbeddingsIndex refreshes the pending embeddings index across
// the underlying engine and includes staged nodes in this namespace.
func (n *NamespacedEngine) RefreshPendingEmbeddingsIndex() int {
	underlyingAdded := 0
	if underlyingMgr, ok := n.inner.(interface {
		RefreshPendingEmbeddingsIndex() int
	}); ok {
		underlyingAdded = underlyingMgr.RefreshPendingEmbeddingsIndex()
		inner := n.inner
		if wal, wrapped := inner.(*WALEngine); wrapped {
			inner = wal.engine
		}
		if _, complete := inner.(*BadgerEngine); complete {
			return underlyingAdded
		}
	}

	// Get all nodes in this namespace
	nodes, err := n.AllNodes()
	if err != nil {
		return underlyingAdded
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
		return underlyingAdded
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
			prefixedID := n.PrefixNodeID(node.ID)
			mgr.AddToPendingEmbeddings(prefixedID)
			added++
		} else if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			// Node has embedding - remove from pending index if present
			prefixedID := n.PrefixNodeID(node.ID)
			mgr.MarkNodeEmbedded(prefixedID)
			removed++
		}
	}

	nsLog := n.namespaceLog()
	if added > 0 || removed > 0 || underlyingAdded > 0 {
		nsLog.Info("pending embeddings index refreshed",
			"subsystem", "embeddings_index",
			"added", added,
			"marked_embedded", removed,
			"underlying_added", underlyingAdded,
		)
	} else {
		// Log even if no changes, to help debug why nodes aren't being found.
		nsLog.Debug("pending embeddings index refreshed: no changes",
			"subsystem", "embeddings_index",
			"scanned", len(nodes),
		)
	}

	return underlyingAdded + added
}

// MarkNodeEmbedded marks a node as embedded (removes from pending index).
// The node ID should be unprefixed (without namespace).
func (n *NamespacedEngine) MarkNodeEmbedded(nodeID NodeID) {
	n.updatePendingEmbedding(nodeID, false)
}

// AddToPendingEmbeddings adds a node back to the pending embeddings index (e.g. after a failed embed so it can be retried).
func (n *NamespacedEngine) AddToPendingEmbeddings(nodeID NodeID) {
	n.updatePendingEmbedding(nodeID, true)
}

// RecordMaterializedAccess records a result-materialization access against the
// fully qualified entity ID in the underlying engine.
func (n *NamespacedEngine) RecordMaterializedAccess(entityID string) {
	if recorder, ok := n.inner.(interface{ RecordMaterializedAccess(string) }); ok {
		recorder.RecordMaterializedAccess(QualifiedEntityID(n, entityID))
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

// toUserNodesInNamespace materializes one database's node views. Stored and
// stream protocols permit absent rows; ordinary collection protocols require
// actual entities and retain their established nil-input behavior.
func (n *NamespacedEngine) toUserNodesInNamespace(nodes []*Node, skipNil bool) []*Node {
	return materializedCollection(nodes, nil, func(nodes []*Node) []*Node {
		var out []*Node
		materializedValues(nodes, func(node *Node) {
			if skipNil && node == nil {
				return
			}
			if n.hasNodePrefix(node.ID) {
				out = append(out, n.toUserNode(node, nodeUserView{}))
			}
		})
		return out
	})
}

// streamNamespaceNodesProjected owns admission and namespace/view delivery;
// the binding selects visible or physically stored read semantics.
func (n *NamespacedEngine) streamNamespaceNodesProjected(label string, properties []string, visit func(*Node) error, stored bool) error {
	if visit == nil {
		return ErrInvalidData
	}
	deliver := func(node *Node) error {
		if node == nil || !n.hasNodePrefix(node.ID) {
			return nil
		}
		return visit(n.toUserNode(node, nodeUserView{}))
	}
	if stored {
		return streamStoredNodesByLabelProjectedInScope(n.inner, n.nodeScope(), label, properties, deliver)
	}
	return streamNodesByLabelProjectedInScope(n.inner, n.nodeScope(), label, properties, deliver)
}

func (n *NamespacedEngine) readNodeBatch(ids []NodeID, withoutEmbeddings bool) (map[NodeID]*Node, error) {
	var projected BatchNodeWithoutEmbeddingsReader
	if withoutEmbeddings {
		var ok bool
		projected, ok = n.inner.(BatchNodeWithoutEmbeddingsReader)
		if !ok {
			return nil, ErrNotImplemented
		}
	}
	physical := materializedIDs(ids, func(id NodeID) NodeID { return n.PrefixNodeID(id) })
	if withoutEmbeddings {
		return materializedRead(func() (map[NodeID]*Node, error) { return projected.BatchGetNodesWithoutEmbeddings(physical) }, func(rows map[NodeID]*Node) map[NodeID]*Node { return n.toUserNodeBatch(rows, true) }, false)
	}
	return materializedRead(func() (map[NodeID]*Node, error) { return n.inner.BatchGetNodes(physical) }, func(rows map[NodeID]*Node) map[NodeID]*Node { return n.toUserNodeBatch(rows, false) }, false)
}

// NodeWriteInput owns namespace qualification and token registration for both
// ordinary writes and their prepared transaction path. The caller retains its
// input; for example a tenant wrapper maps node "n" to "tenant:n".
func (n *NamespacedEngine) NodeWriteInput(node *Node) (*Node, error) {
	if node == nil {
		return nil, ErrInvalidData
	}
	if err := n.GetSchema().RegisterTokens(node.Labels, nil); err != nil {
		return nil, err
	}
	input := copyNode(node)
	input.ID = n.PrefixNodeID(node.ID)
	return input, nil
}

// EdgeWriteInput qualifies the relationship and both endpoint IDs using the
// same namespace rule as ordinary writes; "other:id" remains an ID within the
// current namespace, rather than silently becoming a cross-database request.
func (n *NamespacedEngine) EdgeWriteInput(edge *Edge) (*Edge, error) {
	if edge == nil {
		return nil, ErrInvalidData
	}
	if err := n.GetSchema().RegisterTokens(nil, []string{edge.Type}); err != nil {
		return nil, err
	}
	input := *edge
	input.ID = n.PrefixEdgeID(edge.ID)
	input.StartNode = n.PrefixNodeID(edge.StartNode)
	input.EndNode = n.PrefixNodeID(edge.EndNode)
	return &input, nil
}
