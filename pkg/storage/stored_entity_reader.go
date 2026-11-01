package storage

import (
	"context"
)

// StoredEntityReader reads what storage holds: every node and relationship,
// those the visibility layer hides from user reads included (on-demand
// suppression, knowledge-policy decay and its index tombstones). The layer
// hides entities from users without removing them, so integrity work reads
// through this interface: validating a constraint over existing data,
// backfilling an index, snapshots, replication, backups and data-subject
// requests. User-facing reads keep the Engine methods.
//
// BadgerEngine implements it (MemoryEngine by embedding); WALEngine and
// NamespacedEngine forward it. Callers reach it through StoredReader, which
// unwraps decorators that add no visibility of their own.
// Transaction views implement this capability themselves: stored reads bypass
// suppression/decay, but retain the transaction's snapshot, writes and deletes.
type StoredEntityReader interface {
	GetStoredNode(id NodeID) (*Node, error)
	GetStoredEdge(id EdgeID) (*Edge, error)
	GetStoredOutgoingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error)
	GetStoredIncomingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error)
	GetStoredNodesByLabel(ctx context.Context, label string) ([]*Node, error)
	StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error
	// StreamStoredEdgesByType visits edgeType's relationships; an empty
	// edgeType visits every relationship.
	StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error
}

// ScopedStoredEntityReader is StoredEntityReader's scans within one database
// (scope is its ID prefix, "" every database), so a NamespacedEngine's
// stored scans skip other databases' index entries as its user scans do
// (#851).
type ScopedStoredEntityReader interface {
	GetStoredNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error)
	StreamStoredNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error
	StreamStoredEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error
}

// StoredReader returns the StoredEntityReader of engine, unwrapping the
// decorators that add no visibility of their own (size tracking, tracing,
// replication). ok is false for an engine that cannot read hidden entities
// (composite and remote databases); integrity work then reads through the
// Engine methods, all the engine can give.
func StoredReader(engine Engine) (reader StoredEntityReader, ok bool) {
	for engine != nil {
		if reader, ok := engine.(StoredEntityReader); ok {
			return reader, true
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, false
		}
		inner := unwrapper.GetInnerEngine()
		if inner == engine {
			return nil, false
		}
		engine = inner
	}
	return nil, false
}

// GetStoredNode reads node id for integrity work: the stored node when engine
// can read hidden entities (StoredReader), else engine.GetNode.
func GetStoredNode(engine Engine, id NodeID) (*Node, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredNode(id)
	}
	return engine.GetNode(id)
}

// GetStoredEdge is GetStoredNode for relationship id.
func GetStoredEdge(engine Engine, id EdgeID) (*Edge, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredEdge(id)
	}
	return engine.GetEdge(id)
}

// GetStoredNodesByLabel is GetStoredNode for a label's nodes.
func GetStoredNodesByLabel(ctx context.Context, engine Engine, label string) ([]*Node, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredNodesByLabel(ctx, label)
	}
	return engine.GetNodesByLabel(ctx, label)
}

// GetStoredOutgoingEdges is GetStoredNode for a node's outgoing relationships.
func GetStoredOutgoingEdges(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredOutgoingEdges(ctx, nodeID)
	}
	return engine.GetOutgoingEdges(ctx, nodeID)
}

// GetStoredIncomingEdges is GetStoredNode for a node's incoming relationships.
func GetStoredIncomingEdges(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredIncomingEdges(ctx, nodeID)
	}
	return engine.GetIncomingEdges(ctx, nodeID)
}

// StreamStoredEdgesByType visits edgeType's relationships for integrity work
// ("" visits every relationship): the stored ones when engine can read hidden
// entities (StoredReader), else those its type or whole-graph edge stream
// visits.
func StreamStoredEdgesByType(ctx context.Context, engine Engine, edgeType string, visit func(*Edge) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if reader, ok := StoredReader(engine); ok {
		return reader.StreamStoredEdgesByType(ctx, edgeType, visit)
	}
	if edgeType == "" {
		return StreamEdgesWithFallback(ctx, engine, 1000, visit)
	}
	return StreamEdgesByType(ctx, engine, edgeType, visit)
}

// StreamStoredNodesByLabelProjected visits label's nodes for integrity work,
// decoding only properties: the stored ones when engine can read hidden
// entities (StoredReader), else those its projected label stream visits.
func StreamStoredNodesByLabelProjected(ctx context.Context, engine Engine, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScope(ctx, engine, "", label, properties, visit)
}

// StreamStoredNodes visits every node for integrity work, with
// StreamNodesWithOptions' options: every engine can stream stored nodes
// (ApplyDecayFilter false), so it only rules the visibility out.
func StreamStoredNodes(ctx context.Context, engine Engine, opts StreamNodesOptions, visit func(*Node) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	opts.ApplyDecayFilter = false
	return engine.StreamNodesWithOptions(ctx, opts, visit)
}

// CollectStoredNodes returns every stored node of engine, its separately
// stored embeddings included, for a backup, snapshot or export: the ones the
// visibility layer hides are stored data too (StreamStoredNodes).
func CollectStoredNodes(ctx context.Context, engine Engine) ([]*Node, error) {
	var nodes []*Node
	if err := StreamStoredNodes(ctx, engine, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	}); err != nil {
		return nil, err
	}
	return nodes, nil
}

// CollectStoredEdges is CollectStoredNodes for relationships
// (StreamStoredEdgesByType).
func CollectStoredEdges(ctx context.Context, engine Engine) ([]*Edge, error) {
	var edges []*Edge
	if err := StreamStoredEdgesByType(ctx, engine, "", func(edge *Edge) error {
		edges = append(edges, edge)
		return nil
	}); err != nil {
		return nil, err
	}
	return edges, nil
}

// ============================================================================
// BadgerEngine scoped scans
// ============================================================================

// GetStoredNodesByLabelInScope is GetStoredNodesByLabel within one database
// (ScopedStoredEntityReader).
func (b *BadgerEngine) GetStoredNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error) {
	return b.getNodesByLabelInScope(ctx, scope, label, storedRead)
}

// StreamStoredNodesByLabelProjectedInScope is
// StreamStoredNodesByLabelProjected within one database
// (ScopedStoredEntityReader).
func (b *BadgerEngine) StreamStoredNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScope(ctx, scope, label, properties, storedRead, visit)
}

// StreamStoredEdgesByTypeInScope is StreamStoredEdgesByType within one
// database (ScopedStoredEntityReader).
func (b *BadgerEngine) StreamStoredEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error {
	return b.streamEdgesByTypeInScope(ctx, scope, edgeType, storedRead, visit)
}

// ============================================================================
// WALEngine: reads are the inner engine's
// ============================================================================

// GetStoredNode forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredNode(id NodeID) (*Node, error) {
	return GetStoredNode(w.engine, id)
}

// GetStoredEdge forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredEdge(id EdgeID) (*Edge, error) {
	return GetStoredEdge(w.engine, id)
}

// GetStoredOutgoingEdges forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredOutgoingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	return GetStoredOutgoingEdges(ctx, w.engine, nodeID)
}

// GetStoredIncomingEdges forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredIncomingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	return GetStoredIncomingEdges(ctx, w.engine, nodeID)
}

// GetStoredNodesByLabel forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	return GetStoredNodesByLabel(ctx, w.engine, label)
}

// StreamStoredNodesByLabelProjected forwards to the inner engine
// (StoredEntityReader).
func (w *WALEngine) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return w.StreamStoredNodesByLabelProjectedInScope(ctx, "", label, properties, visit)
}

// StreamStoredEdgesByType forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return StreamStoredEdgesByType(ctx, w.engine, edgeType, visit)
}

// GetStoredNodesByLabelInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) GetStoredNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error) {
	return getStoredNodesByLabelInScope(ctx, w.engine, scope, label)
}

// StreamStoredNodesByLabelProjectedInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) StreamStoredNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScope(ctx, w.engine, scope, label, properties, visit)
}

// StreamStoredEdgesByTypeInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) StreamStoredEdgesByTypeInScope(ctx context.Context, scope, edgeType string, visit func(*Edge) error) error {
	return streamStoredEdgesByTypeInScope(ctx, w.engine, scope, edgeType, visit)
}

// storedScopeReader returns the ScopedStoredEntityReader under engine,
// unwrapping the decorators that add no visibility of their own, as
// StoredReader does. ok is false at a stored reader without scoped scans
// (NamespacedEngine) and at an engine that cannot read hidden entities.
func storedScopeReader(engine Engine) (ScopedStoredEntityReader, bool) {
	for engine != nil {
		if scoped, ok := engine.(ScopedStoredEntityReader); ok {
			return scoped, true
		}
		if _, ok := engine.(StoredEntityReader); ok {
			return nil, false
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, false
		}
		inner := unwrapper.GetInnerEngine()
		if inner == engine {
			return nil, false
		}
		engine = inner
	}
	return nil, false
}

// getStoredNodesByLabelInScope is the scoped stored label scan of an inner
// engine for the wrapping engines: the scoped scan when the store under it
// has one, else its unscoped stored scan (the wrapper keeps filtering by
// database), else, for an engine that cannot read hidden entities, the scoped
// scan of what it can read.
func getStoredNodesByLabelInScope(ctx context.Context, engine Engine, scope, label string) ([]*Node, error) {
	if scoped, ok := storedScopeReader(engine); ok {
		return scoped.GetStoredNodesByLabelInScope(ctx, scope, label)
	}
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredNodesByLabel(ctx, label)
	}
	return getNodesByLabelInScope(ctx, engine, scope, label)
}

// streamStoredNodesByLabelProjectedInScope is getStoredNodesByLabelInScope
// for the projected label stream.
func streamStoredNodesByLabelProjectedInScope(ctx context.Context, engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	if scoped, ok := storedScopeReader(engine); ok {
		return scoped.StreamStoredNodesByLabelProjectedInScope(ctx, scope, label, properties, visit)
	}
	if reader, ok := StoredReader(engine); ok {
		return reader.StreamStoredNodesByLabelProjected(ctx, label, properties, visit)
	}
	if _, ok := engine.(ScopedLabelNodeReader); ok {
		return streamNodesByLabelProjectedInScope(ctx, engine, scope, label, properties, visit)
	}
	return streamNodesByLabelForValidation(ctx, engine, label, properties, visit)
}

// streamStoredEdgesByTypeInScope is getStoredNodesByLabelInScope for the
// relationship type stream.
func streamStoredEdgesByTypeInScope(ctx context.Context, engine Engine, scope, edgeType string, visit func(*Edge) error) error {
	if scoped, ok := storedScopeReader(engine); ok {
		return scoped.StreamStoredEdgesByTypeInScope(ctx, scope, edgeType, visit)
	}
	if reader, ok := StoredReader(engine); ok {
		if ctx == nil {
			ctx = context.Background()
		}
		return reader.StreamStoredEdgesByType(ctx, edgeType, visit)
	}
	return streamEdgesByTypeInScope(ctx, engine, scope, edgeType, visit)
}

// NamespacedEngine: the inner engine's stored reads within the database
// ============================================================================

// GetStoredNode is GetNode's stored read (StoredEntityReader).
func (n *NamespacedEngine) GetStoredNode(id NodeID) (*Node, error) {
	return materializedRead(func() (*Node, error) { return GetStoredNode(n.inner, n.PrefixNodeID(id)) }, func(value *Node) *Node { return n.toUserNode(value, nodeUserView{}) }, false)
}

// GetStoredEdge is GetEdge's stored read (StoredEntityReader).
func (n *NamespacedEngine) GetStoredEdge(id EdgeID) (*Edge, error) {
	return materializedRead(func() (*Edge, error) { return GetStoredEdge(n.inner, n.PrefixEdgeID(id)) }, func(value *Edge) *Edge { return n.toUserEdge(value) }, false)
}

// GetStoredOutgoingEdges is GetOutgoingEdges' stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredOutgoingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	edges, err := GetStoredOutgoingEdges(ctx, n.inner, n.PrefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return n.toUserEdgesInNamespace(ctx, edges, true, false)
}

// GetStoredIncomingEdges is GetIncomingEdges' stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredIncomingEdges(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	edges, err := GetStoredIncomingEdges(ctx, n.inner, n.PrefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return n.toUserEdgesInNamespace(ctx, edges, true, false)
}

// GetStoredNodesByLabel is GetNodesByLabel's stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	nodes, err := getStoredNodesByLabelInScope(ctx, n.inner, n.nodeScope(), label)
	if err != nil {
		return nil, err
	}
	return n.toUserNodesInNamespace(ctx, nodes, true)
}

// StreamStoredNodesByLabelProjected is StreamNodesByLabelProjected's stored
// read (StoredEntityReader).
func (n *NamespacedEngine) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return n.streamNamespaceNodesProjected(ctx, label, properties, visit, true)
}

// StreamStoredEdgesByType is StreamEdgesByType's stored read
// (StoredEntityReader).
func (n *NamespacedEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	return streamStoredEdgesByTypeInScope(ctx, n.inner, n.nodeScope(), edgeType, func(edge *Edge) error {
		if edge == nil || !n.hasEdgePrefix(edge.ID) {
			return nil
		}
		return visit(n.toUserEdge(edge))
	})
}

// toUserEdgesInNamespace keeps the edges of this database, unprefixed. The
// copy loop observes the caller's cancellation: a canceled request returns
// that error and no partial list.
func (n *NamespacedEngine) toUserEdgesInNamespace(ctx context.Context, edges []*Edge, skipNil bool, reserve bool) ([]*Edge, error) {
	var out []*Edge
	if reserve {
		out = make([]*Edge, 0, len(edges))
	}
	var cancel bulkReadCancellation
	for _, edge := range edges {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		if skipNil && edge == nil {
			continue
		}
		if !n.hasEdgePrefix(edge.ID) {
			continue
		}
		out = append(out, n.toUserEdge(edge))
	}
	return out, nil
}
