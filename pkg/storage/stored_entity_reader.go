package storage

import "context"

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
	GetStoredOutgoingEdges(nodeID NodeID) ([]*Edge, error)
	GetStoredIncomingEdges(nodeID NodeID) ([]*Edge, error)
	GetStoredNodesByLabel(label string) ([]*Node, error)
	StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error
	// StreamStoredEdgesByType visits edgeType's relationships; an empty
	// edgeType visits every relationship.
	StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error
}

// ScopedStoredEntityReader is StoredEntityReader's scans within one database
// (scope is its ID prefix, "" every database), so a NamespacedEngine's
// stored scans skip other databases' index entries as its user scans do
// (#851).
type ScopedStoredEntityReader interface {
	GetStoredNodesByLabelInScope(scope, label string) ([]*Node, error)
	StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
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
func GetStoredNodesByLabel(engine Engine, label string) ([]*Node, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredNodesByLabel(label)
	}
	return engine.GetNodesByLabel(label)
}

// GetStoredOutgoingEdges is GetStoredNode for a node's outgoing relationships.
func GetStoredOutgoingEdges(engine Engine, nodeID NodeID) ([]*Edge, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredOutgoingEdges(nodeID)
	}
	return engine.GetOutgoingEdges(nodeID)
}

// GetStoredIncomingEdges is GetStoredNode for a node's incoming relationships.
func GetStoredIncomingEdges(engine Engine, nodeID NodeID) ([]*Edge, error) {
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredIncomingEdges(nodeID)
	}
	return engine.GetIncomingEdges(nodeID)
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
func StreamStoredNodesByLabelProjected(engine Engine, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScope(engine, "", label, properties, visit)
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
func (b *BadgerEngine) GetStoredNodesByLabelInScope(scope, label string) ([]*Node, error) {
	return b.getNodesByLabelInScope(scope, label, storedRead)
}

// StreamStoredNodesByLabelProjectedInScope is
// StreamStoredNodesByLabelProjected within one database
// (ScopedStoredEntityReader).
func (b *BadgerEngine) StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScope(scope, label, properties, storedRead, visit)
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
func (w *WALEngine) GetStoredOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return GetStoredOutgoingEdges(w.engine, nodeID)
}

// GetStoredIncomingEdges forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return GetStoredIncomingEdges(w.engine, nodeID)
}

// GetStoredNodesByLabel forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return GetStoredNodesByLabel(w.engine, label)
}

// StreamStoredNodesByLabelProjected forwards to the inner engine
// (StoredEntityReader).
func (w *WALEngine) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return w.StreamStoredNodesByLabelProjectedInScope("", label, properties, visit)
}

// StreamStoredEdgesByType forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return StreamStoredEdgesByType(ctx, w.engine, edgeType, visit)
}

// GetStoredNodesByLabelInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) GetStoredNodesByLabelInScope(scope, label string) ([]*Node, error) {
	return getStoredNodesByLabelInScope(w.engine, scope, label)
}

// StreamStoredNodesByLabelProjectedInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScope(w.engine, scope, label, properties, visit)
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
func getStoredNodesByLabelInScope(engine Engine, scope, label string) ([]*Node, error) {
	if scoped, ok := storedScopeReader(engine); ok {
		return scoped.GetStoredNodesByLabelInScope(scope, label)
	}
	if reader, ok := StoredReader(engine); ok {
		return reader.GetStoredNodesByLabel(label)
	}
	return getNodesByLabelInScope(engine, scope, label)
}

// streamStoredNodesByLabelProjectedInScope is getStoredNodesByLabelInScope
// for the projected label stream.
func streamStoredNodesByLabelProjectedInScope(engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	if scoped, ok := storedScopeReader(engine); ok {
		return scoped.StreamStoredNodesByLabelProjectedInScope(scope, label, properties, visit)
	}
	if reader, ok := StoredReader(engine); ok {
		return reader.StreamStoredNodesByLabelProjected(label, properties, visit)
	}
	if _, ok := engine.(ScopedLabelNodeReader); ok {
		return streamNodesByLabelProjectedInScope(engine, scope, label, properties, visit)
	}
	return streamNodesByLabelForValidation(engine, label, properties, visit)
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
func (n *NamespacedEngine) GetStoredOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return materializedRead(func() ([]*Edge, error) { return GetStoredOutgoingEdges(n.inner, n.PrefixNodeID(nodeID)) }, func(edges []*Edge) []*Edge { return n.toUserEdgesInNamespace(edges, true, false) }, false)
}

// GetStoredIncomingEdges is GetIncomingEdges' stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return materializedRead(func() ([]*Edge, error) { return GetStoredIncomingEdges(n.inner, n.PrefixNodeID(nodeID)) }, func(edges []*Edge) []*Edge { return n.toUserEdgesInNamespace(edges, true, false) }, false)
}

// GetStoredNodesByLabel is GetNodesByLabel's stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return materializedRead(func() ([]*Node, error) { return getStoredNodesByLabelInScope(n.inner, n.nodeScope(), label) }, func(nodes []*Node) []*Node { return n.toUserNodesInNamespace(nodes, true) }, false)
}

// StreamStoredNodesByLabelProjected is StreamNodesByLabelProjected's stored
// read (StoredEntityReader).
func (n *NamespacedEngine) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return n.streamNamespaceNodesProjected(label, properties, visit, true)
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

// toUserEdgesInNamespace keeps the edges of this database, unprefixed.
func (n *NamespacedEngine) toUserEdgesInNamespace(edges []*Edge, skipNil bool, reserve bool) []*Edge {
	var empty []*Edge
	if reserve {
		empty = []*Edge{}
	}
	return materializedCollection(edges, empty, func(edges []*Edge) []*Edge {
		project := func(edge *Edge) *Edge {
			if skipNil && edge == nil {
				return nil
			}
			if !n.hasEdgePrefix(edge.ID) {
				return nil
			}
			return n.toUserEdge(edge)
		}
		if reserve {
			reserved := make([]*Edge, 0, len(edges))
			materializedValues(edges, func(edge *Edge) {
				if view := project(edge); view != nil {
					reserved = append(reserved, view)
				}
			})
			return reserved
		}
		var out []*Edge
		materializedValues(edges, func(edge *Edge) {
			if view := project(edge); view != nil {
				out = append(out, view)
			}
		})
		return out
	})
}
