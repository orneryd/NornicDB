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
func StoredReader(engine Engine) (StoredEntityReader, bool) {
	if !HasReadCapability[StoredEntityReader](engine) {
		return nil, false
	}
	return storedReadOperation{engine}, true
}

// GetStoredNode reads node id for integrity work: the stored node when engine
// can read hidden entities (StoredReader), else engine.GetNode.
func GetStoredNode(engine Engine, id NodeID) (*Node, error) {
	return ReadWithCapability(engine, func(reader StoredEntityReader) (*Node, error) { return reader.GetStoredNode(id) }, func() (*Node, error) { return engine.GetNode(id) })
}

// GetStoredEdge is GetStoredNode for relationship id.
func GetStoredEdge(engine Engine, id EdgeID) (*Edge, error) {
	return ReadWithCapability(engine, func(reader StoredEntityReader) (*Edge, error) { return reader.GetStoredEdge(id) }, func() (*Edge, error) { return engine.GetEdge(id) })
}

// GetStoredNodesByLabel is GetStoredNode for a label's nodes.
func GetStoredNodesByLabel(engine Engine, label string) ([]*Node, error) {
	return ReadWithCapability(engine, func(reader StoredEntityReader) ([]*Node, error) { return reader.GetStoredNodesByLabel(label) }, func() ([]*Node, error) { return engine.GetNodesByLabel(label) })
}

// GetStoredOutgoingEdges is GetStoredNode for a node's outgoing relationships.
func GetStoredOutgoingEdges(engine Engine, nodeID NodeID) ([]*Edge, error) {
	return GetStoredOutgoingEdgesWithContext(context.Background(), engine, nodeID)
}

// GetStoredOutgoingEdgesWithContext carries the original caller through the
// same-view stored directional operation and its existing ordinary fallback.
func GetStoredOutgoingEdgesWithContext(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader StoredEntityReader) ([]*Edge, error) {
		if contextual, ok := OwnedCapability[interface {
			GetStoredOutgoingEdgesWithContext(context.Context, NodeID) ([]*Edge, error)
		}](reader); ok {
			return contextual.GetStoredOutgoingEdgesWithContext(ctx, nodeID)
		}
		edges, err := reader.GetStoredOutgoingEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return edges, err
	}, func() ([]*Edge, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		edges, err := engine.GetOutgoingEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return edges, err
	})
}

// GetStoredIncomingEdges is GetStoredNode for a node's incoming relationships.
func GetStoredIncomingEdges(engine Engine, nodeID NodeID) ([]*Edge, error) {
	return GetStoredIncomingEdgesWithContext(context.Background(), engine, nodeID)
}

// GetStoredIncomingEdgesWithContext carries the original caller through the
// same-view stored directional operation and its existing ordinary fallback.
func GetStoredIncomingEdgesWithContext(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ReadWithCapability(engine, func(reader StoredEntityReader) ([]*Edge, error) {
		if contextual, ok := OwnedCapability[interface {
			GetStoredIncomingEdgesWithContext(context.Context, NodeID) ([]*Edge, error)
		}](reader); ok {
			return contextual.GetStoredIncomingEdgesWithContext(ctx, nodeID)
		}
		edges, err := reader.GetStoredIncomingEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return edges, err
	}, func() ([]*Edge, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		edges, err := engine.GetIncomingEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return edges, err
	})
}

// StreamStoredEdgesByType visits edgeType's relationships for integrity work
// ("" visits every relationship): the stored ones when engine can read hidden
// entities (StoredReader), else those its type or whole-graph edge stream
// visits.
func StreamStoredEdgesByType(ctx context.Context, engine Engine, edgeType string, visit func(*Edge) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return VisitWithCapability(engine, func(reader StoredEntityReader, deliver func(*Edge) error) error {
		return reader.StreamStoredEdgesByType(ctx, edgeType, deliver)
	}, func(deliver func(*Edge) error) error {
		if edgeType == "" {
			return StreamEdgesWithFallback(ctx, engine, 1000, deliver)
		}
		return StreamEdgesByType(ctx, engine, edgeType, deliver)
	}, visit)
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
	return b.GetStoredNodesByLabelInScopeWithContext(context.Background(), scope, label)
}

// GetStoredNodesByLabelInScopeWithContext carries the caller through validation and label materialization.
func (b *BadgerEngine) GetStoredNodesByLabelInScopeWithContext(ctx context.Context, scope, label string) ([]*Node, error) {
	return b.getNodesByLabelInScopeWithContext(ctx, scope, label, storedRead)
}

// StreamStoredNodesByLabelProjectedInScope is
// StreamStoredNodesByLabelProjected within one database
// (ScopedStoredEntityReader).
func (b *BadgerEngine) StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return b.StreamStoredNodesByLabelProjectedInScopeWithContext(context.Background(), scope, label, properties, visit)
}

// StreamStoredNodesByLabelProjectedInScopeWithContext carries the caller through this read and its materialization.
func (b *BadgerEngine) StreamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return b.streamNodesByLabelProjectedInScopeWithContext(ctx, scope, label, properties, storedRead, visit)
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
	return w.GetStoredOutgoingEdgesWithContext(context.Background(), nodeID)
}

// GetStoredOutgoingEdgesWithContext forwards the same stored read policy.
func (w *WALEngine) GetStoredOutgoingEdgesWithContext(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	return GetStoredOutgoingEdgesWithContext(ctx, w.engine, nodeID)
}

// GetStoredIncomingEdges forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.GetStoredIncomingEdgesWithContext(context.Background(), nodeID)
}

// GetStoredIncomingEdgesWithContext forwards the same stored read policy.
func (w *WALEngine) GetStoredIncomingEdgesWithContext(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	return GetStoredIncomingEdgesWithContext(ctx, w.engine, nodeID)
}

// GetStoredNodesByLabel forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return w.GetStoredNodesByLabelWithContext(context.Background(), label)
}

// GetStoredNodesByLabelWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetStoredNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error) {
	return GetStoredNodesByLabelWithContext(ctx, w.engine, label)
}

// StreamStoredNodesByLabelProjected forwards to the inner engine
// (StoredEntityReader).
func (w *WALEngine) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return w.StreamStoredNodesByLabelProjectedWithContext(context.Background(), label, properties, visit)
}

// StreamStoredNodesByLabelProjectedWithContext carries the caller through this read and its materialization.
func (w *WALEngine) StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return StreamStoredNodesByLabelProjectedInScopeWithContext(ctx, w, "", label, properties, visit)
}

// StreamStoredEdgesByType forwards to the inner engine (StoredEntityReader).
func (w *WALEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return StreamStoredEdgesByType(ctx, w.engine, edgeType, visit)
}

// GetStoredNodesByLabelInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) GetStoredNodesByLabelInScope(scope, label string) ([]*Node, error) {
	return w.GetStoredNodesByLabelInScopeWithContext(context.Background(), scope, label)
}

// GetStoredNodesByLabelInScopeWithContext carries the caller through validation and label materialization.
func (w *WALEngine) GetStoredNodesByLabelInScopeWithContext(ctx context.Context, scope, label string) ([]*Node, error) {
	return getStoredNodesByLabelInScopeWithContext(ctx, w.engine, scope, label)
}

// StreamStoredNodesByLabelProjectedInScope forwards to the inner engine
// (ScopedStoredEntityReader).
func (w *WALEngine) StreamStoredNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return w.StreamStoredNodesByLabelProjectedInScopeWithContext(context.Background(), scope, label, properties, visit)
}

// StreamStoredNodesByLabelProjectedInScopeWithContext carries the caller through this read and its materialization.
func (w *WALEngine) StreamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScopeWithContext(ctx, w.engine, scope, label, properties, visit)
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

// getStoredNodesByLabelInScope is the scoped stored label scan of an inner
// engine for the wrapping engines: the scoped scan when the store under it
// has one, else its unscoped stored scan (the wrapper keeps filtering by
// database), else, for an engine that cannot read hidden entities, the scoped
// scan of what it can read.
func getStoredNodesByLabelInScope(engine Engine, scope, label string) ([]*Node, error) {
	return getStoredNodesByLabelInScopeWithContext(context.Background(), engine, scope, label)
}

// getStoredNodesByLabelInScopeWithContext carries the caller through validation and label materialization.
func getStoredNodesByLabelInScopeWithContext(ctx context.Context, engine Engine, scope, label string) ([]*Node, error) {
	return ReadWithCapability(engine, func(reader ScopedStoredEntityReader) ([]*Node, error) {
		return GetStoredNodesByLabelInScopeWithContext(ctx, reader, scope, label)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader StoredEntityReader) ([]*Node, error) {
			return GetStoredNodesByLabelWithContext(ctx, reader, label)
		}, func() ([]*Node, error) { return getNodesByLabelInScopeWithContext(ctx, engine, scope, label) })
	})
}

// streamStoredNodesByLabelProjectedInScope is getStoredNodesByLabelInScope
// for the projected label stream.
func streamStoredNodesByLabelProjectedInScope(engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	return streamStoredNodesByLabelProjectedInScopeWithContext(context.Background(), engine, scope, label, properties, visit)
}

// streamStoredNodesByLabelProjectedInScopeWithContext carries the caller through this read and its materialization.
func streamStoredNodesByLabelProjectedInScopeWithContext(ctx context.Context, engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	return VisitWithCapability(engine, func(reader ScopedStoredEntityReader, deliver func(*Node) error) error {
		return StreamStoredNodesByLabelProjectedInScopeWithContext(ctx, reader, scope, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return VisitWithCapability(engine, func(reader StoredEntityReader, next func(*Node) error) error {
			return StreamStoredNodesByLabelProjectedWithContext(ctx, reader, label, properties, next)
		}, func(next func(*Node) error) error {
			if HasReadCapability[ScopedLabelNodeReader](engine) {
				return streamNodesByLabelProjectedInScopeWithContext(ctx, engine, scope, label, properties, next)
			}
			return streamNodesByLabelForValidation(engine, label, properties, next)
		}, deliver)
	}, visit)
}

// streamStoredEdgesByTypeInScope is getStoredNodesByLabelInScope for the
// relationship type stream.
func streamStoredEdgesByTypeInScope(ctx context.Context, engine Engine, scope, edgeType string, visit func(*Edge) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return VisitWithCapability(engine, func(reader ScopedStoredEntityReader, deliver func(*Edge) error) error {
		return reader.StreamStoredEdgesByTypeInScope(ctx, scope, edgeType, deliver)
	}, func(deliver func(*Edge) error) error {
		return VisitWithCapability(engine, func(reader StoredEntityReader, next func(*Edge) error) error {
			return reader.StreamStoredEdgesByType(ctx, edgeType, next)
		}, func(next func(*Edge) error) error {
			return streamEdgesByTypeInScope(ctx, engine, scope, edgeType, next)
		}, deliver)
	}, visit)
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
	return n.GetStoredOutgoingEdgesWithContext(context.Background(), nodeID)
}

// GetStoredOutgoingEdgesWithContext observes capture and namespace projection.
func (n *NamespacedEngine) GetStoredOutgoingEdgesWithContext(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	return materializedReadWithContext(ctx, func() ([]*Edge, error) {
		return GetStoredOutgoingEdgesWithContext(ctx, n.inner, n.PrefixNodeID(nodeID))
	}, func(edges []*Edge) ([]*Edge, error) {
		return n.toUserEdgesInNamespaceWithContext(ctx, edges, true, false)
	}, false)
}

// GetStoredIncomingEdges is GetIncomingEdges' stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return n.GetStoredIncomingEdgesWithContext(context.Background(), nodeID)
}

// GetStoredIncomingEdgesWithContext observes capture and namespace projection.
func (n *NamespacedEngine) GetStoredIncomingEdgesWithContext(ctx context.Context, nodeID NodeID) ([]*Edge, error) {
	return materializedReadWithContext(ctx, func() ([]*Edge, error) {
		return GetStoredIncomingEdgesWithContext(ctx, n.inner, n.PrefixNodeID(nodeID))
	}, func(edges []*Edge) ([]*Edge, error) {
		return n.toUserEdgesInNamespaceWithContext(ctx, edges, true, false)
	}, false)
}

// GetStoredNodesByLabel is GetNodesByLabel's stored read
// (StoredEntityReader).
func (n *NamespacedEngine) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return n.GetStoredNodesByLabelWithContext(context.Background(), label)
}

// GetStoredNodesByLabelWithContext carries the caller through this read and its materialization.
func (n *NamespacedEngine) GetStoredNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error) {
	return materializedReadWithContext(ctx, func() ([]*Node, error) {
		return getStoredNodesByLabelInScopeWithContext(ctx, n.inner, n.nodeScope(), label)
	}, func(nodes []*Node) ([]*Node, error) { return n.toUserNodesInNamespaceWithContext(ctx, nodes, true) }, false)
}

// StreamStoredNodesByLabelProjected is StreamNodesByLabelProjected's stored
// read (StoredEntityReader).
func (n *NamespacedEngine) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return n.StreamStoredNodesByLabelProjectedWithContext(context.Background(), label, properties, visit)
}

// StreamStoredNodesByLabelProjectedWithContext carries the caller through this read and its materialization.
func (n *NamespacedEngine) StreamStoredNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return n.streamNamespaceNodesProjectedWithContext(ctx, label, properties, visit, true)
}

// StreamStoredEdgesByType is StreamEdgesByType's stored read
// (StoredEntityReader).
func (n *NamespacedEngine) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	return streamStoredEdgesByTypeInScope(ctx, n.inner, n.nodeScope(), edgeType, func(edge *Edge) error {
		if edge == nil || !n.edgeInScope(edge) {
			return nil
		}
		return visit(n.toUserEdge(edge))
	})
}

// toUserEdgesInNamespace keeps the edges of this database, unprefixed.
func (n *NamespacedEngine) toUserEdgesInNamespace(edges []*Edge, skipNil bool, reserve bool) []*Edge {
	out, _ := n.toUserEdgesInNamespaceWithContext(context.Background(), edges, skipNil, reserve)
	return out
}

func (n *NamespacedEngine) toUserEdgesInNamespaceWithContext(ctx context.Context, edges []*Edge, skipNil bool, reserve bool) ([]*Edge, error) {
	var out []*Edge
	if reserve {
		out = make([]*Edge, 0, len(edges))
	}
	err := materializedValuesWithContext(ctx, edges, func(edge *Edge) {
		if skipNil && edge == nil {
			return
		}
		if n.edgeInScope(edge) {
			out = append(out, n.toUserEdge(edge))
		}
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// storedReadOperation keeps the established StoredReader API while withholding
// the raw capability. Each method re-enters the shared execution owner.
type storedReadOperation struct{ engine Engine }

func (r storedReadOperation) GetStoredNode(id NodeID) (*Node, error) {
	return GetStoredNode(r.engine, id)
}
func (r storedReadOperation) GetStoredEdge(id EdgeID) (*Edge, error) {
	return GetStoredEdge(r.engine, id)
}
func (r storedReadOperation) GetStoredNodesByLabel(label string) ([]*Node, error) {
	return GetStoredNodesByLabel(r.engine, label)
}
func (r storedReadOperation) GetStoredOutgoingEdges(id NodeID) ([]*Edge, error) {
	return GetStoredOutgoingEdges(r.engine, id)
}
func (r storedReadOperation) GetStoredIncomingEdges(id NodeID) ([]*Edge, error) {
	return GetStoredIncomingEdges(r.engine, id)
}
func (r storedReadOperation) StreamStoredNodesByLabelProjected(label string, props []string, visit func(*Node) error) error {
	return StreamStoredNodesByLabelProjected(r.engine, label, props, visit)
}
func (r storedReadOperation) StreamStoredEdgesByType(ctx context.Context, kind string, visit func(*Edge) error) error {
	return StreamStoredEdgesByType(ctx, r.engine, kind, visit)
}
