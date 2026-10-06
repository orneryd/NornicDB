// Storage capability contracts for the production wrapper stack.
//
// The storage chain (BadgerEngine → WALEngine → AsyncEngine → NamespacedEngine,
// with MemoryEngine embedding BadgerEngine) must expose the same optional
// capabilities at every layer so callers that type-assert for a capability
// never silently fall back to a slower or incorrect path depending on which
// wrapper they hold (DIVERGENCE_REPORT.md §2: the pattern behind #420, #424,
// #473).
//
// The var-block below turns every missing forward into a compile error.
// NamespaceLister, NamespaceSchemaProvider, PrefixStatsEngine,
// NamespaceLabelStatsProvider, StartupMaintenanceStateEngine,
// MVCCMaintenanceEngine, TemporalMaintenanceEngine and StorageEventNotifier
// are declared in types.go; the four interfaces below complete the set of
// capabilities the report found forwarded unevenly.
package storage

import "context"

// NodeProjectionReader reports nodes carrying only the requested properties.
type NodeProjectionReader interface {
	GetNodeProjected(id NodeID, properties []string) (*Node, error)
}

// NodeIterator streams every node in the engine view.
type NodeIterator interface {
	IterateNodes(fn func(*Node) bool) error
}

// EmbeddingCountProvider reports the pending embedding work-item count.
type EmbeddingCountProvider interface {
	PendingEmbeddingsCount() int
}

// EmbeddingUpdater is removed: the worker's only writeback is the embedding
// sidecar (EmbeddingSidecarUpdater below, declared in
// badger_embedding_sidecar.go). Engines without it cannot write embeddings.

// Compile-time capability assertions: every production wrapper must expose the
// same optional capabilities as the inner engine. MemoryEngine satisfies these
// through its embedded *BadgerEngine, which is intentional.
var (
	_ NodeProjectionReader          = (*BadgerEngine)(nil)
	_ NodeProjectionReader          = (*WALEngine)(nil)
	_ NodeProjectionReader          = (*AsyncEngine)(nil)
	_ NodeProjectionReader          = (*NamespacedEngine)(nil)
	_ NodeProjectionReader          = (*MemoryEngine)(nil)
	_ RelationshipEndpointChecker   = (*BadgerEngine)(nil)
	_ RelationshipEndpointChecker   = (*WALEngine)(nil)
	_ RelationshipEndpointChecker   = (*AsyncEngine)(nil)
	_ RelationshipEndpointChecker   = (*NamespacedEngine)(nil)
	_ RelationshipEndpointChecker   = (*MemoryEngine)(nil)
	_ EdgeHeaderReader              = (*BadgerEngine)(nil)
	_ EdgeHeaderReader              = (*WALEngine)(nil)
	_ EdgeHeaderReader              = (*AsyncEngine)(nil)
	_ EdgeHeaderReader              = (*NamespacedEngine)(nil)
	_ EdgeHeaderReader              = (*MemoryEngine)(nil)
	_ NodeIterator                  = (*BadgerEngine)(nil)
	_ NodeIterator                  = (*WALEngine)(nil)
	_ NodeIterator                  = (*AsyncEngine)(nil)
	_ NodeIterator                  = (*NamespacedEngine)(nil)
	_ NodeIterator                  = (*MemoryEngine)(nil)
	_ EmbeddingCountProvider        = (*BadgerEngine)(nil)
	_ EmbeddingCountProvider        = (*WALEngine)(nil)
	_ EmbeddingCountProvider        = (*AsyncEngine)(nil)
	_ EmbeddingCountProvider        = (*NamespacedEngine)(nil)
	_ EmbeddingCountProvider        = (*MemoryEngine)(nil)
	_ EmbeddingSidecarUpdater       = (*BadgerEngine)(nil)
	_ EmbeddingSidecarUpdater       = (*WALEngine)(nil)
	_ EmbeddingSidecarUpdater       = (*AsyncEngine)(nil)
	_ EmbeddingSidecarUpdater       = (*NamespacedEngine)(nil)
	_ EmbeddingSidecarUpdater       = (*MemoryEngine)(nil)
	_ EmbeddingFailureStreamer      = (*BadgerEngine)(nil)
	_ EmbeddingFailureStreamer      = (*WALEngine)(nil)
	_ EmbeddingFailureStreamer      = (*AsyncEngine)(nil)
	_ EmbeddingFailureStreamer      = (*NamespacedEngine)(nil)
	_ EmbeddingFailureStreamer      = (*MemoryEngine)(nil)
	_ NamespaceLister               = (*BadgerEngine)(nil)
	_ NamespaceLister               = (*WALEngine)(nil)
	_ NamespaceLister               = (*AsyncEngine)(nil)
	_ NamespaceLister               = (*NamespacedEngine)(nil)
	_ NamespaceLister               = (*MemoryEngine)(nil)
	_ NamespaceSchemaProvider       = (*BadgerEngine)(nil)
	_ NamespaceSchemaProvider       = (*WALEngine)(nil)
	_ NamespaceSchemaProvider       = (*AsyncEngine)(nil)
	_ NamespaceSchemaProvider       = (*NamespacedEngine)(nil)
	_ NamespaceSchemaProvider       = (*MemoryEngine)(nil)
	_ PrefixStatsEngine             = (*BadgerEngine)(nil)
	_ PrefixStatsEngine             = (*WALEngine)(nil)
	_ PrefixStatsEngine             = (*AsyncEngine)(nil)
	_ PrefixStatsEngine             = (*NamespacedEngine)(nil)
	_ PrefixStatsEngine             = (*MemoryEngine)(nil)
	_ NamespaceLabelStatsProvider   = (*BadgerEngine)(nil)
	_ NamespaceLabelStatsProvider   = (*WALEngine)(nil)
	_ NamespaceLabelStatsProvider   = (*AsyncEngine)(nil)
	_ NamespaceLabelStatsProvider   = (*NamespacedEngine)(nil)
	_ NamespaceLabelStatsProvider   = (*MemoryEngine)(nil)
	_ StartupMaintenanceStateEngine = (*BadgerEngine)(nil)
	_ StartupMaintenanceStateEngine = (*WALEngine)(nil)
	_ StartupMaintenanceStateEngine = (*AsyncEngine)(nil)
	_ StartupMaintenanceStateEngine = (*NamespacedEngine)(nil)
	_ StartupMaintenanceStateEngine = (*MemoryEngine)(nil)
	_ MVCCMaintenanceEngine         = (*BadgerEngine)(nil)
	_ MVCCMaintenanceEngine         = (*WALEngine)(nil)
	_ MVCCMaintenanceEngine         = (*AsyncEngine)(nil)
	_ MVCCMaintenanceEngine         = (*NamespacedEngine)(nil)
	_ MVCCMaintenanceEngine         = (*MemoryEngine)(nil)
	_ TemporalMaintenanceEngine     = (*BadgerEngine)(nil)
	_ TemporalMaintenanceEngine     = (*WALEngine)(nil)
	_ TemporalMaintenanceEngine     = (*AsyncEngine)(nil)
	_ TemporalMaintenanceEngine     = (*NamespacedEngine)(nil)
	_ TemporalMaintenanceEngine     = (*MemoryEngine)(nil)
	_ StorageEventNotifier          = (*BadgerEngine)(nil)
	_ StorageEventNotifier          = (*WALEngine)(nil)
	_ StorageEventNotifier          = (*AsyncEngine)(nil)
	_ StorageEventNotifier          = (*NamespacedEngine)(nil)
	_ StorageEventNotifier          = (*MemoryEngine)(nil)
	_ NodeProjectionReader          = (*CompositeEngine)(nil)
	_ NodeIterator                  = (*CompositeEngine)(nil)
	_ EmbeddingCountProvider        = (*CompositeEngine)(nil)
	_ EmbeddingSidecarUpdater       = (*CompositeEngine)(nil)
	_ NamespaceLister               = (*CompositeEngine)(nil)
	_ NamespaceSchemaProvider       = (*CompositeEngine)(nil)
	_ StartupMaintenanceStateEngine = (*CompositeEngine)(nil)
	_ MVCCMaintenanceEngine         = (*CompositeEngine)(nil)
	_ TemporalMaintenanceEngine     = (*CompositeEngine)(nil)
	_ StorageEventNotifier          = (*CompositeEngine)(nil)
	_ MVCCVisibilityEngine          = (*CompositeEngine)(nil)
	_ MVCCIndexedVisibilityEngine   = (*CompositeEngine)(nil)
	_ MVCCHeadEngine                = (*CompositeEngine)(nil)
	_ MVCCLifecycleEngine           = (*CompositeEngine)(nil)
	_ MVCCLifecycleScheduleEngine   = (*CompositeEngine)(nil)
	_ GraphMutationVersionProvider  = (*CompositeEngine)(nil)
	_ LabelNodeIDLookupEngine       = (*CompositeEngine)(nil)
	_ MVCCLatestEffectiveEngine     = (*WALEngine)(nil)
	_ MVCCLatestEffectiveEngine     = (*AsyncEngine)(nil)
	_ MVCCLatestEffectiveEngine     = (*NamespacedEngine)(nil)
	_ MVCCLatestEffectiveEngine     = (*CompositeEngine)(nil)
)

// Wrapper contract: every production engine type must satisfy the full Engine
// contract and, when it decorates another engine, expose the canonical
// EngineUnwrapper accessor. A type that drops a method fails the build instead
// of surfacing a runtime ErrNotImplemented from a wrapper layer.
var (
	_ Engine          = (*BadgerEngine)(nil)
	_ Engine          = (*WALEngine)(nil)
	_ Engine          = (*AsyncEngine)(nil)
	_ Engine          = (*NamespacedEngine)(nil)
	_ Engine          = (*MemoryEngine)(nil)
	_ Engine          = (*CompositeEngine)(nil)
	_ Engine          = (*RemoteEngine)(nil)
	_ EngineUnwrapper = (*WALEngine)(nil)
	_ EngineUnwrapper = (*AsyncEngine)(nil)
	_ EngineUnwrapper = (*NamespacedEngine)(nil)
	_ EngineUnwrapper = (*CompositeEngine)(nil)
	_ EngineUnwrapper = (*TracedEngine)(nil)
)

// GetNodeProjected returns a node with only the requested properties, honoring
// the async overlay: staged writes are projected from cache, deleted nodes
// report ErrNotFound, and everything else reads the underlying engine.
func (ae *AsyncEngine) GetNodeProjected(id NodeID, properties []string) (*Node, error) {
	ae.mu.RLock()
	if ae.deleteNodes[id] {
		ae.mu.RUnlock()
		return nil, ErrNotFound
	}
	if node, ok := ae.nodeCache[id]; ok && node != nil {
		ae.mu.RUnlock()
		return projectCachedNodeForRead(node, properties), nil
	}
	ae.mu.RUnlock()
	return getNodeProjectedThrough(ae.engine, id, properties)
}

// RelationshipEndpointVisible honors the async overlay as GetNode does: a
// staged delete hides the node, a staged write shows it, and everything else
// asks the underlying engine.
func (ae *AsyncEngine) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	ae.mu.RLock()
	deleted := ae.deleteNodes[id]
	_, staged := ae.nodeCache[id]
	ae.mu.RUnlock()
	if deleted || staged {
		return staged && !deleted, true
	}
	if checker, ok := ae.engine.(RelationshipEndpointChecker); ok {
		return checker.RelationshipEndpointVisible(id)
	}
	return false, false
}

// RelationshipEndpointVisible forwards to the underlying engine; WAL adds no
// overlay.
func (w *WALEngine) RelationshipEndpointVisible(id NodeID) (visible, answered bool) {
	if checker, ok := w.engine.(RelationshipEndpointChecker); ok {
		return checker.RelationshipEndpointVisible(id)
	}
	return false, false
}

// OutgoingEdgeHeaders lists the engine's relationship headers merged with the
// async overlay as GetOutgoingEdges does: staged relationships are included
// and staged deletes hidden.
func (ae *AsyncEngine) OutgoingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return ae.edgeHeaders(nodeID, true)
}

// IncomingEdgeHeaders is the incoming-side OutgoingEdgeHeaders.
func (ae *AsyncEngine) IncomingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	return ae.edgeHeaders(nodeID, false)
}

func (ae *AsyncEngine) edgeHeaders(nodeID NodeID, outgoing bool) ([]*Edge, bool, error) {
	reader, ok := ae.engine.(EdgeHeaderReader)
	if !ok {
		return nil, false, nil
	}
	var engineEdges []*Edge
	var answered bool
	var err error
	if outgoing {
		engineEdges, answered, err = reader.OutgoingEdgeHeaders(nodeID)
	} else {
		engineEdges, answered, err = reader.IncomingEdgeHeaders(nodeID)
	}
	if !answered || err != nil {
		return nil, answered, err
	}
	ae.mu.RLock()
	byNode := ae.cacheEdgesByEnd
	if outgoing {
		byNode = ae.cacheEdgesByStart
	}
	var cached []*Edge
	for id := range byNode[nodeID] {
		if ae.deleteEdges[id] {
			continue
		}
		edge, ok := ae.edgeCache[id]
		if !ok || edge == nil || (outgoing && edge.StartNode != nodeID) || (!outgoing && edge.EndNode != nodeID) {
			continue
		}
		cached = append(cached, edge)
	}
	ae.mu.RUnlock()
	return mergeAsyncEdges(ae, cached, engineEdges, nodeID, outgoing), true, nil
}

// OutgoingEdgeHeaders forwards to the underlying engine; WAL adds no overlay.
func (w *WALEngine) OutgoingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	if reader, ok := w.engine.(EdgeHeaderReader); ok {
		return reader.OutgoingEdgeHeaders(nodeID)
	}
	return nil, false, nil
}

// IncomingEdgeHeaders forwards to the underlying engine; WAL adds no overlay.
func (w *WALEngine) IncomingEdgeHeaders(nodeID NodeID) ([]*Edge, bool, error) {
	if reader, ok := w.engine.(EdgeHeaderReader); ok {
		return reader.IncomingEdgeHeaders(nodeID)
	}
	return nil, false, nil
}

// GetNodeProjected forwards the projected read to the underlying engine. WAL
// adds no overlay, so a plain forward preserves parity with GetNode.
func (w *WALEngine) GetNodeProjected(id NodeID, properties []string) (*Node, error) {
	return getNodeProjectedThrough(w.engine, id, properties)
}

// getNodeProjectedThrough reads a projected node through a single engine: a
// projection-capable reader serves the read directly, and any other engine
// serves a full read that is projected on the way out. Async, WAL and the
// composite constituent scan share this tail, so reader preference and error
// propagation cannot drift between wrappers.
func getNodeProjectedThrough(engine Engine, id NodeID, properties []string) (*Node, error) {
	if reader, ok := engine.(NodeProjectionReader); ok {
		return reader.GetNodeProjected(id, properties)
	}
	node, err := engine.GetNode(id)
	if err != nil {
		return nil, err
	}
	return projectCachedNodeForRead(node, properties), nil
}

// Event-callback registration forwards. WAL does not translate IDs, so the
// callback receives exactly what the inner engine emits.
func (w *WALEngine) OnNodeCreated(callback NodeEventCallback) {
	if notifier, ok := w.engine.(StorageEventNotifier); ok {
		notifier.OnNodeCreated(callback)
	}
}

func (w *WALEngine) OnNodeUpdated(callback NodeEventCallback) {
	if notifier, ok := w.engine.(StorageEventNotifier); ok {
		notifier.OnNodeUpdated(callback)
	}
}

func (w *WALEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	if notifier, ok := w.engine.(StorageEventNotifier); ok {
		notifier.OnNodeDeleted(callback)
	}
}

func (w *WALEngine) OnEdgeCreated(callback EdgeEventCallback) {
	if notifier, ok := w.engine.(StorageEventNotifier); ok {
		notifier.OnEdgeCreated(callback)
	}
}

func (w *WALEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	if notifier, ok := w.engine.(StorageEventNotifier); ok {
		notifier.OnEdgeUpdated(callback)
	}
}

func (w *WALEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	if notifier, ok := w.engine.(StorageEventNotifier); ok {
		notifier.OnEdgeDeleted(callback)
	}
}

var _ = context.Background // keep context import for parity with types.go declarations
