// NamespacedEngine maintenance and capability forwards.
//
// Before this file, the namespaced view silently lacked maintenance, counting,
// schema, iteration, embedding-update and event capabilities that the inner
// engine exposes (DIVERGENCE_REPORT.md §2). Callers holding a NamespacedEngine
// either failed a type assertion and fell back to a different path or could not
// reach the operation at all. Every forward here preserves the namespace
// boundary: IDs are prefixed on the way in and stripped on the way out.
package storage

import (
	"context"
	"fmt"
	"strings"
)

// ConsumeCleanShutdownMarker forwards the engine-wide shutdown-marker read.
func (n *NamespacedEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	if provider, ok := OwnedCapability[StartupMaintenanceStateEngine](n.inner); ok {
		return provider.ConsumeCleanShutdownMarker(ctx)
	}
	return false, ErrNotImplemented
}

// MarkCleanShutdown forwards the engine-wide shutdown-marker write.
func (n *NamespacedEngine) MarkCleanShutdown(ctx context.Context) error {
	if provider, ok := OwnedCapability[StartupMaintenanceStateEngine](n.inner); ok {
		return provider.MarkCleanShutdown(ctx)
	}
	return ErrNotImplemented
}

// NodeCountByPrefix counts nodes in this namespace whose ID starts with prefix.
// The caller-supplied prefix is namespace-relative.
func (n *NamespacedEngine) NodeCountByPrefix(prefix string) (int64, error) {
	return namespacedPrefixCount(n, prefix, PrefixStatsEngine.NodeCountByPrefix, func(local string) (int64, error) {
		var count int64
		err := (nodeReadOperation{n, StreamNodesOptions{Prefix: local, Projection: []string{}, ApplyDecayFilter: true}}).visit(context.Background(), func(*Node) error { count++; return nil })
		return count, err
	})
}

// EdgeCountByPrefix counts edges in this namespace whose ID starts with prefix.
// The caller-supplied prefix is namespace-relative.
func (n *NamespacedEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	return namespacedPrefixCount(n, prefix, PrefixStatsEngine.EdgeCountByPrefix, func(local string) (int64, error) {
		var count int64
		err := n.StreamEdges(context.Background(), func(edge *Edge) error {
			if edge != nil && strings.HasPrefix(string(edge.ID), local) {
				count++
			}
			return nil
		})
		return count, err
	})
}

// GetSchemaForNamespace forwards the per-namespace schema lookup.
func (n *NamespacedEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if namespace != n.namespace {
		return nil
	}
	return n.GetSchema()
}

// ListNamespaces reports the database namespaces of the underlying engine.
// Inner engines without the capability still report this view's namespace.
func (n *NamespacedEngine) ListNamespaces() []string {
	return []string{n.namespace}
}

// PropertyKeyKnown reports whether a write in this view's database has
// stored a value under name (PropertyKeyLookup): false when the inner
// engine keeps no PropertyKeyRegistry.
func (n *NamespacedEngine) PropertyKeyKnown(name string) bool {
	registry, ok := ViewMetadataForEngine[NamespacePropertyKeyLookup](n.inner)
	return ok && registry.PropertyKeyKnownInNamespace(n.namespace, name)
}

// NodeCountByLabelInNamespace forwards the namespace-scoped label count.
func (n *NamespacedEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	return n.NodeCountByLabelInNamespaceWithContext(context.Background(), namespace, label)
}

// NodeCountByLabelInNamespaceWithContext carries the caller through this read and its materialization.
func (n *NamespacedEngine) NodeCountByLabelInNamespaceWithContext(ctx context.Context, namespace, label string) (int64, error) {
	if namespace != n.namespace {
		return 0, ErrInvalidID
	}
	return namespaceLabelCountWithContext(ctx, n.inner, namespace, label)
}

// IterateNodes streams this namespace's nodes with namespace prefixes stripped.
// Engines without IterateNodes fall back to a filtered AllNodes scan so the
// capability still works over simpler inner engines.
func (n *NamespacedEngine) IterateNodes(visit func(*Node) bool) error {
	return VisitNodes(n.inner, func(node *Node) bool {
		if node == nil || !n.hasNodePrefix(node.ID) {
			return true
		}
		return visit(n.toUserNode(node, nodeUserView{}))
	})
}

// PendingEmbeddingsCount forwards the pending embedding work-item count.
func (n *NamespacedEngine) PendingEmbeddingsCount() int {
	return CountPendingEmbeddingIDs(n, nil)
}

// PruneMVCCVersions forwards MVCC housekeeping to the underlying engine.
func (n *NamespacedEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if maint, ok := OwnedCapability[MVCCMaintenanceEngine](n.inner); ok {
		return maint.PruneMVCCVersions(ctx, opts)
	}
	return 0, ErrNotImplemented
}

// PruneTemporalHistory forwards temporal-index housekeeping.
func (n *NamespacedEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if maint, ok := OwnedCapability[TemporalMaintenanceEngine](n.inner); ok {
		return maint.PruneTemporalHistory(ctx, opts)
	}
	return 0, nil
}

// RebuildMVCCHeads forwards the MVCC head rebuild.
func (n *NamespacedEngine) RebuildMVCCHeads(ctx context.Context) error {
	if maint, ok := OwnedCapability[MVCCMaintenanceEngine](n.inner); ok {
		return maint.RebuildMVCCHeads(ctx)
	}
	return ErrNotImplemented
}

// RebuildTemporalIndexes forwards the temporal-index rebuild.
func (n *NamespacedEngine) RebuildTemporalIndexes(ctx context.Context) error {
	if maint, ok := OwnedCapability[TemporalMaintenanceEngine](n.inner); ok {
		return maint.RebuildTemporalIndexes(ctx)
	}
	return nil
}

// UpdateNodeEmbeddingSidecar writes managed embedding state in the dedicated
// embedding key space without touching the node record. Every production
// inner engine implements EmbeddingSidecarUpdater (compile-time asserted);
// anything else is reported as an error rather than silently degraded.
func (n *NamespacedEngine) UpdateNodeEmbeddingSidecar(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	sidecar, ok := OwnedCapability[EmbeddingSidecarUpdater](n.inner)
	if !ok {
		return fmt.Errorf("storage engine %T does not support embedding sidecar writes", n.inner)
	}
	namespaced := copyNode(node)
	namespaced.ID = n.PrefixNodeID(node.ID)
	return sidecar.UpdateNodeEmbeddingSidecar(namespaced)
}

// StreamParkedEmbeddingFailures streams this namespace's sidecar failure
// records with namespace-stripped IDs.
func (n *NamespacedEngine) StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error) {
	if visit == nil {
		return 0, ErrInvalidData
	}
	count := 0
	_, err := VisitEmbeddingFailures(n.inner, ctx, nil, func(id NodeID, meta map[string]any) error {
		if !n.hasNodePrefix(id) {
			return nil
		}
		count++
		return visit(NodeID(strings.TrimPrefix(string(id), n.scope)), meta)
	})
	return count, err
}

// Event-callback registration with namespace translation: callbacks only fire
// for this namespace's entities and receive namespace-stripped IDs.
func (n *NamespacedEngine) OnNodeCreated(callback NodeEventCallback) {
	subscribeStorageEvent([]storageEventTarget{{engine: n.inner, views: []*NamespacedEngine{n}}}, callback, StorageEventNotifier.OnNodeCreated)
}
func (n *NamespacedEngine) OnNodeUpdated(callback NodeEventCallback) {
	subscribeStorageEvent([]storageEventTarget{{engine: n.inner, views: []*NamespacedEngine{n}}}, callback, StorageEventNotifier.OnNodeUpdated)
}
func (n *NamespacedEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	subscribeStorageEvent([]storageEventTarget{{engine: n.inner, views: []*NamespacedEngine{n}}}, callback, StorageEventNotifier.OnNodeDeleted)
}
func (n *NamespacedEngine) OnEdgeCreated(callback EdgeEventCallback) {
	subscribeStorageEvent([]storageEventTarget{{engine: n.inner, views: []*NamespacedEngine{n}}}, callback, StorageEventNotifier.OnEdgeCreated)
}
func (n *NamespacedEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	subscribeStorageEvent([]storageEventTarget{{engine: n.inner, views: []*NamespacedEngine{n}}}, callback, StorageEventNotifier.OnEdgeUpdated)
}
func (n *NamespacedEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	subscribeStorageEvent([]storageEventTarget{{engine: n.inner, views: []*NamespacedEngine{n}}}, callback, StorageEventNotifier.OnEdgeDeleted)
}
