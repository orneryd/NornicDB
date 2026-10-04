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
	"strings"
)

// ConsumeCleanShutdownMarker forwards the engine-wide shutdown-marker read.
func (n *NamespacedEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	if provider, ok := n.inner.(StartupMaintenanceStateEngine); ok {
		return provider.ConsumeCleanShutdownMarker(ctx)
	}
	return false, ErrNotImplemented
}

// MarkCleanShutdown forwards the engine-wide shutdown-marker write.
func (n *NamespacedEngine) MarkCleanShutdown(ctx context.Context) error {
	if provider, ok := n.inner.(StartupMaintenanceStateEngine); ok {
		return provider.MarkCleanShutdown(ctx)
	}
	return ErrNotImplemented
}

// NodeCountByPrefix counts nodes in this namespace whose ID starts with prefix.
// The caller-supplied prefix is namespace-relative.
func (n *NamespacedEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if stats, ok := n.inner.(PrefixStatsEngine); ok {
		return stats.NodeCountByPrefix(string(n.prefixNodeID(NodeID(prefix))))
	}
	nodes, err := n.AllNodes()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, node := range nodes {
		if node != nil && strings.HasPrefix(string(node.ID), prefix) {
			count++
		}
	}
	return count, nil
}

// EdgeCountByPrefix counts edges in this namespace whose ID starts with prefix.
// The caller-supplied prefix is namespace-relative.
func (n *NamespacedEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if stats, ok := n.inner.(PrefixStatsEngine); ok {
		return stats.EdgeCountByPrefix(string(n.prefixEdgeID(EdgeID(prefix))))
	}
	edges, err := n.AllEdges()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		if edge != nil && strings.HasPrefix(string(edge.ID), prefix) {
			count++
		}
	}
	return count, nil
}

// GetSchemaForNamespace forwards the per-namespace schema lookup.
func (n *NamespacedEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if provider, ok := n.inner.(NamespaceSchemaProvider); ok {
		return provider.GetSchemaForNamespace(namespace)
	}
	return n.GetSchema()
}

// ListNamespaces reports the database namespaces of the underlying engine.
// Inner engines without the capability still report this view's namespace.
func (n *NamespacedEngine) ListNamespaces() []string {
	if lister, ok := n.inner.(NamespaceLister); ok {
		return lister.ListNamespaces()
	}
	return []string{n.namespace}
}

// NodeCountByLabelInNamespace forwards the namespace-scoped label count.
func (n *NamespacedEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	if stats, ok := n.inner.(NamespaceLabelStatsProvider); ok {
		return stats.NodeCountByLabelInNamespace(namespace, label)
	}
	nodes, err := n.inner.GetNodesByLabel(label)
	if err != nil {
		return 0, err
	}
	prefix := namespace + n.separator
	var count int64
	for _, node := range nodes {
		if node != nil && strings.HasPrefix(string(node.ID), prefix) {
			count++
		}
	}
	return count, nil
}

// IterateNodes streams this namespace's nodes with namespace prefixes stripped.
// Engines without IterateNodes fall back to a filtered AllNodes scan so the
// capability still works over simpler inner engines.
func (n *NamespacedEngine) IterateNodes(fn func(*Node) bool) error {
	if iterator, ok := n.inner.(interface{ IterateNodes(func(*Node) bool) error }); ok {
		return iterator.IterateNodes(func(node *Node) bool {
			if node == nil || !n.hasNodePrefix(node.ID) {
				return true
			}
			return fn(n.toUserNode(node))
		})
	}
	nodes, err := n.inner.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node == nil || !n.hasNodePrefix(node.ID) {
			continue
		}
		if !fn(n.toUserNode(node)) {
			break
		}
	}
	return nil
}

// PendingEmbeddingsCount forwards the pending embedding work-item count.
func (n *NamespacedEngine) PendingEmbeddingsCount() int {
	if provider, ok := n.inner.(interface{ PendingEmbeddingsCount() int }); ok {
		return provider.PendingEmbeddingsCount()
	}
	return 0
}

// PruneMVCCVersions forwards MVCC housekeeping to the underlying engine.
func (n *NamespacedEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if maint, ok := n.inner.(MVCCMaintenanceEngine); ok {
		return maint.PruneMVCCVersions(ctx, opts)
	}
	return 0, ErrNotImplemented
}

// PruneTemporalHistory forwards temporal-index housekeeping.
func (n *NamespacedEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if maint, ok := n.inner.(TemporalMaintenanceEngine); ok {
		return maint.PruneTemporalHistory(ctx, opts)
	}
	return 0, nil
}

// RebuildMVCCHeads forwards the MVCC head rebuild.
func (n *NamespacedEngine) RebuildMVCCHeads(ctx context.Context) error {
	if maint, ok := n.inner.(MVCCMaintenanceEngine); ok {
		return maint.RebuildMVCCHeads(ctx)
	}
	return ErrNotImplemented
}

// RebuildTemporalIndexes forwards the temporal-index rebuild.
func (n *NamespacedEngine) RebuildTemporalIndexes(ctx context.Context) error {
	if maint, ok := n.inner.(TemporalMaintenanceEngine); ok {
		return maint.RebuildTemporalIndexes(ctx)
	}
	return nil
}

// UpdateNodeEmbedding updates only the embedding of an existing node in this
// namespace, returning ErrNotFound instead of creating an orphan. Engines
// without UpdateNodeEmbedding fall back to an existence-checked UpdateNode.
func (n *NamespacedEngine) UpdateNodeEmbedding(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if updater, ok := n.inner.(interface{ UpdateNodeEmbedding(*Node) error }); ok {
		namespaced := copyNode(node)
		namespaced.ID = n.prefixNodeID(node.ID)
		return updater.UpdateNodeEmbedding(namespaced)
	}
	if _, err := n.GetNode(node.ID); err != nil {
		return err
	}
	return n.UpdateNode(node)
}

// UpdateNodeEmbeddingSidecar writes managed embedding state in the dedicated
// embedding key space without touching the node record.
func (n *NamespacedEngine) UpdateNodeEmbeddingSidecar(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if sidecar, ok := n.inner.(EmbeddingSidecarUpdater); ok {
		namespaced := copyNode(node)
		namespaced.ID = n.prefixNodeID(node.ID)
		return sidecar.UpdateNodeEmbeddingSidecar(namespaced)
	}
	return n.UpdateNodeEmbedding(node)
}

// StreamParkedEmbeddingFailures streams this namespace's sidecar failure
// records with namespace-stripped IDs.
func (n *NamespacedEngine) StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error) {
	if streamer, ok := n.inner.(EmbeddingFailureStreamer); ok {
		count := 0
		_, err := streamer.StreamParkedEmbeddingFailures(ctx, func(nodeID NodeID, meta map[string]any) error {
			count++
			return visit(n.unprefixNodeID(nodeID), meta)
		})
		return count, err
	}
	return 0, nil
}

// Event-callback registration with namespace translation: callbacks only fire
// for this namespace's entities and receive namespace-stripped IDs.
func (n *NamespacedEngine) OnNodeCreated(callback NodeEventCallback) {
	if notifier, ok := n.inner.(StorageEventNotifier); ok {
		notifier.OnNodeCreated(func(node *Node) {
			if node == nil || !n.hasNodePrefix(node.ID) {
				return
			}
			callback(n.toUserNode(node))
		})
	}
}

func (n *NamespacedEngine) OnNodeUpdated(callback NodeEventCallback) {
	if notifier, ok := n.inner.(StorageEventNotifier); ok {
		notifier.OnNodeUpdated(func(node *Node) {
			if node == nil || !n.hasNodePrefix(node.ID) {
				return
			}
			callback(n.toUserNode(node))
		})
	}
}

func (n *NamespacedEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	if notifier, ok := n.inner.(StorageEventNotifier); ok {
		notifier.OnNodeDeleted(func(nodeID NodeID) {
			if !n.hasNodePrefix(nodeID) {
				return
			}
			callback(n.unprefixNodeID(nodeID))
		})
	}
}

func (n *NamespacedEngine) OnEdgeCreated(callback EdgeEventCallback) {
	if notifier, ok := n.inner.(StorageEventNotifier); ok {
		notifier.OnEdgeCreated(func(edge *Edge) {
			if edge == nil || !n.hasEdgePrefix(edge.ID) {
				return
			}
			callback(n.toUserEdge(edge))
		})
	}
}

func (n *NamespacedEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	if notifier, ok := n.inner.(StorageEventNotifier); ok {
		notifier.OnEdgeUpdated(func(edge *Edge) {
			if edge == nil || !n.hasEdgePrefix(edge.ID) {
				return
			}
			callback(n.toUserEdge(edge))
		})
	}
}

func (n *NamespacedEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	if notifier, ok := n.inner.(StorageEventNotifier); ok {
		notifier.OnEdgeDeleted(func(edgeID EdgeID) {
			if !n.hasEdgePrefix(edgeID) {
				return
			}
			callback(n.unprefixEdgeID(edgeID))
		})
	}
}
