// CompositeEngine capability dispatchers.
//
// CompositeEngine spans multiple constituent databases and must expose the
// same optional capabilities as the wrappers it aggregates so callers never
// silently fall back based on which engine they hold (DIVERGENCE_REPORT.md
// §2). ID-scoped reads route to every readable constituent until one resolves;
// merge reads aggregate across all readable constituents; maintenance and
// lifecycle operations broadcast.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// StreamNodesWithOptions broadcasts the options-driven scan across every
// readable constituent, preserving early-stop semantics. Constituents without
// the options kernel fall back to a materialized scan with the same transforms.
func (c *CompositeEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(*Node) error) error {
	if fn == nil {
		return ErrInvalidData
	}
	stopped := false
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if err := engine.StreamNodesWithOptions(ctx, opts, func(node *Node) error {
			if stopped {
				return nil
			}
			if err := fn(node); err != nil {
				if err == ErrIterationStopped {
					stopped = true
					return nil
				}
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	return nil
}

// --- MVCC visibility: ID-scoped reads route to the first constituent that
// resolves the ID, mirroring GetNode's search-all semantics.

// GetNodeLatestEffective routes latest-effective node reads to the first
// constituent that resolves the ID.
func (c *CompositeEngine) GetNodeLatestEffective(id NodeID) (*Node, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCLatestEffectiveEngine); ok {
			node, err := provider.GetNodeLatestEffective(id)
			if err == nil {
				return node, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			continue
		}
		node, err := engine.GetNode(id)
		if err == nil {
			return node, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// GetEdgeLatestEffective routes latest-effective edge reads to the first
// constituent that resolves the ID.
func (c *CompositeEngine) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCLatestEffectiveEngine); ok {
			edge, err := provider.GetEdgeLatestEffective(id)
			if err == nil {
				return edge, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			continue
		}
		edge, err := engine.GetEdge(id)
		if err == nil {
			return edge, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCVisibilityEngine); ok {
			node, err := provider.GetNodeLatestVisible(id)
			if err == nil {
				return node, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			continue
		}
		node, err := engine.GetNode(id)
		if err == nil {
			return node, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCVisibilityEngine); ok {
			node, err := provider.GetNodeVisibleAt(id, version)
			if err == nil {
				return node, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			continue
		}
		node, err := engine.GetNode(id)
		if err == nil {
			return node, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCVisibilityEngine); ok {
			edge, err := provider.GetEdgeLatestVisible(id)
			if err == nil {
				return edge, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			continue
		}
		edge, err := engine.GetEdge(id)
		if err == nil {
			return edge, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCVisibilityEngine); ok {
			edge, err := provider.GetEdgeVisibleAt(id, version)
			if err == nil {
				return edge, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			continue
		}
		edge, err := engine.GetEdge(id)
		if err == nil {
			return edge, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// --- MVCC heads: route by ID to the owning constituent.

func (c *CompositeEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCHeadEngine); ok {
			head, err := provider.GetNodeCurrentHead(id)
			if err == nil {
				return head, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return head, err
			}
		}
	}
	return MVCCHead{}, ErrNotFound
}

func (c *CompositeEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCHeadEngine); ok {
			head, err := provider.GetEdgeCurrentHead(id)
			if err == nil {
				return head, nil
			}
			if !errors.Is(err, ErrNotFound) {
				return head, err
			}
		}
	}
	return MVCCHead{}, ErrNotFound
}

// --- MVCC indexed visibility: merge across all readable constituents.

func (c *CompositeEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	var merged []*Node
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
			nodes, err := provider.GetNodesByLabelVisibleAt(label, version)
			if err != nil {
				return nil, err
			}
			merged = append(merged, nodes...)
			continue
		}
		nodes, err := engine.GetNodesByLabel(label)
		if err != nil {
			return nil, err
		}
		merged = append(merged, nodes...)
	}
	return merged, nil
}

func (c *CompositeEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	var merged []*Edge
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
			edges, err := provider.GetOutgoingEdgesVisibleAt(nodeID, version)
			if err != nil {
				return nil, err
			}
			merged = append(merged, edges...)
			continue
		}
		edges, err := engine.GetOutgoingEdges(nodeID)
		if err != nil {
			return nil, err
		}
		merged = append(merged, edges...)
	}
	return merged, nil
}

func (c *CompositeEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	var merged []*Edge
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
			edges, err := provider.GetIncomingEdgesVisibleAt(nodeID, version)
			if err != nil {
				return nil, err
			}
			merged = append(merged, edges...)
			continue
		}
		edges, err := engine.GetIncomingEdges(nodeID)
		if err != nil {
			return nil, err
		}
		merged = append(merged, edges...)
	}
	return merged, nil
}

func (c *CompositeEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	var merged []*Edge
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
			edges, err := provider.GetEdgesByTypeVisibleAt(edgeType, version)
			if err != nil {
				return nil, err
			}
			merged = append(merged, edges...)
			continue
		}
		edges, err := engine.GetEdgesByType(edgeType)
		if err != nil {
			return nil, err
		}
		merged = append(merged, edges...)
	}
	return merged, nil
}

func (c *CompositeEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	var merged []*Edge
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
			edges, err := provider.GetEdgesBetweenVisibleAt(startID, endID, version)
			if err != nil {
				return nil, err
			}
			merged = append(merged, edges...)
			continue
		}
		edges, err := engine.GetEdgesBetween(startID, endID)
		if err != nil {
			return nil, err
		}
		merged = append(merged, edges...)
	}
	return merged, nil
}

// --- Label ID iteration merges constituents with deduplication.

func (c *CompositeEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	seen := make(map[NodeID]struct{})
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		stop := false
		if lookup, ok := engine.(LabelNodeIDLookupEngine); ok {
			err = lookup.ForEachNodeIDByLabel(label, func(id NodeID) bool {
				if _, exists := seen[id]; exists {
					return true
				}
				seen[id] = struct{}{}
				if !visit(id) {
					stop = true
					return false
				}
				return true
			})
		} else {
			nodes, listErr := engine.GetNodesByLabel(label)
			if listErr != nil {
				return listErr
			}
			for _, node := range nodes {
				if node == nil {
					continue
				}
				if _, exists := seen[node.ID]; exists {
					continue
				}
				seen[node.ID] = struct{}{}
				if !visit(node.ID) {
					stop = true
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

// --- Projection, iteration, embedding and mutation versions route across
// constituents like their non-projected counterparts.

func (c *CompositeEngine) GetNodeProjected(id NodeID, properties []string) (*Node, error) {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		var node *Node
		if reader, ok := engine.(NodeProjectionReader); ok {
			node, err = reader.GetNodeProjected(id, properties)
		} else {
			node, err = engine.GetNode(id)
			if err == nil {
				node = projectCachedNodeForRead(node, properties)
			}
		}
		if err == nil {
			return node, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) IterateNodes(fn func(*Node) bool) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if iterator, ok := engine.(NodeIterator); ok {
			stop := false
			if err := iterator.IterateNodes(func(node *Node) bool {
				if !fn(node) {
					stop = true
					return false
				}
				return true
			}); err != nil {
				return err
			}
			if stop {
				return nil
			}
			continue
		}
		nodes, err := engine.AllNodes()
		if err != nil {
			return err
		}
		for _, node := range nodes {
			if node != nil && !fn(node) {
				return nil
			}
		}
	}
	return nil
}

// UpdateNodeEmbeddingSidecar routes the embedding-only sidecar write to the
// constituent holding the node, without touching any node record.
func (c *CompositeEngine) UpdateNodeEmbeddingSidecar(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if _, err := engine.GetNode(node.ID); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}
		sidecar, ok := engine.(EmbeddingSidecarUpdater)
		if !ok {
			return fmt.Errorf("storage engine %T does not support embedding sidecar writes", engine)
		}
		return sidecar.UpdateNodeEmbeddingSidecar(node)
	}
	return ErrNotFound
}

// StreamParkedEmbeddingFailures aggregates sidecar failure scans across
// constituents.
func (c *CompositeEngine) StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error) {
	total := 0
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		streamer, ok := engine.(EmbeddingFailureStreamer)
		if !ok {
			continue
		}
		count, err := streamer.StreamParkedEmbeddingFailures(ctx, visit)
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}

// --- Aggregate reads and broadcasts.

func (c *CompositeEngine) PendingEmbeddingsCount() int {
	total := 0
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(EmbeddingCountProvider); ok {
			total += provider.PendingEmbeddingsCount()
		}
	}
	return total
}

func (c *CompositeEngine) ListNamespaces() []string {
	seen := make(map[string]struct{})
	var namespaces []string
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if lister, ok := engine.(NamespaceLister); ok {
			for _, namespace := range lister.ListNamespaces() {
				if _, exists := seen[namespace]; exists {
					continue
				}
				seen[namespace] = struct{}{}
				namespaces = append(namespaces, namespace)
			}
		}
	}
	return namespaces
}

func (c *CompositeEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if c.constituentNames[alias] != namespace {
			continue
		}
		if provider, ok := engine.(NamespaceSchemaProvider); ok {
			if schema := provider.GetSchemaForNamespace(namespace); schema != nil {
				return schema
			}
		}
	}
	return nil
}

func (c *CompositeEngine) GraphMutationVersion() (uint64, bool) {
	var total uint64
	supported := false
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(GraphMutationVersionProvider); ok {
			version, ok := provider.GraphMutationVersion()
			if ok {
				supported = true
				total += version
			}
		}
	}
	return total, supported
}

func (c *CompositeEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	consumed := false
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(StartupMaintenanceStateEngine); ok {
			got, err := provider.ConsumeCleanShutdownMarker(ctx)
			if err != nil {
				return false, err
			}
			consumed = consumed || got
		}
	}
	return consumed, nil
}

func (c *CompositeEngine) MarkCleanShutdown(ctx context.Context) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if provider, ok := engine.(StartupMaintenanceStateEngine); ok {
			if err := provider.MarkCleanShutdown(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CompositeEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if maint, ok := engine.(MVCCMaintenanceEngine); ok {
			pruned, err := maint.PruneMVCCVersions(ctx, opts)
			if err != nil {
				return total, err
			}
			total += pruned
		}
	}
	return total, nil
}

func (c *CompositeEngine) RebuildMVCCHeads(ctx context.Context) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if maint, ok := engine.(MVCCMaintenanceEngine); ok {
			if err := maint.RebuildMVCCHeads(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CompositeEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if maint, ok := engine.(TemporalMaintenanceEngine); ok {
			pruned, err := maint.PruneTemporalHistory(ctx, opts)
			if err != nil {
				return total, err
			}
			total += pruned
		}
	}
	return total, nil
}

func (c *CompositeEngine) RebuildTemporalIndexes(ctx context.Context) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if maint, ok := engine.(TemporalMaintenanceEngine); ok {
			if err := maint.RebuildTemporalIndexes(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- MVCC lifecycle broadcasts.

func (c *CompositeEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	releases := make([]func(), 0, len(c.getConstituentsForRead()))
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			releases = append(releases, lifecycle.RegisterSnapshotReader(info))
		}
	}
	return func() {
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}
}

func (c *CompositeEngine) LifecycleStatus() map[string]interface{} {
	status := make(map[string]interface{})
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			for key, value := range lifecycle.LifecycleStatus() {
				status[key] = value
			}
		}
	}
	return status
}

func (c *CompositeEngine) TriggerPruneNow(ctx context.Context) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			if err := lifecycle.TriggerPruneNow(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CompositeEngine) PauseLifecycle() {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			lifecycle.PauseLifecycle()
		}
	}
}

func (c *CompositeEngine) ResumeLifecycle() {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			lifecycle.ResumeLifecycle()
		}
	}
}

func (c *CompositeEngine) SetLifecycleSchedule(interval time.Duration) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		if schedule, ok := engine.(MVCCLifecycleScheduleEngine); ok {
			if err := schedule.SetLifecycleSchedule(interval); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- Event registration broadcasts to every readable constituent.

// compositeEventTarget is one unique underlying engine plus the namespaced
// views through which its events must be translated.
type compositeEventTarget struct {
	engine Engine
	views  []*NamespacedEngine
}

// eventTargets deduplicates the readable constituents by their underlying
// engine. Constituents that share one engine (the common multi-namespace
// deployment) get a single registration whose wrapper translates through
// every namespace view, so the last-registered view can never shadow the
// others and event delivery does not depend on map iteration order.
func (c *CompositeEngine) eventTargets() []compositeEventTarget {
	targets := make([]compositeEventTarget, 0, len(c.getConstituentsForRead()))
	seen := make(map[Engine]int)
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		var view *NamespacedEngine
		if ns, ok := engine.(*NamespacedEngine); ok {
			view = ns
			engine = ns.GetInnerEngine()
		}
		if idx, ok := seen[engine]; ok {
			if view != nil {
				targets[idx].views = append(targets[idx].views, view)
			}
			continue
		}
		target := compositeEventTarget{engine: engine}
		if view != nil {
			target.views = []*NamespacedEngine{view}
		}
		seen[engine] = len(targets)
		targets = append(targets, target)
	}
	return targets
}

func (c *CompositeEngine) OnNodeCreated(callback NodeEventCallback) {
	for _, target := range c.eventTargets() {
		notifier, ok := target.engine.(StorageEventNotifier)
		if !ok {
			continue
		}
		if len(target.views) == 0 {
			notifier.OnNodeCreated(callback)
			continue
		}
		notifier.OnNodeCreated(func(node *Node) {
			if node == nil {
				return
			}
			for _, view := range target.views {
				if view.hasNodePrefix(node.ID) {
					callback(view.toUserNode(node))
					return
				}
			}
		})
	}
}

func (c *CompositeEngine) OnNodeUpdated(callback NodeEventCallback) {
	for _, target := range c.eventTargets() {
		notifier, ok := target.engine.(StorageEventNotifier)
		if !ok {
			continue
		}
		if len(target.views) == 0 {
			notifier.OnNodeUpdated(callback)
			continue
		}
		notifier.OnNodeUpdated(func(node *Node) {
			if node == nil {
				return
			}
			for _, view := range target.views {
				if view.hasNodePrefix(node.ID) {
					callback(view.toUserNode(node))
					return
				}
			}
		})
	}
}

func (c *CompositeEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	for _, target := range c.eventTargets() {
		notifier, ok := target.engine.(StorageEventNotifier)
		if !ok {
			continue
		}
		if len(target.views) == 0 {
			notifier.OnNodeDeleted(callback)
			continue
		}
		notifier.OnNodeDeleted(func(nodeID NodeID) {
			for _, view := range target.views {
				if view.hasNodePrefix(nodeID) {
					callback(view.unprefixNodeID(nodeID))
					return
				}
			}
		})
	}
}

func (c *CompositeEngine) OnEdgeCreated(callback EdgeEventCallback) {
	for _, target := range c.eventTargets() {
		notifier, ok := target.engine.(StorageEventNotifier)
		if !ok {
			continue
		}
		if len(target.views) == 0 {
			notifier.OnEdgeCreated(callback)
			continue
		}
		notifier.OnEdgeCreated(func(edge *Edge) {
			if edge == nil {
				return
			}
			for _, view := range target.views {
				if view.hasEdgePrefix(edge.ID) {
					callback(view.toUserEdge(edge))
					return
				}
			}
		})
	}
}

func (c *CompositeEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	for _, target := range c.eventTargets() {
		notifier, ok := target.engine.(StorageEventNotifier)
		if !ok {
			continue
		}
		if len(target.views) == 0 {
			notifier.OnEdgeUpdated(callback)
			continue
		}
		notifier.OnEdgeUpdated(func(edge *Edge) {
			if edge == nil {
				return
			}
			for _, view := range target.views {
				if view.hasEdgePrefix(edge.ID) {
					callback(view.toUserEdge(edge))
					return
				}
			}
		})
	}
}

func (c *CompositeEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	for _, target := range c.eventTargets() {
		notifier, ok := target.engine.(StorageEventNotifier)
		if !ok {
			continue
		}
		if len(target.views) == 0 {
			notifier.OnEdgeDeleted(callback)
			continue
		}
		notifier.OnEdgeDeleted(func(edgeID EdgeID) {
			for _, view := range target.views {
				if view.hasEdgePrefix(edgeID) {
					callback(view.unprefixEdgeID(edgeID))
					return
				}
			}
		})
	}
}

// GetInnerEngine returns the composite itself: a composite spans several
// constituent engines, so there is no single inner engine to unwrap. Callers
// that unwrap for capability probes get the same engine back and fall through
// to its own capability methods.
func (c *CompositeEngine) GetInnerEngine() Engine {
	return c
}
