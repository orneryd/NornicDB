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
	"strings"
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
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	for _, alias := range readConstituents {
		if err := ctx.Err(); err != nil {
			return err
		}
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		database := c.constituentDatabase(alias, engine)
		if err := engine.StreamNodesWithOptions(ctx, opts, func(node *Node) error {
			if stopped {
				return nil
			}
			if err := fn(nodeFromDatabase(node, database)); err != nil {
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
	aliases, local, _ := c.constituentsForEntity("4", string(id))
	id = NodeID(local)
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		value, err := ReadWithCapability(engine, func(reader MVCCLatestEffectiveEngine) (*Node, error) { return reader.GetNodeLatestEffective(id) }, func() (*Node, error) { return engine.GetNode(id) })
		if err == nil {
			return c.sourceNode(alias, engine, value), nil
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
	aliases, local, _ := c.constituentsForEntity("5", string(id))
	id = EdgeID(local)
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		value, err := ReadWithCapability(engine, func(reader MVCCLatestEffectiveEngine) (*Edge, error) { return reader.GetEdgeLatestEffective(id) }, func() (*Edge, error) { return engine.GetEdge(id) })
		if err == nil {
			return c.sourceEdge(alias, engine, value), nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	aliases, local, _ := c.constituentsForEntity("4", string(id))
	id = NodeID(local)
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		value, err := ReadWithCapability(engine, func(reader MVCCVisibilityEngine) (*Node, error) { return reader.GetNodeLatestVisible(id) }, func() (*Node, error) { return engine.GetNode(id) })
		if err == nil {
			return c.sourceNode(alias, engine, value), nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	aliases, local, _ := c.constituentsForEntity("4", string(id))
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		value, err := ReadWithCapability(engine, func(reader snapshotNode) (*Node, error) {
			return reader.GetNodeVisibleAt(NodeID(local), version)
		}, nil)
		if err == nil {
			return c.sourceNode(alias, engine, value), nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	aliases, local, _ := c.constituentsForEntity("5", string(id))
	id = EdgeID(local)
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		value, err := ReadWithCapability(engine, func(reader MVCCVisibilityEngine) (*Edge, error) { return reader.GetEdgeLatestVisible(id) }, func() (*Edge, error) { return engine.GetEdge(id) })
		if err == nil {
			return c.sourceEdge(alias, engine, value), nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	aliases, local, _ := c.constituentsForEntity("5", string(id))
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		value, err := ReadWithCapability(engine, func(reader snapshotEdge) (*Edge, error) {
			return reader.GetEdgeVisibleAt(EdgeID(local), version)
		}, nil)
		if err == nil {
			return c.sourceEdge(alias, engine, value), nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// --- MVCC heads: route by ID to the owning constituent.

func (c *CompositeEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	aliases, local, _ := c.constituentsForEntity("4", string(id))
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return MVCCHead{}, err
		}
		head, err := ReadWithCapability(engine, func(reader nodeHead) (MVCCHead, error) { return reader.GetNodeCurrentHead(NodeID(local)) }, nil)
		if err == nil {
			return head, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return MVCCHead{}, err
		}
	}
	return MVCCHead{}, ErrNotFound
}

func (c *CompositeEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	aliases, local, _ := c.constituentsForEntity("5", string(id))
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return MVCCHead{}, err
		}
		head, err := ReadWithCapability(engine, func(reader edgeHead) (MVCCHead, error) { return reader.GetEdgeCurrentHead(EdgeID(local)) }, nil)
		if err == nil {
			return head, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return MVCCHead{}, err
		}
	}
	return MVCCHead{}, ErrNotFound
}

// --- MVCC indexed visibility: merge across all readable constituents.

func (c *CompositeEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	return c.GetNodesByLabelVisibleAtWithContext(context.Background(), label, version)
}

// GetNodesByLabelVisibleAtWithContext carries the caller through this read and its materialization.
func (c *CompositeEngine) GetNodesByLabelVisibleAtWithContext(ctx context.Context, label string, version MVCCVersion) ([]*Node, error) {
	aliases, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return nil, err
	}
	var merged []*Node
	for _, alias := range aliases {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		values, err := ReadWithCapability(engine, func(reader historicalLabelNodes) ([]*Node, error) {
			return GetNodesByLabelVisibleAtWithContext(ctx, reader, label, version)
		}, nil)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged = append(merged, c.sourceNode(alias, engine, value))
		}
	}
	return merged, nil
}

func (c *CompositeEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	aliases, local, _ := c.constituentsForEntity("4", string(nodeID))
	nodeID = NodeID(local)
	var merged []*Edge
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		values, err := ReadWithCapability(engine, func(reader historicalOutgoingEdges) ([]*Edge, error) {
			return reader.GetOutgoingEdgesVisibleAt(nodeID, version)
		}, nil)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged = append(merged, c.sourceEdge(alias, engine, value))
		}
	}
	return merged, nil
}

func (c *CompositeEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	aliases, local, _ := c.constituentsForEntity("4", string(nodeID))
	nodeID = NodeID(local)
	var merged []*Edge
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		values, err := ReadWithCapability(engine, func(reader historicalIncomingEdges) ([]*Edge, error) {
			return reader.GetIncomingEdgesVisibleAt(nodeID, version)
		}, nil)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged = append(merged, c.sourceEdge(alias, engine, value))
		}
	}
	return merged, nil
}

func (c *CompositeEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	aliases := c.getConstituentsForRead()
	var merged []*Edge
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		values, err := ReadWithCapability(engine, func(reader historicalTypedEdges) ([]*Edge, error) {
			return reader.GetEdgesByTypeVisibleAt(edgeType, version)
		}, nil)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged = append(merged, c.sourceEdge(alias, engine, value))
		}
	}
	return merged, nil
}

func (c *CompositeEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	if engine, start, end, qualified, err := c.constituentForNodePair(startID, endID); qualified {
		if err != nil {
			return nil, err
		}
		if engine == nil {
			return nil, nil
		}
		edges, err := ReadWithCapability(engine, func(reader historicalBetweenEdges) ([]*Edge, error) {
			return reader.GetEdgesBetweenVisibleAt(start, end, version)
		}, nil)
		if err != nil {
			return nil, err
		}
		return edgesFromDatabase(edges, nodePairDatabase(startID, endID)), nil
	}
	aliases := c.getConstituentsForRead()
	var merged []*Edge
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		values, err := ReadWithCapability(engine, func(reader historicalBetweenEdges) ([]*Edge, error) {
			return reader.GetEdgesBetweenVisibleAt(startID, endID, version)
		}, nil)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged = append(merged, c.sourceEdge(alias, engine, value))
		}
	}
	return merged, nil
}

// --- Label ID iteration merges constituents with deduplication.

func (c *CompositeEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	return c.ForEachNodeIDByLabelWithContext(context.Background(), label, visit)
}

// ForEachNodeIDByLabelWithContext carries the caller through this read and its materialization.
func (c *CompositeEngine) ForEachNodeIDByLabelWithContext(ctx context.Context, label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	err := visitCompositeRead(ctx, c, func(reader LabelNodeIDLookupEngine, alias string, engine Engine, deliver func(NodeID) error) error {
		database := c.constituentDatabase(alias, engine)
		var stopped error
		err := ForEachNodeIDByLabelWithContext(ctx, reader, label, func(id NodeID) bool {
			stopped = deliver(NodeID(SourceNodeElementID(database, id)))
			return stopped == nil
		})
		if stopped != nil {
			return stopped
		}
		return err
	}, func(alias string, engine Engine, deliver func(NodeID) error) error {
		database := c.constituentDatabase(alias, engine)
		return (nodeReadOperation{engine, StreamNodesOptions{Label: label, Projection: []string{}, ApplyDecayFilter: true}}).visit(ctx, func(node *Node) error { return deliver(NodeID(SourceNodeElementID(database, node.ID))) })
	}, func(id NodeID) error {
		if !visit(id) {
			return ErrIterationStopped
		}
		return nil
	})
	if errors.Is(err, ErrIterationStopped) {
		return nil
	}
	return err
}

// --- Projection, iteration, embedding and mutation versions route across
// constituents like their non-projected counterparts.

func (c *CompositeEngine) GetNodeProjected(id NodeID, properties []string) (*Node, error) {
	aliases, local, _ := c.constituentsForEntity("4", string(id))
	id = NodeID(local)
	for _, alias := range aliases {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		node, err := getNodeProjectedThrough(engine, id, properties)
		if err == nil {
			return c.sourceNode(alias, engine, node), nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

func (c *CompositeEngine) IterateNodes(visit func(*Node) bool) error {
	if visit == nil {
		return ErrInvalidData
	}
	err := visitCompositeRead(context.Background(), c, func(reader NodeIterator, alias string, engine Engine, deliver func(*Node) error) error {
		database := c.constituentDatabase(alias, engine)
		var stopped error
		err := reader.IterateNodes(func(node *Node) bool { stopped = deliver(nodeFromDatabase(node, database)); return stopped == nil })
		if stopped != nil {
			return stopped
		}
		return err
	}, func(alias string, engine Engine, deliver func(*Node) error) error {
		database := c.constituentDatabase(alias, engine)
		return (nodeReadOperation{engine, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}}).visit(context.Background(), func(node *Node) error { return deliver(nodeFromDatabase(node, database)) })
	}, func(node *Node) error {
		if !visit(node) {
			return ErrIterationStopped
		}
		return nil
	})
	if errors.Is(err, ErrIterationStopped) {
		return nil
	}
	return err
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
			return err
		}
		if _, err := engine.GetNode(node.ID); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}
		sidecar, ok := OwnedCapability[EmbeddingSidecarUpdater](engine)
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
	if visit == nil {
		return 0, ErrInvalidData
	}
	type failure struct {
		id   NodeID
		meta map[string]any
	}
	count := 0
	err := visitCompositeRead(ctx, c, func(_ Engine, alias string, engine Engine, deliver func(failure) error) error {
		return VisitWithCapability(engine, func(reader EmbeddingFailureStreamer, next func(failure) error) error {
			_, err := reader.StreamParkedEmbeddingFailures(ctx, func(id NodeID, meta map[string]any) error {
				return next(failure{id: c.constituentNodeElementID(alias, engine, id), meta: meta})
			})
			return err
		}, nil, deliver)
	}, nil, func(row failure) error {
		count++
		return visit(row.id, row.meta)
	})
	return count, err
}

// --- Aggregate reads and broadcasts.

func (c *CompositeEngine) PendingEmbeddingsCount() int {
	total := 0
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		total += ReadValueWithCapability(engine, func(provider EmbeddingCountProvider) int { return provider.PendingEmbeddingsCount() }, nil)
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
		if lister, ok := ViewMetadataForEngine[NamespaceLister](engine); ok {
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
		if provider, ok := ViewMetadataForEngine[NamespaceSchemaProvider](engine); ok {
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
			return 0, false
		}
		if provider, ok := ViewMetadataForEngine[GraphMutationVersionProvider](engine); ok {
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
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return false, err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return false, err
		}
		if provider, ok := OwnedCapability[StartupMaintenanceStateEngine](engine); ok {
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
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if provider, ok := OwnedCapability[StartupMaintenanceStateEngine](engine); ok {
			if err := provider.MarkCleanShutdown(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CompositeEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	var total int64
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return 0, err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return total, err
		}
		if maint, ok := OwnedCapability[MVCCMaintenanceEngine](engine); ok {
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
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if maint, ok := OwnedCapability[MVCCMaintenanceEngine](engine); ok {
			if err := maint.RebuildMVCCHeads(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CompositeEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	var total int64
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return 0, err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return total, err
		}
		if maint, ok := OwnedCapability[TemporalMaintenanceEngine](engine); ok {
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
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if maint, ok := OwnedCapability[TemporalMaintenanceEngine](engine); ok {
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
		if lifecycle, ok := ViewMetadataForEngine[MVCCLifecycleEngine](engine); ok {
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
		if lifecycle, ok := ViewMetadataForEngine[MVCCLifecycleEngine](engine); ok {
			for key, value := range lifecycle.LifecycleStatus() {
				status[key] = value
			}
		}
	}
	return status
}

func (c *CompositeEngine) TriggerPruneNow(ctx context.Context) error {
	readConstituents, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if lifecycle, ok := OwnedCapability[MVCCLifecycleEngine](engine); ok {
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
		if lifecycle, ok := OwnedCapability[MVCCLifecycleEngine](engine); ok {
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
		if lifecycle, ok := OwnedCapability[MVCCLifecycleEngine](engine); ok {
			lifecycle.ResumeLifecycle()
		}
	}
}

func (c *CompositeEngine) SetLifecycleSchedule(interval time.Duration) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if schedule, ok := OwnedCapability[MVCCLifecycleScheduleEngine](engine); ok {
			if err := schedule.SetLifecycleSchedule(interval); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- Event registration broadcasts to every readable constituent.

// storageEventTarget is one unique underlying engine plus the namespaced
// views through which its events must be translated.
type storageEventTarget struct {
	engine Engine
	views  []*NamespacedEngine
}

// eventTargets deduplicates the readable constituents by their underlying
// engine. Constituents that share one engine (the common multi-namespace
// deployment) get a single registration whose wrapper translates through
// every namespace view, so the last-registered view can never shadow the
// others and event delivery does not depend on map iteration order.
func (c *CompositeEngine) eventTargets() []storageEventTarget {
	targets := make([]storageEventTarget, 0, len(c.getConstituentsForRead()))
	seen := make(map[Engine]int)
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		var view *NamespacedEngine
		if ns, ok := ViewMetadataForEngine[*NamespacedEngine](engine); ok {
			view = ns
			engine = ns.GetInnerEngine()
		}
		if idx, ok := seen[engine]; ok {
			if view != nil {
				targets[idx].views = append(targets[idx].views, view)
			}
			continue
		}
		target := storageEventTarget{engine: engine}
		if view != nil {
			target.views = []*NamespacedEngine{view}
		}
		seen[engine] = len(targets)
		targets = append(targets, target)
	}
	return targets
}

func (c *CompositeEngine) OnNodeCreated(callback NodeEventCallback) {
	subscribeStorageEvent(c.eventTargets(), callback, StorageEventNotifier.OnNodeCreated)
}
func (c *CompositeEngine) OnNodeUpdated(callback NodeEventCallback) {
	subscribeStorageEvent(c.eventTargets(), callback, StorageEventNotifier.OnNodeUpdated)
}
func (c *CompositeEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	subscribeStorageEvent(c.eventTargets(), callback, StorageEventNotifier.OnNodeDeleted)
}
func (c *CompositeEngine) OnEdgeCreated(callback EdgeEventCallback) {
	subscribeStorageEvent(c.eventTargets(), callback, StorageEventNotifier.OnEdgeCreated)
}
func (c *CompositeEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	subscribeStorageEvent(c.eventTargets(), callback, StorageEventNotifier.OnEdgeUpdated)
}
func (c *CompositeEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	subscribeStorageEvent(c.eventTargets(), callback, StorageEventNotifier.OnEdgeDeleted)
}

// GetInnerEngine returns the composite itself: a composite spans several
// constituent engines, so there is no single inner engine to unwrap. Callers
// that unwrap for capability probes get the same engine back and fall through
// to its own capability methods.
func (c *CompositeEngine) GetInnerEngine() Engine {
	return c
}

// NodeCountByLabel reduces each child's complete count operation independently.
// A child with no native label counter scans only its own bound view.
func (c *CompositeEngine) NodeCountByLabel(label string) (int64, error) {
	return c.countNodesByLabel(context.Background(), label)
}
func (c *CompositeEngine) countNodesByLabel(ctx context.Context, label string) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return total, err
		}
		count, err := CountNodesWithLabel(ctx, engine, label)
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}

// Prefix and namespace counters reduce child operations independently. Missing
// counters scan only that child's public view with the same relative selector.
func (c *CompositeEngine) NodeCountByPrefix(prefix string) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return total, err
		}
		count, err := ReadWithCapability(engine, func(counter interface{ NodeCountByPrefix(string) (int64, error) }) (int64, error) {
			return counter.NodeCountByPrefix(prefix)
		}, func() (int64, error) {
			var count int64
			err := (nodeReadOperation{engine, StreamNodesOptions{Prefix: prefix, Projection: []string{}, ApplyDecayFilter: true}}).visit(context.Background(), func(*Node) error { count++; return nil })
			return count, err
		})
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}
func (c *CompositeEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return total, err
		}
		count, err := ReadWithCapability(engine, func(counter interface{ EdgeCountByPrefix(string) (int64, error) }) (int64, error) {
			return counter.EdgeCountByPrefix(prefix)
		}, func() (int64, error) {
			var count int64
			err := StreamEdgesWithFallback(context.Background(), engine, 1000, func(edge *Edge) error {
				if edge != nil && strings.HasPrefix(string(edge.ID), prefix) {
					count++
				}
				return nil
			})
			return count, err
		})
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}
func (c *CompositeEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	var total int64
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return total, err
		}
		count, err := namespaceLabelCount(engine, namespace, label)
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}
