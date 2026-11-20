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
			return err
		}
		if err := engine.StreamNodesWithOptions(ctx, opts, func(node *Node) error {
			if stopped {
				return nil
			}
			if err := fn(c.sourceNode(alias, engine, node)); err != nil {
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
//
// The provider branch and its ordinary fallback stay in each typed adapter's
// read closure: a provider that reports not-found moves to the next source, it
// never retries the same source through the plain read.

// GetNodeLatestEffective routes latest-effective node reads to the first
// constituent that resolves the ID.
func (c *CompositeEngine) GetNodeLatestEffective(ctx context.Context, id NodeID) (*Node, error) {
	aliases, _, local, _, err := c.constituentsForEntity("4", string(id))
	if err != nil {
		return nil, err
	}
	localID := NodeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Node, error) {
			if provider, ok := engine.(MVCCLatestEffectiveEngine); ok {
				return provider.GetNodeLatestEffective(ctx, localID)
			}
			return engine.GetNode(ctx, localID)
		},
		func(node *Node, alias string, engine Engine) *Node { return c.sourceNode(alias, engine, node) },
	)
}

// GetEdgeLatestEffective routes latest-effective edge reads to the first
// constituent that resolves the ID.
func (c *CompositeEngine) GetEdgeLatestEffective(ctx context.Context, id EdgeID) (*Edge, error) {
	aliases, _, local, _, err := c.constituentsForEntity("5", string(id))
	if err != nil {
		return nil, err
	}
	localID := EdgeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Edge, error) {
			if provider, ok := engine.(MVCCLatestEffectiveEngine); ok {
				return provider.GetEdgeLatestEffective(ctx, localID)
			}
			return engine.GetEdge(ctx, localID)
		},
		func(edge *Edge, alias string, engine Engine) *Edge { return c.sourceEdge(alias, engine, edge) },
	)
}

func (c *CompositeEngine) GetNodeLatestVisible(ctx context.Context, id NodeID) (*Node, error) {
	aliases, _, local, _, err := c.constituentsForEntity("4", string(id))
	if err != nil {
		return nil, err
	}
	localID := NodeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Node, error) {
			if provider, ok := engine.(MVCCVisibilityEngine); ok {
				return provider.GetNodeLatestVisible(ctx, localID)
			}
			return engine.GetNode(ctx, localID)
		},
		func(node *Node, alias string, engine Engine) *Node { return c.sourceNode(alias, engine, node) },
	)
}

func (c *CompositeEngine) GetNodeVisibleAt(ctx context.Context, id NodeID, version MVCCVersion) (*Node, error) {
	aliases, _, local, _, err := c.constituentsForEntity("4", string(id))
	if err != nil {
		return nil, err
	}
	localID := NodeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Node, error) {
			if provider, ok := engine.(MVCCVisibilityEngine); ok {
				return provider.GetNodeVisibleAt(ctx, localID, version)
			}
			return engine.GetNode(ctx, localID)
		},
		func(node *Node, alias string, engine Engine) *Node { return c.sourceNode(alias, engine, node) },
	)
}

func (c *CompositeEngine) GetEdgeLatestVisible(ctx context.Context, id EdgeID) (*Edge, error) {
	aliases, _, local, _, err := c.constituentsForEntity("5", string(id))
	if err != nil {
		return nil, err
	}
	localID := EdgeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Edge, error) {
			if provider, ok := engine.(MVCCVisibilityEngine); ok {
				return provider.GetEdgeLatestVisible(ctx, localID)
			}
			return engine.GetEdge(ctx, localID)
		},
		func(edge *Edge, alias string, engine Engine) *Edge { return c.sourceEdge(alias, engine, edge) },
	)
}

func (c *CompositeEngine) GetEdgeVisibleAt(ctx context.Context, id EdgeID, version MVCCVersion) (*Edge, error) {
	aliases, _, local, _, err := c.constituentsForEntity("5", string(id))
	if err != nil {
		return nil, err
	}
	localID := EdgeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Edge, error) {
			if provider, ok := engine.(MVCCVisibilityEngine); ok {
				return provider.GetEdgeVisibleAt(ctx, localID, version)
			}
			return engine.GetEdge(ctx, localID)
		},
		func(edge *Edge, alias string, engine Engine) *Edge { return c.sourceEdge(alias, engine, edge) },
	)
}

// --- MVCC heads: route by ID to the owning constituent. The head API publishes
// the failing source's head beside a non-not-found error, so the lookup keeps
// that value.

func (c *CompositeEngine) GetNodeCurrentHead(ctx context.Context, id NodeID) (MVCCHead, error) {
	aliases, _, local, _, err := c.constituentsForEntity("4", string(id))
	if err != nil {
		return MVCCHead{}, err
	}
	localID := NodeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, true,
		func(engine Engine) (MVCCHead, error) {
			if provider, ok := engine.(MVCCHeadEngine); ok {
				return provider.GetNodeCurrentHead(ctx, localID)
			}
			return MVCCHead{}, ErrNotFound
		},
		nil,
	)
}

func (c *CompositeEngine) GetEdgeCurrentHead(ctx context.Context, id EdgeID) (MVCCHead, error) {
	aliases, _, local, _, err := c.constituentsForEntity("5", string(id))
	if err != nil {
		return MVCCHead{}, err
	}
	localID := EdgeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, true,
		func(engine Engine) (MVCCHead, error) {
			if provider, ok := engine.(MVCCHeadEngine); ok {
				return provider.GetEdgeCurrentHead(ctx, localID)
			}
			return MVCCHead{}, ErrNotFound
		},
		nil,
	)
}

// --- MVCC indexed visibility: merge across all readable constituents.

func (c *CompositeEngine) GetNodesByLabelVisibleAt(ctx context.Context, label string, version MVCCVersion) ([]*Node, error) {
	return materializeConstituents(c, ctx, c.getConstituentsForRead(),
		func(engine Engine) ([]*Node, error) {
			if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
				return provider.GetNodesByLabelVisibleAt(ctx, label, version)
			}
			return engine.GetNodesByLabel(ctx, label)
		},
		nodeFromDatabase,
		compositeFanOutPublishesRaw,
	)
}

// ReadAdjacentEdgesVisibleAt answers one adjacent-edge request at version from
// the constituents that can resolve it, one direction per call.
func (c *CompositeEngine) ReadAdjacentEdgesVisibleAt(ctx context.Context, request AdjacentEdgeRead, version MVCCVersion) ([]*Edge, error) {
	if request.Direction == AdjacentBoth {
		return readBothAdjacentEdges(ctx, request, func(direction AdjacentEdgeRead) ([]*Edge, error) {
			return c.ReadAdjacentEdgesVisibleAt(ctx, direction, version)
		})
	}
	visible := func(engine Engine, local AdjacentEdgeRead) ([]*Edge, error) {
		if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
			return provider.ReadAdjacentEdgesVisibleAt(ctx, local, version)
		}
		return engine.ReadAdjacentEdges(ctx, local)
	}
	if request.Other != "" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if engine, start, end, qualified, err := c.constituentForNodePair(request.Node, request.Other); qualified {
			if err != nil {
				return nil, err
			}
			if engine == nil {
				return nil, nil
			}
			local := request
			local.Node, local.Other = start, end
			edges, err := visible(engine, local)
			projected, projectionErr := entitiesFromDatabase(ctx, edges, nodePairDatabase(request.Node, request.Other))
			if projectionErr != nil {
				return nil, projectionErr
			}
			return projected, err
		}
		return materializeConstituents(c, ctx, c.getConstituentsForRead(),
			func(engine Engine) ([]*Edge, error) { return visible(engine, request) },
			edgeFromDatabase,
			compositeFanOutPublishesRaw,
		)
	}
	aliases, _, local, _, err := c.constituentsForEntity("4", string(request.Node))
	if err != nil {
		return nil, err
	}
	request.Node = NodeID(local)

	return materializeConstituents(c, ctx, aliases,
		func(engine Engine) ([]*Edge, error) { return visible(engine, request) },
		edgeFromDatabase,
		compositeFanOutPublishesRaw,
	)
}

func (c *CompositeEngine) GetEdgesByTypeVisibleAt(ctx context.Context, edgeType string, version MVCCVersion) ([]*Edge, error) {
	return materializeConstituents(c, ctx, c.getConstituentsForRead(),
		func(engine Engine) ([]*Edge, error) {
			if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
				return provider.GetEdgesByTypeVisibleAt(ctx, edgeType, version)
			}
			return engine.GetEdgesByType(ctx, edgeType)
		},
		edgeFromDatabase,
		compositeFanOutPublishesRaw,
	)
}

// --- Label ID iteration merges constituents with deduplication.

func (c *CompositeEngine) ForEachNodeIDByLabel(ctx context.Context, label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		stop := false
		if lookup, ok := engine.(LabelNodeIDLookupEngine); ok {
			err = lookup.ForEachNodeIDByLabel(ctx, label, func(id NodeID) bool {
				if !visit(c.constituentNodeElementID(alias, engine, id)) {
					stop = true
					return false
				}
				return true
			})
		} else {
			nodes, listErr := engine.GetNodesByLabel(ctx, label)
			if listErr != nil {
				return listErr
			}
			for _, node := range nodes {
				if node == nil {
					continue
				}
				if !visit(c.constituentNodeElementID(alias, engine, node.ID)) {
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

func (c *CompositeEngine) GetNodeProjected(ctx context.Context, id NodeID, properties []string) (*Node, error) {
	aliases, _, local, _, err := c.constituentsForEntity("4", string(id))
	if err != nil {
		return nil, err
	}
	localID := NodeID(local)
	return readFirstConstituent(c, aliases, constituentNotFoundWrapped, false,
		func(engine Engine) (*Node, error) {
			return getNodeProjectedThrough(ctx, engine, localID, properties)
		},
		func(node *Node, alias string, engine Engine) *Node { return c.sourceNode(alias, engine, node) },
	)
}

func (c *CompositeEngine) IterateNodes(fn func(*Node) bool) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		if iterator, ok := engine.(NodeIterator); ok {
			stop := false
			if err := iterator.IterateNodes(func(node *Node) bool {
				if !fn(c.sourceNode(alias, engine, node)) {
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
		// IterateNodes keeps its existing public API (no caller context): the
		// constituent walk runs on context.Background at this context-free
		// entrance, not on a forwarded default.
		nodes, err := engine.AllNodes(context.Background())
		if err != nil {
			return err
		}
		for _, node := range nodes {
			if node != nil && !fn(c.sourceNode(alias, engine, node)) {
				return nil
			}
		}
	}
	return nil
}

// UpdateNodeEmbeddingSidecar routes the embedding-only sidecar write to the
// constituent holding the node, without touching any node record. It belongs to
// this file's maintenance class: the derived embedding state is written through
// the readable participant that holds the node, not through the constituent's
// writer admission, because the node record itself is never mutated.
func (c *CompositeEngine) UpdateNodeEmbeddingSidecar(ctx context.Context, node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	return writeFirstConstituent(c, c.getConstituentsForRead(), constituentNotFoundWrapped, constituentWriteMaintenance,
		func(engine Engine) error {
			_, err := engine.GetNode(ctx, node.ID)
			return err
		},
		func(engine Engine) error {
			sidecar, ok := engine.(EmbeddingSidecarUpdater)
			if !ok {
				return fmt.Errorf("storage engine %T does not support embedding sidecar writes", engine)
			}
			return sidecar.UpdateNodeEmbeddingSidecar(ctx, node)
		},
		ErrNotFound,
	)
}

// --- Capability broadcast and sum walks.

// constituentAccess selects what a broadcast or sum walk does with a source it
// cannot open.
type constituentAccess int

const (
	// constituentAccessRequired fails the whole operation on an access error.
	constituentAccessRequired constituentAccess = iota
	// constituentAccessOptional skips an unreadable source.
	constituentAccessOptional
)

// walkReadableConstituents is the single owner of the composite's capability
// broadcast and sum walks: it selects each readable source in the composite's
// order and opens it once. The visitor owns the per-source contribution, and a
// visitor error stops the walk with the accumulated state intact.
func walkReadableConstituents(c *CompositeEngine, access constituentAccess, visit func(alias string, engine Engine) error) error {
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			// Access failure is fatal unless this walk is explicitly partial.
			if access == constituentAccessRequired {
				return err
			}
			continue
		}
		if err := visit(alias, engine); err != nil {
			return err
		}
	}
	return nil
}

// StreamParkedEmbeddingFailures aggregates sidecar failure scans across
// constituents.
func (c *CompositeEngine) StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error) {
	total := 0
	err := walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		streamer, ok := engine.(EmbeddingFailureStreamer)
		if !ok {
			return nil
		}
		count, err := streamer.StreamParkedEmbeddingFailures(ctx, visit)
		if err != nil {
			return err
		}
		total += count
		return nil
	})
	return total, err
}

// --- Aggregate reads and broadcasts.

func (c *CompositeEngine) PendingEmbeddingsCount() int {
	total := 0
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		if provider, ok := engine.(EmbeddingCountProvider); ok {
			total += provider.PendingEmbeddingsCount()
		}
		return nil
	})
	return total
}

func (c *CompositeEngine) ListNamespaces() []string {
	seen := make(map[string]struct{})
	var namespaces []string
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		lister, ok := engine.(NamespaceLister)
		if !ok {
			return nil
		}
		for _, namespace := range lister.ListNamespaces() {
			if _, exists := seen[namespace]; exists {
				continue
			}
			seen[namespace] = struct{}{}
			namespaces = append(namespaces, namespace)
		}
		return nil
	})
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
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		provider, ok := engine.(GraphMutationVersionProvider)
		if !ok {
			return nil
		}
		version, ok := provider.GraphMutationVersion()
		if ok {
			supported = true
			total += version
		}
		return nil
	})
	return total, supported
}

func (c *CompositeEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	consumed := false
	err := walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		provider, ok := engine.(StartupMaintenanceStateEngine)
		if !ok {
			return nil
		}
		got, err := provider.ConsumeCleanShutdownMarker(ctx)
		if err != nil {
			return err
		}
		consumed = consumed || got
		return nil
	})
	if err != nil {
		return false, err
	}
	return consumed, nil
}

func (c *CompositeEngine) MarkCleanShutdown(ctx context.Context) error {
	return walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		provider, ok := engine.(StartupMaintenanceStateEngine)
		if !ok {
			return nil
		}
		return provider.MarkCleanShutdown(ctx)
	})
}

func (c *CompositeEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	var total int64
	err := walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		maint, ok := engine.(MVCCMaintenanceEngine)
		if !ok {
			return nil
		}
		pruned, err := maint.PruneMVCCVersions(ctx, opts)
		if err != nil {
			return err
		}
		total += pruned
		return nil
	})
	return total, err
}

func (c *CompositeEngine) RebuildMVCCHeads(ctx context.Context) error {
	return walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		maint, ok := engine.(MVCCMaintenanceEngine)
		if !ok {
			return nil
		}
		return maint.RebuildMVCCHeads(ctx)
	})
}

func (c *CompositeEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	var total int64
	err := walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		maint, ok := engine.(TemporalMaintenanceEngine)
		if !ok {
			return nil
		}
		pruned, err := maint.PruneTemporalHistory(ctx, opts)
		if err != nil {
			return err
		}
		total += pruned
		return nil
	})
	return total, err
}

func (c *CompositeEngine) RebuildTemporalIndexes(ctx context.Context) error {
	return walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		maint, ok := engine.(TemporalMaintenanceEngine)
		if !ok {
			return nil
		}
		return maint.RebuildTemporalIndexes(ctx)
	})
}

// --- MVCC lifecycle broadcasts.

func (c *CompositeEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	releases := make([]func(), 0, len(c.getConstituentsForRead()))
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			releases = append(releases, lifecycle.RegisterSnapshotReader(info))
		}
		return nil
	})
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
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		lifecycle, ok := engine.(MVCCLifecycleEngine)
		if !ok {
			return nil
		}
		for key, value := range lifecycle.LifecycleStatus() {
			status[key] = value
		}
		return nil
	})
	return status
}

func (c *CompositeEngine) TriggerPruneNow(ctx context.Context) error {
	return walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		lifecycle, ok := engine.(MVCCLifecycleEngine)
		if !ok {
			return nil
		}
		return lifecycle.TriggerPruneNow(ctx)
	})
}

func (c *CompositeEngine) PauseLifecycle() {
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			lifecycle.PauseLifecycle()
		}
		return nil
	})
}

func (c *CompositeEngine) ResumeLifecycle() {
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		if lifecycle, ok := engine.(MVCCLifecycleEngine); ok {
			lifecycle.ResumeLifecycle()
		}
		return nil
	})
}

func (c *CompositeEngine) SetLifecycleSchedule(interval time.Duration) error {
	return walkReadableConstituents(c, constituentAccessRequired, func(_ string, engine Engine) error {
		schedule, ok := engine.(MVCCLifecycleScheduleEngine)
		if !ok {
			return nil
		}
		return schedule.SetLifecycleSchedule(interval)
	})
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
	_ = walkReadableConstituents(c, constituentAccessOptional, func(_ string, engine Engine) error {
		var view *NamespacedEngine
		if ns, ok := engine.(*NamespacedEngine); ok {
			view = ns
			engine = ns.GetInnerEngine()
		}
		if idx, ok := seen[engine]; ok {
			if view != nil {
				targets[idx].views = append(targets[idx].views, view)
			}
			return nil
		}
		target := storageEventTarget{engine: engine}
		if view != nil {
			target.views = []*NamespacedEngine{view}
		}
		seen[engine] = len(targets)
		targets = append(targets, target)
		return nil
	})
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
