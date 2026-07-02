package storage

import (
	"context"
	"time"
)

// ListNamespaces returns known namespaces from the wrapped engine, if supported.
func (ae *AsyncEngine) ListNamespaces() []string {
	if lister, ok := ae.engine.(NamespaceLister); ok {
		return lister.ListNamespaces()
	}
	return nil
}

// IsCurrentTemporalNode delegates current-version checks to the wrapped engine when supported.
func (ae *AsyncEngine) IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error) {
	if provider, ok := ae.engine.(TemporalCurrentNodeEngine); ok {
		return provider.IsCurrentTemporalNode(node, asOf)
	}
	return true, nil
}

// RebuildTemporalIndexes delegates temporal index rebuild to the wrapped engine when supported.
func (ae *AsyncEngine) RebuildTemporalIndexes(ctx context.Context) error {
	if maint, ok := ae.engine.(TemporalMaintenanceEngine); ok {
		return maint.RebuildTemporalIndexes(ctx)
	}
	return nil
}

// PruneTemporalHistory delegates temporal pruning to the wrapped engine when supported.
func (ae *AsyncEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if maint, ok := ae.engine.(TemporalMaintenanceEngine); ok {
		return maint.PruneTemporalHistory(ctx, opts)
	}
	return 0, nil
}

// GetNodeLatestEffective returns the merged latest-visible node across pending, in-flight, and persisted state.
func (ae *AsyncEngine) GetNodeLatestEffective(id NodeID) (*Node, error) {
	return ae.GetNode(id)
}

// GetEdgeLatestEffective returns the merged latest-visible edge across pending, in-flight, and persisted state.
func (ae *AsyncEngine) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	return ae.GetEdge(id)
}

// GetNodeLatestVisible resolves the latest persisted-or-effective node.
func (ae *AsyncEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	if provider, ok := ae.engine.(MVCCVisibilityEngine); ok {
		return provider.GetNodeLatestVisible(id)
	}
	return ae.GetNodeLatestEffective(id)
}

// GetEdgeLatestVisible resolves the latest persisted-or-effective edge.
func (ae *AsyncEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	if provider, ok := ae.engine.(MVCCVisibilityEngine); ok {
		return provider.GetEdgeLatestVisible(id)
	}
	return ae.GetEdgeLatestEffective(id)
}

// GetNodeVisibleAt delegates snapshot-visible node reads to the wrapped engine when supported.
func (ae *AsyncEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	if provider, ok := ae.engine.(MVCCVisibilityEngine); ok {
		return provider.GetNodeVisibleAt(id, version)
	}
	return nil, ErrNotImplemented
}

// GetEdgeVisibleAt delegates snapshot-visible edge reads to the wrapped engine when supported.
func (ae *AsyncEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	if provider, ok := ae.engine.(MVCCVisibilityEngine); ok {
		return provider.GetEdgeVisibleAt(id, version)
	}
	return nil, ErrNotImplemented
}

// GetNodesByLabelVisibleAt delegates snapshot-visible label queries to the wrapped engine when supported.
func (ae *AsyncEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	if provider, ok := ae.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, ErrNotImplemented
}

// GetOutgoingEdgesVisibleAt delegates snapshot-visible outgoing adjacency queries to the wrapped engine when supported.
func (ae *AsyncEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	if provider, ok := ae.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetOutgoingEdgesVisibleAt(nodeID, version)
	}
	return nil, ErrNotImplemented
}

// GetIncomingEdgesVisibleAt delegates snapshot-visible incoming adjacency queries to the wrapped engine when supported.
func (ae *AsyncEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	if provider, ok := ae.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetIncomingEdgesVisibleAt(nodeID, version)
	}
	return nil, ErrNotImplemented
}

// GetEdgesByTypeVisibleAt delegates snapshot-visible edge-type queries to the wrapped engine when supported.
func (ae *AsyncEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	if provider, ok := ae.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}
	return nil, ErrNotImplemented
}

// GetEdgesBetweenVisibleAt delegates snapshot-visible topology queries to the wrapped engine when supported.
func (ae *AsyncEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	if provider, ok := ae.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}
	return nil, ErrNotImplemented
}

// GetNodeCurrentHead delegates node head lookup to the wrapped engine when supported.
func (ae *AsyncEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	if provider, ok := ae.engine.(MVCCHeadEngine); ok {
		return provider.GetNodeCurrentHead(id)
	}
	return MVCCHead{}, ErrNotImplemented
}

// GetEdgeCurrentHead delegates edge head lookup to the wrapped engine when supported.
func (ae *AsyncEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	if provider, ok := ae.engine.(MVCCHeadEngine); ok {
		return provider.GetEdgeCurrentHead(id)
	}
	return MVCCHead{}, ErrNotImplemented
}

// RebuildMVCCHeads delegates MVCC head rebuild to the wrapped engine when supported.
func (ae *AsyncEngine) RebuildMVCCHeads(ctx context.Context) error {
	if maint, ok := ae.engine.(MVCCMaintenanceEngine); ok {
		return maint.RebuildMVCCHeads(ctx)
	}
	return ErrNotImplemented
}

// PruneMVCCVersions delegates MVCC pruning to the wrapped engine when supported.
func (ae *AsyncEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if maint, ok := ae.engine.(MVCCMaintenanceEngine); ok {
		return maint.PruneMVCCVersions(ctx, opts)
	}
	return 0, ErrNotImplemented
}

// RegisterSnapshotReader delegates snapshot-reader registration when supported.
func (ae *AsyncEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	if lce, ok := ae.engine.(MVCCLifecycleEngine); ok {
		return lce.RegisterSnapshotReader(info)
	}
	return func() {}
}

// LifecycleStatus delegates lifecycle status when supported.
func (ae *AsyncEngine) LifecycleStatus() map[string]interface{} {
	if lce, ok := ae.engine.(MVCCLifecycleEngine); ok {
		return lce.LifecycleStatus()
	}
	return map[string]interface{}{"enabled": false}
}

// TriggerPruneNow delegates lifecycle prune-now when supported.
func (ae *AsyncEngine) TriggerPruneNow(ctx context.Context) error {
	if lce, ok := ae.engine.(MVCCLifecycleEngine); ok {
		return lce.TriggerPruneNow(ctx)
	}
	return nil
}

// PauseLifecycle delegates lifecycle pause when supported.
func (ae *AsyncEngine) PauseLifecycle() {
	if lce, ok := ae.engine.(MVCCLifecycleEngine); ok {
		lce.PauseLifecycle()
	}
}

// ResumeLifecycle delegates lifecycle resume when supported.
func (ae *AsyncEngine) ResumeLifecycle() {
	if lce, ok := ae.engine.(MVCCLifecycleEngine); ok {
		lce.ResumeLifecycle()
	}
}

// SetLifecycleSchedule delegates lifecycle cadence updates when supported.
func (ae *AsyncEngine) SetLifecycleSchedule(interval time.Duration) error {
	if scheduler, ok := ae.engine.(MVCCLifecycleScheduleEngine); ok {
		return scheduler.SetLifecycleSchedule(interval)
	}
	return nil
}

// TopLifecycleDebtKeys delegates lifecycle debt inspection when supported.
func (ae *AsyncEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	if provider, ok := ae.engine.(MVCCLifecycleDebtEngine); ok {
		return provider.TopLifecycleDebtKeys(limit)
	}
	return nil
}

// OnNodeCreated sets a callback to be invoked when nodes are created.
func (ae *AsyncEngine) OnNodeCreated(callback NodeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onNodeCreated = callback
}

// OnNodeUpdated sets a callback to be invoked when nodes are updated.
func (ae *AsyncEngine) OnNodeUpdated(callback NodeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onNodeUpdated = callback
}

// OnNodeDeleted sets a callback to be invoked when nodes are deleted.
func (ae *AsyncEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onNodeDeleted = callback
}

// OnEdgeCreated sets a callback to be invoked when edges are created.
func (ae *AsyncEngine) OnEdgeCreated(callback EdgeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onEdgeCreated = callback
}

// OnEdgeUpdated sets a callback to be invoked when edges are updated.
func (ae *AsyncEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onEdgeUpdated = callback
}

// OnEdgeDeleted sets a callback to be invoked when edges are deleted.
func (ae *AsyncEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onEdgeDeleted = callback
}

func (ae *AsyncEngine) notifyNodeDeleted(nodeID NodeID) {
	ae.callbackMu.RLock()
	callback := ae.onNodeDeleted
	ae.callbackMu.RUnlock()
	if callback != nil {
		callback(nodeID)
	}
}

// DefaultAsyncEngineConfig returns sensible defaults.
func DefaultAsyncEngineConfig() *AsyncEngineConfig {
	return &AsyncEngineConfig{
		FlushInterval:    50 * time.Millisecond,
		MaxNodeCacheSize: 50000,  // 50K nodes (~35MB)
		MaxEdgeCacheSize: 100000, // 100K edges (~50MB)
		AdaptiveFlush:    true,
		MinFlushInterval: 10 * time.Millisecond,
		MaxFlushInterval: 200 * time.Millisecond,
		TargetFlushSize:  1000,
	}
}

// GetUnderlying returns the underlying storage engine.
// This is used for transaction support when the underlying engine
// supports ACID transactions (e.g., BadgerEngine).
func (ae *AsyncEngine) GetUnderlying() Engine {
	return ae.engine
}

// Stats returns async engine statistics.
func (ae *AsyncEngine) Stats() (pendingWrites, totalFlushes int64) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return ae.pendingWrites, ae.totalFlushes
}

// HasPendingWrites returns true if there are unflushed writes.
// This is a cheap check that can be used to avoid unnecessary flush calls.
func (ae *AsyncEngine) HasPendingWrites() bool {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return len(ae.nodeCache) > 0 || len(ae.edgeCache) > 0 ||
		len(ae.deleteNodes) > 0 || len(ae.deleteEdges) > 0
}

func (ae *AsyncEngine) pendingWriteCount() int {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return len(ae.nodeCache) + len(ae.edgeCache) + len(ae.deleteNodes) + len(ae.deleteEdges)
}

func (ae *AsyncEngine) adaptiveFlushInterval(pending int) time.Duration {
	if pending <= 0 || ae.targetFlushSize <= 0 {
		return ae.maxFlushInterval
	}
	if ae.maxFlushInterval <= ae.minFlushInterval {
		return ae.minFlushInterval
	}
	ratio := float64(pending) / float64(ae.targetFlushSize)
	if ratio > 1 {
		ratio = 1
	}
	span := ae.maxFlushInterval - ae.minFlushInterval
	return ae.minFlushInterval + time.Duration(ratio*float64(span))
}
