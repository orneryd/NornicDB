package multidb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// sizeTrackingEngine wraps a database-scoped engine and maintains cached storage-size
// bytes in DatabaseManager metadata for O(1) reads.
type sizeTrackingEngine struct {
	storage.Engine
	manager *DatabaseManager
	dbName  string
}

var _ storage.Engine = (*sizeTrackingEngine)(nil)

// Preserve optional streaming capabilities of wrapped engines so query execution
// can keep LIMIT short-circuit behavior on hot paths.
var _ storage.StreamingEngine = (*sizeTrackingEngine)(nil)
var _ storage.LabelNodeIDLookupEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCVisibilityEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCIndexedVisibilityEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCHeadEngine = (*sizeTrackingEngine)(nil)
var _ storage.MVCCLifecycleEngine = (*sizeTrackingEngine)(nil)

func newSizeTrackingEngine(engine storage.Engine, manager *DatabaseManager, dbName string) storage.Engine {
	return &sizeTrackingEngine{
		Engine:  engine,
		manager: manager,
		dbName:  dbName,
	}
}

func (t *sizeTrackingEngine) GetInnerEngine() storage.Engine {
	return t.Engine
}

// RegisterSnapshotReader preserves MVCC lifecycle admission and tracking across
// size-tracking wrapper boundaries.
func (t *sizeTrackingEngine) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	if provider, ok := t.Engine.(storage.MVCCLifecycleEngine); ok {
		return provider.RegisterSnapshotReader(info)
	}
	return func() {}
}

// LifecycleStatus delegates lifecycle status when supported by the wrapped engine.
func (t *sizeTrackingEngine) LifecycleStatus() map[string]interface{} {
	if provider, ok := t.Engine.(storage.MVCCLifecycleEngine); ok {
		return provider.LifecycleStatus()
	}
	return map[string]interface{}{"enabled": false}
}

// TriggerPruneNow delegates lifecycle prune-now when supported by the wrapped engine.
func (t *sizeTrackingEngine) TriggerPruneNow(ctx context.Context) error {
	if provider, ok := t.Engine.(storage.MVCCLifecycleEngine); ok {
		return provider.TriggerPruneNow(ctx)
	}
	return nil
}

// PauseLifecycle delegates lifecycle pause when supported by the wrapped engine.
func (t *sizeTrackingEngine) PauseLifecycle() {
	if provider, ok := t.Engine.(storage.MVCCLifecycleEngine); ok {
		provider.PauseLifecycle()
	}
}

// ResumeLifecycle delegates lifecycle resume when supported by the wrapped engine.
func (t *sizeTrackingEngine) ResumeLifecycle() {
	if provider, ok := t.Engine.(storage.MVCCLifecycleEngine); ok {
		provider.ResumeLifecycle()
	}
}

// SetLifecycleSchedule delegates lifecycle cadence updates when supported by the wrapped engine.
func (t *sizeTrackingEngine) SetLifecycleSchedule(interval time.Duration) error {
	if provider, ok := t.Engine.(storage.MVCCLifecycleScheduleEngine); ok {
		return provider.SetLifecycleSchedule(interval)
	}
	return nil
}

// TopLifecycleDebtKeys delegates lifecycle debt inspection when supported by the wrapped engine.
func (t *sizeTrackingEngine) TopLifecycleDebtKeys(limit int) []storage.MVCCLifecycleDebtKey {
	if provider, ok := t.Engine.(storage.MVCCLifecycleDebtEngine); ok {
		return provider.TopLifecycleDebtKeys(limit)
	}
	return nil
}

// StreamNodes delegates streaming iteration to the wrapped engine when available.
func (t *sizeTrackingEngine) StreamNodes(ctx context.Context, fn func(node *storage.Node) error) error {
	if streamer, ok := t.Engine.(storage.StreamingEngine); ok {
		return streamer.StreamNodes(ctx, fn)
	}
	// Fallback to engine-agnostic helper.
	return storage.StreamNodesWithFallback(ctx, t.Engine, 1000, fn)
}

// StreamEdges delegates streaming iteration to the wrapped engine when available.
func (t *sizeTrackingEngine) StreamEdges(ctx context.Context, fn func(edge *storage.Edge) error) error {
	if streamer, ok := t.Engine.(storage.StreamingEngine); ok {
		return streamer.StreamEdges(ctx, fn)
	}
	edges, err := t.Engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(edge); err != nil {
			if err == storage.ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamNodeChunks delegates chunked streaming to wrapped engine when available.
func (t *sizeTrackingEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*storage.Node) error) error {
	if streamer, ok := t.Engine.(storage.StreamingEngine); ok {
		return streamer.StreamNodeChunks(ctx, chunkSize, fn)
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	chunk := make([]*storage.Node, 0, chunkSize)
	err := t.StreamNodes(ctx, func(node *storage.Node) error {
		chunk = append(chunk, node)
		if len(chunk) >= chunkSize {
			if err := fn(chunk); err != nil {
				return err
			}
			chunk = make([]*storage.Node, 0, chunkSize)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		return fn(chunk)
	}
	return nil
}

// StreamNodesByPrefix preserves prefix-scoped streaming when supported by wrapped engine.
func (t *sizeTrackingEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *storage.Node) error) error {
	if prefixStreamer, ok := t.Engine.(storage.PrefixStreamingEngine); ok {
		return prefixStreamer.StreamNodesByPrefix(ctx, prefix, fn)
	}
	// Fallback to full stream + prefix filter.
	return t.StreamNodes(ctx, func(node *storage.Node) error {
		if prefix == "" || strings.HasPrefix(string(node.ID), prefix) {
			return fn(node)
		}
		return nil
	})
}

// ForEachNodeIDByLabel preserves label->ID lookup capabilities across wrapper
// boundaries so LIMIT + label paths can avoid full node decode.
func (t *sizeTrackingEngine) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	if lookup, ok := t.Engine.(storage.LabelNodeIDLookupEngine); ok {
		return lookup.ForEachNodeIDByLabel(label, visit)
	}
	ids, err := storage.NodeIDsByLabel(t.Engine, label, 0)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if visit != nil && !visit(id) {
			return nil
		}
	}
	return nil
}

func (t *sizeTrackingEngine) GetNodeLatestVisible(id storage.NodeID) (*storage.Node, error) {
	if provider, ok := t.Engine.(storage.MVCCVisibilityEngine); ok {
		return provider.GetNodeLatestVisible(id)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetNodeVisibleAt(id storage.NodeID, version storage.MVCCVersion) (*storage.Node, error) {
	if provider, ok := t.Engine.(storage.MVCCVisibilityEngine); ok {
		return provider.GetNodeVisibleAt(id, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetEdgeLatestVisible(id storage.EdgeID) (*storage.Edge, error) {
	if provider, ok := t.Engine.(storage.MVCCVisibilityEngine); ok {
		return provider.GetEdgeLatestVisible(id)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetEdgeVisibleAt(id storage.EdgeID, version storage.MVCCVersion) (*storage.Edge, error) {
	if provider, ok := t.Engine.(storage.MVCCVisibilityEngine); ok {
		return provider.GetEdgeVisibleAt(id, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetNodesByLabelVisibleAt(label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	if provider, ok := t.Engine.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := t.Engine.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetOutgoingEdgesVisibleAt(nodeID, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := t.Engine.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetIncomingEdgesVisibleAt(nodeID, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := t.Engine.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := t.Engine.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}
	return nil, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetNodeCurrentHead(id storage.NodeID) (storage.MVCCHead, error) {
	if provider, ok := t.Engine.(storage.MVCCHeadEngine); ok {
		return provider.GetNodeCurrentHead(id)
	}
	return storage.MVCCHead{}, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) GetEdgeCurrentHead(id storage.EdgeID) (storage.MVCCHead, error) {
	if provider, ok := t.Engine.(storage.MVCCHeadEngine); ok {
		return provider.GetEdgeCurrentHead(id)
	}
	return storage.MVCCHead{}, storage.ErrNotImplemented
}

func (t *sizeTrackingEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return "", err
	}
	id, err := t.Engine.CreateNode(node)
	if err != nil {
		return id, err
	}
	created, getErr := t.Engine.GetNode(id)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return id, nil
	}
	size, sizeErr := calculateNodeSize(created)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return id, nil
	}
	t.manager.applyStorageSizeDelta(t.dbName, size, 0)
	return id, nil
}

func (t *sizeTrackingEngine) UpdateNode(node *storage.Node) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetNode(node.ID)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateNode(node)
	}
	oldSize, oldErr := calculateNodeSize(existing)
	if oldErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateNode(node)
	}
	if err := t.Engine.UpdateNode(node); err != nil {
		return err
	}
	updated, updatedErr := t.Engine.GetNode(node.ID)
	if updatedErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	newSize, newErr := calculateNodeSize(updated)
	if newErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	t.manager.applyStorageSizeDelta(t.dbName, newSize-oldSize, 0)
	return nil
}

func (t *sizeTrackingEngine) DeleteNode(id storage.NodeID) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetNode(id)
	if getErr != nil {
		// Nothing to track if the node doesn't exist.
		return t.Engine.DeleteNode(id)
	}
	nodeSize, sizeErr := calculateNodeSize(existing)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.DeleteNode(id)
	}
	edgeDelta, edgeErr := t.connectedEdgeBytes(id)
	if edgeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
	}
	if err := t.Engine.DeleteNode(id); err != nil {
		return err
	}
	t.manager.applyStorageSizeDelta(t.dbName, -nodeSize, -edgeDelta)
	return nil
}

func (t *sizeTrackingEngine) CreateEdge(edge *storage.Edge) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	if err := t.Engine.CreateEdge(edge); err != nil {
		return err
	}
	created, getErr := t.Engine.GetEdge(edge.ID)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	size, sizeErr := calculateEdgeSize(created)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	t.manager.applyStorageSizeDelta(t.dbName, 0, size)
	return nil
}

func (t *sizeTrackingEngine) UpdateEdge(edge *storage.Edge) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetEdge(edge.ID)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateEdge(edge)
	}
	oldSize, oldErr := calculateEdgeSize(existing)
	if oldErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateEdge(edge)
	}
	if err := t.Engine.UpdateEdge(edge); err != nil {
		return err
	}
	updated, updatedErr := t.Engine.GetEdge(edge.ID)
	if updatedErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	newSize, newErr := calculateEdgeSize(updated)
	if newErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	t.manager.applyStorageSizeDelta(t.dbName, 0, newSize-oldSize)
	return nil
}

func (t *sizeTrackingEngine) DeleteEdge(id storage.EdgeID) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetEdge(id)
	if getErr != nil {
		return t.Engine.DeleteEdge(id)
	}
	size, sizeErr := calculateEdgeSize(existing)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.DeleteEdge(id)
	}
	if err := t.Engine.DeleteEdge(id); err != nil {
		return err
	}
	t.manager.applyStorageSizeDelta(t.dbName, 0, -size)
	return nil
}

func (t *sizeTrackingEngine) BulkCreateNodes(nodes []*storage.Node) error {
	for _, n := range nodes {
		if _, err := t.CreateNode(n); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) BulkCreateEdges(edges []*storage.Edge) error {
	for _, e := range edges {
		if err := t.CreateEdge(e); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := t.DeleteNode(id); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := t.DeleteEdge(id); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) connectedEdgeBytes(id storage.NodeID) (int64, error) {
	outgoing, err := t.Engine.GetOutgoingEdges(id)
	if err != nil {
		return 0, fmt.Errorf("get outgoing edges: %w", err)
	}
	incoming, err := t.Engine.GetIncomingEdges(id)
	if err != nil {
		return 0, fmt.Errorf("get incoming edges: %w", err)
	}
	seen := make(map[storage.EdgeID]struct{}, len(outgoing)+len(incoming))
	var total int64
	for _, e := range outgoing {
		if _, ok := seen[e.ID]; ok {
			continue
		}
		seen[e.ID] = struct{}{}
		size, sizeErr := calculateEdgeSize(e)
		if sizeErr != nil {
			return 0, sizeErr
		}
		total += size
	}
	for _, e := range incoming {
		if _, ok := seen[e.ID]; ok {
			continue
		}
		seen[e.ID] = struct{}{}
		size, sizeErr := calculateEdgeSize(e)
		if sizeErr != nil {
			return 0, sizeErr
		}
		total += size
	}
	return total, nil
}
