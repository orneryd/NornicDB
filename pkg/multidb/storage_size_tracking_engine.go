package multidb

import (
	"context"
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
	checker *databaseLimitChecker
	writeMu writeAdmission
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
var _ storage.ProjectedLabelNodeReader = (*sizeTrackingEngine)(nil)
var _ storage.PropertyKeyLookup = (*sizeTrackingEngine)(nil)

func (t *sizeTrackingEngine) ensureStorageSizeInitialized() error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine)
}

func newSizeTrackingEngine(engine storage.Engine, manager *DatabaseManager, dbName string, checkers ...*databaseLimitChecker) storage.Engine {
	wrapped := &sizeTrackingEngine{
		Engine:  engine,
		manager: manager,
		dbName:  dbName,
	}
	if len(checkers) > 0 {
		wrapped.checker = checkers[0]
	}
	if manager != nil {
		storage.RegisterAccountingOwner(engine, dbName, wrapped.prepareBaseAccounting)
	}
	return wrapped
}

func (t *sizeTrackingEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return t.checker
}

func (t *sizeTrackingEngine) GetInnerEngine() storage.Engine {
	return t.Engine
}

// GetQueryNode forwards the query-row node read (storage.QueryNodeReader),
// else reads the full node.
func (t *sizeTrackingEngine) GetQueryNode(id storage.NodeID) (*storage.Node, error) {
	if reader, ok := t.Engine.(storage.QueryNodeReader); ok {
		return reader.GetQueryNode(id)
	}
	return t.Engine.GetNode(id)
}

// RecordMaterializedAccess preserves result-access accounting across the
// database size-tracking wrapper.
func (t *sizeTrackingEngine) RecordMaterializedAccess(entityID string) {
	if recorder, ok := t.Engine.(interface{ RecordMaterializedAccess(string) }); ok {
		recorder.RecordMaterializedAccess(entityID)
	}
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

// StreamNodesWithOptions delegates the unified options-driven scan to the
// wrapped engine. Size tracking only concerns writes, so the scan passes
// through untouched.
func (t *sizeTrackingEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(node *storage.Node) error) error {
	return t.Engine.StreamNodesWithOptions(ctx, opts, fn)
}

// StreamNodesByLabelProjected preserves label-indexed candidate streaming
// across this wrapper boundary. Embedding storage.Engine only promotes the
// methods declared on that interface, not every method the wrapped engine's
// dynamic type happens to implement, so a label-scoped MATCH would otherwise
// fail this type assertion and fall back to a whole-namespace StreamNodes
// scan (O(store) instead of O(label); see #561).
func (t *sizeTrackingEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	if reader, ok := t.Engine.(storage.ProjectedLabelNodeReader); ok {
		return reader.StreamNodesByLabelProjected(label, properties, visit)
	}
	return storage.ErrNotImplemented
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

// MatchEdgesBetween keeps projected edge-between matching on the wrapped
// engine (storage.EdgesBetweenMatcher).
func (t *sizeTrackingEngine) MatchEdgesBetween(startID, endID storage.NodeID, edgeType string, properties []string, match func(*storage.Edge) bool) ([]*storage.Edge, error) {
	return storage.MatchEdgesBetween(t.Engine, startID, endID, edgeType, properties, match)
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

// managedWrite stages direct CRUD before entering the same accounting owner as
// public Cypher commits. No direct route measures or publishes its own delta.
func (t *sizeTrackingEngine) managedWrite(op storage.Operation) error {
	tx := storage.NewManagedWrite(t.Engine, t.dbName, op)
	return t.commitTransaction(context.Background(), tx)
}

func (t *sizeTrackingEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if err := t.managedWrite(storage.Operation{Type: storage.OpCreateNode, Node: node}); err != nil {
		return "", err
	}
	return storage.NodeID(strings.TrimPrefix(string(node.ID), t.dbName+":")), nil
}

func (t *sizeTrackingEngine) UpdateNode(node *storage.Node) error {
	return t.managedWrite(storage.Operation{Type: storage.OpUpdateNode, Node: node})
}

func (t *sizeTrackingEngine) DeleteNode(id storage.NodeID) error {
	return t.managedWrite(storage.Operation{Type: storage.OpDeleteNode, NodeID: id})
}

func (t *sizeTrackingEngine) CreateEdge(edge *storage.Edge) error {
	return t.managedWrite(storage.Operation{Type: storage.OpCreateEdge, Edge: edge})
}

func (t *sizeTrackingEngine) UpdateEdge(edge *storage.Edge) error {
	return t.managedWrite(storage.Operation{Type: storage.OpUpdateEdge, Edge: edge})
}

func (t *sizeTrackingEngine) DeleteEdge(id storage.EdgeID) error {
	return t.managedWrite(storage.Operation{Type: storage.OpDeleteEdge, EdgeID: id})
}

// BulkCreateNodes stages the whole batch before the shared quota check. A
// rejection leaves no writes, so cleanup cannot itself consume rate capacity.
func (t *sizeTrackingEngine) BulkCreateNodes(nodes []*storage.Node) error {
	if len(nodes) == 0 {
		return nil
	}
	operations := make([]storage.Operation, len(nodes))
	for i, node := range nodes {
		operations[i] = storage.Operation{Type: storage.OpCreateNode, Node: node}
	}
	return t.commitTransaction(context.Background(), storage.NewManagedCreateBatch(t.Engine, t.dbName, operations))
}

// BulkCreateEdges shares the same atomic staging and accounting boundary.
func (t *sizeTrackingEngine) BulkCreateEdges(edges []*storage.Edge) error {
	if len(edges) == 0 {
		return nil
	}
	operations := make([]storage.Operation, len(edges))
	for i, edge := range edges {
		operations[i] = storage.Operation{Type: storage.OpCreateEdge, Edge: edge}
	}
	return t.commitTransaction(context.Background(), storage.NewManagedCreateBatch(t.Engine, t.dbName, operations))
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

// NodeCountByLabel preserves the label counter across this wrapper, so the
// count-only MATCH fast path (and its transaction-visible form, #683) reads
// it instead of scanning the label. An engine without one counts its label's
// nodes.
func (t *sizeTrackingEngine) NodeCountByLabel(label string) (int64, error) {
	if counter, ok := t.Engine.(interface{ NodeCountByLabel(string) (int64, error) }); ok {
		return counter.NodeCountByLabel(label)
	}
	nodes, err := t.Engine.GetNodesByLabel(label)
	if err != nil {
		return 0, err
	}
	return int64(len(nodes)), nil
}

// Namespace preserves the wrapped engine's namespace (its database name), so
// what a caller reads through this wrapper is the database it tracks; it is
// "" when the wrapped engine has none, which every caller treats as unknown.
func (t *sizeTrackingEngine) Namespace() string {
	if scoped, ok := t.Engine.(interface{ Namespace() string }); ok {
		return scoped.Namespace()
	}
	return ""
}

// GraphMutationVersion preserves query-cache invalidation through this wrapper.
func (t *sizeTrackingEngine) GraphMutationVersion() (uint64, bool) {
	provider, ok := t.Engine.(storage.GraphMutationVersionProvider)
	if !ok {
		return 0, false
	}
	return provider.GraphMutationVersion()
}

// PropertyKeyKnown forwards the database's property-key lookup
// (storage.PropertyKeyLookup), so Cypher's SET counter sees the key names
// through the server's wrapper as it does on the namespaced engine (#907).
func (t *sizeTrackingEngine) PropertyKeyKnown(name string) bool {
	lookup, ok := t.Engine.(storage.PropertyKeyLookup)
	return ok && lookup.PropertyKeyKnown(name)
}
