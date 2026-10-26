// Package storage provides write-ahead logging for NornicDB durability.
package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// logger returns a non-nil *slog.Logger for WALEngine emissions. If the
// wrapped *WAL was constructed without a SlogLogger, a discard logger is
// returned. Calls are O(1); the underlying walLog is set once at NewWAL.
func (w *WALEngine) logger() *slog.Logger {
	if w == nil || w.wal == nil || w.wal.walLog == nil {
		return discardWALSlog()
	}
	return w.wal.walLog
}

// WALEngine wraps a storage engine with write-ahead logging.
//
// All mutating operations are appended to the WAL before they are applied to
// the wrapped engine. This provides crash recovery via snapshot + replay while
// keeping the underlying Engine implementations simple and fast.
//
// Design notes:
//   - Database routing: WALEngine supports multi-database usage by recording the
//     database/namespace in each WAL entry and by normalizing IDs when needed.
//   - Embedding updates: embedding-only updates are logged using OpUpdateEmbedding
//     which is safe to skip during recovery because embeddings are regenerable.
//   - Auto-compaction: optional periodic snapshots + WAL truncation to prevent
//     unbounded WAL growth.
type WALEngine struct {
	readBinding          ReadBindingSlot
	pendingEntityCommits sync.Map
	entityWaitCtx        context.Context
	stopEntityWait       context.CancelFunc
	engine               Engine
	// keyRegistry is engine's PropertyKeyRegistry, nil when it keeps none.
	keyRegistry PropertyKeyRegistry
	wal         *WAL
	// mutationMu serializes auto-compaction snapshots against in-flight mutating
	// operations to avoid WAL/engine state skew during snapshot truncation.
	mutationMu sync.RWMutex

	// Automatic snapshot and compaction
	snapshotDir       string
	snapshotMu        sync.RWMutex // Protects snapshotTicker and stopSnapshot
	snapshotTicker    *time.Ticker
	stopSnapshot      chan struct{}
	snapshotWg        sync.WaitGroup // Waits for auto-compaction goroutine to finish
	lastSnapshotTime  atomic.Int64
	totalSnapshots    atomic.Int64
	compactedSequence atomic.Uint64
	hasCompacted      atomic.Bool
}

// WALEngine declares every optional method; admission is derived from this policy.
var wALEngineReadPolicy = BoundaryReadPolicy{
	{Methods: "BatchGetNodesWithContext", Disposition: CapabilityTransparent, Scope: "Forwards the original context and IDs to the common batch dispatcher on w.engine; WAL owns mutation journaling and adds no node-view transform."},
	{Methods: "BatchGetNodesWithoutEmbeddingsWithContext", Disposition: CapabilityTransparent, Scope: "Delegates the embedding-free request to batchNodesWithContext, which selects its optional provider through the common read owner; no WAL data view is created."},
	{Methods: "GetStoredIncomingEdgesWithContext", Disposition: CapabilityTransparent, Scope: "Forwards incoming stored adjacency to the shared stored helper on w.engine, preserving stored visibility and the caller context."},
	{Methods: "GetStoredOutgoingEdgesWithContext", Disposition: CapabilityTransparent, Scope: "Forwards outgoing stored adjacency to the shared stored helper on w.engine; it does not select current-visible fallback or modify endpoints."},
	{Methods: "IsCurrentTemporalNodeWithContext", Disposition: CapabilityTransparent, Scope: "Forwards the temporal decision through the common contextual helper on w.engine; temporal schema/view availability belongs to that provider."},
	{Methods: "EdgeCountByTypeInNamespace EdgeCountByStartLabelInNamespace EdgeCountByEndLabelInNamespace", Disposition: CapabilityTransparent, Scope: "The maintainer namespace-counter owner retains explicit namespace/tier point-read semantics; common cursor admission adds no scan or counter state."},
	{Methods: "SubmitEntityCommit", Disposition: CapabilityRefused, Scope: "Prepared commit submission is the exact WAL/managed terminal, not an optional data read."},
	{Methods: "ReadStored StoredReadPolicy", Disposition: CapabilityTransparent, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "AllNodesWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "BulkCreateNodesWithContext", Disposition: CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "CreateNodeWithContext", Disposition: CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "ForEachNodeIDByLabelInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "ForEachNodeIDByLabelWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetFirstNodeByLabelInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetFirstNodeByLabelWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetNodesByLabelInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetNodesByLabelVisibleAtInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetNodesByLabelVisibleAtWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetNodesByLabelWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetStoredNodesByLabelInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "GetStoredNodesByLabelWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "NodeCountByLabelInNamespaceWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "NodeCountByLabelWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "StreamNodesByLabelProjectedInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "StreamNodesByLabelProjectedWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "StreamStoredNodesByLabelProjectedInScopeWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "StreamStoredNodesByLabelProjectedWithContext", Disposition: CapabilityTransparent, Scope: "Context-bearing read forwards the same caller and retains this declared scoped view."},
	{Methods: "UpdateNodeWithContext", Disposition: CapabilityRefused, Scope: "Exact contextual mutation retains its issued interceptor; optional read discovery cannot invoke it."},
	{Methods: "AddToPendingEmbeddings Backup BatchGetNodesWithoutEmbeddings BatchGetNodesWithoutEmbeddingsSupported ConsumeCleanShutdownMarker " +
		"CountPendingEmbeddingIDs DisableAutoCompaction EdgeCountByPrefix EnableAutoCompaction FindNodeNeedingEmbedding " +
		"ForEachNodeIDByLabel ForEachNodeIDByLabelInScope GetAdjacentEdges GetEdgeCurrentHead GetEdgeLatestEffective " +
		"GetEdgeLatestVisible GetEdgeVisibleAt GetEdgesBetweenVisibleAt GetEdgesByTypeVisibleAt GetFirstNodeByLabelInScope " +
		"GetIncomingEdgesVisibleAt GetInnerEngine GetNodeCurrentHead GetNodeEmbeddingMeta GetNodeLatestEffective " +
		"GetNodeLatestVisible GetNodeProjected GetNodeVisibleAt GetNodeWithoutEmbeddings GetNodesByLabelInScope " +
		"GetNodesByLabelVisibleAt GetNodesByLabelVisibleAtInScope GetOutgoingEdgesVisibleAt GetQueryNode GetSchemaForNamespace " +
		"GetSnapshotStats GetStoredEdge GetStoredIncomingEdges GetStoredNode GetStoredNodesByLabel " +
		"GetStoredNodesByLabelInScope GetStoredOutgoingEdges GetWAL GraphMutationVersion GraphMutationVersionInNamespace " +
		"IncomingEdgeHeaders IsCurrentTemporalNode IterateNodes LastWriteTime LifecycleStatus " +
		"ListNamespaces MarkCleanShutdown MarkNodeEmbedded MatchEdgesBetween NodeCountByLabel " +
		"NodeCountByLabelInNamespace NodeCountByPrefix NotePropertyKeysInNamespace OnEdgeCreated OnEdgeDeleted " +
		"OnEdgeUpdated OnNodeCreated OnNodeDeleted OnNodeUpdated OpenReadSnapshot " +
		"OutgoingEdgeHeaders PauseLifecycle PendingEmbeddingsCount PropertyKeyKnownInNamespace PruneMVCCVersions " +
		"PruneTemporalHistory RebuildMVCCHeads RebuildTemporalIndexes RecordMaterializedAccess RefreshPendingEmbeddingsIndex " +
		"RegisterSnapshotReader RelationshipEndpointVisible Restore ResumeLifecycle SetManualSuppression " +
		"SetLifecycleSchedule StreamEdges StreamEdgesByType StreamEdgesByTypeInScope " +
		"StreamNodeChunks StreamNodes StreamNodesByLabelProjected StreamNodesByLabelProjectedInScope StreamNodesByPrefix " +
		"StreamNodesByPrefixProjected StreamNodesByPrefixWithoutEmbeddings StreamNodesWithoutEmbeddings StreamParkedEmbeddingFailures StreamStoredEdgesByType " +
		"StreamStoredEdgesByTypeInScope StreamStoredNodesByLabelProjected StreamStoredNodesByLabelProjectedInScope SubscribeEvents TopLifecycleDebtKeys " +
		"TriggerPruneNow UpdateNodeEmbeddingSidecar",
		Disposition: CapabilityTransparent, Scope: "Read view delegates to ReadCapabilitySource; writes and decorator metadata retain their exact owner."},
}

func (*WALEngine) ReadCapabilityPolicy() BoundaryReadPolicy { return wALEngineReadPolicy.Copy() }

// ListNamespaces returns known namespaces from the wrapped engine, if supported.
func (w *WALEngine) ListNamespaces() []string {
	if lister, ok := ViewMetadataForEngine[NamespaceLister](w.engine); ok {
		return lister.ListNamespaces()
	}
	return nil
}

// IsCurrentTemporalNode delegates current-version checks to the wrapped engine when supported.
func (w *WALEngine) IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error) {
	return w.IsCurrentTemporalNodeWithContext(context.Background(), node, asOf)
}

// IsCurrentTemporalNodeWithContext uses the bound scoped operation and carries
// the caller through its actual temporal decision.
func (w *WALEngine) IsCurrentTemporalNodeWithContext(ctx context.Context, node *Node, asOf time.Time) (bool, error) {
	return BindTemporalNodeRead(ctx, w.engine)(node, asOf)
}

// RebuildTemporalIndexes delegates temporal index rebuild to the wrapped engine when supported.
func (w *WALEngine) RebuildTemporalIndexes(ctx context.Context) error {
	if maint, ok := OwnedCapability[TemporalMaintenanceEngine](w.engine); ok {
		return maint.RebuildTemporalIndexes(ctx)
	}
	return nil
}

// ConsumeCleanShutdownMarker delegates startup recovery state to storage.
func (w *WALEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	if provider, ok := OwnedCapability[StartupMaintenanceStateEngine](w.engine); ok {
		return provider.ConsumeCleanShutdownMarker(ctx)
	}
	return false, ErrNotImplemented
}

// MarkCleanShutdown delegates the durable shutdown boundary to storage.
func (w *WALEngine) MarkCleanShutdown(ctx context.Context) error {
	if provider, ok := OwnedCapability[StartupMaintenanceStateEngine](w.engine); ok {
		return provider.MarkCleanShutdown(ctx)
	}
	return ErrNotImplemented
}

// PruneTemporalHistory delegates temporal pruning to the wrapped engine when supported.
func (w *WALEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if maint, ok := OwnedCapability[TemporalMaintenanceEngine](w.engine); ok {
		return maint.PruneTemporalHistory(ctx, opts)
	}
	return 0, nil
}

// RebuildMVCCHeads delegates MVCC head rebuild to the wrapped engine when supported.
func (w *WALEngine) RebuildMVCCHeads(ctx context.Context) error {
	if maint, ok := OwnedCapability[MVCCMaintenanceEngine](w.engine); ok {
		return maint.RebuildMVCCHeads(ctx)
	}
	return ErrNotImplemented
}

// PruneMVCCVersions delegates MVCC pruning to the wrapped engine when supported.
func (w *WALEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if maint, ok := OwnedCapability[MVCCMaintenanceEngine](w.engine); ok {
		return maint.PruneMVCCVersions(ctx, opts)
	}
	return 0, ErrNotImplemented
}

// GetNodeLatestEffective delegates MVCC latest-effective reads to the wrapped engine when supported.
func (w *WALEngine) GetNodeLatestEffective(id NodeID) (*Node, error) {
	return ReadWithCapability(w.engine, func(provider MVCCLatestEffectiveEngine) (*Node, error) {
		return provider.GetNodeLatestEffective(id)
	}, func() (*Node, error) { return w.engine.GetNode(id) })
}

// GetEdgeLatestEffective delegates MVCC latest-effective edge reads to the wrapped engine when supported.
func (w *WALEngine) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCLatestEffectiveEngine) (*Edge, error) {
		return provider.GetEdgeLatestEffective(id)
	}, func() (*Edge, error) { return w.engine.GetEdge(id) })
}

// GetNodeLatestVisible delegates latest-visible node reads when supported.
func (w *WALEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	return ReadWithCapability(w.engine, func(provider MVCCVisibilityEngine) (*Node, error) {
		return provider.GetNodeLatestVisible(id)
	}, func() (*Node, error) { return w.GetNodeLatestEffective(id) })
}

// GetEdgeLatestVisible delegates latest-visible edge reads when supported.
func (w *WALEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCVisibilityEngine) (*Edge, error) {
		return provider.GetEdgeLatestVisible(id)
	}, func() (*Edge, error) { return w.GetEdgeLatestEffective(id) })
}

// GetNodeVisibleAt delegates snapshot-visible node reads when supported.
func (w *WALEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	return ReadWithCapability(w.engine, func(provider MVCCVisibilityEngine) (*Node, error) {
		return provider.GetNodeVisibleAt(id, version)
	}, func() (*Node, error) { return nil, ErrNotImplemented })
}

// GetEdgeVisibleAt delegates snapshot-visible edge reads when supported.
func (w *WALEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCVisibilityEngine) (*Edge, error) {
		return provider.GetEdgeVisibleAt(id, version)
	}, func() (*Edge, error) { return nil, ErrNotImplemented })
}

// GetNodesByLabelVisibleAt delegates snapshot-visible label queries when supported.
func (w *WALEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	return w.GetNodesByLabelVisibleAtWithContext(context.Background(), label, version)
}

// GetNodesByLabelVisibleAtWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetNodesByLabelVisibleAtWithContext(ctx context.Context, label string, version MVCCVersion) ([]*Node, error) {
	return ReadWithCapability(w.engine, func(provider MVCCIndexedVisibilityEngine) ([]*Node, error) {
		return GetNodesByLabelVisibleAtWithContext(ctx, provider, label, version)
	}, func() ([]*Node, error) { return nil, ErrNotImplemented })
}

// GetOutgoingEdgesVisibleAt delegates snapshot-visible outgoing adjacency queries when supported.
func (w *WALEngine) GetOutgoingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCIndexedVisibilityEngine) ([]*Edge, error) {
		return provider.GetOutgoingEdgesVisibleAt(nodeID, version)
	}, func() ([]*Edge, error) { return nil, ErrNotImplemented })
}

// GetIncomingEdgesVisibleAt delegates snapshot-visible incoming adjacency queries when supported.
func (w *WALEngine) GetIncomingEdgesVisibleAt(nodeID NodeID, version MVCCVersion) ([]*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCIndexedVisibilityEngine) ([]*Edge, error) {
		return provider.GetIncomingEdgesVisibleAt(nodeID, version)
	}, func() ([]*Edge, error) { return nil, ErrNotImplemented })
}

// GetEdgesByTypeVisibleAt delegates snapshot-visible edge-type queries when supported.
func (w *WALEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCIndexedVisibilityEngine) ([]*Edge, error) {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}, func() ([]*Edge, error) { return nil, ErrNotImplemented })
}

// GetEdgesBetweenVisibleAt delegates snapshot-visible topology queries when supported.
func (w *WALEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	return ReadWithCapability(w.engine, func(provider MVCCIndexedVisibilityEngine) ([]*Edge, error) {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}, func() ([]*Edge, error) { return nil, ErrNotImplemented })
}

// RegisterSnapshotReader delegates snapshot-reader registration when supported.
func (w *WALEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	if lce, ok := ViewMetadataForEngine[MVCCLifecycleEngine](w.engine); ok {
		return lce.RegisterSnapshotReader(info)
	}
	return func() {}
}

// LifecycleStatus delegates lifecycle status when supported.
func (w *WALEngine) LifecycleStatus() map[string]interface{} {
	if lce, ok := ViewMetadataForEngine[MVCCLifecycleEngine](w.engine); ok {
		return lce.LifecycleStatus()
	}
	return map[string]interface{}{"enabled": false}
}

// TriggerPruneNow delegates lifecycle prune-now when supported.
func (w *WALEngine) TriggerPruneNow(ctx context.Context) error {
	if lce, ok := OwnedCapability[MVCCLifecycleEngine](w.engine); ok {
		return lce.TriggerPruneNow(ctx)
	}
	return nil
}

// PauseLifecycle delegates lifecycle pause when supported.
func (w *WALEngine) PauseLifecycle() {
	if lce, ok := OwnedCapability[MVCCLifecycleEngine](w.engine); ok {
		lce.PauseLifecycle()
	}
}

// ResumeLifecycle delegates lifecycle resume when supported.
func (w *WALEngine) ResumeLifecycle() {
	if lce, ok := OwnedCapability[MVCCLifecycleEngine](w.engine); ok {
		lce.ResumeLifecycle()
	}
}

// SetLifecycleSchedule delegates lifecycle cadence updates when supported.
func (w *WALEngine) SetLifecycleSchedule(interval time.Duration) error {
	if scheduler, ok := OwnedCapability[MVCCLifecycleScheduleEngine](w.engine); ok {
		return scheduler.SetLifecycleSchedule(interval)
	}
	return nil
}

// TopLifecycleDebtKeys delegates lifecycle debt inspection when supported.
func (w *WALEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	return ReadValueWithCapability(w.engine, func(reader MVCCLifecycleDebtEngine) []MVCCLifecycleDebtKey { return reader.TopLifecycleDebtKeys(limit) }, nil)
}

// GetNodeCurrentHead delegates node head lookup when supported.
func (w *WALEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	return ReadWithCapability(w.engine, func(provider MVCCHeadEngine) (MVCCHead, error) {
		return provider.GetNodeCurrentHead(id)
	}, func() (MVCCHead, error) { return MVCCHead{}, ErrNotImplemented })
}

// GetEdgeCurrentHead delegates edge head lookup when supported.
func (w *WALEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	return ReadWithCapability(w.engine, func(provider MVCCHeadEngine) (MVCCHead, error) {
		return provider.GetEdgeCurrentHead(id)
	}, func() (MVCCHead, error) { return MVCCHead{}, ErrNotImplemented })
}

// NewWALEngine creates a WAL-backed storage engine.
func NewWALEngine(engine Engine, wal *WAL) *WALEngine {
	waitCtx, stopWait := context.WithCancel(context.Background())
	w := &WALEngine{
		entityWaitCtx: waitCtx, stopEntityWait: stopWait,
		engine: engine,
		wal:    wal,
	}
	w.readBinding.Store(RebindReadView(w))
	w.keyRegistry, _ = OwnedCapability[PropertyKeyRegistry](engine)
	// Give write-behind enabled engines the WAL so buffered commits can be
	// logged durably before their ACK (OpBufferedCommit replay on crash).
	if wal != nil {
		if setter, ok := OwnedCapability[interface{ SetBufferedCommitWAL(*WAL) }](engine); ok {
			setter.SetBufferedCommitWAL(wal)
		}
	}
	// Existing WAL state predates this process's compaction cadence. Treat it
	// as the baseline so an idle restart does not rewrite the entire database;
	// the first subsequent mutation advances the sequence and triggers a snapshot.
	if wal != nil {
		w.compactedSequence.Store(wal.sequence.Load())
		w.hasCompacted.Store(true)
	}
	return w
}

// GetInnerEngine returns the wrapped storage engine.
func (w *WALEngine) GetInnerEngine() Engine {
	if w == nil {
		return nil
	}
	return w.engine
}

// RecordMaterializedAccess delegates result-materialization access recording to
// the underlying engine, if supported.
func (w *WALEngine) RecordMaterializedAccess(entityID string) {
	RecordMaterializedAccess(w.engine, entityID)
}

// EnableAutoCompaction starts automatic snapshot creation and WAL truncation.
// Snapshots are created at the configured SnapshotInterval, and the WAL is
// truncated after each successful snapshot to prevent unbounded growth.
//
// Snapshots are saved to snapshotDir/snapshot-<timestamp>.json
//
// This solves the "WAL grows forever" problem by automatically removing old
// entries that are already captured in snapshots.
//
// Example:
//
//	walEngine.EnableAutoCompaction("data/snapshots")
//	// WAL will now be automatically truncated every SnapshotInterval
func (w *WALEngine) EnableAutoCompaction(snapshotDir string) error {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	if w.stopSnapshot != nil {
		return fmt.Errorf("wal: auto-compaction already enabled")
	}

	// Create snapshot directory
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("wal: failed to create snapshot directory: %w", err)
	}

	w.snapshotDir = snapshotDir

	interval := w.wal.config.SnapshotInterval
	if interval <= 0 {
		interval = 1 * time.Hour // Default if not configured
	}

	// Create ticker and channel BEFORE starting goroutine to avoid race
	w.snapshotTicker = time.NewTicker(interval)
	w.stopSnapshot = make(chan struct{})

	// Now start goroutine - ticker is already initialized and protected by lock
	w.snapshotWg.Add(1)
	go w.autoSnapshotLoop()

	return nil
}

// DisableAutoCompaction stops automatic snapshot creation and WAL truncation.
// Waits for the auto-compaction goroutine to finish before returning to prevent
// race conditions when the engine is closed.
func (w *WALEngine) DisableAutoCompaction() {
	w.snapshotMu.Lock()
	shouldWait := false
	if w.stopSnapshot != nil {
		close(w.stopSnapshot)
		w.stopSnapshot = nil
		shouldWait = true
	}
	if w.snapshotTicker != nil {
		w.snapshotTicker.Stop()
		w.snapshotTicker = nil
	}
	w.snapshotMu.Unlock()

	// Wait for goroutine to finish (outside lock to avoid deadlock)
	if shouldWait {
		w.snapshotWg.Wait()
	}
}

// autoSnapshotLoop runs in background, creating periodic snapshots and truncating WAL.
func (w *WALEngine) autoSnapshotLoop() {
	defer w.snapshotWg.Done()
	for {
		// Get local copies of channels under lock to avoid races
		w.snapshotMu.RLock()
		ticker := w.snapshotTicker
		stopCh := w.stopSnapshot
		w.snapshotMu.RUnlock()

		// Check if shutdown was requested
		if ticker == nil || stopCh == nil {
			return
		}

		// Select on local channel copies (safe to do without lock)
		select {
		case <-ticker.C:
			if err := w.createSnapshotAndCompact(); err != nil {
				// Log error but continue - don't crash on snapshot failure.
				// Pre-bound subsystem=wal logger from the WAL config keeps
				// per-failure emission allocation-free in the steady path.
				w.logger().Warn("wal auto-compaction failed",
					"subsystem", "wal_compaction",
					slog.Any("error", err),
				)
			}
		case <-stopCh:
			return
		}
	}
}

// createSnapshotAndCompact creates a snapshot and truncates the WAL.
// This is called automatically by the background goroutine.
func (w *WALEngine) createSnapshotAndCompact() error {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if w.wal.closed.Load() {
		return ErrWALClosed
	}
	snapshotSequence := w.wal.sequence.Load()
	if w.hasCompacted.Load() && snapshotSequence == w.compactedSequence.Load() {
		return nil
	}
	if err := w.wal.Checkpoint(); err != nil {
		return fmt.Errorf("failed to create snapshot checkpoint: %w", err)
	}

	// Save snapshot with high-resolution timestamp to avoid filename collisions
	// when snapshots happen multiple times within the same second.
	timestamp := time.Now().Format("20060102-150405.000000000")
	snapshotPath := filepath.Join(w.snapshotDir, fmt.Sprintf("snapshot-%s.json", timestamp))

	// Flush any acknowledged write-behind commits into Badger before the
	// snapshot and WAL truncation. A snapshot must never truncate
	// OpBufferedCommit entries while their data still lives only in the
	// in-memory buffer: the snapshot reads committed Badger state, and the
	// truncation would otherwise delete the only durable copy.
	if flusher, ok := OwnedCapability[interface{ FlushWriteBehind() error }](w.engine); ok {
		if err := flusher.FlushWriteBehind(); err != nil {
			return fmt.Errorf("failed to flush write-behind before snapshot: %w", err)
		}
	}

	if err := SaveStreamingSnapshot(context.Background(), w.engine, snapshotPath, SnapshotOptions{Sequence: snapshotSequence}); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Truncate WAL to remove entries before snapshot
	// This is the key compaction step that prevents unbounded growth
	if err := w.wal.TruncateAfterSnapshot(snapshotSequence); err != nil {
		// Log error but don't fail - snapshot is still valid
		// Next compaction will try again
		return fmt.Errorf("failed to truncate WAL (snapshot saved): %w", err)
	}

	// Prune old snapshot files so disk space stays bounded
	if err := PruneOldSnapshotFiles(w.snapshotDir, w.wal.config); err != nil {
		// Log but don't fail the compaction.
		w.logger().Warn("wal snapshot pruning failed",
			"subsystem", "wal_compaction",
			slog.Any("error", err),
		)
	}

	// Update stats
	w.totalSnapshots.Add(1)
	w.lastSnapshotTime.Store(time.Now().UnixNano())
	w.compactedSequence.Store(w.wal.sequence.Load())
	w.hasCompacted.Store(true)

	return nil
}

// GetSnapshotStats returns statistics about automatic snapshots.
func (w *WALEngine) GetSnapshotStats() (totalSnapshots int64, lastSnapshotTime time.Time) {
	total := w.totalSnapshots.Load()
	lastTS := w.lastSnapshotTime.Load()

	var lastTime time.Time
	if lastTS > 0 {
		lastTime = time.Unix(0, lastTS)
	}

	return total, lastTime
}

// LastWriteTime returns the last WAL entry timestamp (best-effort).
func (w *WALEngine) LastWriteTime() time.Time {
	if w == nil || w.wal == nil {
		return time.Time{}
	}
	stats := w.wal.Stats()
	return stats.LastEntryTime
}

// getDatabaseName extracts the database name from the wrapped engine if it's a NamespacedEngine.
func (w *WALEngine) getDatabaseName() string {
	if namespacedEngine, ok := ViewMetadataForEngine[*NamespacedEngine](w.engine); ok {
		return namespacedEngine.Namespace()
	}
	// Fallback to default if engine is not namespaced
	globalConfig := config.LoadFromEnv()
	dbName := globalConfig.Database.DefaultDatabase
	if dbName == "" {
		return "nornic"
	}
	return dbName
}

func (w *WALEngine) databaseFromNode(node *Node) string {
	if node != nil {
		if db, _, ok := ParseDatabasePrefix(string(node.ID)); ok {
			return db
		}
	}
	return w.getDatabaseName()
}

func (w *WALEngine) databaseFromEdge(edge *Edge) (string, error) {
	if edge == nil {
		return w.getDatabaseName(), nil
	}

	dbCandidates := make([]string, 0, 3)
	if db, _, ok := ParseDatabasePrefix(string(edge.ID)); ok {
		dbCandidates = append(dbCandidates, db)
	}
	if db, _, ok := ParseDatabasePrefix(string(edge.StartNode)); ok {
		dbCandidates = append(dbCandidates, db)
	}
	if db, _, ok := ParseDatabasePrefix(string(edge.EndNode)); ok {
		dbCandidates = append(dbCandidates, db)
	}

	if len(dbCandidates) == 0 {
		return w.getDatabaseName(), nil
	}

	dbName := dbCandidates[0]
	for _, candidate := range dbCandidates[1:] {
		if candidate != dbName {
			return "", fmt.Errorf("wal: inconsistent database prefixes in edge IDs (id=%q start=%q end=%q)", edge.ID, edge.StartNode, edge.EndNode)
		}
	}
	return dbName, nil
}

func cloneNodeForWAL(dbName string, node *Node) *Node {
	if node == nil {
		return nil
	}
	c := *node
	c.ID = NodeID(StripDatabasePrefix(dbName, string(node.ID)))
	return &c
}

func cloneEdgeForWAL(dbName string, edge *Edge) *Edge {
	if edge == nil {
		return nil
	}
	c := *edge
	c.ID = EdgeID(StripDatabasePrefix(dbName, string(edge.ID)))
	c.StartNode = NodeID(StripDatabasePrefix(dbName, string(edge.StartNode)))
	c.EndNode = NodeID(StripDatabasePrefix(dbName, string(edge.EndNode)))
	return &c
}

// CreateNode logs then executes node creation.
func (w *WALEngine) CreateNode(node *Node) (NodeID, error) {
	return w.CreateNodeWithContext(context.Background(), node)
}

// CreateNodeWithContext carries the caller through mutation validation.
func (w *WALEngine) CreateNodeWithContext(ctx context.Context, node *Node) (NodeID, error) {
	if handled, err := w.tryContextEntityOperations(ctx, []Operation{{Type: OpCreateNode, Node: node}}, CommitModeDirect); handled {
		if err != nil {
			return "", err
		}
		return node.ID, nil
	}

	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if node != nil {
		initializeCreationTimes(&node.CreatedAt, &node.UpdatedAt)
	}

	if config.IsWALEnabled() {
		dbName := w.databaseFromNode(node)
		if err := w.wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: cloneNodeForWAL(dbName, node)}, dbName); err != nil {
			return "", fmt.Errorf("wal: failed to log create_node: %w", err)
		}
	}
	return CreateNodeWithContext(journalApplyContext(ctx), w.engine, node)
}

// UpdateNode logs then executes node update.
func (w *WALEngine) UpdateNode(node *Node) error {
	return w.UpdateNodeWithContext(context.Background(), node)
}

// UpdateNodeWithContext carries the caller through mutation validation.
func (w *WALEngine) UpdateNodeWithContext(ctx context.Context, node *Node) error {
	if handled, err := w.tryContextEntityOperations(ctx, []Operation{{Type: OpUpdateNode, Node: node}}, CommitModeDirect); handled {
		return err
	}

	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	// The underlying engine fills a missing update time; log that same value.
	if node != nil && node.UpdatedAt.IsZero() {
		node.UpdatedAt = time.Now()
	}

	if config.IsWALEnabled() {
		dbName := w.databaseFromNode(node)
		if err := w.wal.AppendWithDatabase(OpUpdateNode, WALNodeData{Node: cloneNodeForWAL(dbName, node)}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log update_node: %w", err)
		}
	}
	return UpdateNodeWithContext(journalApplyContext(ctx), w.engine, node)
}

// UpdateNodeEmbeddingSidecar logs the OpUpdateEmbedding audit record and
// executes the sidecar embedding-only write (embedding key space, never the
// node record) on the wrapped engine. Every production inner engine implements
// EmbeddingSidecarUpdater (compile-time asserted); anything else is an
// invariant violation, reported as an error rather than silently degraded.
func (w *WALEngine) UpdateNodeEmbeddingSidecar(node *Node) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName := w.databaseFromNode(node)
		if err := w.wal.AppendWithDatabase(OpUpdateEmbedding, WALNodeData{Node: cloneNodeForWAL(dbName, node)}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log update_embedding: %w", err)
		}
	}
	sidecar, ok := OwnedCapability[EmbeddingSidecarUpdater](w.engine)
	if !ok {
		return fmt.Errorf("storage engine %T does not support embedding sidecar writes", w.engine)
	}
	return sidecar.UpdateNodeEmbeddingSidecar(node)
}

// StreamParkedEmbeddingFailures forwards the sidecar failure scan to the
// wrapped engine.
func (w *WALEngine) StreamParkedEmbeddingFailures(ctx context.Context, visit func(nodeID NodeID, meta map[string]any) error) (int, error) {
	return VisitEmbeddingFailures(w.engine, ctx, nil, visit)
}

// DeleteNode logs then executes node deletion.
func (w *WALEngine) DeleteNode(id NodeID) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName, unprefixedID := w.getDatabaseName(), string(id)
		if parsedDB, parsedID, ok := ParseDatabasePrefix(string(id)); ok {
			dbName, unprefixedID = parsedDB, parsedID
		}
		if err := w.wal.AppendWithDatabase(OpDeleteNode, WALDeleteData{ID: unprefixedID}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log delete_node: %w", err)
		}
	}
	return w.engine.DeleteNode(id)
}

// CreateEdge logs then executes edge creation.
func (w *WALEngine) CreateEdge(edge *Edge) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if edge != nil {
		initializeCreationTimes(&edge.CreatedAt, &edge.UpdatedAt)
	}

	if config.IsWALEnabled() {
		dbName, err := w.databaseFromEdge(edge)
		if err != nil {
			return err
		}
		if err := w.wal.AppendWithDatabase(OpCreateEdge, WALEdgeData{Edge: cloneEdgeForWAL(dbName, edge)}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log create_edge: %w", err)
		}
	}
	return w.engine.CreateEdge(edge)
}

// UpdateEdge logs then executes edge update.
func (w *WALEngine) UpdateEdge(edge *Edge) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName, err := w.databaseFromEdge(edge)
		if err != nil {
			return err
		}
		if err := w.wal.AppendWithDatabase(OpUpdateEdge, WALEdgeData{Edge: cloneEdgeForWAL(dbName, edge)}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log update_edge: %w", err)
		}
	}
	return w.engine.UpdateEdge(edge)
}

// DeleteEdge logs then executes edge deletion.
func (w *WALEngine) DeleteEdge(id EdgeID) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName, unprefixedID := w.getDatabaseName(), string(id)
		if parsedDB, parsedID, ok := ParseDatabasePrefix(string(id)); ok {
			dbName, unprefixedID = parsedDB, parsedID
		}
		if err := w.wal.AppendWithDatabase(OpDeleteEdge, WALDeleteData{ID: unprefixedID}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log delete_edge: %w", err)
		}
	}
	return w.engine.DeleteEdge(id)
}

// BulkCreateNodes logs then executes bulk node creation.
func (w *WALEngine) BulkCreateNodes(nodes []*Node) error {
	return w.BulkCreateNodesWithContext(context.Background(), nodes)
}

// BulkCreateNodesWithContext carries the caller through mutation validation.
func (w *WALEngine) BulkCreateNodesWithContext(ctx context.Context, nodes []*Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() != nil && config.IsWALEnabled() {
		operations := make([]Operation, len(nodes))
		for i, node := range nodes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if node == nil {
				return ErrInvalidData
			}
			operations[i] = Operation{Type: OpCreateNode, Node: node}
		}
		if handled, err := w.tryContextEntityOperations(ctx, operations, CommitModeImplicit); handled {
			return err
		}
	}

	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	for _, entity := range nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entity != nil {
			initializeCreationTimes(&entity.CreatedAt, &entity.UpdatedAt)
		}
	}

	if config.IsWALEnabled() {
		dbName := w.getDatabaseName()
		cloned := make([]*Node, 0, len(nodes))
		for _, node := range nodes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if node == nil {
				cloned = append(cloned, nil)
				continue
			}
			if db, _, ok := ParseDatabasePrefix(string(node.ID)); ok {
				if dbName == "" || dbName == "nornic" {
					dbName = db
				} else if db != dbName {
					return fmt.Errorf("wal: bulk nodes contain multiple databases: %q vs %q", dbName, db)
				}
			}
			cloned = append(cloned, cloneNodeForWAL(dbName, node))
		}
		if err := w.wal.AppendWithDatabase(OpBulkNodes, WALBulkNodesData{Nodes: cloned}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log bulk_create_nodes: %w", err)
		}
	}
	return BulkCreateNodesWithContext(journalApplyContext(ctx), w.engine, nodes)
}

// BulkCreateEdges logs then executes bulk edge creation.
func (w *WALEngine) BulkCreateEdges(edges []*Edge) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	for _, entity := range edges {
		if entity != nil {
			initializeCreationTimes(&entity.CreatedAt, &entity.UpdatedAt)
		}
	}

	if config.IsWALEnabled() {
		dbName := w.getDatabaseName()
		cloned := make([]*Edge, 0, len(edges))
		for _, edge := range edges {
			if edge == nil {
				cloned = append(cloned, nil)
				continue
			}
			db, err := w.databaseFromEdge(edge)
			if err != nil {
				return err
			}
			if dbName == "" || dbName == "nornic" {
				dbName = db
			} else if db != dbName {
				return fmt.Errorf("wal: bulk edges contain multiple databases: %q vs %q", dbName, db)
			}
			cloned = append(cloned, cloneEdgeForWAL(dbName, edge))
		}
		if err := w.wal.AppendWithDatabase(OpBulkEdges, WALBulkEdgesData{Edges: cloned}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log bulk_create_edges: %w", err)
		}
	}
	return w.engine.BulkCreateEdges(edges)
}

// BulkDeleteNodes logs then executes bulk node deletion.
func (w *WALEngine) BulkDeleteNodes(ids []NodeID) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		// Convert to strings for serialization
		strIDs := make([]string, len(ids))
		dbName := w.getDatabaseName()
		for i, id := range ids {
			str := string(id)
			if db, unprefixed, ok := ParseDatabasePrefix(str); ok {
				if dbName == "" || dbName == "nornic" {
					dbName = db
				} else if db != dbName {
					return fmt.Errorf("wal: bulk delete nodes contains multiple databases: %q vs %q", dbName, db)
				}
				str = unprefixed
			}
			strIDs[i] = str
		}
		if err := w.wal.AppendWithDatabase(OpBulkDeleteNodes, WALBulkDeleteNodesData{IDs: strIDs}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log bulk_delete_nodes: %w", err)
		}
	}
	return w.engine.BulkDeleteNodes(ids)
}

// BulkDeleteEdges logs then executes bulk edge deletion.
func (w *WALEngine) BulkDeleteEdges(ids []EdgeID) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		// Convert to strings for serialization
		strIDs := make([]string, len(ids))
		dbName := w.getDatabaseName()
		for i, id := range ids {
			str := string(id)
			if db, unprefixed, ok := ParseDatabasePrefix(str); ok {
				if dbName == "" || dbName == "nornic" {
					dbName = db
				} else if db != dbName {
					return fmt.Errorf("wal: bulk delete edges contains multiple databases: %q vs %q", dbName, db)
				}
				str = unprefixed
			}
			strIDs[i] = str
		}
		if err := w.wal.AppendWithDatabase(OpBulkDeleteEdges, WALBulkDeleteEdgesData{IDs: strIDs}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log bulk_delete_edges: %w", err)
		}
	}
	return w.engine.BulkDeleteEdges(ids)
}

// Delegate read operations directly to underlying engine

// GetNode delegates to underlying engine.
func (w *WALEngine) GetNode(id NodeID) (*Node, error) {
	return w.engine.GetNode(id)
}

// GetQueryNode delegates the query-row node read (QueryNodeReader) when the
// wrapped engine supports it, else reads the full node.
func (w *WALEngine) GetQueryNode(id NodeID) (*Node, error) {
	return ReadWithCapability(w.engine, func(reader QueryNodeReader) (*Node, error) {
		return reader.GetQueryNode(id)
	}, func() (*Node, error) { return w.engine.GetNode(id) })
}

// GetNodeWithoutEmbeddings delegates an embedding-free node read when the
// wrapped engine supports it. The full read fallback preserves compatibility
// for engines that do not expose the optional projection contract.
func (w *WALEngine) GetNodeWithoutEmbeddings(id NodeID) (*Node, error) {
	return ReadWithCapability(w.engine, func(reader NodeWithoutEmbeddingsReader) (*Node, error) { return reader.GetNodeWithoutEmbeddings(id) }, func() (*Node, error) {
		node, err := w.engine.GetNode(id)
		if err != nil {
			return nil, err
		}
		return copyNodeWithoutEmbeddings(node), nil
	})
}

// GetEdge delegates to underlying engine.
func (w *WALEngine) GetEdge(id EdgeID) (*Edge, error) {
	return w.engine.GetEdge(id)
}

// GetNodesByLabel delegates to underlying engine.
func (w *WALEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return w.GetNodesByLabelWithContext(context.Background(), label)
}

// GetNodesByLabelWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetNodesByLabelWithContext(ctx context.Context, label string) ([]*Node, error) {
	return GetNodesByLabelWithContext(ctx, w.engine, label)
}

// StreamNodesByLabelProjected delegates projected label scans to the underlying engine.
func (w *WALEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*Node) error) error {
	return w.StreamNodesByLabelProjectedWithContext(context.Background(), label, properties, visit)
}

// StreamNodesByLabelProjectedWithContext carries the caller through this read and its materialization.
func (w *WALEngine) StreamNodesByLabelProjectedWithContext(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return VisitWithCapability(w.engine, func(reader ProjectedLabelNodeReader, deliver func(*Node) error) error {
		return StreamNodesByLabelProjectedWithContext(ctx, reader, label, properties, deliver)
	}, nil, visit)
}

// ForEachNodeIDByLabel delegates label-to-nodeID iteration to the underlying
// engine when available. This keeps LIMIT + label paths fast without forcing
// full node materialization.
func (w *WALEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	return w.ForEachNodeIDByLabelWithContext(context.Background(), label, visit)
}

// ForEachNodeIDByLabelWithContext carries the caller through this read and its materialization.
func (w *WALEngine) ForEachNodeIDByLabelWithContext(ctx context.Context, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScopeWithContext(ctx, w.engine, "", label, visit)
}

// GetFirstNodeByLabel delegates to underlying engine.
func (w *WALEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	return w.GetFirstNodeByLabelWithContext(context.Background(), label)
}

// GetFirstNodeByLabelWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetFirstNodeByLabelWithContext(ctx context.Context, label string) (*Node, error) {
	return GetFirstNodeByLabelWithContext(ctx, w.engine, label)
}

// GetNodesByLabelInScope delegates a scoped label read (ScopedLabelNodeReader).
func (w *WALEngine) GetNodesByLabelInScope(scope, label string) ([]*Node, error) {
	return w.GetNodesByLabelInScopeWithContext(context.Background(), scope, label)
}

// GetNodesByLabelInScopeWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetNodesByLabelInScopeWithContext(ctx context.Context, scope, label string) ([]*Node, error) {
	return getNodesByLabelInScopeWithContext(ctx, w.engine, scope, label)
}

// GetFirstNodeByLabelInScope delegates a scoped first-node read (ScopedLabelNodeReader).
func (w *WALEngine) GetFirstNodeByLabelInScope(scope, label string) (*Node, error) {
	return w.GetFirstNodeByLabelInScopeWithContext(context.Background(), scope, label)
}

// GetFirstNodeByLabelInScopeWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetFirstNodeByLabelInScopeWithContext(ctx context.Context, scope, label string) (*Node, error) {
	return getFirstNodeByLabelInScopeWithContext(ctx, w.engine, scope, label)
}

// StreamNodesByLabelProjectedInScope delegates a scoped projected label scan (ScopedLabelNodeReader).
func (w *WALEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return w.StreamNodesByLabelProjectedInScopeWithContext(context.Background(), scope, label, properties, visit)
}

// StreamNodesByLabelProjectedInScopeWithContext carries the caller through this read and its materialization.
func (w *WALEngine) StreamNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return streamNodesByLabelProjectedInScopeWithContext(ctx, w.engine, scope, label, properties, visit)
}

// GetNodesByLabelVisibleAtInScope delegates a scoped snapshot label read (ScopedLabelNodeReader).
func (w *WALEngine) GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error) {
	return w.GetNodesByLabelVisibleAtInScopeWithContext(context.Background(), scope, label, version)
}

// GetNodesByLabelVisibleAtInScopeWithContext carries the caller through this read and its materialization.
func (w *WALEngine) GetNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error) {
	return getNodesByLabelVisibleAtInScopeWithContext(ctx, w.engine, scope, label, version)
}

// BatchGetNodes delegates to underlying engine.
func (w *WALEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return w.BatchGetNodesWithContext(context.Background(), ids)
}

// BatchGetNodesWithContext delegates to the canonical contextual batch owner.
func (w *WALEngine) BatchGetNodesWithContext(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	return BatchGetNodesWithContext(ctx, w.engine, ids)
}

// BatchGetNodesWithoutEmbeddings forwards a batched embedding-free read
// without materializing vectors in the WAL wrapper.
func (w *WALEngine) BatchGetNodesWithoutEmbeddings(ids []NodeID) (map[NodeID]*Node, error) {
	return w.BatchGetNodesWithoutEmbeddingsWithContext(context.Background(), ids)
}

// BatchGetNodesWithoutEmbeddingsWithContext delegates without loading vectors.
func (w *WALEngine) BatchGetNodesWithoutEmbeddingsWithContext(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	return batchNodesWithContext(ctx, w.engine, ids, true)
}

// BatchGetNodesWithoutEmbeddingsSupported reports the wrapped engine's real
// capability rather than the presence of this forwarding method.
func (w *WALEngine) BatchGetNodesWithoutEmbeddingsSupported() bool {
	return batchNodeWithoutEmbeddingsSupported(w.engine)
}

// GetOutgoingEdges delegates to underlying engine.
func (w *WALEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.engine.GetOutgoingEdges(nodeID)
}

// GetIncomingEdges delegates to underlying engine.
func (w *WALEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.engine.GetIncomingEdges(nodeID)
}

// GetAdjacentEdges delegates to the inner engine when it implements the
// optional AdjacentEdgesEngine capability. WAL writes are never reflected
// in reads (it only logs), so a forwarded call sees the same data as the
// pair of single-direction methods.
func (w *WALEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	return GetAdjacentEdges(w.engine, nodeID)
}

// GetEdgesBetween delegates to underlying engine.
func (w *WALEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return w.engine.GetEdgesBetween(startID, endID)
}

// GetEdgeBetween delegates to underlying engine.
//
// Deprecated: Use GetEdgesBetween and filter by type to receive errors. Failed lookups may appear as nil.
func (w *WALEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	return w.engine.GetEdgeBetween(startID, endID, edgeType)
}

// AllNodes delegates to underlying engine.
func (w *WALEngine) AllNodes() ([]*Node, error) { return w.AllNodesWithContext(context.Background()) }

// AllNodesWithContext carries the caller through full materialization.
func (w *WALEngine) AllNodesWithContext(ctx context.Context) ([]*Node, error) {
	return AllNodesWithContext(ctx, w.engine)
}

// AllEdges delegates to underlying engine.
func (w *WALEngine) AllEdges() ([]*Edge, error) {
	return w.engine.AllEdges()
}

// GetEdgesByType delegates to underlying engine.
func (w *WALEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return w.engine.GetEdgesByType(edgeType)
}

// GetAllNodes delegates to underlying engine.
//
// Deprecated: Use AllNodes to receive errors. Failed reads are omitted and may appear as an empty collection.
func (w *WALEngine) GetAllNodes() []*Node {
	return w.engine.GetAllNodes()
}

// GetInDegree delegates to underlying engine.
//
// Deprecated: Use GetIncomingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (w *WALEngine) GetInDegree(nodeID NodeID) int {
	return w.engine.GetInDegree(nodeID)
}

// GetOutDegree delegates to underlying engine.
//
// Deprecated: Use GetOutgoingEdges and count the returned edges to receive errors. Failed reads may appear as zero.
func (w *WALEngine) GetOutDegree(nodeID NodeID) int {
	return w.engine.GetOutDegree(nodeID)
}

// GetSchema delegates to underlying engine.
func (w *WALEngine) GetSchema() *SchemaManager {
	return w.engine.GetSchema()
}

// GetSchemaForNamespace implements NamespaceSchemaProvider when the underlying engine supports it.
func (w *WALEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if p, ok := ViewMetadataForEngine[NamespaceSchemaProvider](w.engine); ok {
		return p.GetSchemaForNamespace(namespace)
	}
	return w.engine.GetSchema()
}

// NodeCount delegates to underlying engine.
func (w *WALEngine) NodeCount() (int64, error) {
	return w.engine.NodeCount()
}

func (w *WALEngine) NodeCountByPrefix(prefix string) (int64, error) {
	return ReadWithCapability(w.engine, func(stats PrefixStatsEngine) (int64, error) { return stats.NodeCountByPrefix(prefix) }, func() (int64, error) {
		var count int64
		err := StreamNodesWithFallback(context.Background(), w.engine, 1000, func(value *Node) error {
			if value != nil && strings.HasPrefix(string(value.ID), prefix) {
				count++
			}
			return nil
		})
		return count, err
	})
}

func (w *WALEngine) NodeCountByLabel(label string) (int64, error) {
	return w.NodeCountByLabelWithContext(context.Background(), label)
}

// NodeCountByLabelWithContext carries the caller through this read and its materialization.
func (w *WALEngine) NodeCountByLabelWithContext(ctx context.Context, label string) (int64, error) {
	return CountNodesWithLabel(ctx, w.engine, label)
}

// EdgeCountByType delegates to the inner engine's per-type counter
// (issue #638). No edge materialization on this path.
func (w *WALEngine) EdgeCountByType(edgeType string) (int64, error) {
	return w.engine.EdgeCountByType(edgeType)
}

func (w *WALEngine) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return w.engine.EdgeCountByStartLabel(label, edgeType)
}

func (w *WALEngine) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return w.engine.EdgeCountByEndLabel(label, edgeType)
}

// NotePropertyKeysInNamespace forwards to the wrapped engine
// (PropertyKeyRegistry); the WAL applies each write to it as it logs it.
func (w *WALEngine) NotePropertyKeysInNamespace(namespace string, properties map[string]interface{}) {
	if w.keyRegistry != nil {
		w.keyRegistry.NotePropertyKeysInNamespace(namespace, properties)
	}
}

// PropertyKeyKnownInNamespace forwards the property-key lookup
// (PropertyKeyRegistry).
func (w *WALEngine) PropertyKeyKnownInNamespace(namespace, name string) bool {
	registry, ok := ViewMetadataForEngine[NamespacePropertyKeyLookup](w.engine)
	return ok && registry.PropertyKeyKnownInNamespace(namespace, name)
}

func (w *WALEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	return w.NodeCountByLabelInNamespaceWithContext(context.Background(), namespace, label)
}

// NodeCountByLabelInNamespaceWithContext carries the caller through this read and its materialization.
func (w *WALEngine) NodeCountByLabelInNamespaceWithContext(ctx context.Context, namespace, label string) (int64, error) {
	return namespaceLabelCountWithContext(ctx, w.engine, namespace, label)
}

// EdgeCount delegates to underlying engine.
func (w *WALEngine) EdgeCount() (int64, error) {
	return w.engine.EdgeCount()
}

func (w *WALEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	return ReadWithCapability(w.engine, func(stats PrefixStatsEngine) (int64, error) { return stats.EdgeCountByPrefix(prefix) }, func() (int64, error) {
		var count int64
		err := StreamEdgesWithFallback(context.Background(), w.engine, 1000, func(value *Edge) error {
			if value != nil && strings.HasPrefix(string(value.ID), prefix) {
				count++
			}
			return nil
		})
		return count, err
	})
}

// Close closes both the WAL and underlying engine.
func (w *WALEngine) Close() error {
	// Seal admission before draining registered native frames. Registration uses
	// the same WAL mutex, so no pending frame can appear after this cut.
	_ = w.wal.Close()
	w.stopEntityWait()
	w.pendingEntityCommits.Range(func(_, value any) bool {
		accepted := value.(*walAcceptedEntityCommit)
		accepted.prepared.CloseNative()
		accepted.releaseMutation()
		_ = accepted.resolve.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: ErrStorageClosed})
		return true
	})
	// A snapshot worker may have been waiting behind an accepted mutation lease.
	w.DisableAutoCompaction()
	return w.engine.Close()
}

// GetWAL returns the underlying WAL for direct access.
func (w *WALEngine) GetWAL() *WAL {
	return w.wal
}

// FindNodeNeedingEmbedding delegates to underlying engine if it supports it.
func (w *WALEngine) FindNodeNeedingEmbedding() *Node {
	return ReadValueWithCapability(w.engine, func(reader interface{ FindNodeNeedingEmbedding() *Node }) *Node {
		return reader.FindNodeNeedingEmbedding()
	}, nil)
}

// RefreshPendingEmbeddingsIndex delegates to underlying engine if it supports it.
func (w *WALEngine) RefreshPendingEmbeddingsIndex() int {
	if mgr, ok := OwnedCapability[interface{ RefreshPendingEmbeddingsIndex() int }](w.engine); ok {
		return mgr.RefreshPendingEmbeddingsIndex()
	}
	return 0
}

// MarkNodeEmbedded delegates to underlying engine if it supports it.
func (w *WALEngine) MarkNodeEmbedded(nodeID NodeID) {
	if mgr, ok := OwnedCapability[interface{ MarkNodeEmbedded(NodeID) }](w.engine); ok {
		mgr.MarkNodeEmbedded(nodeID)
	}
}

// AddToPendingEmbeddings delegates to underlying engine if it supports it.
func (w *WALEngine) AddToPendingEmbeddings(nodeID NodeID) {
	if mgr, ok := OwnedCapability[interface{ AddToPendingEmbeddings(NodeID) }](w.engine); ok {
		mgr.AddToPendingEmbeddings(nodeID)
	}
}

// PendingEmbeddingsCount delegates to underlying engine if it supports it.
func (w *WALEngine) PendingEmbeddingsCount() int {
	return ReadValueWithCapability(w.engine, func(reader interface{ PendingEmbeddingsCount() int }) int { return reader.PendingEmbeddingsCount() }, nil)
}

// IterateNodes delegates to underlying engine if it supports streaming iteration.
func (w *WALEngine) IterateNodes(visit func(*Node) bool) error {
	return VisitNodes(w.engine, visit)
}

// ============================================================================
// StreamingEngine Implementation
// ============================================================================

// StreamNodes implements StreamingEngine.StreamNodes by delegating to the underlying engine.
// StreamNodesWithOptions streams nodes through the options-driven kernel.
// The kernel is part of the Engine contract, so the delegation is direct.
func (w *WALEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(node *Node) error) error {
	return w.engine.StreamNodesWithOptions(ctx, opts, fn)
}

func (w *WALEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	return w.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}, fn)
}

// StreamNodesWithoutEmbeddings preserves embedding-free snapshot scans through
// the WAL decorator.
func (w *WALEngine) StreamNodesWithoutEmbeddings(ctx context.Context, fn func(node *Node) error) error {
	return w.StreamNodesWithOptions(ctx, StreamNodesOptions{}, fn)
}

// StreamNodesByPrefix implements PrefixStreamingEngine by delegating prefix-scoped
// iteration to the wrapped engine when available. This preserves namespace-aware
// early termination behavior for MATCH ... LIMIT hot paths.
func (w *WALEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	return w.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, WithEmbeddings: true, ApplyDecayFilter: true}, fn)
}

// StreamNodesByPrefixProjected preserves embedding-free projected scans
// through the WAL decorator.
func (w *WALEngine) StreamNodesByPrefixProjected(ctx context.Context, prefix string, properties []string, fn func(node *Node) error) error {
	if properties == nil {
		return w.StreamNodesByPrefix(ctx, prefix, fn)
	}
	return w.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, Projection: properties}, fn)
}

// StreamNodesByPrefixWithoutEmbeddings preserves lightweight scans through
// the WAL decorator.
func (w *WALEngine) StreamNodesByPrefixWithoutEmbeddings(ctx context.Context, prefix string, fn func(node *Node) error) error {
	return w.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, Projection: []string{}, StripEmbeddings: true}, fn)
}

// StreamEdges implements StreamingEngine.StreamEdges by delegating to the underlying engine.
func (w *WALEngine) StreamEdges(ctx context.Context, fn func(*Edge) error) error {
	return StreamEdgesWithFallback(ctx, w.engine, 1000, fn)
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks by delegating to the underlying engine.
func (w *WALEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func([]*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	chunk := make([]*Node, 0, chunkSize)
	err := w.StreamNodes(ctx, func(node *Node) error {
		chunk = append(chunk, node)
		if len(chunk) >= chunkSize {
			err := fn(chunk)
			chunk = make([]*Node, 0, chunkSize)
			return err
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

// DeleteByPrefix delegates to the underlying engine.
func (w *WALEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	return w.engine.DeleteByPrefix(prefix)
}

// Verify WALEngine implements Engine interface
var _ Engine = (*WALEngine)(nil)

// Verify WALEngine implements StreamingEngine interface
var _ StreamingEngine = (*WALEngine)(nil)

// Verify WALEngine implements PrefixStreamingEngine interface
var _ PrefixStreamingEngine = (*WALEngine)(nil)
