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
	engine Engine
	wal    *WAL
	// mutationMu serializes auto-compaction snapshots against in-flight mutating
	// operations to avoid WAL/engine state skew during snapshot truncation.
	mutationMu sync.RWMutex

	// Automatic snapshot and compaction
	snapshotDir      string
	snapshotMu       sync.RWMutex // Protects snapshotTicker and stopSnapshot
	snapshotTicker   *time.Ticker
	stopSnapshot     chan struct{}
	snapshotWg       sync.WaitGroup // Waits for auto-compaction goroutine to finish
	lastSnapshotTime atomic.Int64
	totalSnapshots   atomic.Int64
}

// ListNamespaces returns known namespaces from the wrapped engine, if supported.
func (w *WALEngine) ListNamespaces() []string {
	if lister, ok := w.engine.(NamespaceLister); ok {
		return lister.ListNamespaces()
	}
	return nil
}

// IsCurrentTemporalNode delegates current-version checks to the wrapped engine when supported.
func (w *WALEngine) IsCurrentTemporalNode(node *Node, asOf time.Time) (bool, error) {
	if provider, ok := w.engine.(TemporalCurrentNodeEngine); ok {
		return provider.IsCurrentTemporalNode(node, asOf)
	}
	return true, nil
}

// RebuildTemporalIndexes delegates temporal index rebuild to the wrapped engine when supported.
func (w *WALEngine) RebuildTemporalIndexes(ctx context.Context) error {
	if maint, ok := w.engine.(TemporalMaintenanceEngine); ok {
		return maint.RebuildTemporalIndexes(ctx)
	}
	return nil
}

// PruneTemporalHistory delegates temporal pruning to the wrapped engine when supported.
func (w *WALEngine) PruneTemporalHistory(ctx context.Context, opts TemporalPruneOptions) (int64, error) {
	if maint, ok := w.engine.(TemporalMaintenanceEngine); ok {
		return maint.PruneTemporalHistory(ctx, opts)
	}
	return 0, nil
}

// RebuildMVCCHeads delegates MVCC head rebuild to the wrapped engine when supported.
func (w *WALEngine) RebuildMVCCHeads(ctx context.Context) error {
	if maint, ok := w.engine.(MVCCMaintenanceEngine); ok {
		return maint.RebuildMVCCHeads(ctx)
	}
	return ErrNotImplemented
}

// PruneMVCCVersions delegates MVCC pruning to the wrapped engine when supported.
func (w *WALEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if maint, ok := w.engine.(MVCCMaintenanceEngine); ok {
		return maint.PruneMVCCVersions(ctx, opts)
	}
	return 0, ErrNotImplemented
}

// GetNodeLatestEffective delegates MVCC latest-effective reads to the wrapped engine when supported.
func (w *WALEngine) GetNodeLatestEffective(id NodeID) (*Node, error) {
	if provider, ok := w.engine.(MVCCLatestEffectiveEngine); ok {
		return provider.GetNodeLatestEffective(id)
	}
	return w.engine.GetNode(id)
}

// GetEdgeLatestEffective delegates MVCC latest-effective edge reads to the wrapped engine when supported.
func (w *WALEngine) GetEdgeLatestEffective(id EdgeID) (*Edge, error) {
	if provider, ok := w.engine.(MVCCLatestEffectiveEngine); ok {
		return provider.GetEdgeLatestEffective(id)
	}
	return w.engine.GetEdge(id)
}

// GetNodeLatestVisible delegates latest-visible node reads when supported.
func (w *WALEngine) GetNodeLatestVisible(id NodeID) (*Node, error) {
	if provider, ok := w.engine.(MVCCVisibilityEngine); ok {
		return provider.GetNodeLatestVisible(id)
	}
	return w.GetNodeLatestEffective(id)
}

// GetEdgeLatestVisible delegates latest-visible edge reads when supported.
func (w *WALEngine) GetEdgeLatestVisible(id EdgeID) (*Edge, error) {
	if provider, ok := w.engine.(MVCCVisibilityEngine); ok {
		return provider.GetEdgeLatestVisible(id)
	}
	return w.GetEdgeLatestEffective(id)
}

// GetNodeVisibleAt delegates snapshot-visible node reads when supported.
func (w *WALEngine) GetNodeVisibleAt(id NodeID, version MVCCVersion) (*Node, error) {
	if provider, ok := w.engine.(MVCCVisibilityEngine); ok {
		return provider.GetNodeVisibleAt(id, version)
	}
	return nil, ErrNotImplemented
}

// GetEdgeVisibleAt delegates snapshot-visible edge reads when supported.
func (w *WALEngine) GetEdgeVisibleAt(id EdgeID, version MVCCVersion) (*Edge, error) {
	if provider, ok := w.engine.(MVCCVisibilityEngine); ok {
		return provider.GetEdgeVisibleAt(id, version)
	}
	return nil, ErrNotImplemented
}

// GetNodesByLabelVisibleAt delegates snapshot-visible label queries when supported.
func (w *WALEngine) GetNodesByLabelVisibleAt(label string, version MVCCVersion) ([]*Node, error) {
	if provider, ok := w.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, ErrNotImplemented
}

// GetEdgesByTypeVisibleAt delegates snapshot-visible edge-type queries when supported.
func (w *WALEngine) GetEdgesByTypeVisibleAt(edgeType string, version MVCCVersion) ([]*Edge, error) {
	if provider, ok := w.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}
	return nil, ErrNotImplemented
}

// GetEdgesBetweenVisibleAt delegates snapshot-visible topology queries when supported.
func (w *WALEngine) GetEdgesBetweenVisibleAt(startID, endID NodeID, version MVCCVersion) ([]*Edge, error) {
	if provider, ok := w.engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}
	return nil, ErrNotImplemented
}

// RegisterSnapshotReader delegates snapshot-reader registration when supported.
func (w *WALEngine) RegisterSnapshotReader(info SnapshotReaderInfo) func() {
	if lce, ok := w.engine.(MVCCLifecycleEngine); ok {
		return lce.RegisterSnapshotReader(info)
	}
	return func() {}
}

// LifecycleStatus delegates lifecycle status when supported.
func (w *WALEngine) LifecycleStatus() map[string]interface{} {
	if lce, ok := w.engine.(MVCCLifecycleEngine); ok {
		return lce.LifecycleStatus()
	}
	return map[string]interface{}{"enabled": false}
}

// TriggerPruneNow delegates lifecycle prune-now when supported.
func (w *WALEngine) TriggerPruneNow(ctx context.Context) error {
	if lce, ok := w.engine.(MVCCLifecycleEngine); ok {
		return lce.TriggerPruneNow(ctx)
	}
	return nil
}

// PauseLifecycle delegates lifecycle pause when supported.
func (w *WALEngine) PauseLifecycle() {
	if lce, ok := w.engine.(MVCCLifecycleEngine); ok {
		lce.PauseLifecycle()
	}
}

// ResumeLifecycle delegates lifecycle resume when supported.
func (w *WALEngine) ResumeLifecycle() {
	if lce, ok := w.engine.(MVCCLifecycleEngine); ok {
		lce.ResumeLifecycle()
	}
}

// SetLifecycleSchedule delegates lifecycle cadence updates when supported.
func (w *WALEngine) SetLifecycleSchedule(interval time.Duration) error {
	if scheduler, ok := w.engine.(MVCCLifecycleScheduleEngine); ok {
		return scheduler.SetLifecycleSchedule(interval)
	}
	return nil
}

// TopLifecycleDebtKeys delegates lifecycle debt inspection when supported.
func (w *WALEngine) TopLifecycleDebtKeys(limit int) []MVCCLifecycleDebtKey {
	if provider, ok := w.engine.(MVCCLifecycleDebtEngine); ok {
		return provider.TopLifecycleDebtKeys(limit)
	}
	return nil
}

// GetNodeCurrentHead delegates node head lookup when supported.
func (w *WALEngine) GetNodeCurrentHead(id NodeID) (MVCCHead, error) {
	if provider, ok := w.engine.(MVCCHeadEngine); ok {
		return provider.GetNodeCurrentHead(id)
	}
	return MVCCHead{}, ErrNotImplemented
}

// GetEdgeCurrentHead delegates edge head lookup when supported.
func (w *WALEngine) GetEdgeCurrentHead(id EdgeID) (MVCCHead, error) {
	if provider, ok := w.engine.(MVCCHeadEngine); ok {
		return provider.GetEdgeCurrentHead(id)
	}
	return MVCCHead{}, ErrNotImplemented
}

// NewWALEngine creates a WAL-backed storage engine.
func NewWALEngine(engine Engine, wal *WAL) *WALEngine {
	return &WALEngine{
		engine: engine,
		wal:    wal,
	}
}

// GetInnerEngine returns the wrapped storage engine.
func (w *WALEngine) GetInnerEngine() Engine {
	if w == nil {
		return nil
	}
	return w.engine
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

	// Create snapshot from current engine state
	snapshot, err := w.wal.CreateSnapshot(w.engine)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Save snapshot with high-resolution timestamp to avoid filename collisions
	// when snapshots happen multiple times within the same second.
	timestamp := time.Now().Format("20060102-150405.000000000")
	snapshotPath := filepath.Join(w.snapshotDir, fmt.Sprintf("snapshot-%s.json", timestamp))

	if err := SaveSnapshot(snapshot, snapshotPath); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Truncate WAL to remove entries before snapshot
	// This is the key compaction step that prevents unbounded growth
	if err := w.wal.TruncateAfterSnapshot(snapshot.Sequence); err != nil {
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
	if namespacedEngine, ok := w.engine.(*NamespacedEngine); ok {
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
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName := w.databaseFromNode(node)
		if err := w.wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: cloneNodeForWAL(dbName, node)}, dbName); err != nil {
			return "", fmt.Errorf("wal: failed to log create_node: %w", err)
		}
	}
	return w.engine.CreateNode(node)
}

// UpdateNode logs then executes node update.
func (w *WALEngine) UpdateNode(node *Node) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName := w.databaseFromNode(node)
		if err := w.wal.AppendWithDatabase(OpUpdateNode, WALNodeData{Node: cloneNodeForWAL(dbName, node)}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log update_node: %w", err)
		}
	}
	return w.engine.UpdateNode(node)
}

// UpdateNodeEmbedding logs then executes embedding-only node update.
// Uses OpUpdateEmbedding which is safe to skip during WAL recovery
// since embeddings can be regenerated automatically.
func (w *WALEngine) UpdateNodeEmbedding(node *Node) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName := w.databaseFromNode(node)
		if err := w.wal.AppendWithDatabase(OpUpdateEmbedding, WALNodeData{Node: cloneNodeForWAL(dbName, node)}, dbName); err != nil {
			return fmt.Errorf("wal: failed to log update_embedding: %w", err)
		}
	}
	// Prefer the embedding-only update path on the wrapped engine (e.g., AsyncEngine)
	// so we don't accidentally treat embedding updates as creates in pending caches.
	if embedUpdater, ok := w.engine.(interface{ UpdateNodeEmbedding(*Node) error }); ok {
		return embedUpdater.UpdateNodeEmbedding(node)
	}
	return w.engine.UpdateNode(node)
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
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

	if config.IsWALEnabled() {
		dbName := w.getDatabaseName()
		cloned := make([]*Node, 0, len(nodes))
		for _, node := range nodes {
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
	return w.engine.BulkCreateNodes(nodes)
}

// BulkCreateEdges logs then executes bulk edge creation.
func (w *WALEngine) BulkCreateEdges(edges []*Edge) error {
	w.mutationMu.RLock()
	defer w.mutationMu.RUnlock()

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

// GetEdge delegates to underlying engine.
func (w *WALEngine) GetEdge(id EdgeID) (*Edge, error) {
	return w.engine.GetEdge(id)
}

// GetNodesByLabel delegates to underlying engine.
func (w *WALEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return w.engine.GetNodesByLabel(label)
}

// ForEachNodeIDByLabel delegates label-to-nodeID iteration to the underlying
// engine when available. This keeps LIMIT + label paths fast without forcing
// full node materialization.
func (w *WALEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	if lookup, ok := w.engine.(LabelNodeIDLookupEngine); ok {
		return lookup.ForEachNodeIDByLabel(label, visit)
	}
	ids, err := NodeIDsByLabel(w.engine, label, 0)
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

// GetFirstNodeByLabel delegates to underlying engine.
func (w *WALEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	return w.engine.GetFirstNodeByLabel(label)
}

// BatchGetNodes delegates to underlying engine.
func (w *WALEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return w.engine.BatchGetNodes(ids)
}

// GetOutgoingEdges delegates to underlying engine.
func (w *WALEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.engine.GetOutgoingEdges(nodeID)
}

// GetIncomingEdges delegates to underlying engine.
func (w *WALEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.engine.GetIncomingEdges(nodeID)
}

// GetEdgesBetween delegates to underlying engine.
func (w *WALEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return w.engine.GetEdgesBetween(startID, endID)
}

// GetEdgeBetween delegates to underlying engine.
func (w *WALEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	return w.engine.GetEdgeBetween(startID, endID, edgeType)
}

// AllNodes delegates to underlying engine.
func (w *WALEngine) AllNodes() ([]*Node, error) {
	return w.engine.AllNodes()
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
func (w *WALEngine) GetAllNodes() []*Node {
	return w.engine.GetAllNodes()
}

// GetInDegree delegates to underlying engine.
func (w *WALEngine) GetInDegree(nodeID NodeID) int {
	return w.engine.GetInDegree(nodeID)
}

// GetOutDegree delegates to underlying engine.
func (w *WALEngine) GetOutDegree(nodeID NodeID) int {
	return w.engine.GetOutDegree(nodeID)
}

// GetSchema delegates to underlying engine.
func (w *WALEngine) GetSchema() *SchemaManager {
	return w.engine.GetSchema()
}

// GetSchemaForNamespace implements NamespaceSchemaProvider when the underlying engine supports it.
func (w *WALEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if p, ok := w.engine.(NamespaceSchemaProvider); ok {
		return p.GetSchemaForNamespace(namespace)
	}
	return w.engine.GetSchema()
}

// NodeCount delegates to underlying engine.
func (w *WALEngine) NodeCount() (int64, error) {
	return w.engine.NodeCount()
}

func (w *WALEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if stats, ok := w.engine.(PrefixStatsEngine); ok {
		return stats.NodeCountByPrefix(prefix)
	}

	// Correctness fallback (slower).
	if streamer, ok := w.engine.(StreamingEngine); ok {
		var count int64
		err := streamer.StreamNodes(context.Background(), func(node *Node) error {
			if strings.HasPrefix(string(node.ID), prefix) {
				count++
			}
			return nil
		})
		return count, err
	}

	nodes, err := w.engine.AllNodes()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, node := range nodes {
		if strings.HasPrefix(string(node.ID), prefix) {
			count++
		}
	}
	return count, nil
}

// EdgeCount delegates to underlying engine.
func (w *WALEngine) EdgeCount() (int64, error) {
	return w.engine.EdgeCount()
}

func (w *WALEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if stats, ok := w.engine.(PrefixStatsEngine); ok {
		return stats.EdgeCountByPrefix(prefix)
	}

	// Correctness fallback (slower).
	if streamer, ok := w.engine.(StreamingEngine); ok {
		var count int64
		err := streamer.StreamEdges(context.Background(), func(edge *Edge) error {
			if strings.HasPrefix(string(edge.ID), prefix) {
				count++
			}
			return nil
		})
		return count, err
	}

	edges, err := w.engine.AllEdges()
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		if strings.HasPrefix(string(edge.ID), prefix) {
			count++
		}
	}
	return count, nil
}

// Close closes both the WAL and underlying engine.
func (w *WALEngine) Close() error {
	// Stop auto-compaction if enabled
	w.DisableAutoCompaction()

	// Sync and close WAL first
	if err := w.wal.Close(); err != nil {
		// Log but continue
	}
	return w.engine.Close()
}

// GetWAL returns the underlying WAL for direct access.
func (w *WALEngine) GetWAL() *WAL {
	return w.wal
}

// GetEngine returns the underlying engine.
func (w *WALEngine) GetEngine() Engine {
	return w.engine
}

// FindNodeNeedingEmbedding delegates to underlying engine if it supports it.
func (w *WALEngine) FindNodeNeedingEmbedding() *Node {
	if finder, ok := w.engine.(interface{ FindNodeNeedingEmbedding() *Node }); ok {
		return finder.FindNodeNeedingEmbedding()
	}
	return nil
}

// RefreshPendingEmbeddingsIndex delegates to underlying engine if it supports it.
func (w *WALEngine) RefreshPendingEmbeddingsIndex() int {
	if mgr, ok := w.engine.(interface{ RefreshPendingEmbeddingsIndex() int }); ok {
		return mgr.RefreshPendingEmbeddingsIndex()
	}
	return 0
}

// MarkNodeEmbedded delegates to underlying engine if it supports it.
func (w *WALEngine) MarkNodeEmbedded(nodeID NodeID) {
	if mgr, ok := w.engine.(interface{ MarkNodeEmbedded(NodeID) }); ok {
		mgr.MarkNodeEmbedded(nodeID)
	}
}

// AddToPendingEmbeddings delegates to underlying engine if it supports it.
func (w *WALEngine) AddToPendingEmbeddings(nodeID NodeID) {
	if mgr, ok := w.engine.(interface{ AddToPendingEmbeddings(NodeID) }); ok {
		mgr.AddToPendingEmbeddings(nodeID)
	}
}

// PendingEmbeddingsCount delegates to underlying engine if it supports it.
func (w *WALEngine) PendingEmbeddingsCount() int {
	if mgr, ok := w.engine.(interface{ PendingEmbeddingsCount() int }); ok {
		return mgr.PendingEmbeddingsCount()
	}
	return 0
}

// IterateNodes delegates to underlying engine if it supports streaming iteration.
func (w *WALEngine) IterateNodes(fn func(*Node) bool) error {
	if iterator, ok := w.engine.(interface{ IterateNodes(func(*Node) bool) error }); ok {
		return iterator.IterateNodes(fn)
	}
	return fmt.Errorf("underlying engine does not support IterateNodes")
}

// ============================================================================
// StreamingEngine Implementation
// ============================================================================

// StreamNodes implements StreamingEngine.StreamNodes by delegating to the underlying engine.
func (w *WALEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	if streamer, ok := w.engine.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, fn)
	}
	// Fallback: load all nodes
	nodes, err := w.engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamNodesByPrefix implements PrefixStreamingEngine by delegating prefix-scoped
// iteration to the wrapped engine when available. This preserves namespace-aware
// early termination behavior for MATCH ... LIMIT hot paths.
func (w *WALEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	if prefixStreamer, ok := w.engine.(PrefixStreamingEngine); ok {
		err := prefixStreamer.StreamNodesByPrefix(ctx, prefix, fn)
		if err == ErrIterationStopped {
			return nil
		}
		return err
	}
	// Fallback to StreamNodes + prefix filter.
	return w.StreamNodes(ctx, func(node *Node) error {
		if !strings.HasPrefix(string(node.ID), prefix) {
			return nil
		}
		return fn(node)
	})
}

// StreamEdges implements StreamingEngine.StreamEdges by delegating to the underlying engine.
func (w *WALEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if streamer, ok := w.engine.(StreamingEngine); ok {
		return streamer.StreamEdges(ctx, fn)
	}
	// Fallback: load all edges
	edges, err := w.engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if err := fn(edge); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks by delegating to the underlying engine.
func (w *WALEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if streamer, ok := w.engine.(StreamingEngine); ok {
		return streamer.StreamNodeChunks(ctx, chunkSize, fn)
	}
	// Fallback: use StreamNodes to build chunks
	chunk := make([]*Node, 0, chunkSize)
	err := w.StreamNodes(ctx, func(node *Node) error {
		chunk = append(chunk, node)
		if len(chunk) >= chunkSize {
			if err := fn(chunk); err != nil {
				return err
			}
			chunk = make([]*Node, 0, chunkSize)
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
