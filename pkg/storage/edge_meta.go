// Package storage provides edge provenance logging for NornicDB.
//
// Edge provenance tracks the audit trail for edges - why they were created,
// when, what evidence supports them, and their lifecycle state.
//
// Feature flag: NORNICDB_EDGE_PROVENANCE_ENABLED (enabled by default)
//
// Usage:
//
//	store := NewEdgeMetaStore()
//	meta := EdgeMeta{
//	    Src:        "node-A",
//	    Dst:        "node-B",
//	    Label:      "relates_to",
//	    Score:      0.85,
//	    SignalType: "similarity",
//	}
//	store.Append(ctx, meta)
//
//	// Query history
//	history, _ := store.GetHistory(ctx, "node-A", "node-B", "relates_to")
package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/config"
)

// SignalType describes how an edge was detected/created.
type SignalType string

const (
	SignalSimilarity   SignalType = "similarity" // Semantic similarity
	SignalCoAccess     SignalType = "coaccess"   // Co-access pattern
	SignalTopology     SignalType = "topology"   // Topological link prediction
	SignalLLMInference SignalType = "llm-infer"  // LLM-based inference
	SignalManual       SignalType = "manual"     // User-created
	SignalTransitive   SignalType = "transitive" // Transitive inference
	SignalTemporal     SignalType = "temporal"   // Temporal proximity
)

// EdgeMeta stores provenance for auto-generated edges.
// This is append-only for auditability.
type EdgeMeta struct {
	// Edge identification
	EdgeID string `json:"edge_id"`
	Src    string `json:"src"`
	Dst    string `json:"dst"`
	Label  string `json:"label"`

	// Confidence and scoring
	Score         float64 `json:"score"`
	SignalType    string  `json:"signal_type"` // "coaccess", "similarity", "topology", "llm-infer", "manual"
	EvidenceCount int     `json:"evidence_count"`
	DecayState    float64 `json:"decay_state"`

	// Temporal
	Timestamp time.Time `json:"timestamp"`
	SessionID string    `json:"session_id,omitempty"`

	// Lifecycle
	Materialized bool   `json:"materialized"`
	Origin       string `json:"origin"` // agent/commit ID that created this

	// TLP-specific (topology link prediction)
	TopologyAlgorithm string  `json:"topology_algorithm,omitempty"`
	TopologyScore     float64 `json:"topology_score,omitempty"`
	SemanticScore     float64 `json:"semantic_score,omitempty"`

	// Method that generated this edge (detailed)
	Method string `json:"method,omitempty"`
	Reason string `json:"reason,omitempty"`

	// Additional metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// EdgeMetaKey uniquely identifies an edge for provenance lookup.
type EdgeMetaKey struct {
	Src   string
	Dst   string
	Label string
}

// String returns a string representation of the key.
func (k EdgeMetaKey) String() string {
	return fmt.Sprintf("%s:%s:%s", k.Src, k.Dst, k.Label)
}

// EdgeMetaStore manages edge provenance.
// Thread-safe and append-only for auditability.
type EdgeMetaStore struct {
	mu       sync.RWMutex
	records  map[string][]*EdgeMeta // key: "src:dst:label" -> history
	allMetas []*EdgeMeta            // All records in order

	// Stats
	totalAppended     int64
	totalMaterialized int64
}

// EdgeMetaStats provides observability into provenance tracking.
type EdgeMetaStats struct {
	TotalRecords      int64
	TotalMaterialized int64
	UniqueEdges       int64
	BySignalType      map[string]int64
}

// NewEdgeMetaStore creates a new edge metadata store.
func NewEdgeMetaStore() *EdgeMetaStore {
	return &EdgeMetaStore{
		records:  make(map[string][]*EdgeMeta),
		allMetas: make([]*EdgeMeta, 0),
	}
}

// Append adds a new evidence record (immutable).
// This is the primary method for recording edge provenance.
func (s *EdgeMetaStore) Append(ctx context.Context, meta EdgeMeta) error {
	// Feature flag check - if disabled, skip recording
	if !config.IsEdgeProvenanceEnabled() {
		return nil
	}

	// Detach the caller's record before the store lock. The metadata copy runs
	// the caller-copy protocol (CopyValue), which asks a caller-declared value
	// for its own copy; that caller code may read this store through its public
	// API, so it must never run while the store lock is held.
	stored := copyEdgeMeta(&meta)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Set timestamp if not provided
	if stored.Timestamp.IsZero() {
		stored.Timestamp = time.Now()
	}

	// Generate edge ID if not provided
	if stored.EdgeID == "" {
		stored.EdgeID = "edge-" + uuid.NewString()
	}

	// Create key
	key := EdgeMetaKey{Src: stored.Src, Dst: stored.Dst, Label: stored.Label}
	keyStr := key.String()

	s.records[keyStr] = append(s.records[keyStr], stored)
	s.allMetas = append(s.allMetas, stored)

	s.totalAppended++
	if stored.Materialized {
		s.totalMaterialized++
	}

	return nil
}

// AppendFromSuggestion creates an EdgeMeta from inference suggestion data.
// Convenience method for inference engine integration.
func (s *EdgeMetaStore) AppendFromSuggestion(
	ctx context.Context,
	src, dst, label string,
	score float64,
	signalType, method, reason, sessionID, origin string,
	materialized bool,
) error {
	meta := EdgeMeta{
		Src:          src,
		Dst:          dst,
		Label:        label,
		Score:        score,
		SignalType:   signalType,
		Method:       method,
		Reason:       reason,
		SessionID:    sessionID,
		Origin:       origin,
		Materialized: materialized,
		Timestamp:    time.Now(),
	}
	return s.Append(ctx, meta)
}

// copyEdgeMeta returns the store's own copy of a record. Value fields are
// copied by value and Metadata is detached through the package copy owner, so
// neither the caller's input nor a delivered record can change retained
// evidence. A nil Metadata stays nil.
func copyEdgeMeta(meta *EdgeMeta) *EdgeMeta {
	return detachEdgeMeta(snapshotEdgeMeta(meta))
}

// snapshotEdgeMeta copies a record's value fields and keeps its Metadata map by
// reference. Retained metadata never changes after the write copy, so a
// snapshot taken under the store lock can be completed after the lock is
// released, where the caller-copy protocol runs.
func snapshotEdgeMeta(meta *EdgeMeta) *EdgeMeta {
	if meta == nil {
		return nil
	}
	snapshot := *meta
	return &snapshot
}

// snapshotEdgeMetas snapshots retained records; the caller holds the store lock.
func snapshotEdgeMetas(metas []*EdgeMeta) []*EdgeMeta {
	snapshots := make([]*EdgeMeta, len(metas))
	for i, meta := range metas {
		snapshots[i] = snapshotEdgeMeta(meta)
	}
	return snapshots
}

// detachEdgeMetas completes snapshots with their own metadata copies. It runs
// the caller-copy protocol, so no store lock may be held.
func detachEdgeMetas(snapshots []*EdgeMeta) []*EdgeMeta {
	for _, snapshot := range snapshots {
		detachEdgeMeta(snapshot)
	}
	return snapshots
}

// detachEdgeMeta replaces a copy's Metadata with the store's own detached map.
func detachEdgeMeta(meta *EdgeMeta) *EdgeMeta {
	if meta == nil {
		return nil
	}
	meta.Metadata = copyCallerMetadata(meta.Metadata)
	return meta
}

// GetHistory returns all evidence for an edge.
func (s *EdgeMetaStore) GetHistory(ctx context.Context, src, dst, label string) ([]*EdgeMeta, error) {
	snapshots, exists := s.snapshotHistory(EdgeMetaKey{Src: src, Dst: dst, Label: label}.String())
	if !exists {
		return []*EdgeMeta{}, nil
	}

	// Return detached copies to prevent mutation of retained evidence.
	return detachEdgeMetas(snapshots), nil
}

// snapshotHistory copies the value fields of one edge's retained records under
// the read lock. The snapshots still reference retained metadata, which
// detachEdgeMetas completes after the lock is released.
func (s *EdgeMetaStore) snapshotHistory(keyStr string) ([]*EdgeMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	history, exists := s.records[keyStr]
	if !exists {
		return nil, false
	}
	return snapshotEdgeMetas(history), true
}

// GetLatest returns the most recent provenance record for an edge.
func (s *EdgeMetaStore) GetLatest(ctx context.Context, src, dst, label string) (*EdgeMeta, error) {
	key := EdgeMetaKey{Src: src, Dst: dst, Label: label}.String()
	s.mu.RLock()
	history := s.records[key]
	var latest *EdgeMeta
	if len(history) > 0 {
		latest = snapshotEdgeMeta(history[len(history)-1])
	}
	s.mu.RUnlock()

	return detachEdgeMeta(latest), nil
}

// filterMetas scans allMetas newest-first under the read lock, snapshotting the
// value fields of every record matching keep until limit records are collected
// (limit <= 0 means unlimited). The snapshots still reference retained metadata,
// which the calling getter detaches after the lock is released.
func (s *EdgeMetaStore) filterMetas(limit int, keep func(*EdgeMeta) bool) []*EdgeMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, 0)
	for i := len(s.allMetas) - 1; i >= 0 && (limit <= 0 || len(result) < limit); i-- {
		meta := s.allMetas[i]
		if keep(meta) {
			result = append(result, snapshotEdgeMeta(meta))
		}
	}
	return result
}

// GetBySignalType filters by how edges were created.
func (s *EdgeMetaStore) GetBySignalType(ctx context.Context, signalType string, limit int) ([]*EdgeMeta, error) {
	return detachEdgeMetas(s.filterMetas(limit, func(meta *EdgeMeta) bool { return meta.SignalType == signalType })), nil
}

// GetMaterialized returns all materialized edges.
func (s *EdgeMetaStore) GetMaterialized(ctx context.Context, limit int) ([]*EdgeMeta, error) {
	return detachEdgeMetas(s.filterMetas(limit, func(meta *EdgeMeta) bool { return meta.Materialized })), nil
}

// GetByTimeRange returns provenance records within a time range.
func (s *EdgeMetaStore) GetByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]*EdgeMeta, error) {
	return detachEdgeMetas(s.filterMetas(limit, func(meta *EdgeMeta) bool {
		return (meta.Timestamp.After(start) || meta.Timestamp.Equal(start)) &&
			(meta.Timestamp.Before(end) || meta.Timestamp.Equal(end))
	})), nil
}

// GetByOrigin returns provenance records by origin (agent/commit ID).
func (s *EdgeMetaStore) GetByOrigin(ctx context.Context, origin string, limit int) ([]*EdgeMeta, error) {
	return detachEdgeMetas(s.filterMetas(limit, func(meta *EdgeMeta) bool { return meta.Origin == origin })), nil
}

// GetBySession returns provenance records by session.
func (s *EdgeMetaStore) GetBySession(ctx context.Context, sessionID string, limit int) ([]*EdgeMeta, error) {
	return detachEdgeMetas(s.filterMetas(limit, func(meta *EdgeMeta) bool { return meta.SessionID == sessionID })), nil
}

// HasHistory returns true if any provenance records exist for this edge.
func (s *EdgeMetaStore) HasHistory(src, dst, label string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := EdgeMetaKey{Src: src, Dst: dst, Label: label}
	history, exists := s.records[key.String()]
	return exists && len(history) > 0
}

// CountHistory returns the number of provenance records for an edge.
func (s *EdgeMetaStore) CountHistory(src, dst, label string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := EdgeMetaKey{Src: src, Dst: dst, Label: label}
	history, exists := s.records[key.String()]
	if !exists {
		return 0
	}
	return len(history)
}

// MarkMaterialized marks an edge as materialized (actually created).
// Appends a new record with Materialized=true.
func (s *EdgeMetaStore) MarkMaterialized(ctx context.Context, src, dst, label, origin string) error {
	meta := EdgeMeta{
		Src:          src,
		Dst:          dst,
		Label:        label,
		Materialized: true,
		Origin:       origin,
		SignalType:   "materialization",
		Reason:       "Edge materialized",
		Timestamp:    time.Now(),
	}
	return s.Append(ctx, meta)
}

// Stats returns current provenance store statistics.
func (s *EdgeMetaStore) Stats() EdgeMetaStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := EdgeMetaStats{
		TotalRecords:      s.totalAppended,
		TotalMaterialized: s.totalMaterialized,
		UniqueEdges:       int64(len(s.records)),
		BySignalType:      make(map[string]int64),
	}

	for _, meta := range s.allMetas {
		stats.BySignalType[meta.SignalType]++
	}

	return stats
}

// Size returns the total number of provenance records.
func (s *EdgeMetaStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.allMetas)
}

// UniqueEdgeCount returns the number of unique edges with provenance.
func (s *EdgeMetaStore) UniqueEdgeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}

// Clear removes all provenance records.
// Use with caution - audit trail will be lost.
func (s *EdgeMetaStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = make(map[string][]*EdgeMeta)
	s.allMetas = make([]*EdgeMeta, 0)
	s.totalAppended = 0
	s.totalMaterialized = 0
}

// Cleanup removes records older than maxAge.
// Returns the number of records removed.
func (s *EdgeMetaStore) Cleanup(maxAge time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	// Filter allMetas
	newAllMetas := make([]*EdgeMeta, 0, len(s.allMetas))
	for _, meta := range s.allMetas {
		if meta.Timestamp.After(cutoff) {
			newAllMetas = append(newAllMetas, meta)
		} else {
			removed++
		}
	}
	s.allMetas = newAllMetas

	// Rebuild records map
	newRecords := make(map[string][]*EdgeMeta)
	for _, meta := range s.allMetas {
		key := EdgeMetaKey{Src: meta.Src, Dst: meta.Dst, Label: meta.Label}
		newRecords[key.String()] = append(newRecords[key.String()], meta)
	}
	s.records = newRecords

	return removed
}

// Export returns all provenance records for backup/export.
func (s *EdgeMetaStore) Export() []*EdgeMeta {
	s.mu.RLock()
	snapshots := snapshotEdgeMetas(s.allMetas)
	s.mu.RUnlock()

	return detachEdgeMetas(snapshots)
}

// Import loads provenance records from backup/import.
// Existing records are NOT cleared - use Clear() first if needed.
func (s *EdgeMetaStore) Import(records []*EdgeMeta) int {
	// Detach every imported record before the store lock (see Append).
	stored := make([]*EdgeMeta, len(records))
	for i, meta := range records {
		stored[i] = copyEdgeMeta(meta)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	imported := 0
	for _, meta := range stored {
		if meta == nil {
			continue
		}

		key := EdgeMetaKey{Src: meta.Src, Dst: meta.Dst, Label: meta.Label}
		keyStr := key.String()

		s.records[keyStr] = append(s.records[keyStr], meta)
		s.allMetas = append(s.allMetas, meta)
		s.totalAppended++
		if meta.Materialized {
			s.totalMaterialized++
		}
		imported++
	}
	return imported
}

// EdgeMetaStoreOption configures an EdgeMetaStore.
type EdgeMetaStoreOption func(*EdgeMetaStore)

// NewEdgeMetaStoreWithOptions creates a store with functional options.
func NewEdgeMetaStoreWithOptions(opts ...EdgeMetaStoreOption) *EdgeMetaStore {
	s := NewEdgeMetaStore()
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Global singleton for convenience
var globalEdgeMetaStore *EdgeMetaStore
var globalEdgeMetaOnce sync.Once

// GlobalEdgeMetaStore returns the global edge meta store singleton.
func GlobalEdgeMetaStore() *EdgeMetaStore {
	globalEdgeMetaOnce.Do(func() {
		globalEdgeMetaStore = NewEdgeMetaStore()
	})
	return globalEdgeMetaStore
}

// ResetGlobalEdgeMetaStore resets the global edge meta store.
// Primarily for testing.
func ResetGlobalEdgeMetaStore() {
	globalEdgeMetaOnce = sync.Once{}
	globalEdgeMetaStore = nil
}
