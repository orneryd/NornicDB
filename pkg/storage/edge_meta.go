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

	s.mu.Lock()
	defer s.mu.Unlock()

	// Set timestamp if not provided
	if meta.Timestamp.IsZero() {
		meta.Timestamp = time.Now()
	}

	// Generate edge ID if not provided
	if meta.EdgeID == "" {
		meta.EdgeID = fmt.Sprintf("edge-%d", time.Now().UnixNano())
	}

	// Create key
	key := EdgeMetaKey{Src: meta.Src, Dst: meta.Dst, Label: meta.Label}
	keyStr := key.String()

	// Append to history
	metaCopy := meta // Create copy to prevent mutation
	s.records[keyStr] = append(s.records[keyStr], &metaCopy)
	s.allMetas = append(s.allMetas, &metaCopy)

	s.totalAppended++
	if meta.Materialized {
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

// GetHistory returns all evidence for an edge.
func (s *EdgeMetaStore) GetHistory(ctx context.Context, src, dst, label string) ([]*EdgeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := EdgeMetaKey{Src: src, Dst: dst, Label: label}
	history, exists := s.records[key.String()]
	if !exists {
		return []*EdgeMeta{}, nil
	}

	// Return copies to prevent mutation
	result := make([]*EdgeMeta, len(history))
	for i, meta := range history {
		metaCopy := *meta
		result[i] = &metaCopy
	}
	return result, nil
}

// GetLatest returns the most recent provenance record for an edge.
func (s *EdgeMetaStore) GetLatest(ctx context.Context, src, dst, label string) (*EdgeMeta, error) {
	history, err := s.GetHistory(ctx, src, dst, label)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 {
		return nil, nil
	}
	return history[len(history)-1], nil
}

// GetBySignalType filters by how edges were created.
func (s *EdgeMetaStore) GetBySignalType(ctx context.Context, signalType string, limit int) ([]*EdgeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, 0)
	for i := len(s.allMetas) - 1; i >= 0 && (limit <= 0 || len(result) < limit); i-- {
		meta := s.allMetas[i]
		if meta.SignalType == signalType {
			metaCopy := *meta
			result = append(result, &metaCopy)
		}
	}
	return result, nil
}

// GetMaterialized returns all materialized edges.
func (s *EdgeMetaStore) GetMaterialized(ctx context.Context, limit int) ([]*EdgeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, 0)
	for i := len(s.allMetas) - 1; i >= 0 && (limit <= 0 || len(result) < limit); i-- {
		meta := s.allMetas[i]
		if meta.Materialized {
			metaCopy := *meta
			result = append(result, &metaCopy)
		}
	}
	return result, nil
}

// GetByTimeRange returns provenance records within a time range.
func (s *EdgeMetaStore) GetByTimeRange(ctx context.Context, start, end time.Time, limit int) ([]*EdgeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, 0)
	for i := len(s.allMetas) - 1; i >= 0 && (limit <= 0 || len(result) < limit); i-- {
		meta := s.allMetas[i]
		if (meta.Timestamp.After(start) || meta.Timestamp.Equal(start)) &&
			(meta.Timestamp.Before(end) || meta.Timestamp.Equal(end)) {
			metaCopy := *meta
			result = append(result, &metaCopy)
		}
	}
	return result, nil
}

// GetByOrigin returns provenance records by origin (agent/commit ID).
func (s *EdgeMetaStore) GetByOrigin(ctx context.Context, origin string, limit int) ([]*EdgeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, 0)
	for i := len(s.allMetas) - 1; i >= 0 && (limit <= 0 || len(result) < limit); i-- {
		meta := s.allMetas[i]
		if meta.Origin == origin {
			metaCopy := *meta
			result = append(result, &metaCopy)
		}
	}
	return result, nil
}

// GetBySession returns provenance records by session.
func (s *EdgeMetaStore) GetBySession(ctx context.Context, sessionID string, limit int) ([]*EdgeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, 0)
	for i := len(s.allMetas) - 1; i >= 0 && (limit <= 0 || len(result) < limit); i-- {
		meta := s.allMetas[i]
		if meta.SessionID == sessionID {
			metaCopy := *meta
			result = append(result, &metaCopy)
		}
	}
	return result, nil
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
	defer s.mu.RUnlock()

	result := make([]*EdgeMeta, len(s.allMetas))
	for i, meta := range s.allMetas {
		metaCopy := *meta
		result[i] = &metaCopy
	}
	return result
}

// Import loads provenance records from backup/import.
// Existing records are NOT cleared - use Clear() first if needed.
func (s *EdgeMetaStore) Import(records []*EdgeMeta) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	imported := 0
	for _, meta := range records {
		if meta == nil {
			continue
		}

		metaCopy := *meta
		key := EdgeMetaKey{Src: metaCopy.Src, Dst: metaCopy.Dst, Label: metaCopy.Label}
		keyStr := key.String()

		s.records[keyStr] = append(s.records[keyStr], &metaCopy)
		s.allMetas = append(s.allMetas, &metaCopy)
		s.totalAppended++
		if metaCopy.Materialized {
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
