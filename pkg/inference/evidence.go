// Package inference provides edge materialization with evidence buffering.
//
// Evidence buffering accumulates signals before materializing edges, reducing
// false positives by requiring multiple corroborating evidence points.
//
// Feature flag: NORNICDB_EVIDENCE_BUFFERING_ENABLED (enabled by default)
//
// Usage Example 1: Basic evidence accumulation
//
//	buffer := NewEvidenceBuffer()
//	
//	// First co-access (not enough evidence yet)
//	shouldMat := buffer.AddEvidence("nodeA", "nodeB", "relates_to", 0.8, "coaccess", "session-1")
//	// → false (need more evidence)
//	
//	// Second co-access (still accumulating)
//	shouldMat = buffer.AddEvidence("nodeA", "nodeB", "relates_to", 0.7, "coaccess", "session-2")
//	// → false (need one more signal)
//	
//	// Third co-access (threshold met!)
//	shouldMat = buffer.AddEvidence("nodeA", "nodeB", "relates_to", 0.9, "similarity", "session-3")
//	// → true (3 signals from 3 sessions, avg score 0.8)
//	
//	if shouldMat {
//	    db.CreateEdge("nodeA", "nodeB", "relates_to")
//	}
//
// Usage Example 2: Custom thresholds per edge type
//
//	customThresholds := map[string]EvidenceThreshold{
//	    "high_confidence_link": {MinCount: 5, MinScore: 0.9, MinSessions: 3},
//	    "low_confidence_link":  {MinCount: 2, MinScore: 0.5, MinSessions: 1},
//	}
//	buffer := NewEvidenceBufferWithConfig(customThresholds)
//
// Usage Example 3: Checking current evidence state
//
//	canMat, reason := buffer.CheckThreshold("nodeA", "nodeB", "relates_to")
//	if !canMat {
//	    log.Printf("Not ready: %s", reason)  // "need 1 more signal (2/3)"
//	}
//
// ELI12 (Explain Like I'm 12):
//
// Imagine you're deciding if two kids should be study partners:
//   - They sit together in class (1 signal)
//   - You notice them talking about homework (2 signals)
//   - They're both in the science club (3 signals)
//   - NOW you're confident they'd be good partners!
//
// Evidence buffering prevents jumping to conclusions based on one coincidence:
//   - Single co-access? Could be random
//   - Two co-accesses in same session? Could be a fluke
//   - Three co-accesses across different sessions? Strong pattern!
//
// Like a detective collecting clues:
//   - 1 fingerprint = suspicious
//   - 2 fingerprints + 1 witness = very suspicious  
//   - 3 fingerprints + 2 witnesses + video = confident!
//
// Without evidence buffering, you'd create edges from random noise.
// With it, you only create edges when you have solid proof of a relationship.
package inference

import (
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// DefaultThresholds defines standard evidence thresholds per edge label.
var DefaultThresholds = map[string]EvidenceThreshold{
	"relates_to": {
		MinCount:    3,
		MinScore:    0.5,
		MinSessions: 2,
		MaxAge:      24 * time.Hour,
	},
	"similar_to": {
		MinCount:    2,
		MinScore:    0.7,
		MinSessions: 1,
		MaxAge:      48 * time.Hour,
	},
	"coaccess": {
		MinCount:    5,
		MinScore:    0.3,
		MinSessions: 3,
		MaxAge:      12 * time.Hour,
	},
	"topology": {
		MinCount:    2,
		MinScore:    0.6,
		MinSessions: 1,
		MaxAge:      72 * time.Hour,
	},
	"depends_on": {
		MinCount:    3,
		MinScore:    0.6,
		MinSessions: 2,
		MaxAge:      168 * time.Hour, // 1 week
	},
}

// DefaultEvidenceThreshold is used when no label-specific threshold is configured.
var DefaultEvidenceThreshold = EvidenceThreshold{
	MinCount:    3,
	MinScore:    0.5,
	MinSessions: 2,
	MaxAge:      24 * time.Hour,
}

// EvidenceKey uniquely identifies an edge pair.
type EvidenceKey struct {
	Src   string
	Dst   string
	Label string
}

// String returns a string representation of the key.
func (k EvidenceKey) String() string {
	return fmt.Sprintf("%s:%s:%s", k.Src, k.Dst, k.Label)
}

// Evidence accumulates signals for a potential edge.
type Evidence struct {
	Key      EvidenceKey
	Count    int                    // Total evidence count
	ScoreSum float64                // Cumulative score
	ScoreAvg float64                // Average score (updated on add)
	FirstTs  time.Time              // When first evidence was added
	LastTs   time.Time              // When last evidence was added
	Sessions map[string]bool        // Unique session IDs
	Signals  []string               // Signal types seen (coaccess, similarity, etc.)
	Metadata map[string]interface{} // Additional context
}

// EvidenceThreshold defines when evidence is sufficient for materialization.
type EvidenceThreshold struct {
	MinCount    int           // Minimum evidence count required
	MinScore    float64       // Minimum cumulative score required
	MinSessions int           // Minimum unique sessions required
	MaxAge      time.Duration // Evidence expires after this duration
}

// EvidenceBuffer accumulates signals before materialization.
// Thread-safe for concurrent access.
type EvidenceBuffer struct {
	mu         sync.RWMutex
	entries    map[string]*Evidence         // key: "src:dst:label"
	thresholds map[string]EvidenceThreshold // per-label thresholds

	// Stats
	totalAdded        int64
	totalMaterialized int64
	totalExpired      int64
}

// EvidenceStats provides observability into evidence buffer behavior.
type EvidenceStats struct {
	TotalEntries      int64
	TotalAdded        int64
	TotalMaterialized int64
	TotalExpired      int64
	MaterializeRate   float64 // Materialized / Added (0.0 - 1.0)
}

// NewEvidenceBuffer creates a new evidence buffer with default thresholds.
func NewEvidenceBuffer() *EvidenceBuffer {
	return &EvidenceBuffer{
		entries:    make(map[string]*Evidence),
		thresholds: copyThresholds(DefaultThresholds),
	}
}

// NewEvidenceBufferWithConfig creates an evidence buffer with custom thresholds.
func NewEvidenceBufferWithConfig(labelThresholds map[string]EvidenceThreshold) *EvidenceBuffer {
	eb := NewEvidenceBuffer()
	for label, threshold := range labelThresholds {
		eb.thresholds[label] = threshold
	}
	return eb
}

// AddEvidence adds a new evidence point for an edge pair.
// Returns true if the evidence threshold is now met (edge should be materialized).
// The signal is added regardless of feature flag; the flag only affects the return value.
func (eb *EvidenceBuffer) AddEvidence(src, dst, label string, score float64, signalType, sessionID string) bool {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.totalAdded++

	key := EvidenceKey{Src: src, Dst: dst, Label: label}
	keyStr := key.String()

	ev, exists := eb.entries[keyStr]
	if !exists {
		ev = &Evidence{
			Key:      key,
			FirstTs:  time.Now(),
			Sessions: make(map[string]bool),
			Metadata: make(map[string]interface{}),
		}
		eb.entries[keyStr] = ev
	}

	// Update evidence
	ev.Count++
	ev.ScoreSum += score
	ev.ScoreAvg = ev.ScoreSum / float64(ev.Count)
	ev.LastTs = time.Now()
	if sessionID != "" {
		ev.Sessions[sessionID] = true
	}
	ev.Signals = append(ev.Signals, signalType)

	// Check if threshold is met
	if eb.shouldMaterialize(key, ev) {
		eb.totalMaterialized++
		// Don't delete - keep for audit trail, cleanup will handle expiry
		return true
	}

	return false
}

// AddEvidenceWithMetadata adds evidence with additional metadata.
func (eb *EvidenceBuffer) AddEvidenceWithMetadata(src, dst, label string, score float64, signalType, sessionID string, metadata map[string]interface{}) bool {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.totalAdded++

	key := EvidenceKey{Src: src, Dst: dst, Label: label}
	keyStr := key.String()

	ev, exists := eb.entries[keyStr]
	if !exists {
		ev = &Evidence{
			Key:      key,
			FirstTs:  time.Now(),
			Sessions: make(map[string]bool),
			Metadata: make(map[string]interface{}),
		}
		eb.entries[keyStr] = ev
	}

	// Update evidence
	ev.Count++
	ev.ScoreSum += score
	ev.ScoreAvg = ev.ScoreSum / float64(ev.Count)
	ev.LastTs = time.Now()
	if sessionID != "" {
		ev.Sessions[sessionID] = true
	}
	ev.Signals = append(ev.Signals, signalType)

	// Merge metadata
	for k, v := range metadata {
		ev.Metadata[k] = v
	}

	if eb.shouldMaterialize(key, ev) {
		eb.totalMaterialized++
		return true
	}

	return false
}

// shouldMaterialize checks if evidence meets the threshold.
// Must be called with lock held.
func (eb *EvidenceBuffer) shouldMaterialize(key EvidenceKey, ev *Evidence) bool {
	// If feature is disabled, always return true (immediate materialization)
	if !config.IsEvidenceBufferingEnabled() {
		return true
	}

	threshold := eb.getThreshold(key.Label)

	// Check age - if evidence is too old, it doesn't count
	if time.Since(ev.FirstTs) > threshold.MaxAge {
		return false
	}

	// Check minimum count
	if ev.Count < threshold.MinCount {
		return false
	}

	// Check minimum score (cumulative or average depending on use case)
	if ev.ScoreAvg < threshold.MinScore {
		return false
	}

	// Check minimum unique sessions
	if len(ev.Sessions) < threshold.MinSessions {
		return false
	}

	return true
}

// getThreshold returns the threshold for a label.
// Must be called with lock held.
func (eb *EvidenceBuffer) getThreshold(label string) EvidenceThreshold {
	if threshold, ok := eb.thresholds[label]; ok {
		return threshold
	}
	return DefaultEvidenceThreshold
}

// SetThreshold sets the threshold for a specific label.
func (eb *EvidenceBuffer) SetThreshold(label string, threshold EvidenceThreshold) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.thresholds[label] = threshold
}

// GetThreshold returns the threshold for a label.
func (eb *EvidenceBuffer) GetThreshold(label string) EvidenceThreshold {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.getThreshold(label)
}

// GetEvidence returns the current evidence for an edge pair.
// Returns nil if no evidence exists.
func (eb *EvidenceBuffer) GetEvidence(src, dst, label string) *Evidence {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	key := EvidenceKey{Src: src, Dst: dst, Label: label}
	ev, exists := eb.entries[key.String()]
	if !exists {
		return nil
	}

	// Return a copy to prevent mutation
	return &Evidence{
		Key:      ev.Key,
		Count:    ev.Count,
		ScoreSum: ev.ScoreSum,
		ScoreAvg: ev.ScoreAvg,
		FirstTs:  ev.FirstTs,
		LastTs:   ev.LastTs,
		Sessions: copyStringBoolMap(ev.Sessions),
		Signals:  copyStringSlice(ev.Signals),
		Metadata: copyMetadata(ev.Metadata),
	}
}

// CheckThreshold checks if evidence meets threshold without adding new evidence.
func (eb *EvidenceBuffer) CheckThreshold(src, dst, label string) (bool, string) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if !config.IsEvidenceBufferingEnabled() {
		return true, "evidence buffering feature disabled"
	}

	key := EvidenceKey{Src: src, Dst: dst, Label: label}
	ev, exists := eb.entries[key.String()]
	if !exists {
		return false, "no evidence accumulated"
	}

	threshold := eb.getThreshold(label)

	// Check age
	if time.Since(ev.FirstTs) > threshold.MaxAge {
		return false, fmt.Sprintf("evidence expired (age: %s, max: %s)",
			time.Since(ev.FirstTs).Round(time.Hour), threshold.MaxAge)
	}

	// Check count
	if ev.Count < threshold.MinCount {
		return false, fmt.Sprintf("insufficient count (%d/%d)", ev.Count, threshold.MinCount)
	}

	// Check score
	if ev.ScoreAvg < threshold.MinScore {
		return false, fmt.Sprintf("insufficient score (%.2f/%.2f)", ev.ScoreAvg, threshold.MinScore)
	}

	// Check sessions
	if len(ev.Sessions) < threshold.MinSessions {
		return false, fmt.Sprintf("insufficient sessions (%d/%d)", len(ev.Sessions), threshold.MinSessions)
	}

	return true, "threshold met"
}

// Clear removes all evidence entries.
func (eb *EvidenceBuffer) Clear() {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.entries = make(map[string]*Evidence)
	eb.totalAdded = 0
	eb.totalMaterialized = 0
	eb.totalExpired = 0
}

// ClearEntry removes evidence for a specific edge pair.
func (eb *EvidenceBuffer) ClearEntry(src, dst, label string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	key := EvidenceKey{Src: src, Dst: dst, Label: label}
	delete(eb.entries, key.String())
}

// Cleanup removes expired evidence entries to prevent memory growth.
// Should be called periodically (e.g., every hour).
// Returns the number of entries removed.
func (eb *EvidenceBuffer) Cleanup() int {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	removed := 0
	now := time.Now()

	for keyStr, ev := range eb.entries {
		threshold := eb.getThreshold(ev.Key.Label)
		// Remove entries that have exceeded their max age
		if now.Sub(ev.FirstTs) > threshold.MaxAge {
			delete(eb.entries, keyStr)
			removed++
			eb.totalExpired++
		}
	}

	return removed
}

// Stats returns current evidence buffer statistics.
func (eb *EvidenceBuffer) Stats() EvidenceStats {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	stats := EvidenceStats{
		TotalEntries:      int64(len(eb.entries)),
		TotalAdded:        eb.totalAdded,
		TotalMaterialized: eb.totalMaterialized,
		TotalExpired:      eb.totalExpired,
	}

	if stats.TotalAdded > 0 {
		stats.MaterializeRate = float64(stats.TotalMaterialized) / float64(stats.TotalAdded)
	}

	return stats
}

// Size returns the number of tracked evidence entries.
func (eb *EvidenceBuffer) Size() int {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return len(eb.entries)
}

// GetPendingEdges returns evidence entries that are close to threshold.
// Useful for monitoring and proactive materialization.
func (eb *EvidenceBuffer) GetPendingEdges(minProgress float64) []Evidence {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	var pending []Evidence
	for _, ev := range eb.entries {
		threshold := eb.getThreshold(ev.Key.Label)
		progress := calculateProgress(ev, threshold)
		if progress >= minProgress && progress < 1.0 {
			pending = append(pending, *ev)
		}
	}
	return pending
}

// calculateProgress returns how close evidence is to threshold (0.0 to 1.0+)
func calculateProgress(ev *Evidence, threshold EvidenceThreshold) float64 {
	if threshold.MinCount == 0 {
		return 1.0
	}

	countProgress := float64(ev.Count) / float64(threshold.MinCount)
	scoreProgress := ev.ScoreAvg / threshold.MinScore
	sessionProgress := float64(len(ev.Sessions)) / float64(threshold.MinSessions)

	// Average of all progress metrics
	return (countProgress + scoreProgress + sessionProgress) / 3.0
}

// Helper functions

func copyThresholds(thresholds map[string]EvidenceThreshold) map[string]EvidenceThreshold {
	copy := make(map[string]EvidenceThreshold, len(thresholds))
	for k, v := range thresholds {
		copy[k] = v
	}
	return copy
}

func copyStringBoolMap(m map[string]bool) map[string]bool {
	copy := make(map[string]bool, len(m))
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

func copyStringSlice(s []string) []string {
	copy := make([]string, len(s))
	for i, v := range s {
		copy[i] = v
	}
	return copy
}

func copyMetadata(m map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{}, len(m))
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

// EvidenceBufferOption configures an EvidenceBuffer.
type EvidenceBufferOption func(*EvidenceBuffer)

// WithThreshold sets a specific label threshold during initialization.
func WithThreshold(label string, threshold EvidenceThreshold) EvidenceBufferOption {
	return func(eb *EvidenceBuffer) {
		eb.thresholds[label] = threshold
	}
}

// NewEvidenceBufferWithOptions creates a buffer with functional options.
func NewEvidenceBufferWithOptions(opts ...EvidenceBufferOption) *EvidenceBuffer {
	eb := NewEvidenceBuffer()
	for _, opt := range opts {
		opt(eb)
	}
	return eb
}

// Global singleton for convenience
var globalEvidenceBuffer *EvidenceBuffer
var globalEvidenceOnce sync.Once

// GlobalEvidenceBuffer returns the global evidence buffer singleton.
func GlobalEvidenceBuffer() *EvidenceBuffer {
	globalEvidenceOnce.Do(func() {
		globalEvidenceBuffer = NewEvidenceBuffer()
	})
	return globalEvidenceBuffer
}

// ResetGlobalEvidenceBuffer resets the global evidence buffer.
// Primarily for testing.
func ResetGlobalEvidenceBuffer() {
	globalEvidenceOnce = sync.Once{}
	globalEvidenceBuffer = nil
}
