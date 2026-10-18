// Package temporal - Relationship evolution tracking for dynamic graphs.
//
// RelationshipEvolution tracks how edge weights change over time using
// KalmanVelocity filters. This enables:
//   - Detecting strengthening relationships (increasing co-access)
//   - Detecting weakening relationships (decreasing relevance)
//   - Predicting future relationship strength
//   - Identifying emerging connections
//
// Use cases:
//   - Recommend strengthening edges for pre-fetching
//   - Prune weakening edges to save memory
//   - Detect new relationship patterns automatically
//   - Power dynamic graph visualizations
//
// Example usage:
//
//	re := temporal.NewRelationshipEvolution(temporal.DefaultRelationshipConfig())
//
//	// Record co-access (updates edge weight)
//	re.RecordCoAccess("node-1", "node-2", 1.0)
//
//	// Get relationship trend
//	trend := re.GetTrend("node-1", "node-2")
//	fmt.Printf("Relationship is %s (velocity: %.3f)\n", trend.Direction, trend.Velocity)
//
//	// Predict future strength
//	future := re.PredictStrength("node-1", "node-2", 10)
//	fmt.Printf("In 10 steps, strength will be: %.3f\n", future)
//
// # ELI12 (Explain Like I'm 12)
//
// Think about your friendships. Some get STRONGER over time, some fade:
//
//	👫 Best friend in 1st grade → Moved away → Don't talk anymore (WEAKENING)
//	👫 New kid at school → Hang out more → Now best friends! (STRENGTHENING)
//	👫 Neighbor → See them same amount → Just neighbors (STABLE)
//
// RelationshipEvolution tracks how "connected" two things are, and whether
// that connection is growing or shrinking.
//
// In a database, "relationships" are like friendships between data:
//
//	📚 "JavaScript" ←→ "React" : Strong connection (always accessed together)
//	📚 "JavaScript" ←→ "Python": Weak connection (rarely together)
//
// The Kalman filter tracks the TREND:
//
//	Week 1: JS+React accessed together 10 times
//	Week 2: JS+React accessed together 15 times
//	Week 3: JS+React accessed together 20 times
//	→ Velocity is POSITIVE! Relationship is STRENGTHENING! 📈
//
//	Week 1: JS+Python accessed together 5 times
//	Week 2: JS+Python accessed together 3 times
//	Week 3: JS+Python accessed together 1 time
//	→ Velocity is NEGATIVE! Relationship is WEAKENING! 📉
//
// Why this matters:
//
//	✅ STRENGTHENING relationships: Pre-fetch! If you access JS, also load React
//	❌ WEAKENING relationships: Maybe delete the connection, save memory
//	🌱 EMERGING relationships: "Ooh, this is new and growing fast - watch it!"
//
// The Kalman filter makes this smooth. One weird week doesn't change everything.
// It looks for REAL TRENDS, not random noise.
package temporal

import (
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/filter"
)

// RelationshipTrend represents the evolution trend of a relationship.
type RelationshipTrend struct {
	// Direction: "strengthening", "weakening", "stable"
	Direction string

	// Velocity: rate of change (positive = strengthening)
	Velocity float64

	// CurrentStrength: current filtered weight
	CurrentStrength float64

	// PredictedStrength: predicted weight in 5 steps
	PredictedStrength float64

	// Confidence: confidence in the trend (0-1)
	Confidence float64

	// ObservationCount: number of weight updates
	ObservationCount int

	// LastUpdate: when the relationship was last updated
	LastUpdate time.Time
}

// RelationshipConfig holds configuration for relationship evolution tracking.
type RelationshipConfig struct {
	// FilterConfig for the underlying Kalman velocity filter
	FilterConfig filter.VelocityConfig

	// MaxTrackedRelationships - maximum relationships to track (LRU eviction)
	MaxTrackedRelationships int

	// StrengthenThreshold - velocity above which is "strengthening"
	StrengthenThreshold float64

	// WeakenThreshold - velocity below which is "weakening"
	WeakenThreshold float64

	// MinObservationsForTrend - minimum observations before reporting trend
	MinObservationsForTrend int

	// DecayIdleRelationships - decay weight of idle relationships
	DecayIdleRelationships bool

	// IdleDecayRate - how much to decay per hour of inactivity
	IdleDecayRate float64
}

// DefaultRelationshipConfig returns sensible defaults.
func DefaultRelationshipConfig() RelationshipConfig {
	return RelationshipConfig{
		FilterConfig: filter.VelocityConfig{
			ProcessNoisePos:    0.01,
			ProcessNoiseVel:    0.001,
			MeasurementNoise:   0.1,
			InitialPosVariance: 1.0,
			InitialVelVariance: 0.1,
			Dt:                 1.0,
		},
		MaxTrackedRelationships: 100000,
		StrengthenThreshold:     0.01,
		WeakenThreshold:         -0.01,
		MinObservationsForTrend: 3,
		DecayIdleRelationships:  true,
		IdleDecayRate:           0.01, // 1% per hour
	}
}

// RelationshipEvolution tracks edge weight changes over time.
type RelationshipEvolution struct {
	mu     sync.RWMutex
	config RelationshipConfig

	// Edge trackers (edgeKey -> tracker)
	edges map[string]*edgeTracker

	// LRU ordering
	accessOrder []string

	// Statistics
	totalUpdates int64
	startTime    time.Time
}

// edgeTracker tracks a single relationship.
type edgeTracker struct {
	sourceID string
	targetID string

	// Kalman filter for weight tracking
	weightFilter *filter.KalmanVelocity

	// Statistics
	observations int
	firstUpdate  time.Time
	lastUpdate   time.Time

	// Cached values
	lastWeight   float64
	lastVelocity float64
}

// NewRelationshipEvolution creates a new relationship evolution tracker.
func NewRelationshipEvolution(cfg RelationshipConfig) *RelationshipEvolution {
	return &RelationshipEvolution{
		config:      cfg,
		edges:       make(map[string]*edgeTracker),
		accessOrder: make([]string, 0, cfg.MaxTrackedRelationships),
		startTime:   time.Now(),
	}
}

// edgeKey generates a consistent key for an edge.
func edgeKey(sourceID, targetID string) string {
	// Ensure consistent ordering for undirected relationships
	if sourceID > targetID {
		sourceID, targetID = targetID, sourceID
	}
	return fmt.Sprintf("%s->%s", sourceID, targetID)
}

// RecordCoAccess records a co-access event between two nodes.
// weight should be 1.0 for simple co-access, or can be weighted.
func (re *RelationshipEvolution) RecordCoAccess(sourceID, targetID string, weight float64) {
	re.RecordCoAccessAt(sourceID, targetID, weight, time.Now())
}

// RecordCoAccessAt records co-access at a specific time.
func (re *RelationshipEvolution) RecordCoAccessAt(sourceID, targetID string, weight float64, timestamp time.Time) {
	re.mu.Lock()
	defer re.mu.Unlock()

	key := edgeKey(sourceID, targetID)
	re.totalUpdates++

	tracker, exists := re.edges[key]
	if !exists {
		tracker = &edgeTracker{
			sourceID:     sourceID,
			targetID:     targetID,
			weightFilter: filter.NewKalmanVelocity(re.config.FilterConfig),
			firstUpdate:  timestamp,
		}
		re.edges[key] = tracker

		// Check for eviction
		if len(re.edges) > re.config.MaxTrackedRelationships {
			re.evictOldest()
		}
	}

	// Update the filter with the new weight observation
	filtered := tracker.weightFilter.Process(weight)
	tracker.lastWeight = filtered
	tracker.lastVelocity = tracker.weightFilter.Velocity()
	tracker.lastUpdate = timestamp
	tracker.observations++

	// Update LRU
	re.updateLRU(key)
}

// UpdateWeight updates the weight of an existing relationship.
// Use this for explicit weight updates (not just co-access).
func (re *RelationshipEvolution) UpdateWeight(sourceID, targetID string, newWeight float64) {
	re.RecordCoAccess(sourceID, targetID, newWeight)
}

// GetTrend returns the evolution trend for a relationship.
func (re *RelationshipEvolution) GetTrend(sourceID, targetID string) *RelationshipTrend {
	re.mu.RLock()
	defer re.mu.RUnlock()

	key := edgeKey(sourceID, targetID)
	tracker, exists := re.edges[key]
	if !exists {
		return nil
	}

	return re.calculateTrend(tracker)
}

// calculateTrend computes the trend from a tracker.
func (re *RelationshipEvolution) calculateTrend(tracker *edgeTracker) *RelationshipTrend {
	velocity := tracker.lastVelocity
	var direction string

	if tracker.observations < re.config.MinObservationsForTrend {
		direction = "unknown"
	} else if velocity > re.config.StrengthenThreshold {
		direction = "strengthening"
	} else if velocity < re.config.WeakenThreshold {
		direction = "weakening"
	} else {
		direction = "stable"
	}

	// Calculate confidence based on observations
	confidence := float64(tracker.observations) / float64(tracker.observations+10)

	// Predict future strength
	predicted := tracker.weightFilter.Predict(5)

	return &RelationshipTrend{
		Direction:         direction,
		Velocity:          velocity,
		CurrentStrength:   tracker.lastWeight,
		PredictedStrength: predicted,
		Confidence:        confidence,
		ObservationCount:  tracker.observations,
		LastUpdate:        tracker.lastUpdate,
	}
}

// PredictStrength predicts the relationship strength n steps ahead.
func (re *RelationshipEvolution) PredictStrength(sourceID, targetID string, steps int) float64 {
	re.mu.RLock()
	defer re.mu.RUnlock()

	key := edgeKey(sourceID, targetID)
	tracker, exists := re.edges[key]
	if !exists {
		return 0
	}

	return tracker.weightFilter.Predict(steps)
}

// GetStrengtheningRelationships returns relationships that are getting stronger.
func (re *RelationshipEvolution) GetStrengtheningRelationships(limit int) []RelationshipTrend {
	re.mu.RLock()
	defer re.mu.RUnlock()

	var results []RelationshipTrend
	for _, tracker := range re.edges {
		if tracker.observations >= re.config.MinObservationsForTrend {
			if tracker.lastVelocity > re.config.StrengthenThreshold {
				trend := re.calculateTrend(tracker)
				results = append(results, *trend)
			}
		}
	}

	// Sort by velocity (descending)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Velocity > results[i].Velocity {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// GetWeakeningRelationships returns relationships that are getting weaker.
func (re *RelationshipEvolution) GetWeakeningRelationships(limit int) []RelationshipTrend {
	re.mu.RLock()
	defer re.mu.RUnlock()

	var results []RelationshipTrend
	for _, tracker := range re.edges {
		if tracker.observations >= re.config.MinObservationsForTrend {
			if tracker.lastVelocity < re.config.WeakenThreshold {
				trend := re.calculateTrend(tracker)
				results = append(results, *trend)
			}
		}
	}

	// Sort by velocity (ascending - most negative first)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Velocity < results[i].Velocity {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// GetEmergingRelationships returns new relationships with positive velocity.
func (re *RelationshipEvolution) GetEmergingRelationships(limit int) []RelationshipTrend {
	re.mu.RLock()
	defer re.mu.RUnlock()

	minAge := 10 // Minimum observations to consider "emerging"
	maxAge := 50 // Maximum observations to still be "emerging"

	var results []RelationshipTrend
	for _, tracker := range re.edges {
		if tracker.observations >= minAge && tracker.observations <= maxAge {
			if tracker.lastVelocity > 0 {
				trend := re.calculateTrend(tracker)
				results = append(results, *trend)
			}
		}
	}

	// Sort by velocity (descending)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Velocity > results[i].Velocity {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	if len(results) > limit {
		results = results[:limit]
	}

	return results
}

// ShouldPrune checks if a relationship should be pruned (very weak and weakening).
func (re *RelationshipEvolution) ShouldPrune(sourceID, targetID string, threshold float64) bool {
	trend := re.GetTrend(sourceID, targetID)
	if trend == nil {
		return false
	}

	// Prune if weak AND getting weaker
	return trend.CurrentStrength < threshold && trend.Direction == "weakening"
}

// DecayIdleRelationships applies decay to relationships not updated recently.
func (re *RelationshipEvolution) DecayIdleRelationships(maxIdleHours float64) int {
	if !re.config.DecayIdleRelationships {
		return 0
	}

	re.mu.Lock()
	defer re.mu.Unlock()

	now := time.Now()
	decayed := 0

	for _, tracker := range re.edges {
		idleHours := now.Sub(tracker.lastUpdate).Hours()
		if idleHours > maxIdleHours {
			// Apply decay
			decayFactor := 1.0 - (re.config.IdleDecayRate * idleHours)
			if decayFactor < 0.1 {
				decayFactor = 0.1 // Minimum decay
			}

			newWeight := tracker.lastWeight * decayFactor
			tracker.weightFilter.Process(newWeight)
			tracker.lastWeight = newWeight
			decayed++
		}
	}

	return decayed
}

// updateLRU updates LRU order for an edge.
func (re *RelationshipEvolution) updateLRU(key string) {
	// Remove from current position
	for i, k := range re.accessOrder {
		if k == key {
			re.accessOrder = append(re.accessOrder[:i], re.accessOrder[i+1:]...)
			break
		}
	}
	// Add to end
	re.accessOrder = append(re.accessOrder, key)
}

// evictOldest removes the least recently used edge.
func (re *RelationshipEvolution) evictOldest() {
	if len(re.accessOrder) == 0 {
		return
	}
	oldest := re.accessOrder[0]
	re.accessOrder[0] = ""
	re.accessOrder = re.accessOrder[1:]
	delete(re.edges, oldest)
}

// RelationshipStats holds statistics about relationship tracking.
type RelationshipStats struct {
	TrackedRelationships int
	TotalUpdates         int64
	Strengthening        int
	Weakening            int
	Stable               int
	UptimeSeconds        float64
}

// GetStats returns statistics about relationship tracking.
func (re *RelationshipEvolution) GetStats() RelationshipStats {
	re.mu.RLock()
	defer re.mu.RUnlock()

	stats := RelationshipStats{
		TrackedRelationships: len(re.edges),
		TotalUpdates:         re.totalUpdates,
		UptimeSeconds:        time.Since(re.startTime).Seconds(),
	}

	for _, tracker := range re.edges {
		if tracker.observations >= re.config.MinObservationsForTrend {
			if tracker.lastVelocity > re.config.StrengthenThreshold {
				stats.Strengthening++
			} else if tracker.lastVelocity < re.config.WeakenThreshold {
				stats.Weakening++
			} else {
				stats.Stable++
			}
		}
	}

	return stats
}

// Reset clears all relationship data.
func (re *RelationshipEvolution) Reset() {
	re.mu.Lock()
	defer re.mu.Unlock()

	re.edges = make(map[string]*edgeTracker)
	re.accessOrder = make([]string, 0, re.config.MaxTrackedRelationships)
	re.totalUpdates = 0
	re.startTime = time.Now()
}
