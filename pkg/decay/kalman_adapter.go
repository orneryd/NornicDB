// Package decay - Kalman filter integration for adaptive decay.
//
// This file provides the KalmanAdapter that enhances the decay manager with:
//   - Temporal awareness: Frequently accessed nodes decay slower
//   - Kalman smoothing: Smooth out noisy decay score calculations
//   - Velocity tracking: Detect if memory is "heating up" or "cooling down"
//   - Prediction: Predict future decay scores for archival planning
//
// # Integration Architecture
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                    KalmanAdapter                            │
//	├─────────────────────────────────────────────────────────────┤
//	│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
//	│  │ Decay Manager   │───▶│ Kalman Filter (score smoothing)│ │
//	│  │ (raw scores)    │    └─────────────────────────────────┘ │
//	│  └────────┬────────┘                                        │
//	│           │                                                 │
//	│           ▼                                                 │
//	│  ┌─────────────────────────────────────────────────────────┐│
//	│  │     Temporal Integration (access patterns)              ││
//	│  │  • Access velocity → decay rate modifier               ││
//	│  │  • Session detection → burst protection                ││
//	│  │  • Pattern detection → routine preservation            ││
//	│  └─────────────────────────────────────────────────────────┘│
//	│           │                                                 │
//	│           ▼                                                 │
//	│  ┌─────────────────────────────────────────────────────────┐│
//	│  │     Final Score = raw × temporal_modifier × smoothing  ││
//	│  └─────────────────────────────────────────────────────────┘│
//	└─────────────────────────────────────────────────────────────┘
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine your memories are like phone battery percentages:
//
//	📱 Without Kalman: Battery shows 80%, 75%, 82%, 71%, 78% - jumpy!
//	📱 With Kalman:    Battery shows 80%, 79%, 78%, 77%, 76% - smooth!
//
// The Kalman filter smooths out the noise so you don't panic when
// a memory briefly dips low - it waits to see if it's a REAL trend.
//
// It also notices HOW you use your phone:
//
//	📈 Using it more lately? Charge slower (decay slower)!
//	📉 Ignoring it? Let battery drop faster (decay faster)!
//
// This makes the "forgetting" system smart - it doesn't forget things
// you're actively using, even if you used them slightly less today.
package decay

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/filter"
	"github.com/orneryd/nornicdb/pkg/temporal"
)

// KalmanAdapterConfig holds configuration for the Kalman-enhanced decay adapter.
type KalmanAdapterConfig struct {
	// EnableKalmanSmoothing enables Kalman filtering of raw decay scores
	EnableKalmanSmoothing bool

	// EnableTemporalModifier enables temporal-aware decay modification
	EnableTemporalModifier bool

	// SmoothingConfig for the score smoothing filter
	SmoothingConfig filter.Config

	// PredictionHorizon is how many hours ahead to predict decay (for archival planning)
	PredictionHorizon int

	// MinScoreChange is the minimum score change to trigger an update
	// Helps prevent unnecessary recalculations
	MinScoreChange float64
}

// DefaultKalmanAdapterConfig returns sensible defaults.
func DefaultKalmanAdapterConfig() KalmanAdapterConfig {
	return KalmanAdapterConfig{
		EnableKalmanSmoothing:  true,
		EnableTemporalModifier: true,
		SmoothingConfig:        filter.DecayPredictionConfig(),
		PredictionHorizon:      168, // 1 week
		MinScoreChange:         0.001,
	}
}

// KalmanAdapter wraps a decay Manager with Kalman filtering and temporal awareness.
type KalmanAdapter struct {
	mu     sync.RWMutex
	config KalmanAdapterConfig

	// The underlying decay manager
	manager *Manager

	// Temporal integration (optional)
	temporal *temporal.DecayIntegration

	// Per-node Kalman filters for score smoothing
	nodeFilters map[string]*filter.Kalman

	// Per-node smoothed scores cache
	cachedScores map[string]*smoothedScore

	// Statistics
	stats AdapterStats
}

// smoothedScore holds cached score data for a node.
type smoothedScore struct {
	Raw             float64
	Smoothed        float64
	Velocity        float64
	Modifier        float64
	PredictedScore  float64
	LastCalculation time.Time
}

// AdapterStats holds statistics about the adapter's operation.
type AdapterStats struct {
	TotalCalculations    int64
	KalmanSmoothed       int64
	TemporalModified     int64
	CacheHits            int64
	ArchivePredictions   int64
}

// NewKalmanAdapter creates a new Kalman-enhanced decay adapter.
//
// The adapter wraps an existing decay.Manager and enhances it with:
//   - Kalman smoothing of decay scores
//   - Temporal-aware decay modification
//   - Predictive archival planning
//
// Example:
//
//	manager := decay.New(decay.DefaultConfig())
//	adapter := decay.NewKalmanAdapter(manager, decay.DefaultKalmanAdapterConfig())
//
//	// Optionally connect temporal integration
//	temporal := temporal.NewDecayIntegration(temporal.DefaultDecayIntegrationConfig())
//	adapter.SetTemporal(temporal)
//
//	// Use enhanced score calculation
//	score := adapter.CalculateScore(info)
func NewKalmanAdapter(manager *Manager, config KalmanAdapterConfig) *KalmanAdapter {
	return &KalmanAdapter{
		config:       config,
		manager:      manager,
		nodeFilters:  make(map[string]*filter.Kalman),
		cachedScores: make(map[string]*smoothedScore),
	}
}

// SetTemporal connects a temporal integration for access-pattern-aware decay.
func (ka *KalmanAdapter) SetTemporal(t *temporal.DecayIntegration) {
	ka.mu.Lock()
	defer ka.mu.Unlock()
	ka.temporal = t
}

// CalculateScore calculates the Kalman-smoothed, temporally-adjusted decay score.
//
// The final score is computed as:
//
//	1. Raw score from decay.Manager.CalculateScore()
//	2. Apply temporal modifier (if enabled and temporal is set)
//	3. Smooth with Kalman filter (if enabled)
//
// Returns a score between 0.0 (forgotten) and 1.0 (fresh).
func (ka *KalmanAdapter) CalculateScore(info *MemoryInfo) float64 {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	ka.stats.TotalCalculations++

	// Step 1: Get raw score from underlying manager
	rawScore := ka.manager.CalculateScore(info)

	// Step 2: Apply temporal modifier (hot nodes decay slower)
	modifiedScore := rawScore
	modifier := 1.0

	if ka.config.EnableTemporalModifier && ka.temporal != nil {
		decayMod := ka.temporal.GetDecayModifier(info.ID)
		modifier = decayMod.Multiplier
		
		// Invert modifier for score: low decay multiplier = higher score retention
		// Decay multiplier 0.5 = 2x slower decay = score decays to 50% slower
		scoreModifier := 1.0 / modifier
		if scoreModifier > 2.0 {
			scoreModifier = 2.0 // Cap at 2x boost
		}
		if scoreModifier < 0.5 {
			scoreModifier = 0.5 // Cap at 0.5x penalty
		}
		
		// Blend: move score toward 1.0 for hot nodes, toward 0.0 for cold nodes
		if scoreModifier > 1.0 {
			// Hot node: score gets a boost toward 1.0
			modifiedScore = rawScore + (1.0-rawScore)*(scoreModifier-1.0)*0.5
		} else {
			// Cold node: score gets a penalty toward 0.0
			modifiedScore = rawScore * scoreModifier
		}
		
		ka.stats.TemporalModified++
	}

	// Step 3: Kalman smooth the score
	finalScore := modifiedScore

	if ka.config.EnableKalmanSmoothing {
		kf, exists := ka.nodeFilters[info.ID]
		if !exists {
			kf = filter.NewKalman(ka.config.SmoothingConfig)
			ka.nodeFilters[info.ID] = kf
		}

		// Use feature flag for A/B testing
		result := kf.ProcessIfEnabled(config.FeatureKalmanDecay, modifiedScore, 0.5)
		finalScore = result.Filtered

		if result.WasFiltered {
			ka.stats.KalmanSmoothed++
		}
	}

	// Clamp to [0, 1]
	if finalScore < 0 {
		finalScore = 0
	}
	if finalScore > 1 {
		finalScore = 1
	}

	// Cache the result
	ka.cachedScores[info.ID] = &smoothedScore{
		Raw:             rawScore,
		Smoothed:        finalScore,
		Velocity:        ka.getVelocity(info.ID),
		Modifier:        modifier,
		PredictedScore:  ka.predictScore(info.ID, ka.config.PredictionHorizon),
		LastCalculation: time.Now(),
	}

	return finalScore
}

// getVelocity returns the Kalman-tracked velocity for a node's score.
func (ka *KalmanAdapter) getVelocity(nodeID string) float64 {
	if kf, exists := ka.nodeFilters[nodeID]; exists {
		return kf.Velocity()
	}
	return 0
}

// predictScore predicts the decay score N hours from now.
func (ka *KalmanAdapter) predictScore(nodeID string, hoursAhead int) float64 {
	if kf, exists := ka.nodeFilters[nodeID]; exists {
		predicted := kf.Predict(hoursAhead)
		if predicted < 0 {
			predicted = 0
		}
		if predicted > 1 {
			predicted = 1
		}
		return predicted
	}
	return 0
}

// GetSmoothedScore returns the cached smoothed score for a node.
// Returns nil if the node has no cached score.
func (ka *KalmanAdapter) GetSmoothedScore(nodeID string) *smoothedScore {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	if score, exists := ka.cachedScores[nodeID]; exists {
		ka.stats.CacheHits++
		return score
	}
	return nil
}

// ShouldArchive checks if a memory should be archived based on Kalman-enhanced scoring.
//
// This method considers:
//   - Current smoothed score (not raw)
//   - Score velocity (is it rising or falling?)
//   - Predicted future score
//
// A memory is suggested for archival if:
//   - Current score is below threshold AND
//   - Velocity is negative or near-zero AND
//   - Predicted score is also below threshold
func (ka *KalmanAdapter) ShouldArchive(info *MemoryInfo) bool {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	ka.stats.ArchivePredictions++

	// Get or calculate current score
	cached := ka.cachedScores[info.ID]
	if cached == nil || time.Since(cached.LastCalculation) > time.Hour {
		ka.mu.Unlock()
		ka.CalculateScore(info)
		ka.mu.Lock()
		cached = ka.cachedScores[info.ID]
	}

	if cached == nil {
		// Fallback to raw check
		return ka.manager.ShouldArchive(ka.manager.CalculateScore(info))
	}

	threshold := ka.manager.config.ArchiveThreshold

	// Current score below threshold?
	if cached.Smoothed >= threshold {
		return false // Still strong
	}

	// Is velocity positive? (score recovering)
	if cached.Velocity > 0.001 {
		return false // Trending up, give it a chance
	}

	// Is predicted score also below threshold?
	if cached.PredictedScore >= threshold {
		return false // Expected to recover
	}

	// All conditions met - suggest archival
	return true
}

// GetArchivalCandidates returns nodes that are candidates for archival.
//
// Unlike the basic ShouldArchive, this considers Kalman velocity and prediction
// to avoid archiving memories that are about to be accessed again.
func (ka *KalmanAdapter) GetArchivalCandidates(memories []*MemoryInfo, limit int) []*MemoryInfo {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	type candidate struct {
		info    *MemoryInfo
		score   float64
		urgency float64 // Lower = more urgent to archive
	}

	var candidates []candidate

	for _, info := range memories {
		cached := ka.cachedScores[info.ID]
		if cached == nil {
			continue
		}

		threshold := ka.manager.config.ArchiveThreshold
		if cached.Smoothed < threshold && cached.Velocity <= 0 {
			// Urgency = score + predicted score + velocity boost
			// Lower urgency = more likely to archive
			urgency := cached.Smoothed + cached.PredictedScore*0.5 + cached.Velocity*10
			candidates = append(candidates, candidate{info, cached.Smoothed, urgency})
		}
	}

	// Sort by urgency (lowest first)
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].urgency < candidates[i].urgency {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Return top N
	result := make([]*MemoryInfo, 0, limit)
	for i := 0; i < len(candidates) && i < limit; i++ {
		result = append(result, candidates[i].info)
	}

	return result
}

// RecordAccess should be called when a memory is accessed.
// This updates both the underlying manager and the temporal integration.
func (ka *KalmanAdapter) RecordAccess(nodeID string) {
	if ka.temporal != nil {
		ka.temporal.RecordAccess(nodeID)
	}
}

// Reinforce reinforces a memory, extending its lifetime.
func (ka *KalmanAdapter) Reinforce(info *MemoryInfo) *MemoryInfo {
	// Record access for temporal tracking
	ka.RecordAccess(info.ID)
	
	// Reinforce via underlying manager
	return ka.manager.Reinforce(info)
}

// GetStats returns adapter statistics.
func (ka *KalmanAdapter) GetStats() AdapterStats {
	ka.mu.RLock()
	defer ka.mu.RUnlock()
	return ka.stats
}

// Reset clears all cached data and filters.
func (ka *KalmanAdapter) Reset() {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	ka.nodeFilters = make(map[string]*filter.Kalman)
	ka.cachedScores = make(map[string]*smoothedScore)
	ka.stats = AdapterStats{}
}

// GetManager returns the underlying decay manager.
func (ka *KalmanAdapter) GetManager() *Manager {
	return ka.manager
}

// RunDecayCycle runs a decay cycle with Kalman-enhanced processing.
// This is typically called periodically (e.g., hourly).
func (ka *KalmanAdapter) RunDecayCycle(ctx context.Context, memories []*MemoryInfo) error {
	for _, info := range memories {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ka.CalculateScore(info)
		}
	}
	return nil
}
