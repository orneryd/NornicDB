// Package inference - Kalman filter integration for adaptive relationship detection.
//
// This file provides the KalmanAdapter that enhances the inference engine with:
//   - Session-aware co-access: Uses temporal.SessionDetector for accurate sessions
//   - Confidence smoothing: Smooth noisy relationship confidence scores
//   - Trend detection: Detect strengthening/weakening relationships
//   - Predictive linking: Predict likely future relationships
//
// # Integration Architecture
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                   Kalman Inference Adapter                   │
//	├─────────────────────────────────────────────────────────────┤
//	│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
//	│  │ Inference Engine│───▶│ Kalman Filter (confidence)      │ │
//	│  │ (raw confidence)│    └─────────────────────────────────┘ │
//	│  └────────┬────────┘                                        │
//	│           │                                                 │
//	│           ▼                                                 │
//	│  ┌─────────────────────────────────────────────────────────┐│
//	│  │     Session Detector (from temporal package)            ││
//	│  │  • Real session boundaries via velocity changes         ││
//	│  │  • Session-scoped co-access (not time-window)          ││
//	│  │  • Cross-session linking for repeated patterns         ││
//	│  └─────────────────────────────────────────────────────────┘│
//	│           │                                                 │
//	│           ▼                                                 │
//	│  ┌─────────────────────────────────────────────────────────┐│
//	│  │     Enhanced Edge Suggestions                           ││
//	│  │  • Smoothed confidence scores                           ││
//	│  │  • Relationship strength trends                         ││
//	│  │  • Predicted future relationships                       ││
//	│  └─────────────────────────────────────────────────────────┘│
//	└─────────────────────────────────────────────────────────────┘
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you're trying to figure out which of your friends hang out together:
//
// **Without Kalman:**
//   - "Sarah and Mike were at the same place at 3pm" → friends!
//   - But wait, that was just the cafeteria. Everyone was there.
//
// **With Kalman + Sessions:**
//   - "Sarah and Mike have been together for the WHOLE AFTERNOON" → probably friends
//   - "They keep ending up together across MULTIPLE days" → definitely friends!
//
// The Kalman filter also smooths out mistakes:
//   - Day 1: "70% sure they're friends"
//   - Day 2: "30% sure" (oops, they argued)
//   - Day 3: "60% sure"
//   - Kalman says: "Smoothed: 55% - probably friends but something's up"
//
// This helps find REAL relationships, not just coincidences!
package inference

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/filter"
	"github.com/orneryd/nornicdb/pkg/temporal"
)

// KalmanAdapterConfig holds configuration for the Kalman-enhanced inference adapter.
type KalmanAdapterConfig struct {
	// EnableConfidenceSmoothing enables Kalman filtering of confidence scores
	EnableConfidenceSmoothing bool

	// EnableSessionTracking uses temporal.SessionDetector for session-aware co-access
	EnableSessionTracking bool

	// EnableStrengthTracking tracks relationship strength changes over time
	EnableStrengthTracking bool

	// CoAccessConfig for the co-access confidence filter
	CoAccessConfig filter.Config

	// MinConfidenceChange is the minimum change to trigger an update
	MinConfidenceChange float64

	// SessionCoAccessWeight is how much session-based co-access contributes
	SessionCoAccessWeight float64

	// CrossSessionBoost boosts relationships that appear across multiple sessions
	CrossSessionBoost float64
}

// DefaultKalmanAdapterConfig returns sensible defaults.
func DefaultKalmanAdapterConfig() KalmanAdapterConfig {
	return KalmanAdapterConfig{
		EnableConfidenceSmoothing: true,
		EnableSessionTracking:     true,
		EnableStrengthTracking:    true,
		CoAccessConfig:            filter.CoAccessConfig(),
		MinConfidenceChange:       0.01,
		SessionCoAccessWeight:     0.4,
		CrossSessionBoost:         1.3,
	}
}

// KalmanAdapter wraps an inference Engine with Kalman filtering and session awareness.
type KalmanAdapter struct {
	mu     sync.RWMutex
	config KalmanAdapterConfig

	// The underlying inference engine
	engine *Engine

	// Session detector from temporal package (optional)
	session *temporal.SessionDetector

	// Access tracker from temporal package (optional)
	tracker *temporal.Tracker

	// Per-edge Kalman filters for confidence smoothing
	edgeFilters map[edgeKey]*filter.KalmanVelocity

	// Cached smoothed confidences
	cachedConfidence map[edgeKey]*smoothedConfidence

	// Cross-session co-access tracking
	sessionCoAccess map[edgeKey]*crossSessionData

	// Statistics
	stats InferenceAdapterStats
}

type edgeKey struct {
	Source string
	Target string
}

type smoothedConfidence struct {
	Raw             float64
	Smoothed        float64
	Velocity        float64
	SessionCount    int
	LastUpdate      time.Time
}

type crossSessionData struct {
	SessionIDs    []string  // Session IDs where this pair co-occurred
	TotalCoAccess int       // Total co-access count
	FirstSeen     time.Time
	LastSeen      time.Time
}

// InferenceAdapterStats holds statistics about the adapter's operation.
type InferenceAdapterStats struct {
	TotalSuggestions        int64
	KalmanSmoothed          int64
	SessionEnhanced         int64
	CrossSessionBoosted     int64
	RelationshipsStrengthened int64
	RelationshipsWeakened   int64
}

// NewKalmanAdapter creates a new Kalman-enhanced inference adapter.
//
// Example:
//
//	engine := inference.New(inference.DefaultConfig())
//	adapter := inference.NewKalmanAdapter(engine, inference.DefaultKalmanAdapterConfig())
//
//	// Connect temporal session detector
//	session := temporal.NewSessionDetector(temporal.DefaultSessionConfig())
//	adapter.SetSessionDetector(session)
//
//	// Use enhanced suggestions
//	suggestions := adapter.OnAccess(ctx, nodeID)
func NewKalmanAdapter(engine *Engine, config KalmanAdapterConfig) *KalmanAdapter {
	return &KalmanAdapter{
		config:           config,
		engine:           engine,
		edgeFilters:      make(map[edgeKey]*filter.KalmanVelocity),
		cachedConfidence: make(map[edgeKey]*smoothedConfidence),
		sessionCoAccess:  make(map[edgeKey]*crossSessionData),
	}
}

// SetSessionDetector connects a temporal session detector.
func (ka *KalmanAdapter) SetSessionDetector(s *temporal.SessionDetector) {
	ka.mu.Lock()
	defer ka.mu.Unlock()
	ka.session = s
}

// SetTracker connects a temporal access tracker.
func (ka *KalmanAdapter) SetTracker(t *temporal.Tracker) {
	ka.mu.Lock()
	defer ka.mu.Unlock()
	ka.tracker = t
}

// OnAccess processes a node access and returns enhanced edge suggestions.
//
// This method:
//  1. Records access in the temporal tracker (if set)
//  2. Gets base suggestions from the inference engine
//  3. Enhances with session-based co-access
//  4. Smooths confidence scores with Kalman filter
//  5. Detects cross-session patterns
func (ka *KalmanAdapter) OnAccess(ctx context.Context, nodeID string) ([]EdgeSuggestion, error) {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	// Record access in temporal tracker
	if ka.tracker != nil {
		ka.tracker.RecordAccess(nodeID)
	}

	// Get base suggestions from engine
	baseSuggestions := ka.engine.OnAccess(ctx, nodeID)

	// Enhance with session-aware co-access
	sessionSuggestions := ka.getSessionCoAccessSuggestions(nodeID)
	baseSuggestions = ka.mergeSuggestions(baseSuggestions, sessionSuggestions)

	// Apply Kalman smoothing and track strength changes
	enhancedSuggestions := make([]EdgeSuggestion, 0, len(baseSuggestions))
	for _, sug := range baseSuggestions {
		enhanced := ka.enhanceSuggestion(sug)
		ka.stats.TotalSuggestions++
		enhancedSuggestions = append(enhancedSuggestions, enhanced)
	}

	return enhancedSuggestions, nil
}

// OnStore processes a new node and returns enhanced suggestions.
func (ka *KalmanAdapter) OnStore(ctx context.Context, nodeID string, embedding []float32) ([]EdgeSuggestion, error) {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	// Record in temporal tracker
	if ka.tracker != nil {
		ka.tracker.RecordAccess(nodeID)
	}

	// Get base suggestions from engine
	baseSuggestions, err := ka.engine.OnStore(ctx, nodeID, embedding)
	if err != nil {
		return nil, err
	}

	// Apply Kalman smoothing
	enhancedSuggestions := make([]EdgeSuggestion, 0, len(baseSuggestions))
	for _, sug := range baseSuggestions {
		enhanced := ka.enhanceSuggestion(sug)
		ka.stats.TotalSuggestions++
		enhancedSuggestions = append(enhancedSuggestions, enhanced)
	}

	return enhancedSuggestions, nil
}

// getSessionCoAccessSuggestions returns co-access suggestions based on session boundaries.
func (ka *KalmanAdapter) getSessionCoAccessSuggestions(nodeID string) []EdgeSuggestion {
	if !ka.config.EnableSessionTracking || ka.session == nil {
		return nil
	}

	// Get current session for this node
	sessionInfo := ka.session.GetCurrentSession(nodeID)
	if sessionInfo == nil {
		return nil
	}

	currentSessionID := sessionInfo.ID

	// Get all nodes accessed in current session (from session's NodeIDs)
	sessionNodes := sessionInfo.NodeIDs
	if len(sessionNodes) < 2 {
		return nil
	}

	var suggestions []EdgeSuggestion

	for _, otherNode := range sessionNodes {
		if otherNode == nodeID {
			continue
		}

		// Create edge key (sorted for consistency)
		key := ka.makeEdgeKey(nodeID, otherNode)

		// Track cross-session co-access
		if ka.sessionCoAccess[key] == nil {
			ka.sessionCoAccess[key] = &crossSessionData{
				SessionIDs: []string{currentSessionID},
				TotalCoAccess: 1,
				FirstSeen: time.Now(),
				LastSeen:  time.Now(),
			}
		} else {
			data := ka.sessionCoAccess[key]
			data.TotalCoAccess++
			data.LastSeen = time.Now()
			
			// Check if this is a new session
			isNewSession := true
			for _, sid := range data.SessionIDs {
				if sid == currentSessionID {
					isNewSession = false
					break
				}
			}
			if isNewSession {
				data.SessionIDs = append(data.SessionIDs, currentSessionID)
			}
		}

		// Calculate confidence based on session co-access
		coAccessData := ka.sessionCoAccess[key]
		confidence := ka.calculateSessionConfidence(coAccessData)

		suggestions = append(suggestions, EdgeSuggestion{
			SourceID:   nodeID,
			TargetID:   otherNode,
			Type:       "SESSION_CO_ACCESS",
			Confidence: confidence,
			Reason:     "Accessed together in same session",
			Method:     "session_co_access",
		})

		ka.stats.SessionEnhanced++
	}

	return suggestions
}

// calculateSessionConfidence calculates confidence based on cross-session data.
func (ka *KalmanAdapter) calculateSessionConfidence(data *crossSessionData) float64 {
	if data == nil {
		return 0
	}

	// Base confidence from co-access count
	coAccessConf := 0.3 + 0.1*float64(data.TotalCoAccess)
	if coAccessConf > 0.7 {
		coAccessConf = 0.7
	}

	// Cross-session boost
	sessionCount := len(data.SessionIDs)
	if sessionCount > 1 {
		boost := 1.0 + 0.1*float64(sessionCount-1)
		if boost > ka.config.CrossSessionBoost {
			boost = ka.config.CrossSessionBoost
		}
		coAccessConf *= boost
		ka.stats.CrossSessionBoosted++
	}

	// Time decay - older relationships get slightly lower confidence
	age := time.Since(data.FirstSeen)
	if age > 7*24*time.Hour {
		coAccessConf *= 0.9
	}

	if coAccessConf > 1.0 {
		coAccessConf = 1.0
	}

	return coAccessConf
}

// enhanceSuggestion applies Kalman smoothing and tracking to a suggestion.
func (ka *KalmanAdapter) enhanceSuggestion(sug EdgeSuggestion) EdgeSuggestion {
	key := ka.makeEdgeKey(sug.SourceID, sug.TargetID)

	// Get or create Kalman filter for this edge
	kf, exists := ka.edgeFilters[key]
	if !exists && ka.config.EnableConfidenceSmoothing {
		kf = filter.NewKalmanVelocity(filter.DefaultVelocityConfig())
		ka.edgeFilters[key] = kf
	}

	// Get previous state for comparison
	prevConfidence := float64(0)
	if cached := ka.cachedConfidence[key]; cached != nil {
		prevConfidence = cached.Smoothed
	}

	// Apply Kalman smoothing
	smoothedConf := sug.Confidence
	velocity := float64(0)

	if ka.config.EnableConfidenceSmoothing && kf != nil {
		result := kf.ProcessIfEnabled(config.FeatureKalmanCoAccess, sug.Confidence)
		smoothedConf = result.Filtered
		velocity = kf.Velocity()

		if result.WasFiltered {
			ka.stats.KalmanSmoothed++
		}
	}

	// Track strength changes
	if ka.config.EnableStrengthTracking && prevConfidence > 0 {
		if velocity > 0.05 {
			ka.stats.RelationshipsStrengthened++
			sug.Reason += " [strengthening]"
		} else if velocity < -0.05 {
			ka.stats.RelationshipsWeakened++
			sug.Reason += " [weakening]"
		}
	}

	// Cache the result
	ka.cachedConfidence[key] = &smoothedConfidence{
		Raw:          sug.Confidence,
		Smoothed:     smoothedConf,
		Velocity:     velocity,
		SessionCount: ka.getSessionCount(key),
		LastUpdate:   time.Now(),
	}

	// Return enhanced suggestion
	sug.Confidence = smoothedConf
	return sug
}

// mergeSuggestions combines base and session suggestions, preferring higher confidence.
func (ka *KalmanAdapter) mergeSuggestions(base, session []EdgeSuggestion) []EdgeSuggestion {
	merged := make(map[edgeKey]EdgeSuggestion)

	for _, sug := range base {
		key := ka.makeEdgeKey(sug.SourceID, sug.TargetID)
		merged[key] = sug
	}

	for _, sug := range session {
		key := ka.makeEdgeKey(sug.SourceID, sug.TargetID)
		if existing, exists := merged[key]; exists {
			// Combine confidence using weighted average
			combined := existing.Confidence*(1-ka.config.SessionCoAccessWeight) +
				sug.Confidence*ka.config.SessionCoAccessWeight
			existing.Confidence = combined
			existing.Reason += " + " + sug.Reason
			merged[key] = existing
		} else {
			merged[key] = sug
		}
	}

	result := make([]EdgeSuggestion, 0, len(merged))
	for _, sug := range merged {
		result = append(result, sug)
	}
	return result
}

// makeEdgeKey creates a consistent key for an edge (sorted).
func (ka *KalmanAdapter) makeEdgeKey(source, target string) edgeKey {
	if source < target {
		return edgeKey{Source: source, Target: target}
	}
	return edgeKey{Source: target, Target: source}
}

// getSessionCount returns how many sessions an edge has appeared in.
func (ka *KalmanAdapter) getSessionCount(key edgeKey) int {
	if data := ka.sessionCoAccess[key]; data != nil {
		return len(data.SessionIDs)
	}
	return 0
}

// GetRelationshipStrength returns the Kalman-smoothed relationship strength.
func (ka *KalmanAdapter) GetRelationshipStrength(source, target string) *smoothedConfidence {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	key := ka.makeEdgeKey(source, target)
	return ka.cachedConfidence[key]
}

// GetStrentheningRelationships returns relationships that are getting stronger.
func (ka *KalmanAdapter) GetStrengtheningRelationships(minVelocity float64) []EdgeSuggestion {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	var results []EdgeSuggestion

	for key, conf := range ka.cachedConfidence {
		if conf.Velocity >= minVelocity {
			results = append(results, EdgeSuggestion{
				SourceID:   key.Source,
				TargetID:   key.Target,
				Confidence: conf.Smoothed,
				Reason:     "Relationship strengthening",
				Method:     "velocity_detection",
			})
		}
	}

	return results
}

// GetWeakeningRelationships returns relationships that are getting weaker.
func (ka *KalmanAdapter) GetWeakeningRelationships(maxVelocity float64) []EdgeSuggestion {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	var results []EdgeSuggestion

	for key, conf := range ka.cachedConfidence {
		if conf.Velocity <= maxVelocity {
			results = append(results, EdgeSuggestion{
				SourceID:   key.Source,
				TargetID:   key.Target,
				Confidence: conf.Smoothed,
				Reason:     "Relationship weakening",
				Method:     "velocity_detection",
			})
		}
	}

	return results
}

// PredictFutureRelationships predicts which relationships will likely form.
//
// Returns suggestions for node pairs that are trending toward each other
// based on their co-access velocity.
func (ka *KalmanAdapter) PredictFutureRelationships(threshold float64) []EdgeSuggestion {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	var predictions []EdgeSuggestion

	for key, kf := range ka.edgeFilters {
		// Predict confidence in N steps
		predictedConf := kf.Predict(24) // 24 hours ahead
		currentConf := kf.State()

		// If predicted to exceed threshold and currently below
		if predictedConf >= threshold && currentConf < threshold {
			predictions = append(predictions, EdgeSuggestion{
				SourceID:   key.Source,
				TargetID:   key.Target,
				Confidence: predictedConf,
				Reason:     "Predicted to become related",
				Method:     "kalman_prediction",
			})
		}
	}

	return predictions
}

// GetStats returns adapter statistics.
func (ka *KalmanAdapter) GetStats() InferenceAdapterStats {
	ka.mu.RLock()
	defer ka.mu.RUnlock()
	return ka.stats
}

// GetEngine returns the underlying inference engine.
func (ka *KalmanAdapter) GetEngine() *Engine {
	return ka.engine
}

// Reset clears all cached data and filters.
func (ka *KalmanAdapter) Reset() {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	ka.edgeFilters = make(map[edgeKey]*filter.KalmanVelocity)
	ka.cachedConfidence = make(map[edgeKey]*smoothedConfidence)
	ka.sessionCoAccess = make(map[edgeKey]*crossSessionData)
	ka.stats = InferenceAdapterStats{}
}
