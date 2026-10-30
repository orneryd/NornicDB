// Package temporal - Decay integration for adaptive memory retention.
//
// DecayIntegration modifies NornicDB's decay system based on temporal patterns:
//   - Frequently accessed nodes decay SLOWER (important memories persist)
//   - Rarely accessed nodes decay FASTER (forgotten memories fade)
//   - Nodes with daily patterns maintain longer (routine knowledge)
//   - Burst-accessed nodes get temporary boost (current focus)
//
// This creates a more human-like memory system where:
//   - Things you use often stay fresh
//   - Things you forget naturally fade
//   - Context matters (current session nodes are prioritized)
//
// Integration points:
//   - DecayManager: Call GetDecayModifier() to adjust decay rate
//   - ArchiveManager: Call ShouldArchive() to identify cold nodes
//   - SearchRanker: Call GetRelevanceBoost() to rank results
//
// # ELI12 (Explain Like I'm 12)
//
// Your brain forgets things! But it's SMART about what it forgets:
//
//	🧠 Your best friend's name?     → NEVER forget (use it daily!)
//	🧠 What you had for lunch today → Remember for a bit, then forget
//	🧠 Random fact from 5 years ago → Probably already forgot it
//
// DecayIntegration makes the database work like your brain:
//
// The "decay" is like forgetting. Every memory slowly fades over time.
// But the Kalman filter velocity tells us HOW to adjust the forgetting speed:
//
//	📈 Velocity positive (accessing MORE often):
//	   "Hey, you're using this a lot lately - slow down the forgetting!"
//	   Decay multiplier: 0.1 (10x slower decay)
//
//	📉 Velocity negative (accessing LESS often):
//	   "You used to look at this every day, now it's been weeks..."
//	   Decay multiplier: 2.0 (2x faster decay)
//
//	📊 Velocity stable:
//	   "Normal usage pattern, normal forgetting speed"
//	   Decay multiplier: 1.0 (normal decay)
//
// Special cases:
//
//	🔥 BURST: Looking at something 10 times RIGHT NOW?
//	   → "Super important right now!" → Nearly zero decay
//
//	📅 DAILY PATTERN: Access every morning at 9am?
//	   → "Part of your routine!" → Slower decay
//
//	❄️ COLD: Haven't touched in 2 weeks?
//	   → "Probably not important anymore" → Faster decay, maybe archive
//
// The Kalman filter smooths out noise. If you access something once by accident,
// it doesn't suddenly become "important". It waits to see a TREND.
package temporal

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/filter"
)

// DecayModifier represents how decay should be adjusted.
type DecayModifier struct {
	// Multiplier for decay rate (0.5 = half decay speed, 2.0 = double decay speed)
	Multiplier float64

	// Reason for the modification
	Reason string

	// Confidence in this modification (0-1)
	Confidence float64

	// Components that contributed to this modifier
	Components []DecayComponent
}

// DecayComponent represents a single factor affecting decay.
type DecayComponent struct {
	Name       string
	Multiplier float64
	Weight     float64
}

// DecayIntegrationConfig holds configuration for decay integration.
type DecayIntegrationConfig struct {
	// BaseDecayRate - the default decay rate per hour (0-1)
	BaseDecayRate float64

	// FrequentAccessBoost - how much to slow decay for frequent access (0.1 = 10x slower)
	FrequentAccessBoost float64

	// RareAccessPenalty - how much to speed decay for rare access (2.0 = 2x faster)
	RareAccessPenalty float64

	// DailyPatternBoost - boost for nodes with daily patterns
	DailyPatternBoost float64

	// BurstBoostDuration - how long burst boost lasts (seconds)
	BurstBoostDuration float64

	// BurstBoostMultiplier - decay multiplier during burst
	BurstBoostMultiplier float64

	// SessionBoostMultiplier - decay multiplier for current session nodes
	SessionBoostMultiplier float64

	// MinDecayMultiplier - minimum decay multiplier (prevent immortal nodes)
	MinDecayMultiplier float64

	// MaxDecayMultiplier - maximum decay multiplier (prevent instant death)
	MaxDecayMultiplier float64

	// VelocityWeight - how much velocity affects decay
	VelocityWeight float64

	// PatternWeight - how much patterns affect decay
	PatternWeight float64

	// RecencyWeight - how much recency affects decay
	RecencyWeight float64
}

// DefaultDecayIntegrationConfig returns sensible defaults.
func DefaultDecayIntegrationConfig() DecayIntegrationConfig {
	return DecayIntegrationConfig{
		BaseDecayRate:          0.01, // 1% per hour base decay
		FrequentAccessBoost:    0.1,  // 10x slower decay for frequent access
		RareAccessPenalty:      2.0,  // 2x faster decay for rare access
		DailyPatternBoost:      0.5,  // 2x slower for daily patterns
		BurstBoostDuration:     300,  // 5 minute burst boost
		BurstBoostMultiplier:   0.1,  // 10x slower during burst
		SessionBoostMultiplier: 0.2,  // 5x slower for current session
		MinDecayMultiplier:     0.05, // Never slower than 20x base
		MaxDecayMultiplier:     5.0,  // Never faster than 5x base
		VelocityWeight:         0.4,  // 40% weight for velocity
		PatternWeight:          0.3,  // 30% weight for patterns
		RecencyWeight:          0.3,  // 30% weight for recency
	}
}

// ConservativeDecayConfig returns config that preserves more memories.
func ConservativeDecayConfig() DecayIntegrationConfig {
	cfg := DefaultDecayIntegrationConfig()
	cfg.FrequentAccessBoost = 0.05 // 20x slower for frequent
	cfg.MinDecayMultiplier = 0.02  // Can be 50x slower
	cfg.MaxDecayMultiplier = 2.0   // Never faster than 2x
	return cfg
}

// AggressiveDecayConfig returns config that forgets faster.
func AggressiveDecayConfig() DecayIntegrationConfig {
	cfg := DefaultDecayIntegrationConfig()
	cfg.RareAccessPenalty = 5.0   // 5x faster for rare
	cfg.MinDecayMultiplier = 0.2  // Can only be 5x slower
	cfg.MaxDecayMultiplier = 10.0 // Can be 10x faster
	return cfg
}

// DecayIntegration manages decay rate modifications based on temporal data.
type DecayIntegration struct {
	mu     sync.RWMutex
	config DecayIntegrationConfig

	// Components
	tracker         *Tracker
	patternDetector *PatternDetector
	sessionDetector *SessionDetector

	// Per-node decay state
	nodeDecay map[string]*nodeDecayState

	// Kalman filter for smoothing decay adjustments
	decayFilter *filter.KalmanAdaptive
}

// nodeDecayState tracks decay-related state for a node.
type nodeDecayState struct {
	// Last calculated modifier
	lastModifier DecayModifier

	// Last update time
	lastUpdate time.Time

	// Burst state
	burstStart time.Time
	inBurst    bool

	// Cached scores
	velocityScore float64
	patternScore  float64
	recencyScore  float64
}

// NewDecayIntegration creates a new decay integration system.
func NewDecayIntegration(cfg DecayIntegrationConfig) *DecayIntegration {
	return &DecayIntegration{
		config:          cfg,
		tracker:         NewTracker(DefaultConfig()),
		patternDetector: NewPatternDetector(DefaultPatternDetectorConfig()),
		sessionDetector: NewSessionDetector(DefaultSessionDetectorConfig()),
		nodeDecay:       make(map[string]*nodeDecayState),
		decayFilter:     filter.NewKalmanAdaptive(filter.DefaultAdaptiveConfig()),
	}
}

// NewDecayIntegrationWithComponents creates decay integration with existing components.
func NewDecayIntegrationWithComponents(
	cfg DecayIntegrationConfig,
	tracker *Tracker,
	pattern *PatternDetector,
	session *SessionDetector,
) *DecayIntegration {
	return &DecayIntegration{
		config:          cfg,
		tracker:         tracker,
		patternDetector: pattern,
		sessionDetector: session,
		nodeDecay:       make(map[string]*nodeDecayState),
		decayFilter:     filter.NewKalmanAdaptive(filter.DefaultAdaptiveConfig()),
	}
}

// RecordAccess records an access and updates all temporal components.
func (di *DecayIntegration) RecordAccess(nodeID string) {
	di.RecordAccessAt(nodeID, time.Now())
}

// RecordAccessAt records an access at a specific time.
func (di *DecayIntegration) RecordAccessAt(nodeID string, timestamp time.Time) {
	// Update all components
	di.tracker.RecordAccessAt(nodeID, timestamp)
	di.patternDetector.RecordAccess(nodeID, timestamp)
	di.sessionDetector.RecordAccess(nodeID, timestamp)

	// Update decay state
	di.mu.Lock()
	defer di.mu.Unlock()

	state, exists := di.nodeDecay[nodeID]
	if !exists {
		state = &nodeDecayState{}
		di.nodeDecay[nodeID] = state
	}

	// Check for burst
	if exists && time.Since(state.lastUpdate).Seconds() < 10 {
		if !state.inBurst {
			state.inBurst = true
			state.burstStart = timestamp
		}
	}

	state.lastUpdate = timestamp
}

// GetDecayModifier returns the decay rate modifier for a node.
func (di *DecayIntegration) GetDecayModifier(nodeID string) DecayModifier {
	di.mu.RLock()
	state := di.nodeDecay[nodeID]
	di.mu.RUnlock()

	components := make([]DecayComponent, 0, 5)
	totalWeight := 0.0
	weightedMultiplier := 0.0

	// 1. Velocity component (access rate trend)
	velocity, trend := di.tracker.GetAccessRateTrend(nodeID)
	velocityMult := di.calculateVelocityMultiplier(velocity, trend)
	components = append(components, DecayComponent{
		Name:       "velocity",
		Multiplier: velocityMult,
		Weight:     di.config.VelocityWeight,
	})
	weightedMultiplier += velocityMult * di.config.VelocityWeight
	totalWeight += di.config.VelocityWeight

	// 2. Pattern component (daily/weekly patterns)
	patterns := di.patternDetector.DetectPatterns(nodeID, velocity)
	patternMult := di.calculatePatternMultiplier(patterns)
	components = append(components, DecayComponent{
		Name:       "pattern",
		Multiplier: patternMult,
		Weight:     di.config.PatternWeight,
	})
	weightedMultiplier += patternMult * di.config.PatternWeight
	totalWeight += di.config.PatternWeight

	// 3. Recency component (time since last access)
	stats := di.tracker.GetStats(nodeID)
	recencyMult := di.calculateRecencyMultiplier(stats)
	components = append(components, DecayComponent{
		Name:       "recency",
		Multiplier: recencyMult,
		Weight:     di.config.RecencyWeight,
	})
	weightedMultiplier += recencyMult * di.config.RecencyWeight
	totalWeight += di.config.RecencyWeight

	// 4. Session component (current session boost)
	session := di.sessionDetector.GetCurrentSession(nodeID)
	if session != nil && session.IsCurrent {
		sessionMult := di.config.SessionBoostMultiplier
		components = append(components, DecayComponent{
			Name:       "session",
			Multiplier: sessionMult,
			Weight:     0.5, // High weight for current session
		})
		weightedMultiplier += sessionMult * 0.5
		totalWeight += 0.5
	}

	// 5. Burst component (recent rapid access)
	if state != nil && state.inBurst {
		burstAge := time.Since(state.burstStart).Seconds()
		if burstAge < di.config.BurstBoostDuration {
			burstMult := di.config.BurstBoostMultiplier
			components = append(components, DecayComponent{
				Name:       "burst",
				Multiplier: burstMult,
				Weight:     0.3,
			})
			weightedMultiplier += burstMult * 0.3
			totalWeight += 0.3
		} else {
			// Burst expired
			di.mu.Lock()
			if s := di.nodeDecay[nodeID]; s != nil {
				s.inBurst = false
			}
			di.mu.Unlock()
		}
	}

	// Calculate final multiplier
	var finalMultiplier float64
	if totalWeight > 0 {
		finalMultiplier = weightedMultiplier / totalWeight
	} else {
		finalMultiplier = 1.0
	}

	// Clamp to min/max
	if finalMultiplier < di.config.MinDecayMultiplier {
		finalMultiplier = di.config.MinDecayMultiplier
	}
	if finalMultiplier > di.config.MaxDecayMultiplier {
		finalMultiplier = di.config.MaxDecayMultiplier
	}

	// Smooth with Kalman filter
	smoothed := di.decayFilter.Process(finalMultiplier)
	if smoothed > 0 {
		finalMultiplier = smoothed
	}

	// Determine reason
	reason := di.determineReason(components, finalMultiplier)

	// Calculate confidence
	confidence := di.calculateConfidence(stats, components)

	return DecayModifier{
		Multiplier: finalMultiplier,
		Reason:     reason,
		Confidence: confidence,
		Components: components,
	}
}

// calculateVelocityMultiplier converts velocity to decay multiplier.
func (di *DecayIntegration) calculateVelocityMultiplier(velocity float64, trend string) float64 {
	switch trend {
	case "increasing":
		// More access = slower decay
		return di.config.FrequentAccessBoost * (1.0 + velocity)
	case "decreasing":
		// Less access = faster decay
		return di.config.RareAccessPenalty * (1.0 - velocity)
	default:
		return 1.0
	}
}

// calculatePatternMultiplier converts patterns to decay multiplier.
func (di *DecayIntegration) calculatePatternMultiplier(patterns []DetectedPattern) float64 {
	if len(patterns) == 0 {
		return 1.0
	}

	bestMult := 1.0
	for _, p := range patterns {
		var mult float64
		switch p.Type {
		case PatternDaily:
			mult = di.config.DailyPatternBoost * (1.0 - p.Confidence*0.5)
		case PatternWeekly:
			mult = di.config.DailyPatternBoost * (1.0 - p.Confidence*0.3)
		case PatternBurst:
			mult = di.config.BurstBoostMultiplier
		case PatternGrowing:
			mult = di.config.FrequentAccessBoost
		case PatternDecaying:
			mult = di.config.RareAccessPenalty
		default:
			mult = 1.0
		}
		if mult < bestMult {
			bestMult = mult
		}
	}

	return bestMult
}

// calculateRecencyMultiplier converts time since last access to multiplier.
func (di *DecayIntegration) calculateRecencyMultiplier(stats *NodeStats) float64 {
	if stats == nil {
		return di.config.RareAccessPenalty
	}

	hoursSinceAccess := time.Since(stats.LastAccess).Hours()

	if hoursSinceAccess < 1 {
		return di.config.FrequentAccessBoost // Very recent
	} else if hoursSinceAccess < 24 {
		return 0.5 // Accessed today
	} else if hoursSinceAccess < 168 { // 1 week
		return 1.0 // Normal decay
	} else {
		return di.config.RareAccessPenalty // Old
	}
}

// determineReason generates a human-readable reason for the modifier.
func (di *DecayIntegration) determineReason(components []DecayComponent, multiplier float64) string {
	if multiplier < 0.3 {
		for _, c := range components {
			if c.Name == "session" {
				return "current_session"
			}
			if c.Name == "burst" {
				return "burst_activity"
			}
		}
		return "frequently_accessed"
	} else if multiplier > 1.5 {
		return "rarely_accessed"
	}
	return "normal_activity"
}

// calculateConfidence calculates confidence in the decay modifier.
func (di *DecayIntegration) calculateConfidence(stats *NodeStats, components []DecayComponent) float64 {
	if stats == nil {
		return 0.1 // Low confidence without data
	}

	// More accesses = more confidence
	accessConfidence := math.Min(float64(stats.TotalAccesses)/100.0, 1.0)

	// More components = more confidence
	componentConfidence := math.Min(float64(len(components))/5.0, 1.0)

	return (accessConfidence + componentConfidence) / 2.0
}

// GetEffectiveDecayRate returns the actual decay rate for a node.
func (di *DecayIntegration) GetEffectiveDecayRate(nodeID string) float64 {
	modifier := di.GetDecayModifier(nodeID)
	return di.config.BaseDecayRate * modifier.Multiplier
}

// ShouldArchive checks if a node should be archived based on temporal data.
func (di *DecayIntegration) ShouldArchive(nodeID string, currentScore float64, archiveThreshold float64) bool {
	modifier := di.GetDecayModifier(nodeID)

	// Adjust threshold based on temporal data
	// Hot nodes get a lower effective threshold (harder to archive)
	// Cold nodes get a higher effective threshold (easier to archive)
	effectiveThreshold := archiveThreshold * modifier.Multiplier

	return currentScore < effectiveThreshold
}

// GetRelevanceBoost returns a relevance boost for search ranking.
func (di *DecayIntegration) GetRelevanceBoost(nodeID string) float64 {
	modifier := di.GetDecayModifier(nodeID)

	// Invert the decay modifier - low decay = high relevance
	// Clamp to reasonable range (0.5 - 2.0)
	boost := 1.0 / modifier.Multiplier
	if boost < 0.5 {
		boost = 0.5
	}
	if boost > 2.0 {
		boost = 2.0
	}

	return boost
}

// GetHotNodes returns nodes that should be prioritized (slow decay).
func (di *DecayIntegration) GetHotNodes(limit int) []string {
	return di.tracker.GetHotNodes(limit)
}

// GetColdNodes returns nodes that are candidates for archival.
func (di *DecayIntegration) GetColdNodes(limit int) []string {
	return di.tracker.GetColdNodes(limit)
}

// Reset clears all temporal data.
func (di *DecayIntegration) Reset() {
	di.mu.Lock()
	defer di.mu.Unlock()

	di.tracker.Reset()
	di.patternDetector.Reset()
	di.sessionDetector.Reset()
	di.nodeDecay = make(map[string]*nodeDecayState)
}

// GetStats returns statistics for the decay integration.
type DecayIntegrationStats struct {
	TrackedNodes      int
	ActiveSessions    int
	TotalAccesses     int64
	AverageMultiplier float64
}

// GetStats returns current statistics.
func (di *DecayIntegration) GetStats() DecayIntegrationStats {
	globalStats := di.tracker.GetGlobalStats()
	activeSessions := di.sessionDetector.GetActiveSessions()

	di.mu.RLock()
	nodeCount := len(di.nodeDecay)
	di.mu.RUnlock()

	return DecayIntegrationStats{
		TrackedNodes:   nodeCount,
		ActiveSessions: len(activeSessions),
		TotalAccesses:  globalStats.TotalAccesses,
	}
}
