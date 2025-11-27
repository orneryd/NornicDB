// Package search - Kalman filter integration for stable search ranking.
//
// This file provides the KalmanAdapter that enhances the search service with:
//   - Similarity score smoothing: Reduce noise in vector similarity scores
//   - Ranking stability: Prevent results from jumping around between queries
//   - Trend detection: Identify documents becoming more/less relevant
//   - Latency prediction: Smooth query latency for better resource planning
//
// # Why Smooth Search Scores?
//
// Vector similarity scores can be noisy due to:
//   - Embedding model variability (same query, slightly different embeddings)
//   - Approximation in ANN (approximate nearest neighbor) indexes
//   - Edge cases where documents are equidistant from query
//
// Kalman filtering smooths these variations to provide:
//   - More consistent rankings across similar queries
//   - Gradual relevance transitions (not sudden jumps)
//   - Confidence estimates for borderline results
//
// # Integration Architecture
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                  Kalman Search Adapter                       │
//	├─────────────────────────────────────────────────────────────┤
//	│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
//	│  │  Search Service │───▶│ Kalman Filter (scores)          │ │
//	│  │  (raw scores)   │    │ - Per-document filters          │ │
//	│  └────────┬────────┘    │ - Score history smoothing       │ │
//	│           │             └─────────────────────────────────┘ │
//	│           │                                                 │
//	│           ▼                                                 │
//	│  ┌─────────────────────────────────────────────────────────┐│
//	│  │     Ranking Stabilizer                                  ││
//	│  │  • Detect score ties                                    ││
//	│  │  • Apply stability boost to consistent docs             ││
//	│  │  • Prevent rank oscillation                             ││
//	│  └─────────────────────────────────────────────────────────┘│
//	│           │                                                 │
//	│           ▼                                                 │
//	│  ┌─────────────────────────────────────────────────────────┐│
//	│  │     Latency Predictor                                   ││
//	│  │  • Smooth query latency measurements                    ││
//	│  │  • Predict P99 latency                                  ││
//	│  │  • Resource scaling suggestions                         ││
//	│  └─────────────────────────────────────────────────────────┘│
//	└─────────────────────────────────────────────────────────────┘
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you're searching Google for "cute cats":
//
// **Without Kalman:**
//   - Search 1: Cat A is #1, Cat B is #2
//   - Search 2: Cat B is #1, Cat A is #2 (they swapped!)
//   - Search 3: Cat C is #1 (where did that come from?!)
//
// **With Kalman:**
//   - Kalman remembers: "Cat A has been top 3 for a while"
//   - When Cat B briefly gets a tiny score boost, Kalman says:
//     "That's probably noise. Cat A stays #1 until I see a REAL trend."
//   - Result: Rankings stay consistent, you find things where you expect them!
//
// It's like having a wise librarian who remembers "this book was popular before"
// instead of reshuffling the entire library every time someone asks a question.
package search

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/filter"
)

// KalmanSearchConfig holds configuration for the Kalman-enhanced search adapter.
type KalmanSearchConfig struct {
	// EnableScoreSmoothing enables Kalman filtering of similarity scores
	EnableScoreSmoothing bool

	// EnableRankingStability applies stability boost to consistent results
	EnableRankingStability bool

	// EnableLatencyPrediction tracks and predicts query latency
	EnableLatencyPrediction bool

	// SimilarityConfig for score smoothing
	SimilarityConfig filter.Config

	// LatencyConfig for latency prediction
	LatencyConfig filter.Config

	// StabilityBoost is the boost factor for consistently-ranked documents
	StabilityBoost float64

	// StabilityWindow is how many recent queries to consider for stability
	StabilityWindow int

	// ScoreHistoryLimit is max history per document
	ScoreHistoryLimit int
}

// DefaultKalmanSearchConfig returns sensible defaults.
func DefaultKalmanSearchConfig() KalmanSearchConfig {
	return KalmanSearchConfig{
		EnableScoreSmoothing:    true,
		EnableRankingStability:  true,
		EnableLatencyPrediction: true,
		SimilarityConfig: filter.Config{
			ProcessNoise:      0.05,  // Scores are relatively stable
			MeasurementNoise:  30.0,  // ANN can be noisy
			InitialCovariance: 20.0,
			VarianceScale:     8.0,
		},
		LatencyConfig:     filter.LatencyConfig(),
		StabilityBoost:    1.05, // 5% boost for stable docs
		StabilityWindow:   10,
		ScoreHistoryLimit: 50,
	}
}

// KalmanSearchAdapter wraps a search Service with Kalman filtering.
type KalmanSearchAdapter struct {
	mu     sync.RWMutex
	config KalmanSearchConfig

	// The underlying search service
	service *Service

	// Per-document Kalman filters for score smoothing
	// Key: documentID, Value: filter tracking that doc's relevance to queries
	docFilters map[string]*filter.Kalman

	// Recent rankings for stability detection
	recentRankings []queryRanking

	// Latency predictor
	latencyFilter *filter.KalmanVelocity

	// Statistics
	stats SearchAdapterStats
}

// queryRanking stores the top results from a query.
type queryRanking struct {
	Query     string
	Timestamp time.Time
	TopDocs   []string // Document IDs in ranked order
}

// SearchAdapterStats holds adapter statistics.
type SearchAdapterStats struct {
	TotalQueries       int64
	ScoresSmoothed     int64
	StabilityApplied   int64
	LatencyPredictions int64
	AverageLatencyMs   float64
	PredictedLatencyMs float64
}

// NewKalmanSearchAdapter creates a new Kalman-enhanced search adapter.
//
// Example:
//
//	service := search.NewService(engine, 1024)
//	adapter := search.NewKalmanSearchAdapter(service, search.DefaultKalmanSearchConfig())
//
//	// Search with enhanced ranking
//	results, _ := adapter.Search(ctx, "semantic search", embedding, &SearchOptions{Limit: 10})
func NewKalmanSearchAdapter(service *Service, config KalmanSearchConfig) *KalmanSearchAdapter {
	return &KalmanSearchAdapter{
		config:         config,
		service:        service,
		docFilters:     make(map[string]*filter.Kalman),
		recentRankings: make([]queryRanking, 0, config.StabilityWindow),
		latencyFilter:  filter.NewKalmanVelocity(filter.DefaultVelocityConfig()),
	}
}

// Search performs a Kalman-enhanced search.
//
// The enhancement pipeline:
//  1. Execute underlying search
//  2. Smooth similarity scores with Kalman filter
//  3. Apply ranking stability boost
//  4. Track latency for prediction
//  5. Return enhanced results
func (ka *KalmanSearchAdapter) Search(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	start := time.Now()

	// Execute underlying search
	response, err := ka.service.Search(ctx, query, embedding, opts)
	if err != nil {
		return nil, err
	}

	// Track latency
	latencyMs := float64(time.Since(start).Milliseconds())

	ka.mu.Lock()
	defer ka.mu.Unlock()

	ka.stats.TotalQueries++

	// Update latency prediction
	if ka.config.EnableLatencyPrediction {
		result := ka.latencyFilter.ProcessIfEnabled(config.FeatureKalmanLatency, latencyMs)
		if result.WasFiltered {
			ka.stats.LatencyPredictions++
		}
		ka.stats.AverageLatencyMs = ka.latencyFilter.State()
		ka.stats.PredictedLatencyMs = ka.latencyFilter.Predict(10)
	}

	// Enhance results
	enhancedResults := make([]SearchResult, 0, len(response.Results))

	for _, result := range response.Results {
		enhanced := ka.enhanceResult(result, query)
		enhancedResults = append(enhancedResults, enhanced)
	}

	// Apply ranking stability
	if ka.config.EnableRankingStability {
		enhancedResults = ka.applyRankingStability(enhancedResults, query)
	}

	// Re-sort by enhanced score
	ka.sortByScore(enhancedResults)

	// Record this ranking for future stability
	ka.recordRanking(query, enhancedResults)

	response.Results = enhancedResults
	return response, nil
}

// enhanceResult applies Kalman smoothing to a single result.
func (ka *KalmanSearchAdapter) enhanceResult(result SearchResult, query string) SearchResult {
	if !ka.config.EnableScoreSmoothing {
		return result
	}

	// Get or create filter for this document
	docFilter, exists := ka.docFilters[result.ID]
	if !exists {
		docFilter = filter.NewKalman(ka.config.SimilarityConfig)
		ka.docFilters[result.ID] = docFilter
	}

	// Smooth the similarity score
	rawScore := result.Score
	filtered := docFilter.ProcessIfEnabled(config.FeatureKalmanSimilarity, rawScore, 1.0)

	if filtered.WasFiltered {
		result.Score = filtered.Filtered
		ka.stats.ScoresSmoothed++
	}

	return result
}

// applyRankingStability boosts documents that have been consistently ranked high.
func (ka *KalmanSearchAdapter) applyRankingStability(results []SearchResult, query string) []SearchResult {
	if len(ka.recentRankings) == 0 {
		return results
	}

	// Count how often each doc appeared in top positions
	docAppearances := make(map[string]int)
	docPositions := make(map[string][]int)

	for _, ranking := range ka.recentRankings {
		for pos, docID := range ranking.TopDocs {
			docAppearances[docID]++
			docPositions[docID] = append(docPositions[docID], pos)
		}
	}

	// Apply stability boost
	for i, result := range results {
		appearances := docAppearances[result.ID]
		if appearances >= 2 { // Appeared in at least 2 recent queries
			// Calculate average position
			positions := docPositions[result.ID]
			avgPos := float64(0)
			for _, p := range positions {
				avgPos += float64(p)
			}
			avgPos /= float64(len(positions))

			// Boost more for consistently high-ranked docs
			if avgPos < 5 { // Top 5 average
				results[i].Score *= ka.config.StabilityBoost
				ka.stats.StabilityApplied++
			}
		}
	}

	return results
}

// recordRanking stores the current ranking for future stability checks.
func (ka *KalmanSearchAdapter) recordRanking(query string, results []SearchResult) {
	topDocs := make([]string, 0, min(10, len(results)))
	for i := 0; i < len(results) && i < 10; i++ {
		topDocs = append(topDocs, results[i].ID)
	}

	ranking := queryRanking{
		Query:     query,
		Timestamp: time.Now(),
		TopDocs:   topDocs,
	}

	ka.recentRankings = append(ka.recentRankings, ranking)

	// Trim to window size
	if len(ka.recentRankings) > ka.config.StabilityWindow {
		ka.recentRankings = ka.recentRankings[1:]
	}
}

// sortByScore sorts results by score (highest first).
func (ka *KalmanSearchAdapter) sortByScore(results []SearchResult) {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

// GetPredictedLatency returns the predicted latency for the next query.
func (ka *KalmanSearchAdapter) GetPredictedLatency(stepsAhead int) float64 {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	return ka.latencyFilter.Predict(stepsAhead)
}

// GetLatencyTrend returns the current latency velocity (positive = getting slower).
func (ka *KalmanSearchAdapter) GetLatencyTrend() float64 {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	return ka.latencyFilter.Velocity()
}

// GetDocumentRelevanceTrend returns whether a document is becoming more or less relevant.
//
// Returns:
//   - positive velocity: document is becoming more relevant (appearing higher)
//   - negative velocity: document is becoming less relevant (appearing lower)
//   - zero: stable relevance
func (ka *KalmanSearchAdapter) GetDocumentRelevanceTrend(docID string) float64 {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	if docFilter, exists := ka.docFilters[docID]; exists {
		return docFilter.Velocity()
	}
	return 0
}

// GetRisingDocuments returns documents whose relevance is increasing.
func (ka *KalmanSearchAdapter) GetRisingDocuments(minVelocity float64) []string {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	var rising []string
	for docID, docFilter := range ka.docFilters {
		if docFilter.Velocity() >= minVelocity {
			rising = append(rising, docID)
		}
	}
	return rising
}

// GetFallingDocuments returns documents whose relevance is decreasing.
func (ka *KalmanSearchAdapter) GetFallingDocuments(maxVelocity float64) []string {
	ka.mu.RLock()
	defer ka.mu.RUnlock()

	var falling []string
	for docID, docFilter := range ka.docFilters {
		if docFilter.Velocity() <= maxVelocity {
			falling = append(falling, docID)
		}
	}
	return falling
}

// GetStats returns adapter statistics.
func (ka *KalmanSearchAdapter) GetStats() SearchAdapterStats {
	ka.mu.RLock()
	defer ka.mu.RUnlock()
	return ka.stats
}

// GetService returns the underlying search service.
func (ka *KalmanSearchAdapter) GetService() *Service {
	return ka.service
}

// Reset clears all cached data and filters.
func (ka *KalmanSearchAdapter) Reset() {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	ka.docFilters = make(map[string]*filter.Kalman)
	ka.recentRankings = ka.recentRankings[:0]
	ka.latencyFilter = filter.NewKalmanVelocity(filter.DefaultVelocityConfig())
	ka.stats = SearchAdapterStats{}
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
