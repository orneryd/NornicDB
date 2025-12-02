// Package eval provides an evaluation harness for testing and validating
// NornicDB's search quality, ranking accuracy, and performance.
//
// The eval harness allows you to:
//   - Define test cases with queries and expected results
//   - Run evaluations and compute standard IR metrics
//   - Compare before/after changes to verify improvements
//   - Track performance over time
//
// Metrics computed:
//   - Precision@K: What fraction of top-K results are relevant?
//   - Recall@K: What fraction of all relevant docs appear in top-K?
//   - MRR (Mean Reciprocal Rank): Where does the first relevant result appear?
//   - NDCG (Normalized Discounted Cumulative Gain): Ranking quality
//   - Diversity: How different are the results from each other?
//
// Example usage:
//
//	harness := eval.NewHarness(searchService)
//	harness.AddTestCase(eval.TestCase{
//	    Name:     "ML concepts",
//	    Query:    "machine learning algorithms",
//	    Expected: []string{"node-1", "node-2", "node-3"},
//	})
//
//	results, err := harness.Run(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	fmt.Printf("Precision@10: %.2f\n", results.Precision10)
//	fmt.Printf("MRR: %.2f\n", results.MRR)
//
// ELI12 (Explain Like I'm 12):
//
// Think of it like grading a test:
//   - You have questions (search queries)
//   - You have answer keys (expected results)
//   - The harness checks how well the search did
//   - It gives you a score so you know if changes help or hurt
package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
)

// TestCase defines a single evaluation test case.
type TestCase struct {
	// Name is a human-readable identifier for this test
	Name string `json:"name"`

	// Query is the search query text
	Query string `json:"query"`

	// Embedding is optional pre-computed query embedding
	// If nil, the harness will use text-only search
	Embedding []float32 `json:"embedding,omitempty"`

	// Expected is the list of node IDs that should be returned
	// Order matters for ranking metrics (first = most relevant)
	Expected []string `json:"expected"`

	// RelevanceGrades allows graded relevance (0-3 scale)
	// If nil, binary relevance is assumed (in Expected = relevant)
	// Map of nodeID -> grade (0=not relevant, 1=marginal, 2=relevant, 3=highly relevant)
	RelevanceGrades map[string]int `json:"relevance_grades,omitempty"`

	// Tags for grouping and filtering test cases
	Tags []string `json:"tags,omitempty"`

	// Options overrides default search options for this test
	Options *search.SearchOptions `json:"options,omitempty"`
}

// TestSuite is a collection of test cases.
type TestSuite struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Version     string     `json:"version"`
	Created     time.Time  `json:"created"`
	TestCases   []TestCase `json:"test_cases"`
}

// Metrics contains all computed evaluation metrics.
type Metrics struct {
	// Precision at various K values
	Precision1  float64 `json:"precision@1"`
	Precision5  float64 `json:"precision@5"`
	Precision10 float64 `json:"precision@10"`

	// Recall at various K values
	Recall5  float64 `json:"recall@5"`
	Recall10 float64 `json:"recall@10"`
	Recall50 float64 `json:"recall@50"`

	// Mean Reciprocal Rank - where does first relevant result appear?
	MRR float64 `json:"mrr"`

	// Normalized Discounted Cumulative Gain
	NDCG5  float64 `json:"ndcg@5"`
	NDCG10 float64 `json:"ndcg@10"`

	// Mean Average Precision
	MAP float64 `json:"map"`

	// Diversity - how different are results from each other? (0-1)
	Diversity float64 `json:"diversity"`

	// Hit Rate - fraction of queries with at least one relevant result
	HitRate float64 `json:"hit_rate"`
}

// TestResult contains results for a single test case.
type TestResult struct {
	TestCase     TestCase      `json:"test_case"`
	Metrics      Metrics       `json:"metrics"`
	Returned     []string      `json:"returned"`
	Duration     time.Duration `json:"duration"`
	Error        string        `json:"error,omitempty"`
	SearchMethod string        `json:"search_method"`
}

// EvalResult contains the complete evaluation results.
type EvalResult struct {
	// Suite info
	SuiteName string        `json:"suite_name"`
	Timestamp time.Time     `json:"timestamp"`
	Duration  time.Duration `json:"duration"`

	// Aggregate metrics (averaged across all test cases)
	Aggregate Metrics `json:"aggregate"`

	// Per-test results
	Results []TestResult `json:"results"`

	// Summary statistics
	TotalTests  int `json:"total_tests"`
	PassedTests int `json:"passed_tests"`
	FailedTests int `json:"failed_tests"`

	// Thresholds used for pass/fail
	Thresholds Thresholds `json:"thresholds"`
}

// Thresholds define minimum acceptable metric values.
type Thresholds struct {
	Precision10 float64 `json:"precision@10"`
	Recall10    float64 `json:"recall@10"`
	MRR         float64 `json:"mrr"`
	NDCG10      float64 `json:"ndcg@10"`
	HitRate     float64 `json:"hit_rate"`
}

// DefaultThresholds returns sensible default thresholds.
func DefaultThresholds() Thresholds {
	return Thresholds{
		Precision10: 0.5, // At least 50% of top-10 should be relevant
		Recall10:    0.3, // At least 30% of relevant docs in top-10
		MRR:         0.5, // First relevant result in top 2 on average
		NDCG10:      0.5, // Reasonable ranking quality
		HitRate:     0.8, // 80% of queries should have at least one hit
	}
}

// Harness is the main evaluation harness.
type Harness struct {
	searchService *search.Service
	testCases     []TestCase
	thresholds    Thresholds
	mu            sync.RWMutex
}

// NewHarness creates a new evaluation harness.
func NewHarness(searchService *search.Service) *Harness {
	return &Harness{
		searchService: searchService,
		testCases:     make([]TestCase, 0),
		thresholds:    DefaultThresholds(),
	}
}

// SetThresholds sets the pass/fail thresholds.
func (h *Harness) SetThresholds(t Thresholds) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.thresholds = t
}

// AddTestCase adds a single test case.
func (h *Harness) AddTestCase(tc TestCase) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.testCases = append(h.testCases, tc)
}

// AddTestCases adds multiple test cases.
func (h *Harness) AddTestCases(cases []TestCase) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.testCases = append(h.testCases, cases...)
}

// LoadSuite loads a test suite from a JSON file.
func (h *Harness) LoadSuite(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read suite file: %w", err)
	}

	var suite TestSuite
	if err := json.Unmarshal(data, &suite); err != nil {
		return fmt.Errorf("failed to parse suite JSON: %w", err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.testCases = append(h.testCases, suite.TestCases...)

	return nil
}

// Run executes the evaluation and returns results.
func (h *Harness) Run(ctx context.Context) (*EvalResult, error) {
	h.mu.RLock()
	cases := make([]TestCase, len(h.testCases))
	copy(cases, h.testCases)
	thresholds := h.thresholds
	h.mu.RUnlock()

	if len(cases) == 0 {
		return nil, fmt.Errorf("no test cases defined")
	}

	startTime := time.Now()
	results := make([]TestResult, 0, len(cases))

	// Run each test case
	for _, tc := range cases {
		result := h.runTestCase(ctx, tc)
		results = append(results, result)
	}

	// Compute aggregate metrics
	aggregate := h.computeAggregate(results)

	// Count passed/failed
	passed, failed := h.countPassFail(results, thresholds)

	return &EvalResult{
		SuiteName:   "default",
		Timestamp:   startTime,
		Duration:    time.Since(startTime),
		Aggregate:   aggregate,
		Results:     results,
		TotalTests:  len(results),
		PassedTests: passed,
		FailedTests: failed,
		Thresholds:  thresholds,
	}, nil
}

// runTestCase executes a single test case.
func (h *Harness) runTestCase(ctx context.Context, tc TestCase) TestResult {
	start := time.Now()

	opts := tc.Options
	if opts == nil {
		opts = search.DefaultSearchOptions()
		opts.Limit = 50 // Get enough for recall calculation
	}

	response, err := h.searchService.Search(ctx, tc.Query, tc.Embedding, opts)
	if err != nil {
		return TestResult{
			TestCase: tc,
			Error:    err.Error(),
			Duration: time.Since(start),
		}
	}

	// Extract returned IDs
	returned := make([]string, len(response.Results))
	for i, r := range response.Results {
		returned[i] = r.ID
	}

	// Compute metrics
	metrics := h.computeMetrics(tc, returned)

	return TestResult{
		TestCase:     tc,
		Metrics:      metrics,
		Returned:     returned,
		Duration:     time.Since(start),
		SearchMethod: response.SearchMethod,
	}
}

// computeMetrics calculates all metrics for a test case.
func (h *Harness) computeMetrics(tc TestCase, returned []string) Metrics {
	expected := make(map[string]bool)
	for _, id := range tc.Expected {
		expected[id] = true
	}

	// Get relevance grades (default to binary)
	grades := tc.RelevanceGrades
	if grades == nil {
		grades = make(map[string]int)
		for _, id := range tc.Expected {
			grades[id] = 1 // Binary: relevant = 1
		}
	}

	m := Metrics{}

	// Precision@K
	m.Precision1 = precision(returned, expected, 1)
	m.Precision5 = precision(returned, expected, 5)
	m.Precision10 = precision(returned, expected, 10)

	// Recall@K
	m.Recall5 = recall(returned, expected, 5)
	m.Recall10 = recall(returned, expected, 10)
	m.Recall50 = recall(returned, expected, 50)

	// MRR
	m.MRR = mrr(returned, expected)

	// NDCG
	m.NDCG5 = ndcg(returned, grades, 5)
	m.NDCG10 = ndcg(returned, grades, 10)

	// MAP
	m.MAP = averagePrecision(returned, expected)

	// Hit Rate (1 if any relevant in results, 0 otherwise)
	m.HitRate = hitRate(returned, expected)

	// Diversity (placeholder - would need embeddings)
	m.Diversity = 0.0

	return m
}

// computeAggregate averages metrics across all results.
func (h *Harness) computeAggregate(results []TestResult) Metrics {
	if len(results) == 0 {
		return Metrics{}
	}

	var agg Metrics
	validCount := 0

	for _, r := range results {
		if r.Error != "" {
			continue
		}
		validCount++
		agg.Precision1 += r.Metrics.Precision1
		agg.Precision5 += r.Metrics.Precision5
		agg.Precision10 += r.Metrics.Precision10
		agg.Recall5 += r.Metrics.Recall5
		agg.Recall10 += r.Metrics.Recall10
		agg.Recall50 += r.Metrics.Recall50
		agg.MRR += r.Metrics.MRR
		agg.NDCG5 += r.Metrics.NDCG5
		agg.NDCG10 += r.Metrics.NDCG10
		agg.MAP += r.Metrics.MAP
		agg.HitRate += r.Metrics.HitRate
		agg.Diversity += r.Metrics.Diversity
	}

	if validCount > 0 {
		n := float64(validCount)
		agg.Precision1 /= n
		agg.Precision5 /= n
		agg.Precision10 /= n
		agg.Recall5 /= n
		agg.Recall10 /= n
		agg.Recall50 /= n
		agg.MRR /= n
		agg.NDCG5 /= n
		agg.NDCG10 /= n
		agg.MAP /= n
		agg.HitRate /= n
		agg.Diversity /= n
	}

	return agg
}

// countPassFail counts tests that meet thresholds.
func (h *Harness) countPassFail(results []TestResult, t Thresholds) (passed, failed int) {
	for _, r := range results {
		if r.Error != "" {
			failed++
			continue
		}
		if r.Metrics.Precision10 >= t.Precision10 &&
			r.Metrics.MRR >= t.MRR &&
			r.Metrics.HitRate >= t.HitRate {
			passed++
		} else {
			failed++
		}
	}
	return
}

// === Metric calculation functions ===

// precision calculates Precision@K.
// Precision = (relevant docs in top K) / K
func precision(returned []string, expected map[string]bool, k int) float64 {
	if k <= 0 || len(returned) == 0 {
		return 0.0
	}

	limit := min(k, len(returned))
	relevant := 0
	for i := 0; i < limit; i++ {
		if expected[returned[i]] {
			relevant++
		}
	}

	return float64(relevant) / float64(k)
}

// recall calculates Recall@K.
// Recall = (relevant docs in top K) / (total relevant docs)
func recall(returned []string, expected map[string]bool, k int) float64 {
	if len(expected) == 0 {
		return 0.0
	}

	limit := min(k, len(returned))
	relevant := 0
	for i := 0; i < limit; i++ {
		if expected[returned[i]] {
			relevant++
		}
	}

	return float64(relevant) / float64(len(expected))
}

// mrr calculates Mean Reciprocal Rank.
// MRR = 1 / (rank of first relevant result)
func mrr(returned []string, expected map[string]bool) float64 {
	for i, id := range returned {
		if expected[id] {
			return 1.0 / float64(i+1)
		}
	}
	return 0.0
}

// ndcg calculates Normalized Discounted Cumulative Gain.
// NDCG@K = DCG@K / IDCG@K
func ndcg(returned []string, grades map[string]int, k int) float64 {
	dcg := dcg(returned, grades, k)
	idcg := idealDCG(grades, k)

	if idcg == 0 {
		return 0.0
	}
	return dcg / idcg
}

// dcg calculates Discounted Cumulative Gain.
func dcg(returned []string, grades map[string]int, k int) float64 {
	limit := min(k, len(returned))
	sum := 0.0

	for i := 0; i < limit; i++ {
		grade := grades[returned[i]] // 0 if not in grades
		// DCG formula: (2^grade - 1) / log2(i + 2)
		sum += (math.Pow(2, float64(grade)) - 1) / math.Log2(float64(i+2))
	}

	return sum
}

// idealDCG calculates the ideal DCG (perfect ranking).
func idealDCG(grades map[string]int, k int) float64 {
	// Sort grades descending
	sortedGrades := make([]int, 0, len(grades))
	for _, g := range grades {
		sortedGrades = append(sortedGrades, g)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sortedGrades)))

	limit := min(k, len(sortedGrades))
	sum := 0.0

	for i := 0; i < limit; i++ {
		sum += (math.Pow(2, float64(sortedGrades[i])) - 1) / math.Log2(float64(i+2))
	}

	return sum
}

// averagePrecision calculates Average Precision for a single query.
func averagePrecision(returned []string, expected map[string]bool) float64 {
	if len(expected) == 0 {
		return 0.0
	}

	sum := 0.0
	relevantSeen := 0

	for i, id := range returned {
		if expected[id] {
			relevantSeen++
			// Precision at this point
			sum += float64(relevantSeen) / float64(i+1)
		}
	}

	return sum / float64(len(expected))
}

// hitRate returns 1 if any expected doc is in returned, 0 otherwise.
func hitRate(returned []string, expected map[string]bool) float64 {
	for _, id := range returned {
		if expected[id] {
			return 1.0
		}
	}
	return 0.0
}
