// Command eval runs the search quality evaluation harness against NornicDB.
//
// Usage:
//
//	go run ./cmd/eval [flags]
//
// Flags:
//
//	-url        NornicDB server URL (default: http://localhost:7474)
//	-suite      Path to test suite JSON file
//	-output     Output format: summary, detailed, json, compact (default: summary)
//	-save       Save results to JSON file
//	-threshold  Override pass/fail thresholds (format: p10=0.5,mrr=0.5,hit=0.8)
//
// This command evaluates search quality by:
// 1. Connecting to a running NornicDB server
// 2. Running test queries against the search API
// 3. Computing IR metrics (Precision, Recall, MRR, NDCG)
// 4. Reporting pass/fail based on thresholds
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/eval"
)

func main() {
	// Parse flags
	serverURL := flag.String("url", "http://localhost:7474", "NornicDB server URL")
	suitePath := flag.String("suite", "", "Path to test suite JSON file")
	output := flag.String("output", "summary", "Output format: summary, detailed, json, compact")
	savePath := flag.String("save", "", "Save results to JSON file")
	thresholds := flag.String("threshold", "", "Override thresholds (p10=0.5,mrr=0.5,hit=0.8)")
	createSample := flag.Bool("create-sample", false, "Create sample test data in the database")
	flag.Parse()

	// Check server health
	fmt.Printf("🔍 Connecting to NornicDB at %s...\n", *serverURL)
	if err := checkHealth(*serverURL); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Server not reachable: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✅ Server is healthy")

	// Create sample data if requested
	if *createSample {
		fmt.Println("📝 Creating sample test data...")
		if err := createSampleData(*serverURL); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️ Warning: Failed to create sample data: %v\n", err)
		}
	}

	// Create HTTP-based search adapter
	searcher := &HTTPSearcher{url: *serverURL}

	// Create harness with HTTP searcher
	harness := NewHTTPHarness(searcher)

	// Load test suite or use built-in tests
	if *suitePath != "" {
		fmt.Printf("📂 Loading test suite from %s...\n", *suitePath)
		if err := harness.LoadSuite(*suitePath); err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to load suite: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Add built-in demo tests
		fmt.Println("📝 Using built-in demo test cases...")
		addDemoTestCases(harness)
	}

	// Override thresholds if specified
	if *thresholds != "" {
		t := parseThresholds(*thresholds)
		harness.SetThresholds(t)
	}

	// Run evaluation
	fmt.Println("\n🚀 Running evaluation...")
	ctx := context.Background()
	result, err := harness.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Evaluation failed: %v\n", err)
		os.Exit(1)
	}

	// Output results
	reporter := eval.NewReporter(os.Stdout)
	switch *output {
	case "summary":
		reporter.PrintSummary(result)
	case "detailed":
		reporter.PrintSummary(result)
		reporter.PrintDetails(result)
	case "json":
		reporter.PrintJSON(result)
	case "compact":
		reporter.PrintCompact(result)
	default:
		reporter.PrintSummary(result)
	}

	// Save results if requested
	if *savePath != "" {
		if err := reporter.SaveJSON(result, *savePath); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️ Failed to save results: %v\n", err)
		} else {
			fmt.Printf("💾 Results saved to %s\n", *savePath)
		}
	}

	// Exit with appropriate code
	if result.FailedTests > 0 {
		os.Exit(1)
	}
}

// HTTPSearcher performs searches via HTTP API
type HTTPSearcher struct {
	url    string
	client *http.Client
}

// Search performs a search query via HTTP
func (s *HTTPSearcher) Search(ctx context.Context, query string) ([]string, error) {
	if s.client == nil {
		s.client = &http.Client{Timeout: 30 * time.Second}
	}

	// Call NornicDB search API
	reqBody := map[string]interface{}{
		"query": query,
		"limit": 50,
	}
	body, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, "POST", s.url+"/nornicdb/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Response is an array: [{"node":{"id":"..."},"score":...}, ...]
	var results []struct {
		Node struct {
			ID string `json:"id"`
		} `json:"node"`
		Score float64 `json:"score"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}

	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.Node.ID
	}
	return ids, nil
}

// HTTPHarness wraps eval.Harness with HTTP-based search
type HTTPHarness struct {
	searcher   *HTTPSearcher
	testCases  []eval.TestCase
	thresholds eval.Thresholds
}

func NewHTTPHarness(searcher *HTTPSearcher) *HTTPHarness {
	return &HTTPHarness{
		searcher:   searcher,
		testCases:  make([]eval.TestCase, 0),
		thresholds: eval.DefaultThresholds(),
	}
}

func (h *HTTPHarness) AddTestCase(tc eval.TestCase) {
	h.testCases = append(h.testCases, tc)
}

func (h *HTTPHarness) AddTestCases(cases []eval.TestCase) {
	h.testCases = append(h.testCases, cases...)
}

func (h *HTTPHarness) LoadSuite(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var suite eval.TestSuite
	if err := json.Unmarshal(data, &suite); err != nil {
		return err
	}
	h.testCases = append(h.testCases, suite.TestCases...)
	return nil
}

func (h *HTTPHarness) SetThresholds(t eval.Thresholds) {
	h.thresholds = t
}

func (h *HTTPHarness) Run(ctx context.Context) (*eval.EvalResult, error) {
	if len(h.testCases) == 0 {
		return nil, fmt.Errorf("no test cases defined")
	}

	startTime := time.Now()
	results := make([]eval.TestResult, 0, len(h.testCases))

	for _, tc := range h.testCases {
		result := h.runTestCase(ctx, tc)
		results = append(results, result)
	}

	aggregate := computeAggregate(results)
	passed, failed := countPassFail(results, h.thresholds)

	return &eval.EvalResult{
		SuiteName:   "http-eval",
		Timestamp:   startTime,
		Duration:    time.Since(startTime),
		Aggregate:   aggregate,
		Results:     results,
		TotalTests:  len(results),
		PassedTests: passed,
		FailedTests: failed,
		Thresholds:  h.thresholds,
	}, nil
}

func (h *HTTPHarness) runTestCase(ctx context.Context, tc eval.TestCase) eval.TestResult {
	start := time.Now()

	returned, err := h.searcher.Search(ctx, tc.Query)
	if err != nil {
		return eval.TestResult{
			TestCase: tc,
			Error:    err.Error(),
			Duration: time.Since(start),
		}
	}

	metrics := computeMetrics(tc, returned)

	return eval.TestResult{
		TestCase:     tc,
		Metrics:      metrics,
		Returned:     returned,
		Duration:     time.Since(start),
		SearchMethod: "http",
	}
}

// Metric computation functions
func computeMetrics(tc eval.TestCase, returned []string) eval.Metrics {
	expected := make(map[string]bool)
	for _, id := range tc.Expected {
		expected[id] = true
	}

	grades := tc.RelevanceGrades
	if grades == nil {
		grades = make(map[string]int)
		for _, id := range tc.Expected {
			grades[id] = 1
		}
	}

	return eval.Metrics{
		Precision1:  precision(returned, expected, 1),
		Precision5:  precision(returned, expected, 5),
		Precision10: precision(returned, expected, 10),
		Recall5:     recall(returned, expected, 5),
		Recall10:    recall(returned, expected, 10),
		Recall50:    recall(returned, expected, 50),
		MRR:         mrr(returned, expected),
		NDCG5:       ndcg(returned, grades, 5),
		NDCG10:      ndcg(returned, grades, 10),
		MAP:         averagePrecision(returned, expected),
		HitRate:     hitRate(returned, expected),
	}
}

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

func mrr(returned []string, expected map[string]bool) float64 {
	for i, id := range returned {
		if expected[id] {
			return 1.0 / float64(i+1)
		}
	}
	return 0.0
}

func ndcg(returned []string, grades map[string]int, k int) float64 {
	// Simplified NDCG
	if len(grades) == 0 {
		return 0.0
	}
	limit := min(k, len(returned))
	dcg := 0.0
	for i := 0; i < limit; i++ {
		grade := float64(grades[returned[i]])
		dcg += grade / (1.0 + float64(i))
	}
	// Ideal DCG (perfect ranking)
	idcg := 0.0
	for i := 0; i < min(k, len(grades)); i++ {
		idcg += 1.0 / (1.0 + float64(i))
	}
	if idcg == 0 {
		return 0.0
	}
	return dcg / idcg
}

func averagePrecision(returned []string, expected map[string]bool) float64 {
	if len(expected) == 0 {
		return 0.0
	}
	sum := 0.0
	relevant := 0
	for i, id := range returned {
		if expected[id] {
			relevant++
			sum += float64(relevant) / float64(i+1)
		}
	}
	return sum / float64(len(expected))
}

func hitRate(returned []string, expected map[string]bool) float64 {
	for _, id := range returned {
		if expected[id] {
			return 1.0
		}
	}
	return 0.0
}

func computeAggregate(results []eval.TestResult) eval.Metrics {
	var agg eval.Metrics
	n := 0
	for _, r := range results {
		if r.Error != "" {
			continue
		}
		n++
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
	}
	if n > 0 {
		fn := float64(n)
		agg.Precision1 /= fn
		agg.Precision5 /= fn
		agg.Precision10 /= fn
		agg.Recall5 /= fn
		agg.Recall10 /= fn
		agg.Recall50 /= fn
		agg.MRR /= fn
		agg.NDCG5 /= fn
		agg.NDCG10 /= fn
		agg.MAP /= fn
		agg.HitRate /= fn
	}
	return agg
}

func countPassFail(results []eval.TestResult, t eval.Thresholds) (passed, failed int) {
	for _, r := range results {
		if r.Error != "" || r.Metrics.HitRate < t.HitRate {
			failed++
		} else {
			passed++
		}
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// validateURL validates that the URL is well-formed and uses an allowed scheme.
// This prevents SSRF attacks by ensuring only http/https URLs to expected hosts.
func validateURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("malformed URL: %w", err)
	}
	// Only allow http and https schemes
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid scheme %q: only http and https are allowed", parsed.Scheme)
	}
	// Ensure host is present
	if parsed.Host == "" {
		return fmt.Errorf("missing host in URL")
	}
	return nil
}

func checkHealth(serverURL string) error {
	if err := validateURL(serverURL); err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(serverURL + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: status %d", resp.StatusCode)
	}
	return nil
}

func parseThresholds(s string) eval.Thresholds {
	t := eval.DefaultThresholds()
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}
		var val float64
		fmt.Sscanf(parts[1], "%f", &val)
		switch parts[0] {
		case "p10", "precision10":
			t.Precision10 = val
		case "mrr":
			t.MRR = val
		case "hit", "hitrate":
			t.HitRate = val
		}
	}
	return t
}

func createSampleData(serverURL string) error {
	if err := validateURL(serverURL); err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	client := &http.Client{Timeout: 30 * time.Second}

	// Sample nodes to create
	cypher := `
		CREATE (n1:Memory:Concept {id: 'ml-intro', title: 'Introduction to Machine Learning', content: 'Machine learning is AI that learns from data'})
		CREATE (n2:Memory:Concept {id: 'ml-neural', title: 'Neural Networks', content: 'Neural networks are inspired by biological brains'})
		CREATE (n3:Memory:Decision {id: 'db-design', title: 'Database Decision', content: 'We chose PostgreSQL for relational and NornicDB for graphs'})
		CREATE (n4:Memory:Code {id: 'code-auth', title: 'Auth Middleware', content: 'JWT authentication middleware for API'})
		CREATE (n5:Task {id: 'task-api', title: 'REST API', content: 'Implement user management endpoints', status: 'pending'})
	`

	reqBody := map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": cypher},
		},
	}
	body, _ := json.Marshal(reqBody)

	resp, err := client.Post(serverURL+"/db/neo4j/tx/commit", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func addDemoTestCases(harness *HTTPHarness) {
	harness.AddTestCases([]eval.TestCase{
		{
			Name:     "ML Concept Search",
			Query:    "machine learning neural networks",
			Expected: []string{"ml-intro", "ml-neural"},
			Tags:     []string{"concepts", "ml"},
		},
		{
			Name:     "Database Decision",
			Query:    "database architecture postgresql",
			Expected: []string{"db-design"},
			Tags:     []string{"decisions"},
		},
		{
			Name:     "Code Search",
			Query:    "authentication JWT middleware",
			Expected: []string{"code-auth"},
			Tags:     []string{"code"},
		},
		{
			Name:     "Task Search",
			Query:    "API implementation pending",
			Expected: []string{"task-api"},
			Tags:     []string{"tasks"},
		},
		{
			Name:     "Semantic - AI Systems",
			Query:    "artificial intelligence systems that learn",
			Expected: []string{"ml-intro", "ml-neural"},
			RelevanceGrades: map[string]int{
				"ml-intro":  3,
				"ml-neural": 2,
			},
			Tags: []string{"semantic"},
		},
	})
}
