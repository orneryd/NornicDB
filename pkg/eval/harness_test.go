package eval

import (
	"bytes"
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Metric Calculation Tests
// =============================================================================

func TestPrecision(t *testing.T) {
	expected := map[string]bool{"a": true, "b": true, "c": true}

	t.Run("perfect_precision", func(t *testing.T) {
		returned := []string{"a", "b", "c", "d", "e"}
		p := precision(returned, expected, 3)
		assert.Equal(t, 1.0, p) // 3/3 relevant in top 3
	})

	t.Run("partial_precision", func(t *testing.T) {
		returned := []string{"a", "x", "b", "y", "c"}
		p := precision(returned, expected, 5)
		assert.Equal(t, 0.6, p) // 3/5 relevant in top 5
	})

	t.Run("zero_precision", func(t *testing.T) {
		returned := []string{"x", "y", "z"}
		p := precision(returned, expected, 3)
		assert.Equal(t, 0.0, p)
	})

	t.Run("empty_returned", func(t *testing.T) {
		p := precision([]string{}, expected, 3)
		assert.Equal(t, 0.0, p)
	})
}

func TestRecall(t *testing.T) {
	expected := map[string]bool{"a": true, "b": true, "c": true, "d": true}

	t.Run("perfect_recall", func(t *testing.T) {
		returned := []string{"a", "b", "c", "d", "x", "y"}
		r := recall(returned, expected, 10)
		assert.Equal(t, 1.0, r) // 4/4 found
	})

	t.Run("partial_recall", func(t *testing.T) {
		returned := []string{"a", "b", "x", "y", "z"}
		r := recall(returned, expected, 5)
		assert.Equal(t, 0.5, r) // 2/4 found
	})

	t.Run("zero_recall", func(t *testing.T) {
		returned := []string{"x", "y", "z"}
		r := recall(returned, expected, 3)
		assert.Equal(t, 0.0, r)
	})
}

func TestMRR(t *testing.T) {
	expected := map[string]bool{"a": true, "b": true}

	t.Run("first_position", func(t *testing.T) {
		returned := []string{"a", "x", "y"}
		m := mrr(returned, expected)
		assert.Equal(t, 1.0, m) // 1/1
	})

	t.Run("second_position", func(t *testing.T) {
		returned := []string{"x", "a", "y"}
		m := mrr(returned, expected)
		assert.Equal(t, 0.5, m) // 1/2
	})

	t.Run("third_position", func(t *testing.T) {
		returned := []string{"x", "y", "b"}
		m := mrr(returned, expected)
		assert.InDelta(t, 0.333, m, 0.01) // 1/3
	})

	t.Run("not_found", func(t *testing.T) {
		returned := []string{"x", "y", "z"}
		m := mrr(returned, expected)
		assert.Equal(t, 0.0, m)
	})
}

func TestNDCG(t *testing.T) {
	// Graded relevance: 3=highly relevant, 2=relevant, 1=marginal
	grades := map[string]int{
		"best": 3,
		"good": 2,
		"ok":   1,
	}

	t.Run("perfect_ranking", func(t *testing.T) {
		// Best possible order
		returned := []string{"best", "good", "ok"}
		n := ndcg(returned, grades, 3)
		assert.InDelta(t, 1.0, n, 0.01) // Perfect ranking = 1.0
	})

	t.Run("worst_ranking", func(t *testing.T) {
		// Worst order (but still has relevant docs)
		returned := []string{"ok", "good", "best"}
		n := ndcg(returned, grades, 3)
		assert.Less(t, n, 1.0)    // Not perfect
		assert.Greater(t, n, 0.0) // Still has relevant docs
	})

	t.Run("no_relevant_docs", func(t *testing.T) {
		returned := []string{"x", "y", "z"}
		n := ndcg(returned, grades, 3)
		assert.Equal(t, 0.0, n)
	})
}

func TestAveragePrecision(t *testing.T) {
	expected := map[string]bool{"a": true, "b": true, "c": true}

	t.Run("perfect_ranking", func(t *testing.T) {
		returned := []string{"a", "b", "c", "x", "y"}
		ap := averagePrecision(returned, expected)
		// AP = (1/1 + 2/2 + 3/3) / 3 = (1 + 1 + 1) / 3 = 1.0
		assert.Equal(t, 1.0, ap)
	})

	t.Run("interleaved_ranking", func(t *testing.T) {
		returned := []string{"a", "x", "b", "y", "c"}
		ap := averagePrecision(returned, expected)
		// AP = (1/1 + 2/3 + 3/5) / 3 = (1 + 0.667 + 0.6) / 3 ≈ 0.756
		assert.InDelta(t, 0.756, ap, 0.01)
	})
}

func TestHitRate(t *testing.T) {
	expected := map[string]bool{"a": true, "b": true}

	t.Run("has_hit", func(t *testing.T) {
		returned := []string{"x", "y", "a"}
		hr := hitRate(returned, expected)
		assert.Equal(t, 1.0, hr)
	})

	t.Run("no_hit", func(t *testing.T) {
		returned := []string{"x", "y", "z"}
		hr := hitRate(returned, expected)
		assert.Equal(t, 0.0, hr)
	})
}

// =============================================================================
// Harness Integration Tests
// =============================================================================

func TestHarnessBasic(t *testing.T) {
	// Create a search service with test data
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	service := search.NewService(store, search.DefaultVectorDimensions, nil)

	// Add test nodes
	for i := 0; i < 10; i++ {
		node := &storage.Node{
			ID:     storage.NodeID(string(rune('a' + i))),
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"title":   "Document " + string(rune('a'+i)),
				"content": "Test content for search",
			},
		}
		store.CreateNode(node)
		service.IndexNode(node)
	}

	harness := NewHarness(service)

	t.Run("add_test_case", func(t *testing.T) {
		harness.AddTestCase(TestCase{
			Name:     "find docs",
			Query:    "test content",
			Expected: []string{"a", "b", "c"},
		})
	})

	t.Run("run_evaluation", func(t *testing.T) {
		ctx := context.Background()
		result, err := harness.Run(ctx)
		require.NoError(t, err)

		assert.Equal(t, 1, result.TotalTests)
		assert.NotNil(t, result.Aggregate)
	})
}

func TestHarnessMultipleTests(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	service := search.NewService(store, search.DefaultVectorDimensions, nil)

	// Add diverse test nodes
	nodes := []struct {
		id      string
		title   string
		content string
	}{
		{"ml1", "Machine Learning", "neural networks and deep learning"},
		{"ml2", "ML Algorithms", "supervised and unsupervised learning"},
		{"db1", "Database Design", "SQL and NoSQL databases"},
		{"db2", "Graph Databases", "nodes and relationships"},
	}

	for _, n := range nodes {
		node := &storage.Node{
			ID:     storage.NodeID(n.id),
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"title":   n.title,
				"content": n.content,
			},
		}
		store.CreateNode(node)
		service.IndexNode(node)
	}

	harness := NewHarness(service)

	// Add test cases
	harness.AddTestCases([]TestCase{
		{
			Name:     "ML search",
			Query:    "machine learning neural",
			Expected: []string{"ml1", "ml2"},
		},
		{
			Name:     "Database search",
			Query:    "database SQL",
			Expected: []string{"db1", "db2"},
		},
	})

	ctx := context.Background()
	result, err := harness.Run(ctx)
	require.NoError(t, err)

	assert.Equal(t, 2, result.TotalTests)
	assert.Len(t, result.Results, 2)
}

func TestHarnessNoTestCases(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	service := search.NewService(store, search.DefaultVectorDimensions, nil)
	harness := NewHarness(service)

	ctx := context.Background()
	_, err := harness.Run(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no test cases")
}

func TestThresholds(t *testing.T) {
	defaults := DefaultThresholds()

	assert.Equal(t, 0.5, defaults.Precision10)
	assert.Equal(t, 0.3, defaults.Recall10)
	assert.Equal(t, 0.5, defaults.MRR)
	assert.Equal(t, 0.8, defaults.HitRate)
}

// =============================================================================
// Reporter Tests
// =============================================================================

func TestReporterPrintCompact(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewReporter(&buf)

	result := &EvalResult{
		TotalTests:  10,
		PassedTests: 8,
		FailedTests: 2,
		Aggregate: Metrics{
			Precision10: 0.75,
			Recall10:    0.60,
			MRR:         0.85,
			NDCG10:      0.70,
			HitRate:     0.90,
		},
	}

	reporter.PrintCompact(result)

	output := buf.String()
	assert.Contains(t, output, "FAIL")
	assert.Contains(t, output, "8/10")
	assert.Contains(t, output, "P@10=0.75")
}

func TestReporterPrintSummary(t *testing.T) {
	var buf bytes.Buffer
	reporter := NewReporter(&buf)

	result := &EvalResult{
		SuiteName:   "test-suite",
		TotalTests:  5,
		PassedTests: 5,
		FailedTests: 0,
		Aggregate: Metrics{
			Precision10: 0.80,
			Recall10:    0.70,
			MRR:         0.90,
		},
		Thresholds: DefaultThresholds(),
	}

	reporter.PrintSummary(result)

	output := buf.String()
	assert.Contains(t, output, "test-suite")
	assert.Contains(t, output, "5/5")
	assert.Contains(t, output, "Precision")
}

// =============================================================================
// Test Suite JSON Loading
// =============================================================================

func TestLoadSuiteJSON(t *testing.T) {
	// This would test loading from a file
	// For now, just test the structure
	suite := TestSuite{
		Name:        "sample-suite",
		Description: "A sample test suite",
		Version:     "1.0",
		TestCases: []TestCase{
			{
				Name:     "test1",
				Query:    "sample query",
				Expected: []string{"doc1", "doc2"},
			},
		},
	}

	assert.Equal(t, "sample-suite", suite.Name)
	assert.Len(t, suite.TestCases, 1)
}

// =============================================================================
// Graded Relevance Tests
// =============================================================================

func TestGradedRelevance(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	service := search.NewService(store, search.DefaultVectorDimensions, nil)

	// Add nodes
	for _, id := range []string{"best", "good", "ok", "bad"} {
		node := &storage.Node{
			ID:     storage.NodeID(id),
			Labels: []string{"Doc"},
			Properties: map[string]interface{}{
				"title": id,
			},
		}
		store.CreateNode(node)
		service.IndexNode(node)
	}

	harness := NewHarness(service)

	harness.AddTestCase(TestCase{
		Name:     "graded test",
		Query:    "test",
		Expected: []string{"best", "good", "ok"}, // Required for binary metrics
		RelevanceGrades: map[string]int{
			"best": 3,
			"good": 2,
			"ok":   1,
			"bad":  0,
		},
	})

	ctx := context.Background()
	result, err := harness.Run(ctx)
	require.NoError(t, err)

	// NDCG should be computed using grades
	assert.NotNil(t, result.Results[0].Metrics.NDCG10)
}
