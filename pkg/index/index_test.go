// Package index tests for vector and full-text indexing.
package index

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

func TestDefaultHNSWConfig(t *testing.T) {
	config := DefaultHNSWConfig(1024)

	if config.Dimensions != 1024 {
		t.Errorf("expected 1024 dimensions, got %d", config.Dimensions)
	}
	if config.M != 16 {
		t.Errorf("expected M=16, got %d", config.M)
	}
	if config.EfConstruction != 200 {
		t.Errorf("expected EfConstruction=200, got %d", config.EfConstruction)
	}
	if config.EfSearch != 100 {
		t.Errorf("expected EfSearch=100, got %d", config.EfSearch)
	}
}

func TestNewHNSW(t *testing.T) {
	config := DefaultHNSWConfig(128)
	index := NewHNSW(config)

	if index.dimensions != 128 {
		t.Errorf("expected 128 dimensions, got %d", index.dimensions)
	}
	if index.M != 16 {
		t.Errorf("expected M=16, got %d", index.M)
	}
	if index.vectors == nil {
		t.Error("vectors map should be initialized")
	}
}

func TestHNSWAdd(t *testing.T) {
	config := DefaultHNSWConfig(4)
	index := NewHNSW(config)

	t.Run("add valid vector", func(t *testing.T) {
		err := index.Add("vec-1", []float32{1.0, 0.0, 0.0, 0.0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		if len(index.vectors) != 1 {
			t.Error("vector should be stored")
		}
	})

	t.Run("add multiple vectors", func(t *testing.T) {
		err := index.Add("vec-2", []float32{0.0, 1.0, 0.0, 0.0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		if len(index.vectors) != 2 {
			t.Error("should have 2 vectors")
		}
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		err := index.Add("vec-bad", []float32{1.0, 0.0}) // Wrong dimensions
		if err != ErrDimensionMismatch {
			t.Errorf("expected ErrDimensionMismatch, got %v", err)
		}
	})

	t.Run("overwrite existing", func(t *testing.T) {
		err := index.Add("vec-1", []float32{0.5, 0.5, 0.0, 0.0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		// Should overwrite
		if index.vectors["vec-1"][0] != 0.5 {
			t.Error("should overwrite existing vector")
		}
	})
}

func TestHNSWRemove(t *testing.T) {
	config := DefaultHNSWConfig(4)
	index := NewHNSW(config)

	index.Add("vec-1", []float32{1.0, 0.0, 0.0, 0.0})
	index.Add("vec-2", []float32{0.0, 1.0, 0.0, 0.0})

	t.Run("remove existing", func(t *testing.T) {
		err := index.Remove("vec-1")
		if err != nil {
			t.Fatalf("Remove() error = %v", err)
		}

		if len(index.vectors) != 1 {
			t.Error("should have 1 vector after removal")
		}
	})

	t.Run("remove non-existing", func(t *testing.T) {
		err := index.Remove("non-existent")
		if err != nil {
			t.Error("removing non-existent should not error")
		}
	})
}

func TestHNSWSearch(t *testing.T) {
	config := DefaultHNSWConfig(4)
	index := NewHNSW(config)

	// Add some test vectors
	index.Add("vec-1", []float32{1.0, 0.0, 0.0, 0.0})     // Points in X direction
	index.Add("vec-2", []float32{0.0, 1.0, 0.0, 0.0})     // Points in Y direction
	index.Add("vec-3", []float32{0.0, 0.0, 1.0, 0.0})     // Points in Z direction
	index.Add("vec-4", []float32{0.707, 0.707, 0.0, 0.0}) // 45 degrees between X and Y

	ctx := context.Background()

	t.Run("exact match", func(t *testing.T) {
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 3, 0.9)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) == 0 {
			t.Error("expected at least one result")
			return
		}

		// First result should be vec-1 with score ~1.0
		if results[0].ID != "vec-1" {
			t.Errorf("expected vec-1, got %s", results[0].ID)
		}
		if results[0].Score < 0.99 {
			t.Errorf("expected score ~1.0, got %f", results[0].Score)
		}
	})

	t.Run("partial match", func(t *testing.T) {
		// Query between X and Y
		results, err := index.Search(ctx, []float32{0.707, 0.707, 0.0, 0.0}, 3, 0.5)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) == 0 {
			t.Error("expected results")
			return
		}

		// vec-4 should be first (exact match)
		if results[0].ID != "vec-4" {
			t.Errorf("expected vec-4 first, got %s", results[0].ID)
		}
	})

	t.Run("threshold filtering", func(t *testing.T) {
		results, err := index.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 10, 0.99)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		// Only vec-1 should meet the high threshold
		if len(results) != 1 {
			t.Errorf("expected 1 result above threshold, got %d", len(results))
		}
	})

	t.Run("limit results", func(t *testing.T) {
		results, err := index.Search(ctx, []float32{0.5, 0.5, 0.5, 0.0}, 2, 0.0)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) > 2 {
			t.Errorf("expected max 2 results, got %d", len(results))
		}
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		_, err := index.Search(ctx, []float32{1.0, 0.0}, 3, 0.5)
		if err != ErrDimensionMismatch {
			t.Errorf("expected ErrDimensionMismatch, got %v", err)
		}
	})

	t.Run("empty index", func(t *testing.T) {
		emptyIndex := NewHNSW(config)
		results, err := emptyIndex.Search(ctx, []float32{1.0, 0.0, 0.0, 0.0}, 3, 0.0)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) != 0 {
			t.Error("empty index should return no results")
		}
	})
}

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float64
		epsilon  float64
	}{
		{
			name:     "identical vectors",
			a:        []float32{1.0, 0.0, 0.0},
			b:        []float32{1.0, 0.0, 0.0},
			expected: 1.0,
			epsilon:  0.001,
		},
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0, 0.0},
			b:        []float32{0.0, 1.0, 0.0},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "opposite vectors",
			a:        []float32{1.0, 0.0, 0.0},
			b:        []float32{-1.0, 0.0, 0.0},
			expected: -1.0,
			epsilon:  0.001,
		},
		{
			name:     "45 degree angle",
			a:        []float32{1.0, 0.0},
			b:        []float32{0.707, 0.707},
			expected: 0.707,
			epsilon:  0.01,
		},
		{
			name:     "different lengths",
			a:        []float32{1.0, 0.0},
			b:        []float32{2.0, 0.0},
			expected: 1.0, // Cosine similarity is length-independent
			epsilon:  0.001,
		},
		{
			name:     "mismatched dimensions",
			a:        []float32{1.0, 0.0},
			b:        []float32{1.0, 0.0, 0.0},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "zero vector",
			a:        []float32{0.0, 0.0},
			b:        []float32{1.0, 0.0},
			expected: 0.0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := vector.CosineSimilarity(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestMathSqrt(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
		epsilon  float64
	}{
		{4.0, 2.0, 0.001},
		{9.0, 3.0, 0.001},
		{2.0, 1.414, 0.01},
		{0.0, 0.0, 0.001},
	}

	for _, tt := range tests {
		result := math.Sqrt(tt.input)
		if math.Abs(result-tt.expected) > tt.epsilon {
			t.Errorf("math.Sqrt(%f): expected %f, got %f", tt.input, tt.expected, result)
		}
	}
}

func TestSortResultsByScore(t *testing.T) {
	results := []SearchResult{
		{ID: "low", Score: 0.3},
		{ID: "high", Score: 0.9},
		{ID: "mid", Score: 0.6},
	}

	sortResultsByScore(results)

	if results[0].ID != "high" {
		t.Error("highest score should be first")
	}
	if results[1].ID != "mid" {
		t.Error("middle score should be second")
	}
	if results[2].ID != "low" {
		t.Error("lowest score should be last")
	}
}

func TestSearchResult(t *testing.T) {
	result := SearchResult{
		ID:    "test-id",
		Score: 0.95,
	}

	if result.ID != "test-id" {
		t.Error("wrong ID")
	}
	if result.Score != 0.95 {
		t.Error("wrong score")
	}
}

func TestHNSWConfig(t *testing.T) {
	config := &HNSWConfig{
		Dimensions:     512,
		M:              32,
		EfConstruction: 400,
		EfSearch:       200,
	}

	if config.Dimensions != 512 {
		t.Error("wrong dimensions")
	}
	if config.M != 32 {
		t.Error("wrong M")
	}
	if config.EfConstruction != 400 {
		t.Error("wrong EfConstruction")
	}
	if config.EfSearch != 200 {
		t.Error("wrong EfSearch")
	}
}

func TestNewBleve(t *testing.T) {
	index, err := NewBleve("")
	if err != nil {
		t.Fatalf("NewBleve() error = %v", err)
	}
	if index == nil {
		t.Error("expected index")
	}
}

func TestBleveIndex(t *testing.T) {
	index, _ := NewBleve("")

	t.Run("index document", func(t *testing.T) {
		err := index.Index("doc-1", "hello world")
		if err != nil {
			t.Fatalf("Index() error = %v", err)
		}
	})

	t.Run("delete document", func(t *testing.T) {
		err := index.Delete("doc-1")
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
	})

	t.Run("search", func(t *testing.T) {
		results, err := index.Search(context.Background(), "hello", 10)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		// Results may be nil/empty since Bleve is not fully implemented
		_ = results
	})

	t.Run("close", func(t *testing.T) {
		err := index.Close()
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
}

func TestIndexError(t *testing.T) {
	err := &IndexError{Message: "test error"}
	if err.Error() != "test error" {
		t.Errorf("expected 'test error', got %s", err.Error())
	}
}

func TestErrors(t *testing.T) {
	if ErrDimensionMismatch.Error() != "vector dimension mismatch" {
		t.Error("wrong error message")
	}
	if ErrNotFound.Error() != "not found" {
		t.Error("wrong error message")
	}
}

func TestConcurrentAccess(t *testing.T) {
	config := DefaultHNSWConfig(4)
	index := NewHNSW(config)

	// Add vectors concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			vec := []float32{float32(id), 0.0, 0.0, 0.0}
			index.Add(string(rune('a'+id)), vec)
			done <- true
		}(i)
	}

	// Wait for all to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Search concurrently
	for i := 0; i < 10; i++ {
		go func() {
			index.Search(context.Background(), []float32{1.0, 0.0, 0.0, 0.0}, 5, 0.0)
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func BenchmarkCosineSimilarity(b *testing.B) {
	a := make([]float32, 1024)
	vecB := make([]float32, 1024)
	for i := range a {
		a[i] = float32(i) / 1024
		vecB[i] = float32(1024-i) / 1024
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vector.CosineSimilarity(a, vecB)
	}
}

func BenchmarkHNSWSearch(b *testing.B) {
	config := DefaultHNSWConfig(128)
	index := NewHNSW(config)

	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32((i+j)%100) / 100
		}
		index.Add(string(rune(i)), vec)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = 0.5
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Search(ctx, query, 10, 0.5)
	}
}
