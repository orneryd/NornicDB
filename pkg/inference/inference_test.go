package inference

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	require.NotNil(t, config)

	assert.Equal(t, 0.82, config.SimilarityThreshold)
	assert.Equal(t, 10, config.SimilarityTopK)
	assert.True(t, config.CoAccessEnabled)
	assert.Equal(t, 30*time.Second, config.CoAccessWindow)
	assert.Equal(t, 3, config.CoAccessMinCount)
	assert.True(t, config.TemporalEnabled)
	assert.Equal(t, 30*time.Minute, config.TemporalWindow)
	assert.True(t, config.TransitiveEnabled)
	assert.Equal(t, 0.5, config.TransitiveMinConf)
}

func TestNew(t *testing.T) {
	t.Run("with config", func(t *testing.T) {
		config := &Config{
			SimilarityThreshold: 0.9,
			SimilarityTopK:      5,
		}
		engine := New(config)
		require.NotNil(t, engine)
		assert.Equal(t, 0.9, engine.config.SimilarityThreshold)
		assert.Equal(t, 5, engine.config.SimilarityTopK)
	})

	t.Run("nil config uses defaults", func(t *testing.T) {
		engine := New(nil)
		require.NotNil(t, engine)
		assert.Equal(t, 0.82, engine.config.SimilarityThreshold)
	})

	t.Run("initializes tracking maps", func(t *testing.T) {
		engine := New(nil)
		assert.NotNil(t, engine.accessHistory)
		assert.NotNil(t, engine.coAccessCounts)
	})
}

func TestEngine_SetSimilaritySearch(t *testing.T) {
	engine := New(nil)

	searchFunc := func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
		return nil, nil
	}

	engine.SetSimilaritySearch(searchFunc)
	assert.NotNil(t, engine.similaritySearch)
}

func TestEngine_OnStore(t *testing.T) {
	t.Run("no similarity search configured", func(t *testing.T) {
		engine := New(nil)

		suggestions, err := engine.OnStore(context.Background(), "node-1", []float32{0.1, 0.2})
		require.NoError(t, err)
		assert.Empty(t, suggestions)
	})

	t.Run("empty embedding", func(t *testing.T) {
		engine := New(nil)
		engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
			return []SimilarityResult{{ID: "other", Score: 0.95}}, nil
		})

		suggestions, err := engine.OnStore(context.Background(), "node-1", nil)
		require.NoError(t, err)
		assert.Empty(t, suggestions)
	})

	t.Run("suggests edges for similar nodes", func(t *testing.T) {
		config := &Config{
			SimilarityThreshold: 0.8,
			SimilarityTopK:      5,
		}
		engine := New(config)
		engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
			return []SimilarityResult{
				{ID: "similar-1", Score: 0.95},
				{ID: "similar-2", Score: 0.85},
				{ID: "not-similar", Score: 0.7}, // Below threshold
			}, nil
		})

		suggestions, err := engine.OnStore(context.Background(), "node-1", []float32{0.1, 0.2})
		require.NoError(t, err)

		// Should have 2 suggestions (above threshold)
		assert.Len(t, suggestions, 2)

		// Check first suggestion
		assert.Equal(t, "node-1", suggestions[0].SourceID)
		assert.Equal(t, "similar-1", suggestions[0].TargetID)
		assert.Equal(t, "RELATES_TO", suggestions[0].Type)
		assert.Equal(t, "similarity", suggestions[0].Method)
		assert.Greater(t, suggestions[0].Confidence, 0.0)
	})

	t.Run("skips self in results", func(t *testing.T) {
		engine := New(nil)
		engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
			return []SimilarityResult{
				{ID: "node-1", Score: 1.0}, // Self
				{ID: "other", Score: 0.9},
			}, nil
		})

		suggestions, err := engine.OnStore(context.Background(), "node-1", []float32{0.1})
		require.NoError(t, err)

		// Should skip self
		for _, s := range suggestions {
			assert.NotEqual(t, "node-1", s.TargetID)
		}
	})
}

func TestEngine_OnAccess(t *testing.T) {
	t.Run("co-access disabled", func(t *testing.T) {
		config := &Config{CoAccessEnabled: false}
		engine := New(config)

		suggestions := engine.OnAccess(context.Background(), "node-1")
		assert.Empty(t, suggestions)
	})

	t.Run("tracks access history", func(t *testing.T) {
		config := &Config{
			CoAccessEnabled:  true,
			CoAccessWindow:   time.Minute,
			CoAccessMinCount: 5, // High threshold to avoid suggestions
		}
		engine := New(config)

		engine.OnAccess(context.Background(), "node-1")
		engine.OnAccess(context.Background(), "node-2")

		assert.Len(t, engine.accessHistory, 2)
	})

	t.Run("suggests edges after threshold accesses", func(t *testing.T) {
		config := &Config{
			CoAccessEnabled:  true,
			CoAccessWindow:   time.Minute,
			CoAccessMinCount: 2, // Low threshold
		}
		engine := New(config)

		// Access nodes in pattern
		engine.OnAccess(context.Background(), "node-1")
		engine.OnAccess(context.Background(), "node-2")
		engine.OnAccess(context.Background(), "node-1")
		suggestions := engine.OnAccess(context.Background(), "node-2")

		// Should suggest edge between node-1 and node-2
		found := false
		for _, s := range suggestions {
			if (s.SourceID == "node-1" && s.TargetID == "node-2") ||
				(s.SourceID == "node-2" && s.TargetID == "node-1") {
				found = true
				assert.Equal(t, "co_access", s.Method)
				assert.Contains(t, s.Reason, "Frequently accessed together")
			}
		}
		assert.True(t, found, "Expected co-access suggestion")
	})

	t.Run("confidence capped at 0.8", func(t *testing.T) {
		config := &Config{
			CoAccessEnabled:  true,
			CoAccessWindow:   time.Minute,
			CoAccessMinCount: 1,
		}
		engine := New(config)

		// Many co-accesses
		for i := 0; i < 100; i++ {
			engine.OnAccess(context.Background(), "node-1")
			engine.OnAccess(context.Background(), "node-2")
		}

		suggestions := engine.OnAccess(context.Background(), "node-1")
		for _, s := range suggestions {
			assert.LessOrEqual(t, s.Confidence, 0.8)
		}
	})

	t.Run("prunes old history", func(t *testing.T) {
		config := &Config{
			CoAccessEnabled:  true,
			CoAccessWindow:   10 * time.Millisecond,
			CoAccessMinCount: 3,
		}
		engine := New(config)

		engine.OnAccess(context.Background(), "old-node")

		// Wait for window to pass
		time.Sleep(20 * time.Millisecond)

		engine.OnAccess(context.Background(), "new-node")

		// Old access should be pruned
		assert.Len(t, engine.accessHistory, 1)
		assert.Equal(t, "new-node", engine.accessHistory[0].NodeID)
	})
}

func TestEngine_SuggestTransitive(t *testing.T) {
	t.Run("transitive disabled", func(t *testing.T) {
		config := &Config{TransitiveEnabled: false}
		engine := New(config)

		edges := []ExistingEdge{
			{SourceID: "A", TargetID: "B", Confidence: 0.9},
			{SourceID: "B", TargetID: "C", Confidence: 0.9},
		}

		suggestions := engine.SuggestTransitive(context.Background(), edges)
		assert.Empty(t, suggestions)
	})

	t.Run("suggests A->C from A->B->C", func(t *testing.T) {
		config := &Config{
			TransitiveEnabled: true,
			TransitiveMinConf: 0.5,
		}
		engine := New(config)

		edges := []ExistingEdge{
			{SourceID: "A", TargetID: "B", Confidence: 0.8},
			{SourceID: "B", TargetID: "C", Confidence: 0.8},
		}

		suggestions := engine.SuggestTransitive(context.Background(), edges)

		require.Len(t, suggestions, 1)
		assert.Equal(t, "A", suggestions[0].SourceID)
		assert.Equal(t, "C", suggestions[0].TargetID)
		assert.Equal(t, "transitive", suggestions[0].Method)
		assert.InDelta(t, 0.64, suggestions[0].Confidence, 0.01) // 0.8 * 0.8
		assert.Contains(t, suggestions[0].Reason, "Transitive via B")
	})

	t.Run("respects minimum confidence", func(t *testing.T) {
		config := &Config{
			TransitiveEnabled: true,
			TransitiveMinConf: 0.7, // High threshold
		}
		engine := New(config)

		edges := []ExistingEdge{
			{SourceID: "A", TargetID: "B", Confidence: 0.8},
			{SourceID: "B", TargetID: "C", Confidence: 0.8},
		}

		suggestions := engine.SuggestTransitive(context.Background(), edges)

		// 0.8 * 0.8 = 0.64 < 0.7, so no suggestion
		assert.Empty(t, suggestions)
	})

	t.Run("skips cycles back to origin", func(t *testing.T) {
		config := &Config{
			TransitiveEnabled: true,
			TransitiveMinConf: 0.5,
		}
		engine := New(config)

		edges := []ExistingEdge{
			{SourceID: "A", TargetID: "B", Confidence: 0.9},
			{SourceID: "B", TargetID: "A", Confidence: 0.9}, // Cycle back to A
		}

		suggestions := engine.SuggestTransitive(context.Background(), edges)

		// Should not suggest A->A
		for _, s := range suggestions {
			assert.NotEqual(t, s.SourceID, s.TargetID)
		}
	})

	t.Run("handles empty edges", func(t *testing.T) {
		config := &Config{TransitiveEnabled: true}
		engine := New(config)

		suggestions := engine.SuggestTransitive(context.Background(), []ExistingEdge{})
		assert.Empty(t, suggestions)
	})

	t.Run("multiple transitive paths", func(t *testing.T) {
		config := &Config{
			TransitiveEnabled: true,
			TransitiveMinConf: 0.5,
		}
		engine := New(config)

		edges := []ExistingEdge{
			{SourceID: "A", TargetID: "B", Confidence: 0.9},
			{SourceID: "B", TargetID: "C", Confidence: 0.9},
			{SourceID: "B", TargetID: "D", Confidence: 0.9},
		}

		suggestions := engine.SuggestTransitive(context.Background(), edges)

		// Should suggest A->C and A->D
		assert.Len(t, suggestions, 2)
	})
}

func TestEngine_scoreToConfidence(t *testing.T) {
	engine := New(nil)

	tests := []struct {
		score    float64
		expected float64
	}{
		{0.99, 0.9}, // >= 0.95
		{0.95, 0.9}, // >= 0.95
		{0.94, 0.7}, // >= 0.90
		{0.90, 0.7}, // >= 0.90
		{0.89, 0.5}, // >= 0.85
		{0.85, 0.5}, // >= 0.85
		{0.84, 0.3}, // < 0.85
		{0.50, 0.3}, // < 0.85
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			result := engine.scoreToConfidence(tc.score)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestEngine_makeCoAccessKey(t *testing.T) {
	engine := New(nil)

	t.Run("consistent regardless of order", func(t *testing.T) {
		key1 := engine.makeCoAccessKey("a", "b")
		key2 := engine.makeCoAccessKey("b", "a")

		assert.Equal(t, key1, key2)
	})

	t.Run("different pairs have different keys", func(t *testing.T) {
		key1 := engine.makeCoAccessKey("a", "b")
		key2 := engine.makeCoAccessKey("a", "c")

		assert.NotEqual(t, key1, key2)
	})
}

func TestEngine_GetStats(t *testing.T) {
	t.Run("returns tracked co-access count", func(t *testing.T) {
		config := &Config{
			CoAccessEnabled:  true,
			CoAccessWindow:   time.Minute,
			CoAccessMinCount: 100, // High to avoid suggestions
		}
		engine := New(config)

		// Generate some co-accesses
		engine.OnAccess(context.Background(), "a")
		engine.OnAccess(context.Background(), "b")
		engine.OnAccess(context.Background(), "a")
		engine.OnAccess(context.Background(), "c")

		stats := engine.GetStats()

		// Should have tracked co-access pairs
		assert.Greater(t, stats.TrackedCoAccesses, 0)
	})
}

func TestEngine_Concurrency(t *testing.T) {
	t.Run("concurrent OnAccess", func(t *testing.T) {
		config := &Config{
			CoAccessEnabled:  true,
			CoAccessWindow:   time.Second,
			CoAccessMinCount: 100,
		}
		engine := New(config)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				nodeID := string(rune('a' + id%26))
				engine.OnAccess(context.Background(), nodeID)
			}(i)
		}
		wg.Wait()

		// Should not panic
		stats := engine.GetStats()
		assert.GreaterOrEqual(t, stats.TrackedCoAccesses, 0)
	})

	t.Run("concurrent OnStore", func(t *testing.T) {
		engine := New(nil)
		engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
			return []SimilarityResult{{ID: "test", Score: 0.9}}, nil
		})

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				nodeID := string(rune('a' + id%26))
				_, _ = engine.OnStore(context.Background(), nodeID, []float32{float32(id) * 0.1})
			}(i)
		}
		wg.Wait()
	})
}

// Benchmarks

func BenchmarkOnAccess(b *testing.B) {
	config := &Config{
		CoAccessEnabled:  true,
		CoAccessWindow:   time.Second,
		CoAccessMinCount: 3,
	}
	engine := New(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := string(rune('a' + i%26))
		engine.OnAccess(context.Background(), nodeID)
	}
}

func BenchmarkOnStore(b *testing.B) {
	engine := New(nil)
	engine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
		return []SimilarityResult{
			{ID: "a", Score: 0.9},
			{ID: "b", Score: 0.85},
		}, nil
	})

	embedding := make([]float32, 384)
	for i := range embedding {
		embedding[i] = float32(i) * 0.001
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := string(rune('a' + i%26))
		engine.OnStore(context.Background(), nodeID, embedding)
	}
}

func BenchmarkSuggestTransitive(b *testing.B) {
	config := &Config{
		TransitiveEnabled: true,
		TransitiveMinConf: 0.5,
	}
	engine := New(config)

	// Create a graph with 100 edges
	edges := make([]ExistingEdge, 100)
	for i := range edges {
		edges[i] = ExistingEdge{
			SourceID:   string(rune('a' + i%26)),
			TargetID:   string(rune('a' + (i+1)%26)),
			Confidence: 0.8,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.SuggestTransitive(context.Background(), edges)
	}
}
