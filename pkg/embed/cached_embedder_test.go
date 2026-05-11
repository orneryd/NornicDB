package embed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// mockEmbedder tracks calls for testing
type mockEmbedder struct {
	calls     int64
	batchSize int
}

func (m *mockEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	atomic.AddInt64(&m.calls, 1)
	// Return deterministic embedding based on text hash
	return []float32{float32(len(text)), 0.5, 0.5}, nil
}

func (m *mockEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	atomic.AddInt64(&m.calls, int64(len(texts)))
	m.batchSize = len(texts)
	results := make([][]float32, len(texts))
	for i, text := range texts {
		results[i] = []float32{float32(len(text)), 0.5, 0.5}
	}
	return results, nil
}

func (m *mockEmbedder) Model() string    { return "mock" }
func (m *mockEmbedder) Dimensions() int  { return 3 }
func (m *mockEmbedder) Backend() string  { return "cpu" } // Plan 04-05 D-06: closed enum
func (m *mockEmbedder) CallCount() int64 { return atomic.LoadInt64(&m.calls) }

func (m *mockEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}

func TestCachedEmbedder_CacheHit(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 100)
	ctx := context.Background()

	// First call - cache miss
	_, err := cached.Embed(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	if mock.CallCount() != 1 {
		t.Errorf("Expected 1 call, got %d", mock.CallCount())
	}

	// Second call with same text - cache hit
	_, err = cached.Embed(ctx, "hello world")
	if err != nil {
		t.Fatal(err)
	}
	if mock.CallCount() != 1 {
		t.Errorf("Expected still 1 call (cache hit), got %d", mock.CallCount())
	}

	// Third call with different text - cache miss
	_, err = cached.Embed(ctx, "different text")
	if err != nil {
		t.Fatal(err)
	}
	if mock.CallCount() != 2 {
		t.Errorf("Expected 2 calls, got %d", mock.CallCount())
	}

	// Verify stats
	stats := cached.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Errorf("Expected 2 misses, got %d", stats.Misses)
	}
	if stats.Size != 2 {
		t.Errorf("Expected cache size 2, got %d", stats.Size)
	}
}

func TestCachedEmbedder_BatchCaching(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 100)
	ctx := context.Background()

	// Pre-cache one text
	_, _ = cached.Embed(ctx, "cached")

	// Batch with mix of cached and new
	texts := []string{"cached", "new1", "new2"}
	_, err := cached.EmbedBatch(ctx, texts)
	if err != nil {
		t.Fatal(err)
	}

	// Should only call base for new texts (2), plus the pre-cache (1)
	// Total: 1 (pre-cache) + 2 (batch misses) = 3
	if mock.CallCount() != 3 {
		t.Errorf("Expected 3 total calls, got %d", mock.CallCount())
	}

	// Verify batch size was only the misses
	if mock.batchSize != 2 {
		t.Errorf("Expected batch of 2 (misses only), got %d", mock.batchSize)
	}

	stats := cached.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit (from batch), got %d", stats.Hits)
	}
}

func TestCachedEmbedder_LRUEviction(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 3) // Small cache
	ctx := context.Background()

	// Fill cache
	_, _ = cached.Embed(ctx, "a")
	_, _ = cached.Embed(ctx, "b")
	_, _ = cached.Embed(ctx, "c")

	if cached.Stats().Size != 3 {
		t.Errorf("Expected size 3, got %d", cached.Stats().Size)
	}

	// Add one more - should evict "a" (oldest)
	_, _ = cached.Embed(ctx, "d")

	if cached.Stats().Size != 3 {
		t.Errorf("Expected size still 3 after eviction, got %d", cached.Stats().Size)
	}

	// "a" should be evicted, so this should be a cache miss
	callsBefore := mock.CallCount()
	_, _ = cached.Embed(ctx, "a")
	if mock.CallCount() == callsBefore {
		t.Error("Expected cache miss for evicted 'a', but got hit")
	}
}

func TestCachedEmbedder_Concurrent(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 1000)
	ctx := context.Background()

	// Warm the two keys first so this test verifies concurrent cache hits
	// deterministically instead of relying on goroutine scheduling.
	_, err := cached.Embed(ctx, "text")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cached.Embed(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			text := "text"
			if i%2 == 0 {
				text = "other"
			}
			_, err := cached.Embed(ctx, text)
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	// Should only have 2 unique texts cached
	stats := cached.Stats()
	if stats.Size != 2 {
		t.Errorf("Expected 2 unique cached, got %d", stats.Size)
	}

	// After warm-up all concurrent requests should be hits.
	if stats.Hits < 100 {
		t.Errorf("Expected at least 100 cache hits, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Errorf("Expected exactly 2 warm-up misses, got %d", stats.Misses)
	}
	if mock.CallCount() != 2 {
		t.Errorf("Expected exactly 2 underlying embed calls, got %d", mock.CallCount())
	}
}

func BenchmarkCachedEmbedder_CacheHit(b *testing.B) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 1000)
	ctx := context.Background()

	// Pre-warm cache
	_, _ = cached.Embed(ctx, "benchmark text")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cached.Embed(ctx, "benchmark text")
	}
}

func BenchmarkCachedEmbedder_CacheMiss(b *testing.B) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, b.N+1)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		text := string(rune('a' + i%26))
		_, _ = cached.Embed(ctx, text)
	}
}
