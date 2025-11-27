package cache

import (
	"sync"
	"testing"
	"time"
)

// =============================================================================
// NewQueryCache Tests
// =============================================================================

func TestNewQueryCache(t *testing.T) {
	t.Run("valid parameters", func(t *testing.T) {
		cache := NewQueryCache(100, 5*time.Minute)

		if cache.maxSize != 100 {
			t.Errorf("maxSize = %d, want 100", cache.maxSize)
		}
		if cache.ttl != 5*time.Minute {
			t.Errorf("ttl = %v, want 5m", cache.ttl)
		}
		if !cache.enabled {
			t.Error("cache should be enabled by default")
		}
	})

	t.Run("zero maxSize uses default", func(t *testing.T) {
		cache := NewQueryCache(0, time.Minute)

		if cache.maxSize != 1000 {
			t.Errorf("maxSize = %d, want 1000 (default)", cache.maxSize)
		}
	})

	t.Run("negative maxSize uses default", func(t *testing.T) {
		cache := NewQueryCache(-10, time.Minute)

		if cache.maxSize != 1000 {
			t.Errorf("maxSize = %d, want 1000 (default)", cache.maxSize)
		}
	})

	t.Run("zero TTL is valid (no expiration)", func(t *testing.T) {
		cache := NewQueryCache(100, 0)

		if cache.ttl != 0 {
			t.Errorf("ttl = %v, want 0", cache.ttl)
		}
	})
}

// =============================================================================
// Key Generation Tests
// =============================================================================

func TestQueryCache_Key(t *testing.T) {
	cache := NewQueryCache(100, time.Minute)

	t.Run("same query same key", func(t *testing.T) {
		key1 := cache.Key("MATCH (n) RETURN n", nil)
		key2 := cache.Key("MATCH (n) RETURN n", nil)

		if key1 != key2 {
			t.Errorf("same query produced different keys: %d vs %d", key1, key2)
		}
	})

	t.Run("different query different key", func(t *testing.T) {
		key1 := cache.Key("MATCH (n) RETURN n", nil)
		key2 := cache.Key("MATCH (m) RETURN m", nil)

		if key1 == key2 {
			t.Error("different queries produced same key")
		}
	})

	t.Run("params affect key", func(t *testing.T) {
		params1 := map[string]interface{}{"id": 1}
		params2 := map[string]interface{}{"name": "test"}

		key1 := cache.Key("MATCH (n) RETURN n", params1)
		key2 := cache.Key("MATCH (n) RETURN n", params2)

		if key1 == key2 {
			t.Error("different params produced same key")
		}
	})

	t.Run("nil params", func(t *testing.T) {
		key := cache.Key("MATCH (n) RETURN n", nil)
		if key == 0 {
			t.Error("key should not be 0")
		}
	})
}

// =============================================================================
// Get/Put Tests
// =============================================================================

func TestQueryCache_GetPut(t *testing.T) {
	t.Run("put and get", func(t *testing.T) {
		cache := NewQueryCache(100, time.Minute)
		key := cache.Key("MATCH (n) RETURN n", nil)

		cache.Put(key, "plan1")

		val, ok := cache.Get(key)
		if !ok {
			t.Fatal("Get returned false for existing key")
		}
		if val != "plan1" {
			t.Errorf("Get returned %v, want %v", val, "plan1")
		}
	})

	t.Run("get non-existent key", func(t *testing.T) {
		cache := NewQueryCache(100, time.Minute)

		val, ok := cache.Get(12345)
		if ok {
			t.Error("Get returned true for non-existent key")
		}
		if val != nil {
			t.Errorf("Get returned %v for non-existent key, want nil", val)
		}
	})

	t.Run("update existing key", func(t *testing.T) {
		cache := NewQueryCache(100, time.Minute)
		key := cache.Key("query", nil)

		cache.Put(key, "plan1")
		cache.Put(key, "plan2")

		val, ok := cache.Get(key)
		if !ok {
			t.Fatal("Get returned false")
		}
		if val != "plan2" {
			t.Errorf("Get returned %v, want plan2", val)
		}

		if cache.Len() != 1 {
			t.Errorf("Len = %d, want 1", cache.Len())
		}
	})
}

// =============================================================================
// TTL Tests
// =============================================================================

func TestQueryCache_TTL(t *testing.T) {
	t.Run("entry expires after TTL", func(t *testing.T) {
		cache := NewQueryCache(100, 50*time.Millisecond)
		key := cache.Key("query", nil)

		cache.Put(key, "plan")

		// Should exist immediately
		if _, ok := cache.Get(key); !ok {
			t.Error("entry should exist before TTL")
		}

		// Wait for TTL
		time.Sleep(100 * time.Millisecond)

		// Should be expired
		if _, ok := cache.Get(key); ok {
			t.Error("entry should be expired after TTL")
		}
	})

	t.Run("zero TTL means no expiration", func(t *testing.T) {
		cache := NewQueryCache(100, 0)
		key := cache.Key("query", nil)

		cache.Put(key, "plan")

		// Should exist
		time.Sleep(50 * time.Millisecond)

		if _, ok := cache.Get(key); !ok {
			t.Error("entry should not expire with zero TTL")
		}
	})

	t.Run("update refreshes TTL", func(t *testing.T) {
		cache := NewQueryCache(100, 100*time.Millisecond)
		key := cache.Key("query", nil)

		cache.Put(key, "plan1")

		// Wait partway through TTL
		time.Sleep(60 * time.Millisecond)

		// Update entry
		cache.Put(key, "plan2")

		// Wait past original TTL
		time.Sleep(60 * time.Millisecond)

		// Should still exist due to refresh
		if _, ok := cache.Get(key); !ok {
			t.Error("entry should exist after TTL refresh")
		}
	})
}

// =============================================================================
// LRU Eviction Tests
// =============================================================================

func TestQueryCache_LRUEviction(t *testing.T) {
	t.Run("evicts oldest when full", func(t *testing.T) {
		cache := NewQueryCache(3, time.Hour)

		cache.Put(1, "plan1")
		cache.Put(2, "plan2")
		cache.Put(3, "plan3")

		if cache.Len() != 3 {
			t.Fatalf("Len = %d, want 3", cache.Len())
		}

		// Add one more, should evict oldest (key 1)
		cache.Put(4, "plan4")

		if cache.Len() != 3 {
			t.Errorf("Len = %d, want 3", cache.Len())
		}

		// Key 1 should be evicted
		if _, ok := cache.Get(1); ok {
			t.Error("key 1 should have been evicted")
		}

		// Key 4 should exist
		if _, ok := cache.Get(4); !ok {
			t.Error("key 4 should exist")
		}
	})

	t.Run("access promotes entry", func(t *testing.T) {
		cache := NewQueryCache(3, time.Hour)

		cache.Put(1, "plan1")
		cache.Put(2, "plan2")
		cache.Put(3, "plan3")

		// Access key 1 to promote it
		cache.Get(1)

		// Add one more, should evict key 2 (now oldest)
		cache.Put(4, "plan4")

		// Key 1 should still exist (was promoted)
		if _, ok := cache.Get(1); !ok {
			t.Error("key 1 should still exist (was accessed)")
		}

		// Key 2 should be evicted
		if _, ok := cache.Get(2); ok {
			t.Error("key 2 should have been evicted")
		}
	})
}

// =============================================================================
// Remove and Clear Tests
// =============================================================================

func TestQueryCache_Remove(t *testing.T) {
	cache := NewQueryCache(100, time.Hour)

	cache.Put(1, "plan1")
	cache.Put(2, "plan2")

	cache.Remove(1)

	if _, ok := cache.Get(1); ok {
		t.Error("removed key should not exist")
	}

	if _, ok := cache.Get(2); !ok {
		t.Error("other key should still exist")
	}

	if cache.Len() != 1 {
		t.Errorf("Len = %d, want 1", cache.Len())
	}
}

func TestQueryCache_Clear(t *testing.T) {
	cache := NewQueryCache(100, time.Hour)

	cache.Put(1, "plan1")
	cache.Put(2, "plan2")
	cache.Put(3, "plan3")

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Len = %d after clear, want 0", cache.Len())
	}

	if _, ok := cache.Get(1); ok {
		t.Error("cleared cache should not have any entries")
	}
}

// =============================================================================
// Statistics Tests
// =============================================================================

func TestQueryCache_Stats(t *testing.T) {
	cache := NewQueryCache(100, time.Hour)

	cache.Put(1, "plan1")
	cache.Put(2, "plan2")

	// 2 hits
	cache.Get(1)
	cache.Get(2)

	// 2 misses
	cache.Get(999)
	cache.Get(888)

	stats := cache.Stats()

	if stats.Size != 2 {
		t.Errorf("Size = %d, want 2", stats.Size)
	}
	if stats.MaxSize != 100 {
		t.Errorf("MaxSize = %d, want 100", stats.MaxSize)
	}
	if stats.Hits != 2 {
		t.Errorf("Hits = %d, want 2", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Errorf("Misses = %d, want 2", stats.Misses)
	}
	if stats.HitRate != 50.0 {
		t.Errorf("HitRate = %.2f, want 50.00", stats.HitRate)
	}
}

func TestQueryCache_StatsZeroTotal(t *testing.T) {
	cache := NewQueryCache(100, time.Hour)

	stats := cache.Stats()

	if stats.HitRate != 0 {
		t.Errorf("HitRate = %.2f with no operations, want 0", stats.HitRate)
	}
}

// =============================================================================
// SetEnabled Tests
// =============================================================================

func TestQueryCache_SetEnabled(t *testing.T) {
	t.Run("disable clears cache", func(t *testing.T) {
		cache := NewQueryCache(100, time.Hour)

		cache.Put(1, "plan1")
		cache.Put(2, "plan2")

		cache.SetEnabled(false)

		if cache.Len() != 0 {
			t.Errorf("disabled cache Len = %d, want 0", cache.Len())
		}
	})

	t.Run("disabled cache returns miss", func(t *testing.T) {
		cache := NewQueryCache(100, time.Hour)
		cache.SetEnabled(false)

		cache.Put(1, "plan1") // Should be no-op

		if _, ok := cache.Get(1); ok {
			t.Error("disabled cache should return miss")
		}
	})

	t.Run("re-enable works", func(t *testing.T) {
		cache := NewQueryCache(100, time.Hour)

		cache.SetEnabled(false)
		cache.SetEnabled(true)

		cache.Put(1, "plan1")

		if _, ok := cache.Get(1); !ok {
			t.Error("re-enabled cache should work")
		}
	})
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestQueryCache_ConcurrentAccess(t *testing.T) {
	cache := NewQueryCache(1000, time.Hour)

	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // readers + writers

	// Writers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := uint64(id*iterations + j)
				cache.Put(key, "plan")
			}
		}(i)
	}

	// Readers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := uint64(id*iterations + j)
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Should not panic and stats should be reasonable
	stats := cache.Stats()
	if stats.Hits+stats.Misses == 0 {
		t.Error("expected some operations")
	}
}

func TestQueryCache_ConcurrentEviction(t *testing.T) {
	cache := NewQueryCache(10, time.Hour) // Small cache to force evictions

	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := uint64(id*iterations + j)
				cache.Put(key, "plan")
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Should not exceed max size
	if cache.Len() > 10 {
		t.Errorf("Len = %d, should not exceed maxSize 10", cache.Len())
	}
}

// =============================================================================
// Global Cache Tests
// =============================================================================

func TestGlobalQueryCache(t *testing.T) {
	// Note: GlobalQueryCache uses sync.Once, so we can only test basic functionality
	cache := GlobalQueryCache()

	if cache == nil {
		t.Fatal("GlobalQueryCache returned nil")
	}

	// Should be the same instance
	cache2 := GlobalQueryCache()
	if cache != cache2 {
		t.Error("GlobalQueryCache should return same instance")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkQueryCache_Key(b *testing.B) {
	cache := NewQueryCache(1000, time.Hour)
	query := "MATCH (n:Person {name: $name}) RETURN n"
	params := map[string]interface{}{"name": "Alice"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Key(query, params)
	}
}

func BenchmarkQueryCache_Put(b *testing.B) {
	cache := NewQueryCache(10000, time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(uint64(i), "plan")
	}
}

func BenchmarkQueryCache_Get_Hit(b *testing.B) {
	cache := NewQueryCache(10000, time.Hour)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		cache.Put(uint64(i), "plan")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(uint64(i % 1000))
	}
}

func BenchmarkQueryCache_Get_Miss(b *testing.B) {
	cache := NewQueryCache(1000, time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(uint64(i + 1000000))
	}
}

func BenchmarkQueryCache_ConcurrentReadWrite(b *testing.B) {
	cache := NewQueryCache(10000, time.Hour)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := uint64(i % 1000)
			if i%2 == 0 {
				cache.Put(key, "plan")
			} else {
				cache.Get(key)
			}
			i++
		}
	})
}

func BenchmarkQueryCache_WithEviction(b *testing.B) {
	cache := NewQueryCache(100, time.Hour) // Small to force evictions

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(uint64(i), "plan")
	}
}
