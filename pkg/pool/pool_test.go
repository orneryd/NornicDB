package pool

import (
	"sync"
	"testing"
)

// =============================================================================
// Configuration Tests
// =============================================================================

func TestConfigure(t *testing.T) {
	// Save original config
	origConfig := globalConfig
	defer func() {
		Configure(origConfig)
	}()

	t.Run("enable pooling", func(t *testing.T) {
		Configure(PoolConfig{Enabled: true, MaxSize: 500})

		if !IsEnabled() {
			t.Error("IsEnabled() = false, want true")
		}
		if globalConfig.MaxSize != 500 {
			t.Errorf("MaxSize = %d, want 500", globalConfig.MaxSize)
		}
	})

	t.Run("disable pooling", func(t *testing.T) {
		Configure(PoolConfig{Enabled: false, MaxSize: 1000})

		if IsEnabled() {
			t.Error("IsEnabled() = true, want false")
		}
	})
}

// =============================================================================
// Row Slice Pool Tests
// =============================================================================

func TestRowSlicePool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("get returns empty slice", func(t *testing.T) {
		rows := GetRowSlice()
		if len(rows) != 0 {
			t.Errorf("len = %d, want 0", len(rows))
		}
		if cap(rows) == 0 {
			t.Error("cap should be > 0 (pre-allocated)")
		}
		PutRowSlice(rows)
	})

	t.Run("put and reuse", func(t *testing.T) {
		rows := GetRowSlice()
		rows = append(rows, []interface{}{"test", 123})
		PutRowSlice(rows)

		// Get again - should be cleared
		rows2 := GetRowSlice()
		if len(rows2) != 0 {
			t.Errorf("reused slice len = %d, want 0", len(rows2))
		}
		PutRowSlice(rows2)
	})

	t.Run("oversized slices not pooled", func(t *testing.T) {
		Configure(PoolConfig{Enabled: true, MaxSize: 10})

		// Create oversized slice
		rows := make([][]interface{}, 0, 100)
		PutRowSlice(rows) // Should not panic, just not pool it

		Configure(PoolConfig{Enabled: true, MaxSize: 1000})
	})

	t.Run("disabled pooling creates new slices", func(t *testing.T) {
		Configure(PoolConfig{Enabled: false, MaxSize: 1000})
		defer Configure(PoolConfig{Enabled: true, MaxSize: 1000})

		rows := GetRowSlice()
		if rows == nil {
			t.Error("GetRowSlice returned nil when pooling disabled")
		}
		PutRowSlice(rows) // Should not panic
	})
}

// =============================================================================
// Node Slice Pool Tests
// =============================================================================

func TestNodeSlicePool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("get returns empty slice", func(t *testing.T) {
		nodes := GetNodeSlice()
		if len(nodes) != 0 {
			t.Errorf("len = %d, want 0", len(nodes))
		}
		PutNodeSlice(nodes)
	})

	t.Run("put clears references", func(t *testing.T) {
		nodes := GetNodeSlice()
		nodes = append(nodes, &PooledNode{ID: "test"})
		PutNodeSlice(nodes)

		// Slice should be cleared
		nodes2 := GetNodeSlice()
		if len(nodes2) != 0 {
			t.Errorf("reused slice len = %d, want 0", len(nodes2))
		}
		PutNodeSlice(nodes2)
	})
}

// =============================================================================
// String Builder Pool Tests
// =============================================================================

func TestStringBuilderPool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("basic operations", func(t *testing.T) {
		b := GetStringBuilder()
		if b.Len() != 0 {
			t.Errorf("Len() = %d, want 0", b.Len())
		}

		b.WriteString("hello")
		b.WriteByte(' ')
		b.WriteString("world")

		if b.String() != "hello world" {
			t.Errorf("String() = %q, want %q", b.String(), "hello world")
		}
		if b.Len() != 11 {
			t.Errorf("Len() = %d, want 11", b.Len())
		}

		PutStringBuilder(b)
	})

	t.Run("reset on reuse", func(t *testing.T) {
		b := GetStringBuilder()
		b.WriteString("test")
		PutStringBuilder(b)

		b2 := GetStringBuilder()
		if b2.Len() != 0 {
			t.Errorf("reused builder Len() = %d, want 0", b2.Len())
		}
		PutStringBuilder(b2)
	})

	t.Run("nil put does not panic", func(t *testing.T) {
		PutStringBuilder(nil) // Should not panic
	})

	t.Run("oversized buffer not pooled", func(t *testing.T) {
		b := GetStringBuilder()
		// Write > 64KB to exceed pool limit
		for i := 0; i < 70000; i++ {
			b.WriteByte('x')
		}
		PutStringBuilder(b) // Should not panic, just not pool it
	})
}

// =============================================================================
// Byte Buffer Pool Tests
// =============================================================================

func TestByteBufferPool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("get returns empty buffer", func(t *testing.T) {
		buf := GetByteBuffer()
		if len(buf) != 0 {
			t.Errorf("len = %d, want 0", len(buf))
		}
		if cap(buf) == 0 {
			t.Error("cap should be > 0")
		}
		PutByteBuffer(buf)
	})

	t.Run("reuse", func(t *testing.T) {
		buf := GetByteBuffer()
		buf = append(buf, []byte("test data")...)
		PutByteBuffer(buf)

		buf2 := GetByteBuffer()
		if len(buf2) != 0 {
			t.Errorf("reused buffer len = %d, want 0", len(buf2))
		}
		PutByteBuffer(buf2)
	})
}

// =============================================================================
// Map Pool Tests
// =============================================================================

func TestMapPool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("get returns empty map", func(t *testing.T) {
		m := GetMap()
		if len(m) != 0 {
			t.Errorf("len = %d, want 0", len(m))
		}
		PutMap(m)
	})

	t.Run("map is cleared on put", func(t *testing.T) {
		m := GetMap()
		m["key1"] = "value1"
		m["key2"] = 123
		PutMap(m)

		m2 := GetMap()
		if len(m2) != 0 {
			t.Errorf("reused map len = %d, want 0", len(m2))
		}
		PutMap(m2)
	})

	t.Run("nil put does not panic", func(t *testing.T) {
		PutMap(nil) // Should not panic
	})

	t.Run("oversized map not pooled", func(t *testing.T) {
		Configure(PoolConfig{Enabled: true, MaxSize: 10})
		defer Configure(PoolConfig{Enabled: true, MaxSize: 1000})

		m := GetMap()
		for i := 0; i < 20; i++ {
			m[string(rune('a'+i))] = i
		}
		PutMap(m) // Should not panic, just not pool it
	})
}

// =============================================================================
// String Slice Pool Tests
// =============================================================================

func TestStringSlicePool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("get returns empty slice", func(t *testing.T) {
		s := GetStringSlice()
		if len(s) != 0 {
			t.Errorf("len = %d, want 0", len(s))
		}
		PutStringSlice(s)
	})

	t.Run("reuse", func(t *testing.T) {
		s := GetStringSlice()
		s = append(s, "hello", "world")
		PutStringSlice(s)

		s2 := GetStringSlice()
		if len(s2) != 0 {
			t.Errorf("reused slice len = %d, want 0", len(s2))
		}
		PutStringSlice(s2)
	})
}

// =============================================================================
// Interface Slice Pool Tests
// =============================================================================

func TestInterfaceSlicePool(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	t.Run("get returns empty slice", func(t *testing.T) {
		s := GetInterfaceSlice()
		if len(s) != 0 {
			t.Errorf("len = %d, want 0", len(s))
		}
		PutInterfaceSlice(s)
	})

	t.Run("references cleared on put", func(t *testing.T) {
		s := GetInterfaceSlice()
		obj := make(map[string]int)
		s = append(s, obj, "test", 123)
		PutInterfaceSlice(s)

		s2 := GetInterfaceSlice()
		if len(s2) != 0 {
			t.Errorf("reused slice len = %d, want 0", len(s2))
		}
		PutInterfaceSlice(s2)
	})

	t.Run("nil put does not panic", func(t *testing.T) {
		PutInterfaceSlice(nil) // Should not panic
	})
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestConcurrentPoolAccess(t *testing.T) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	const goroutines = 100
	const iterations = 100

	t.Run("row slice pool concurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					rows := GetRowSlice()
					rows = append(rows, []interface{}{j})
					PutRowSlice(rows)
				}
			}()
		}

		wg.Wait()
	})

	t.Run("map pool concurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					m := GetMap()
					m["id"] = id
					m["iter"] = j
					PutMap(m)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("string builder pool concurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					b := GetStringBuilder()
					b.WriteString("test")
					_ = b.String()
					PutStringBuilder(b)
				}
			}()
		}

		wg.Wait()
	})
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkRowSlicePool(b *testing.B) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	b.Run("pooled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows := GetRowSlice()
			rows = append(rows, []interface{}{1, "test"})
			PutRowSlice(rows)
		}
	})

	b.Run("unpooled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows := make([][]interface{}, 0, 64)
			rows = append(rows, []interface{}{1, "test"})
			_ = rows
		}
	})
}

func BenchmarkMapPool(b *testing.B) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	b.Run("pooled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := GetMap()
			m["key"] = "value"
			PutMap(m)
		}
	})

	b.Run("unpooled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m := make(map[string]interface{}, 8)
			m["key"] = "value"
			_ = m
		}
	})
}

func BenchmarkStringBuilderPool(b *testing.B) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	b.Run("pooled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sb := GetStringBuilder()
			sb.WriteString("hello world")
			_ = sb.String()
			PutStringBuilder(sb)
		}
	})

	b.Run("unpooled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 0, 256)
			buf = append(buf, "hello world"...)
			_ = string(buf)
		}
	})
}

func BenchmarkConcurrentPoolAccess(b *testing.B) {
	Configure(PoolConfig{Enabled: true, MaxSize: 1000})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := GetMap()
			m["key"] = "value"
			PutMap(m)
		}
	})
}
