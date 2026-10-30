package gpu

import (
	"fmt"
	"runtime"
	"testing"
)

func TestNewAccelerator(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		accel, err := NewAccelerator(nil)
		if err != nil {
			t.Fatalf("NewAccelerator() error = %v", err)
		}
		defer accel.Release()

		if accel.IsEnabled() {
			t.Error("should be disabled by default")
		}
		if accel.Backend() != BackendNone {
			t.Error("backend should be none")
		}
	})

	t.Run("enabled with fallback", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: true,
		}
		accel, err := NewAccelerator(config)
		if err != nil {
			t.Fatalf("NewAccelerator() error = %v", err)
		}
		defer accel.Release()

		// On macOS with Apple Silicon, should be enabled
		// On other platforms, should fall back gracefully
		if runtime.GOOS == "darwin" && accel.IsEnabled() {
			t.Logf("GPU enabled: %s (%s)", accel.DeviceName(), accel.Backend())
		} else {
			t.Log("GPU not available, running in CPU mode")
		}
	})

	t.Run("device info", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: true,
		}
		accel, err := NewAccelerator(config)
		if err != nil {
			t.Fatalf("NewAccelerator() error = %v", err)
		}
		defer accel.Release()

		name := accel.DeviceName()
		mem := accel.DeviceMemoryMB()

		if accel.IsEnabled() {
			if name == "" || name == "CPU" {
				t.Error("expected GPU device name")
			}
			t.Logf("Device: %s, Memory: %d MB", name, mem)
		}
	})
}

func TestAcceleratorStats(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	stats := accel.Stats()
	if stats.SearchesGPU != 0 {
		t.Error("initial GPU searches should be 0")
	}
	if stats.SearchesCPU != 0 {
		t.Error("initial CPU searches should be 0")
	}
}

func TestGPUEmbeddingIndex(t *testing.T) {
	accel, _ := NewAccelerator(&Config{
		Enabled:         true,
		FallbackOnError: true,
	})
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(4)

	t.Run("add and count", func(t *testing.T) {
		err := idx.Add("node-1", []float32{1, 0, 0, 0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		err = idx.Add("node-2", []float32{0, 1, 0, 0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		if idx.Count() != 2 {
			t.Errorf("expected count 2, got %d", idx.Count())
		}
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		err := idx.Add("bad", []float32{1, 0}) // Wrong dims
		if err != ErrInvalidDimensions {
			t.Errorf("expected ErrInvalidDimensions, got %v", err)
		}
	})

	t.Run("search CPU", func(t *testing.T) {
		results, err := idx.Search([]float32{1, 0, 0, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// First should be node-1 (exact match)
		if results[0].ID != "node-1" {
			t.Errorf("expected node-1, got %s", results[0].ID)
		}
	})

	t.Run("sync and search GPU", func(t *testing.T) {
		if !accel.IsEnabled() {
			t.Skip("GPU not available")
		}

		err := idx.SyncToGPU()
		if err != nil {
			t.Fatalf("SyncToGPU() error = %v", err)
		}

		if !idx.IsGPUSynced() {
			t.Error("should be synced after SyncToGPU")
		}

		results, err := idx.Search([]float32{1, 0, 0, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		stats := idx.Stats()
		if stats.SearchesGPU == 0 {
			t.Error("expected GPU search to be recorded")
		}
	})

	t.Run("update invalidates sync", func(t *testing.T) {
		idx.Add("node-3", []float32{0, 0, 1, 0})

		if idx.IsGPUSynced() {
			t.Error("should not be synced after add")
		}
	})

	t.Run("remove", func(t *testing.T) {
		removed := idx.Remove("node-2")
		if !removed {
			t.Error("Remove() should return true")
		}

		if idx.Count() != 2 {
			t.Errorf("expected count 2 after remove, got %d", idx.Count())
		}

		removed = idx.Remove("nonexistent")
		if removed {
			t.Error("Remove() should return false for nonexistent")
		}
	})

	idx.Release()
}

func TestGPUEmbeddingIndexBatch(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)

	nodeIDs := []string{"a", "b", "c"}
	embeddings := [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	}

	err := idx.AddBatch(nodeIDs, embeddings)
	if err != nil {
		t.Fatalf("AddBatch() error = %v", err)
	}

	if idx.Count() != 3 {
		t.Errorf("expected count 3, got %d", idx.Count())
	}
}

func TestGPUEmbeddingIndexStats(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(1024)

	// Add some data - use unique keys for each vector
	for i := 0; i < 100; i++ {
		vec := make([]float32, 1024)
		for j := range vec {
			vec[j] = float32(i*j%1000) / 1000
		}
		idx.Add(fmt.Sprintf("node-%d", i), vec)
	}

	stats := idx.Stats()

	if stats.Count != 100 {
		t.Errorf("expected count 100, got %d", stats.Count)
	}
	if stats.Dimensions != 1024 {
		t.Errorf("expected dimensions 1024, got %d", stats.Dimensions)
	}
	if stats.MemoryMB < 0.1 {
		t.Errorf("expected MemoryMB > 0.1, got %f", stats.MemoryMB)
	}
}

func BenchmarkGPUEmbeddingIndexSearch(b *testing.B) {
	accel, _ := NewAccelerator(&Config{
		Enabled:         true,
		FallbackOnError: true,
	})
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(1024)

	// Add 10K embeddings
	for i := 0; i < 10000; i++ {
		vec := make([]float32, 1024)
		for j := range vec {
			vec[j] = float32(i*j%1000) / 1000
		}
		idx.Add(string(rune(i)), vec)
	}

	// Sync to GPU if available
	if accel.IsEnabled() {
		if err := idx.SyncToGPU(); err != nil {
			b.Logf("GPU sync failed: %v", err)
		}
	}

	query := make([]float32, 1024)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Search(query, 10)
	}

	stats := idx.Stats()
	b.Logf("GPU searches: %d, CPU searches: %d", stats.SearchesGPU, stats.SearchesCPU)
}

func BenchmarkGPUEmbeddingIndexAdd(b *testing.B) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(1024)

	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i) / 1024
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add(string(rune(i%65536)), vec)
	}
}

func BenchmarkGPUvsCPUSearch(b *testing.B) {
	// Benchmark to compare GPU vs CPU performance
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		// CPU-only
		b.Run(fmt.Sprintf("CPU_%d", size), func(b *testing.B) {
			accel, _ := NewAccelerator(nil) // GPU disabled
			idx := accel.NewGPUEmbeddingIndex(1024)

			for i := 0; i < size; i++ {
				vec := make([]float32, 1024)
				for j := range vec {
					vec[j] = float32(i*j%1000) / 1000
				}
				idx.Add(string(rune(i)), vec)
			}

			query := make([]float32, 1024)
			for i := range query {
				query[i] = 0.5
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.Search(query, 10)
			}
		})

		// GPU (if available)
		b.Run(fmt.Sprintf("GPU_%d", size), func(b *testing.B) {
			accel, _ := NewAccelerator(&Config{
				Enabled:         true,
				FallbackOnError: true,
			})

			if !accel.IsEnabled() {
				b.Skip("GPU not available")
			}

			idx := accel.NewGPUEmbeddingIndex(1024)

			for i := 0; i < size; i++ {
				vec := make([]float32, 1024)
				for j := range vec {
					vec[j] = float32(i*j%1000) / 1000
				}
				idx.Add(string(rune(i)), vec)
			}

			idx.SyncToGPU()

			query := make([]float32, 1024)
			for i := range query {
				query[i] = 0.5
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.Search(query, 10)
			}
		})
	}
}
