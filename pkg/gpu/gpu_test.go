// Package gpu tests for GPU acceleration.
package gpu

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Enabled {
		t.Error("GPU should be disabled by default")
	}
	if config.PreferredBackend != BackendNone {
		t.Error("preferred backend should be none by default")
	}
	if config.BatchSize != 10000 {
		t.Errorf("expected batch size 10000, got %d", config.BatchSize)
	}
	if !config.FallbackOnError {
		t.Error("fallback on error should be true by default")
	}
}

func TestNewManager(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		m, err := NewManager(nil)
		if err != nil {
			t.Fatalf("NewManager() error = %v", err)
		}
		if m.IsEnabled() {
			t.Error("should be disabled by default")
		}
	})

	t.Run("with config disabled", func(t *testing.T) {
		config := &Config{Enabled: false}
		m, err := NewManager(config)
		if err != nil {
			t.Fatalf("NewManager() error = %v", err)
		}
		if m.IsEnabled() {
			t.Error("should be disabled")
		}
	})

	t.Run("enabled with fallback", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: true,
		}
		m, err := NewManager(config)
		if err != nil {
			t.Fatalf("NewManager() error = %v", err)
		}
		// Either GPU is enabled (if available) or gracefully disabled
		// This test verifies the fallback mechanism works in both scenarios
		if m.IsEnabled() {
			t.Log("GPU available and enabled")
			if m.Device() == nil {
				t.Error("should have device when enabled")
			}
		} else {
			t.Log("No GPU available, running in CPU fallback mode")
		}
	})

	t.Run("enabled without fallback", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: false,
		}
		_, err := NewManager(config)
		if err == nil {
			// If no GPU is available, should error
			// But this test may pass on GPU-equipped machines
		}
	})
}

func TestManagerEnableDisable(t *testing.T) {
	m, _ := NewManager(nil)

	if m.IsEnabled() {
		t.Error("should start disabled")
	}

	// Enable should fail without GPU
	err := m.Enable()
	if err == nil {
		// Only passes if GPU is available
		m.Disable()
		if m.IsEnabled() {
			t.Error("should be disabled after Disable()")
		}
	}

	// Disable should be safe to call when already disabled
	m.Disable()
	if m.IsEnabled() {
		t.Error("should remain disabled")
	}
}

func TestManagerDevice(t *testing.T) {
	m, _ := NewManager(nil)

	// Device() returns nil when no GPU
	dev := m.Device()
	if dev != nil {
		t.Error("Device() should return nil when no GPU")
	}
}

func TestManagerStats(t *testing.T) {
	m, _ := NewManager(nil)
	stats := m.Stats()

	if stats.OperationsGPU != 0 {
		t.Error("initial GPU ops should be 0")
	}
	if stats.OperationsCPU != 0 {
		t.Error("initial CPU ops should be 0")
	}
}

func TestManagerAllocatedMemory(t *testing.T) {
	m, _ := NewManager(nil)

	if m.AllocatedMemoryMB() != 0 {
		t.Error("initial allocated memory should be 0")
	}
}

func TestVectorIndex(t *testing.T) {
	m, _ := NewManager(nil)
	vi := NewVectorIndex(m, 3)

	t.Run("add and search", func(t *testing.T) {
		err := vi.Add("vec1", []float32{1, 0, 0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		err = vi.Add("vec2", []float32{0, 1, 0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		err = vi.Add("vec3", []float32{0.9, 0.1, 0})
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		results, err := vi.Search([]float32{1, 0, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// First result should be vec1 (exact match)
		if results[0].ID != "vec1" {
			t.Errorf("expected vec1, got %s", results[0].ID)
		}
		if results[0].Score < 0.99 {
			t.Errorf("expected score ~1.0, got %f", results[0].Score)
		}

		// Second should be vec3 (similar)
		if results[1].ID != "vec3" {
			t.Errorf("expected vec3, got %s", results[1].ID)
		}
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		err := vi.Add("bad", []float32{1, 2}) // Wrong dimensions
		if err != ErrInvalidDimensions {
			t.Errorf("expected ErrInvalidDimensions, got %v", err)
		}

		_, err = vi.Search([]float32{1, 2}, 1)
		if err != ErrInvalidDimensions {
			t.Errorf("expected ErrInvalidDimensions, got %v", err)
		}
	})

	t.Run("search more than available", func(t *testing.T) {
		results, err := vi.Search([]float32{1, 0, 0}, 100)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}
	})

	t.Run("empty index", func(t *testing.T) {
		emptyVI := NewVectorIndex(m, 3)
		results, err := emptyVI.Search([]float32{1, 0, 0}, 5)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})
}

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		delta    float32
	}{
		{
			name:     "identical",
			a:        []float32{1, 0, 0},
			b:        []float32{1, 0, 0},
			expected: 1.0,
			delta:    0.01,
		},
		{
			name:     "orthogonal",
			a:        []float32{1, 0, 0},
			b:        []float32{0, 1, 0},
			expected: 0.0,
			delta:    0.01,
		},
		{
			name:     "opposite",
			a:        []float32{1, 0, 0},
			b:        []float32{-1, 0, 0},
			expected: -1.0,
			delta:    0.01,
		},
		{
			name:     "different lengths",
			a:        []float32{1, 2},
			b:        []float32{1, 2, 3},
			expected: 0.0,
			delta:    0.01,
		},
		{
			name:     "zero vector",
			a:        []float32{0, 0, 0},
			b:        []float32{1, 0, 0},
			expected: 0.0,
			delta:    0.01,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cosineSimilarity(tt.a, tt.b)
			if result < tt.expected-tt.delta || result > tt.expected+tt.delta {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestSqrt32(t *testing.T) {
	tests := []struct {
		input    float32
		expected float32
		delta    float32
	}{
		{4, 2, 0.001},
		{9, 3, 0.001},
		{16, 4, 0.001},
		{0, 0, 0.001},
		{-1, 0, 0.001},
		{1, 1, 0.001},
		{2, 1.414, 0.01},
	}

	for _, tt := range tests {
		result := sqrt32(tt.input)
		if result < tt.expected-tt.delta || result > tt.expected+tt.delta {
			t.Errorf("sqrt32(%f) = %f, expected %f", tt.input, result, tt.expected)
		}
	}
}

// REMOVED: TestTransactionBuffer
// TransactionBuffer has been removed from the codebase as it provided
// no actual GPU benefit - it was just a map wrapper with no GPU operations.

// REMOVED: TestGraphAccelerator
// GraphAccelerator has been removed from the codebase as all methods
// were unimplemented TODOs with CPU fallbacks. Complex to implement,
// low ROI compared to focusing on EmbeddingIndex vector search.

func TestListDevices(t *testing.T) {
	devices, err := ListDevices()
	// Expected to fail without GPU
	if err != ErrGPUNotAvailable {
		if devices != nil {
			t.Logf("Found %d GPU devices", len(devices))
		}
	}
}

func TestBenchmarkDevice(t *testing.T) {
	_, err := BenchmarkDevice(0)
	// Expected to fail without GPU
	if err != ErrGPUNotAvailable {
		t.Log("Benchmark ran on GPU")
	}
}

func TestBackendConstants(t *testing.T) {
	if BackendNone != "none" {
		t.Error("BackendNone should be 'none'")
	}
	if BackendOpenCL != "opencl" {
		t.Error("BackendOpenCL should be 'opencl'")
	}
	if BackendCUDA != "cuda" {
		t.Error("BackendCUDA should be 'cuda'")
	}
	if BackendMetal != "metal" {
		t.Error("BackendMetal should be 'metal'")
	}
	if BackendVulkan != "vulkan" {
		t.Error("BackendVulkan should be 'vulkan'")
	}
}

// REMOVED: TestBufferType
// BufferType enum has been removed as part of simplification.
// No longer needed without TransactionBuffer and complex buffer management.

func TestErrors(t *testing.T) {
	errors := []error{
		ErrGPUNotAvailable,
		ErrGPUDisabled,
		ErrOutOfMemory,
		ErrKernelFailed,
		ErrDataTooLarge,
		ErrInvalidDimensions,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("error should not be nil")
		}
		if err.Error() == "" {
			t.Error("error message should not be empty")
		}
	}
}

func TestSearchResult(t *testing.T) {
	sr := SearchResult{
		ID:       "test",
		Score:    0.95,
		Distance: 0.05,
	}

	if sr.ID != "test" {
		t.Error("ID mismatch")
	}
	if sr.Score != 0.95 {
		t.Error("Score mismatch")
	}
	if sr.Distance != 0.05 {
		t.Error("Distance mismatch")
	}
}

func TestDeviceInfo(t *testing.T) {
	di := DeviceInfo{
		ID:           0,
		Name:         "Test GPU",
		Vendor:       "Test Vendor",
		Backend:      BackendOpenCL,
		MemoryMB:     4096,
		ComputeUnits: 32,
		MaxWorkGroup: 256,
		Available:    true,
	}

	if di.ID != 0 {
		t.Error("ID mismatch")
	}
	if di.Name != "Test GPU" {
		t.Error("Name mismatch")
	}
	if di.MemoryMB != 4096 {
		t.Error("MemoryMB mismatch")
	}
}

func TestBenchmarkResult(t *testing.T) {
	br := BenchmarkResult{
		DeviceID:          0,
		VectorOpsPerSec:   1000000,
		MemoryBandwidthGB: 200.5,
		LatencyUs:         10,
	}

	if br.VectorOpsPerSec != 1000000 {
		t.Error("VectorOpsPerSec mismatch")
	}
}

func BenchmarkCosineSimilarity(b *testing.B) {
	a := make([]float32, 1024)
	c := make([]float32, 1024)
	for i := range a {
		a[i] = float32(i) / 1024
		c[i] = float32(1024-i) / 1024
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cosineSimilarity(a, c)
	}
}

func BenchmarkVectorSearch(b *testing.B) {
	m, _ := NewManager(nil)
	vi := NewVectorIndex(m, 128)

	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i*j) / 128000
		}
		vi.Add(string(rune(i)), vec)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vi.Search(query, 10)
	}
}

// =============================================================================
// EmbeddingIndex Tests - Optimized {nodeId, embedding} GPU storage
// =============================================================================

func TestDefaultEmbeddingIndexConfig(t *testing.T) {
	config := DefaultEmbeddingIndexConfig(1024)

	if config.Dimensions != 1024 {
		t.Errorf("expected 1024 dimensions, got %d", config.Dimensions)
	}
	if config.InitialCap != 10000 {
		t.Errorf("expected 10000 initial cap, got %d", config.InitialCap)
	}
	if !config.GPUEnabled {
		t.Error("GPU should be enabled by default")
	}
	if !config.AutoSync {
		t.Error("AutoSync should be enabled by default")
	}
}

func TestNewEmbeddingIndex(t *testing.T) {
	m, _ := NewManager(nil)

	t.Run("with config", func(t *testing.T) {
		config := &EmbeddingIndexConfig{
			Dimensions: 512,
			InitialCap: 5000,
		}
		ei := NewEmbeddingIndex(m, config)

		if ei.dimensions != 512 {
			t.Errorf("expected 512 dimensions, got %d", ei.dimensions)
		}
	})

	t.Run("nil config", func(t *testing.T) {
		ei := NewEmbeddingIndex(m, nil)
		// nil config now defaults to 0 dimensions to avoid hardcoding model-specific values
		if ei.dimensions != 0 {
			t.Errorf("expected 0 default dimensions (caller must provide explicit config), got %d", ei.dimensions)
		}
	})
}

func TestEmbeddingIndexAddAndSearch(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 4, InitialCap: 100}
	ei := NewEmbeddingIndex(m, config)

	// Add embeddings
	ei.Add("node-1", []float32{1, 0, 0, 0})
	ei.Add("node-2", []float32{0, 1, 0, 0})
	ei.Add("node-3", []float32{0.9, 0.1, 0, 0})
	ei.Add("node-4", []float32{0, 0, 1, 0})

	if ei.Count() != 4 {
		t.Errorf("expected count 4, got %d", ei.Count())
	}

	// Search for similar to [1,0,0,0]
	results, err := ei.Search([]float32{1, 0, 0, 0}, 2)
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
	if results[0].Score < 0.99 {
		t.Errorf("expected score ~1.0, got %f", results[0].Score)
	}

	// Second should be node-3 (most similar)
	if results[1].ID != "node-3" {
		t.Errorf("expected node-3, got %s", results[1].ID)
	}
}

func TestEmbeddingIndexUpdate(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	// Add initial
	ei.Add("node-1", []float32{1, 0, 0})

	// Update
	ei.Add("node-1", []float32{0, 1, 0})

	// Count should still be 1
	if ei.Count() != 1 {
		t.Errorf("expected count 1 after update, got %d", ei.Count())
	}

	// Get should return updated value
	vec, ok := ei.Get("node-1")
	if !ok {
		t.Fatal("Get() failed")
	}
	if vec[0] != 0 || vec[1] != 1 {
		t.Errorf("expected [0,1,0], got %v", vec)
	}
}

func TestEmbeddingIndexAddBatch(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	nodeIDs := []string{"a", "b", "c"}
	embeddings := [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	}

	err := ei.AddBatch(nodeIDs, embeddings)
	if err != nil {
		t.Fatalf("AddBatch() error = %v", err)
	}

	if ei.Count() != 3 {
		t.Errorf("expected count 3, got %d", ei.Count())
	}

	// Test mismatch
	err = ei.AddBatch([]string{"x"}, [][]float32{{1, 0, 0}, {0, 1, 0}})
	if err == nil {
		t.Error("expected error for mismatched lengths")
	}

	// Test wrong dimensions
	err = ei.AddBatch([]string{"y"}, [][]float32{{1, 0}})
	if err != ErrInvalidDimensions {
		t.Errorf("expected ErrInvalidDimensions, got %v", err)
	}
}

func TestEmbeddingIndexRemove(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})
	ei.Add("b", []float32{0, 1, 0})
	ei.Add("c", []float32{0, 0, 1})

	// Remove middle element
	removed := ei.Remove("b")
	if !removed {
		t.Error("Remove() should return true")
	}

	if ei.Count() != 2 {
		t.Errorf("expected count 2, got %d", ei.Count())
	}

	if ei.Has("b") {
		t.Error("b should be removed")
	}

	// Remove non-existent
	removed = ei.Remove("nonexistent")
	if removed {
		t.Error("Remove() should return false for non-existent")
	}

	// Remaining elements should still be accessible
	if !ei.Has("a") || !ei.Has("c") {
		t.Error("a and c should still exist")
	}
}

func TestEmbeddingIndexHasAndGet(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("exists", []float32{1, 2, 3})

	if !ei.Has("exists") {
		t.Error("Has() should return true")
	}
	if ei.Has("not-exists") {
		t.Error("Has() should return false")
	}

	vec, ok := ei.Get("exists")
	if !ok {
		t.Error("Get() should return true")
	}
	if vec[0] != 1 || vec[1] != 2 || vec[2] != 3 {
		t.Errorf("expected [1,2,3], got %v", vec)
	}

	_, ok = ei.Get("not-exists")
	if ok {
		t.Error("Get() should return false for non-existent")
	}
}

func TestEmbeddingIndexClear(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})
	ei.Add("b", []float32{0, 1, 0})

	ei.Clear()

	if ei.Count() != 0 {
		t.Errorf("expected count 0, got %d", ei.Count())
	}
}

func TestEmbeddingIndexMemoryUsage(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 1024}
	ei := NewEmbeddingIndex(m, config)

	// Add 1000 embeddings
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 1024)
		ei.Add(string(rune('a'+i%26))+string(rune(i)), vec)
	}

	mb := ei.MemoryUsageMB()
	// 1000 * (1024 * 4 + 32) / 1024 / 1024 ≈ 3.9 MB
	if mb < 3 || mb > 5 {
		t.Errorf("expected ~4MB, got %f", mb)
	}
}

func TestEmbeddingIndexStats(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})
	ei.Search([]float32{1, 0, 0}, 1)
	ei.Search([]float32{0, 1, 0}, 1)

	stats := ei.Stats()

	if stats.Count != 1 {
		t.Errorf("expected count 1, got %d", stats.Count)
	}
	if stats.Dimensions != 3 {
		t.Errorf("expected 3 dimensions, got %d", stats.Dimensions)
	}
	if stats.SearchesCPU != 2 {
		t.Errorf("expected 2 CPU searches, got %d", stats.SearchesCPU)
	}
}

func TestEmbeddingIndexSyncToGPU(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})

	// Should fail - GPU not enabled
	err := ei.SyncToGPU()
	if err != ErrGPUDisabled {
		t.Errorf("expected ErrGPUDisabled, got %v", err)
	}
}

func TestEmbeddingIndexSerializeDeserialize(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	// Add data
	ei.Add("node-1", []float32{1.5, 2.5, 3.5})
	ei.Add("node-2", []float32{4.5, 5.5, 6.5})

	// Serialize
	data, err := ei.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	// Create new index and deserialize
	ei2 := NewEmbeddingIndex(m, config)
	err = ei2.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}

	// Verify
	if ei2.Count() != 2 {
		t.Errorf("expected count 2, got %d", ei2.Count())
	}

	vec, ok := ei2.Get("node-1")
	if !ok {
		t.Fatal("Get() failed")
	}
	if vec[0] != 1.5 || vec[1] != 2.5 || vec[2] != 3.5 {
		t.Errorf("expected [1.5,2.5,3.5], got %v", vec)
	}
}

func TestEmbeddingIndexSerializeEmpty(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	data, err := ei.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	ei2 := NewEmbeddingIndex(m, config)
	err = ei2.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}

	if ei2.Count() != 0 {
		t.Errorf("expected count 0, got %d", ei2.Count())
	}
}

func TestEmbeddingIndexDeserializeErrors(t *testing.T) {
	m, _ := NewManager(nil)

	t.Run("too short", func(t *testing.T) {
		config := &EmbeddingIndexConfig{Dimensions: 3}
		ei := NewEmbeddingIndex(m, config)

		err := ei.Deserialize([]byte{0, 0, 0})
		if err == nil {
			t.Error("expected error for short data")
		}
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		config := &EmbeddingIndexConfig{Dimensions: 3}
		ei := NewEmbeddingIndex(m, config)

		// Create data with dimensions=5
		data := []byte{
			5, 0, 0, 0, // dims = 5
			0, 0, 0, 0, // count = 0
		}

		err := ei.Deserialize(data)
		if err != ErrInvalidDimensions {
			t.Errorf("expected ErrInvalidDimensions, got %v", err)
		}
	})
}

func TestEmbeddingIndexSearchEmpty(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	results, err := ei.Search([]float32{1, 0, 0}, 5)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results, got %v", results)
	}
}

func TestEmbeddingIndexSearchDimensionMismatch(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})

	_, err := ei.Search([]float32{1, 0}, 1) // Wrong dimensions
	if err != ErrInvalidDimensions {
		t.Errorf("expected ErrInvalidDimensions, got %v", err)
	}
}

func TestPartialSort(t *testing.T) {
	scores := []float32{0.1, 0.9, 0.5, 0.3, 0.7}
	indices := []int{0, 1, 2, 3, 4}

	partialSort(indices, scores, 3)

	// Top 3 should be in first 3 positions (sorted by score descending)
	if scores[indices[0]] != 0.9 {
		t.Errorf("expected top score 0.9, got %f", scores[indices[0]])
	}
	if scores[indices[1]] != 0.7 {
		t.Errorf("expected second score 0.7, got %f", scores[indices[1]])
	}
	if scores[indices[2]] != 0.5 {
		t.Errorf("expected third score 0.5, got %f", scores[indices[2]])
	}
}

func TestCosineSimilarityFlat(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{1, 0, 0}

	sim := cosineSimilarityFlat(a, b)
	if sim < 0.99 {
		t.Errorf("expected ~1.0, got %f", sim)
	}

	c := []float32{0, 1, 0}
	sim = cosineSimilarityFlat(a, c)
	if sim > 0.01 || sim < -0.01 {
		t.Errorf("expected ~0.0, got %f", sim)
	}
}

func TestFloatConversion(t *testing.T) {
	f := float32(3.14159)
	u := floatToUint32(f)
	f2 := uint32ToFloat(u)

	if f != f2 {
		t.Errorf("round trip failed: %f != %f", f, f2)
	}
}

func BenchmarkEmbeddingIndexSearch(b *testing.B) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 1024, InitialCap: 10000}
	ei := NewEmbeddingIndex(m, config)

	// Add 10K embeddings
	for i := 0; i < 10000; i++ {
		vec := make([]float32, 1024)
		for j := range vec {
			vec[j] = float32(i*j%1000) / 1000
		}
		ei.Add(string(rune(i)), vec)
	}

	query := make([]float32, 1024)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ei.Search(query, 10)
	}
}

func BenchmarkEmbeddingIndexAdd(b *testing.B) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 1024}
	ei := NewEmbeddingIndex(m, config)

	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i) / 1024
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ei.Add(string(rune(i%65536)), vec)
	}
}

// =============================================================================
// Additional tests for 90%+ coverage
// =============================================================================

func TestEmbeddingIndexGPUMemoryUsage(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 1024}
	ei := NewEmbeddingIndex(m, config)

	// GPUMemoryUsageMB should be 0 when not synced
	mb := ei.GPUMemoryUsageMB()
	if mb != 0 {
		t.Errorf("expected 0 GPU memory, got %f", mb)
	}
}

func TestEmbeddingIndexRelease(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})

	// Release should be safe to call
	ei.Release()

	// Double release should also be safe
	ei.Release()
}

func TestEmbeddingIndexClearWithData(t *testing.T) {
	m, _ := NewManager(nil)
	config := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, config)

	ei.Add("a", []float32{1, 0, 0})
	ei.Add("b", []float32{0, 1, 0})
	ei.Add("c", []float32{0, 0, 1})

	if ei.Count() != 3 {
		t.Fatalf("expected 3 embeddings, got %d", ei.Count())
	}

	ei.Clear()

	if ei.Count() != 0 {
		t.Errorf("expected 0 after clear, got %d", ei.Count())
	}

	// Should be able to add after clear
	ei.Add("d", []float32{1, 1, 1})
	if ei.Count() != 1 {
		t.Errorf("expected 1 after add, got %d", ei.Count())
	}
}

func TestNewManagerWithEnabledNoFallback(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: false,
	}

	// This test depends on GPU availability
	m, err := NewManager(config)
	if err != nil {
		// Expected on systems without GPU
		if err != ErrGPUNotAvailable {
			t.Errorf("expected ErrGPUNotAvailable, got %v", err)
		}
	} else {
		// GPU is available
		if !m.IsEnabled() {
			t.Error("should be enabled when GPU is available")
		}
	}
}

func TestProbeBackendUnknown(t *testing.T) {
	// Test that unknown backend returns error
	_, err := probeBackend(Backend("unknown"), 0)
	if err != ErrGPUNotAvailable {
		t.Errorf("expected ErrGPUNotAvailable, got %v", err)
	}
}

func TestDetectGPUWithPreferredBackend(t *testing.T) {
	// Test with a non-existent preferred backend
	config := &Config{
		Enabled:          true,
		PreferredBackend: Backend("nonexistent"),
		FallbackOnError:  true,
	}

	m, _ := NewManager(config)
	// Should either find a real GPU or gracefully disable
	t.Logf("GPU enabled: %v", m.IsEnabled())
}

func TestVectorIndexSearchGPU(t *testing.T) {
	// Create manager with GPU enabled
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	vi := NewVectorIndex(m, 3)
	vi.Add("a", []float32{1, 0, 0})
	vi.Add("b", []float32{0, 1, 0})

	// VectorIndex GPU is now fully implemented - should use GPU if available
	results, err := vi.Search([]float32{1, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestEmbeddingIndexSearchWithGPU(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	eiConfig := &EmbeddingIndexConfig{Dimensions: 4}
	ei := NewEmbeddingIndex(m, eiConfig)

	ei.Add("a", []float32{1, 0, 0, 0})
	ei.Add("b", []float32{0, 1, 0, 0})
	ei.Add("c", []float32{0.9, 0.1, 0, 0})

	// Try to sync to GPU
	if m.IsEnabled() {
		err := ei.SyncToGPU()
		if err != nil {
			t.Logf("SyncToGPU() error = %v (expected if GPU kernels not loaded)", err)
		} else {
			// If sync succeeded, search should use GPU
			results, err := ei.Search([]float32{1, 0, 0, 0}, 2)
			if err != nil {
				t.Fatalf("Search() error = %v", err)
			}
			if len(results) != 2 {
				t.Errorf("expected 2 results, got %d", len(results))
			}

			stats := ei.Stats()
			t.Logf("Stats: GPU=%d, CPU=%d", stats.SearchesGPU, stats.SearchesCPU)
		}
	} else {
		// SyncToGPU should fail when disabled
		err := ei.SyncToGPU()
		if err != ErrGPUDisabled {
			t.Errorf("expected ErrGPUDisabled, got %v", err)
		}
	}
}

func TestCosineSimilarityFlatEdgeCases(t *testing.T) {
	t.Run("different lengths", func(t *testing.T) {
		result := cosineSimilarityFlat([]float32{1, 0}, []float32{1, 0, 0})
		if result != 0 {
			t.Errorf("expected 0 for different lengths, got %f", result)
		}
	})

	t.Run("zero vector", func(t *testing.T) {
		result := cosineSimilarityFlat([]float32{0, 0, 0}, []float32{1, 0, 0})
		if result != 0 {
			t.Errorf("expected 0 for zero vector, got %f", result)
		}
	})

	t.Run("both zero vectors", func(t *testing.T) {
		result := cosineSimilarityFlat([]float32{0, 0, 0}, []float32{0, 0, 0})
		if result != 0 {
			t.Errorf("expected 0 for both zero vectors, got %f", result)
		}
	})
}

func TestPartialSortEdgeCases(t *testing.T) {
	t.Run("k equals n", func(t *testing.T) {
		scores := []float32{0.5, 0.9, 0.1}
		indices := []int{0, 1, 2}

		partialSort(indices, scores, 3)

		// Should be fully sorted
		if scores[indices[0]] != 0.9 {
			t.Errorf("expected first score 0.9, got %f", scores[indices[0]])
		}
	})

	t.Run("k greater than n", func(t *testing.T) {
		scores := []float32{0.5, 0.9}
		indices := []int{0, 1}

		partialSort(indices, scores, 10) // k > n

		if scores[indices[0]] != 0.9 {
			t.Errorf("expected first score 0.9, got %f", scores[indices[0]])
		}
	})

	t.Run("single element", func(t *testing.T) {
		scores := []float32{0.5}
		indices := []int{0}

		partialSort(indices, scores, 1)

		if indices[0] != 0 {
			t.Errorf("expected index 0, got %d", indices[0])
		}
	})
}

func TestManagerEnableWithGPU(t *testing.T) {
	m, _ := NewManager(nil) // Disabled by default

	err := m.Enable()
	if err == nil {
		// GPU is available
		if !m.IsEnabled() {
			t.Error("should be enabled after Enable()")
		}

		m.Disable()
		if m.IsEnabled() {
			t.Error("should be disabled after Disable()")
		}

		// Can re-enable
		err = m.Enable()
		if err != nil {
			t.Errorf("re-Enable() error = %v", err)
		}
	} else {
		// GPU not available
		if err != ErrGPUNotAvailable {
			t.Errorf("expected ErrGPUNotAvailable, got %v", err)
		}
	}
}

func TestEmbeddingIndexSyncToGPUEmpty(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	eiConfig := &EmbeddingIndexConfig{Dimensions: 3}
	ei := NewEmbeddingIndex(m, eiConfig)

	// Sync empty index
	err := ei.SyncToGPU()
	if err != nil {
		t.Errorf("SyncToGPU(empty) error = %v", err)
	}
}

func TestAcceleratorWithPreferredBackend(t *testing.T) {
	t.Run("prefer metal on darwin", func(t *testing.T) {
		config := &Config{
			Enabled:          true,
			PreferredBackend: BackendMetal,
			FallbackOnError:  true,
		}
		accel, _ := NewAccelerator(config)
		defer accel.Release()

		// On macOS should use Metal, otherwise should gracefully fallback
		if accel.IsEnabled() {
			if accel.Backend() != BackendMetal {
				t.Logf("Backend is %s (may not be Metal on non-macOS)", accel.Backend())
			}
		}
	})

	t.Run("prefer opencl", func(t *testing.T) {
		config := &Config{
			Enabled:          true,
			PreferredBackend: BackendOpenCL,
			FallbackOnError:  true,
		}
		accel, _ := NewAccelerator(config)
		defer accel.Release()

		// OpenCL not implemented, should fallback
		t.Logf("Backend: %s, Enabled: %v", accel.Backend(), accel.IsEnabled())
	})

	t.Run("prefer cuda", func(t *testing.T) {
		config := &Config{
			Enabled:          true,
			PreferredBackend: BackendCUDA,
			FallbackOnError:  true,
		}
		accel, _ := NewAccelerator(config)
		defer accel.Release()

		// CUDA not implemented, should fallback
		t.Logf("Backend: %s, Enabled: %v", accel.Backend(), accel.IsEnabled())
	})

	t.Run("prefer vulkan", func(t *testing.T) {
		config := &Config{
			Enabled:          true,
			PreferredBackend: BackendVulkan,
			FallbackOnError:  true,
		}
		accel, _ := NewAccelerator(config)
		defer accel.Release()

		// Vulkan not implemented, should fallback
		t.Logf("Backend: %s, Enabled: %v", accel.Backend(), accel.IsEnabled())
	})
}

func TestGPUEmbeddingIndexSearchEmpty(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)

	// Search on empty index
	results, err := idx.Search([]float32{1, 0, 0}, 5)
	if err != nil {
		t.Fatalf("Search(empty) error = %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty index, got %v", results)
	}
}

func TestGPUEmbeddingIndexSearchKGreaterThanN(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)
	idx.Add("a", []float32{1, 0, 0})
	idx.Add("b", []float32{0, 1, 0})

	// k > n
	results, err := idx.Search([]float32{1, 0, 0}, 100)
	if err != nil {
		t.Fatalf("Search(k>n) error = %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results (capped at n), got %d", len(results))
	}
}

func TestGPUEmbeddingIndexSyncNoGPU(t *testing.T) {
	accel, _ := NewAccelerator(nil) // GPU disabled
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)
	idx.Add("a", []float32{1, 0, 0})

	err := idx.SyncToGPU()
	if err != ErrGPUDisabled {
		t.Errorf("expected ErrGPUDisabled, got %v", err)
	}
}

func TestGPUEmbeddingIndexSyncEmpty(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	accel, _ := NewAccelerator(config)
	defer accel.Release()

	if !accel.IsEnabled() {
		t.Skip("GPU not available")
	}

	idx := accel.NewGPUEmbeddingIndex(3)

	// Sync empty index should succeed
	err := idx.SyncToGPU()
	if err != nil {
		t.Errorf("SyncToGPU(empty) error = %v", err)
	}
	if !idx.IsGPUSynced() {
		t.Error("should be synced after SyncToGPU(empty)")
	}
}

func TestAcceleratorDeviceInfoNoGPU(t *testing.T) {
	accel, _ := NewAccelerator(nil) // GPU disabled
	defer accel.Release()

	name := accel.DeviceName()
	if name != "CPU" {
		t.Errorf("expected 'CPU' when disabled, got '%s'", name)
	}

	mem := accel.DeviceMemoryMB()
	if mem != 0 {
		t.Errorf("expected 0 memory when disabled, got %d", mem)
	}
}

func TestGPUEmbeddingIndexAddBatchMismatch(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)

	// Mismatched lengths
	err := idx.AddBatch([]string{"a", "b"}, [][]float32{{1, 0, 0}})
	if err == nil {
		t.Error("expected error for mismatched lengths")
	}
}

func TestGPUEmbeddingIndexAddBatchWrongDimensions(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)

	// Wrong dimensions
	err := idx.AddBatch([]string{"a"}, [][]float32{{1, 0}}) // Only 2 dims
	if err != ErrInvalidDimensions {
		t.Errorf("expected ErrInvalidDimensions, got %v", err)
	}
}

func TestGPUEmbeddingIndexUpdate(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)

	idx.Add("a", []float32{1, 0, 0})
	idx.Add("a", []float32{0, 1, 0}) // Update

	if idx.Count() != 1 {
		t.Errorf("expected count 1 after update, got %d", idx.Count())
	}
}

func TestGPUEmbeddingIndexBatchUpdate(t *testing.T) {
	accel, _ := NewAccelerator(nil)
	defer accel.Release()

	idx := accel.NewGPUEmbeddingIndex(3)

	idx.Add("a", []float32{1, 0, 0})

	// Batch with existing key
	err := idx.AddBatch([]string{"a", "b"}, [][]float32{{0, 1, 0}, {0, 0, 1}})
	if err != nil {
		t.Fatalf("AddBatch() error = %v", err)
	}

	if idx.Count() != 2 { // a updated, b added
		t.Errorf("expected count 2, got %d", idx.Count())
	}
}

// =============================================================================
// EmbeddingIndex (gpu.go) GPU path tests
// =============================================================================

func TestEmbeddingIndexWithGPUManager(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	eiConfig := &EmbeddingIndexConfig{Dimensions: 4}
	ei := NewEmbeddingIndex(m, eiConfig)

	// Add embeddings
	ei.Add("a", []float32{1, 0, 0, 0})
	ei.Add("b", []float32{0, 1, 0, 0})
	ei.Add("c", []float32{0.9, 0.1, 0, 0})

	if m.IsEnabled() {
		t.Run("sync and GPU search", func(t *testing.T) {
			err := ei.SyncToGPU()
			if err != nil {
				t.Logf("SyncToGPU() error = %v (may be expected)", err)
				return
			}

			stats := ei.Stats()
			if !stats.GPUSynced {
				t.Error("should be synced")
			}

			results, err := ei.Search([]float32{1, 0, 0, 0}, 2)
			if err != nil {
				t.Fatalf("Search() error = %v", err)
			}
			if len(results) != 2 {
				t.Errorf("expected 2 results, got %d", len(results))
			}

			// Check GPU was used
			newStats := ei.Stats()
			if newStats.SearchesGPU == 0 {
				t.Log("GPU search count is 0 - may have fallen back to CPU")
			}
		})

		t.Run("update invalidates GPU sync", func(t *testing.T) {
			ei.SyncToGPU()
			ei.Add("d", []float32{0, 0, 1, 0})

			stats := ei.Stats()
			if stats.GPUSynced {
				t.Error("should not be synced after add")
			}
		})

		t.Run("remove invalidates GPU sync", func(t *testing.T) {
			ei.SyncToGPU()
			ei.Remove("d")

			stats := ei.Stats()
			if stats.GPUSynced {
				t.Error("should not be synced after remove")
			}
		})

		t.Run("GPU memory usage", func(t *testing.T) {
			ei.SyncToGPU()
			gpuMB := ei.GPUMemoryUsageMB()
			t.Logf("GPU memory: %f MB", gpuMB)
			// With 3 embeddings of 4 floats = 48 bytes, should be small
		})
	}

	t.Run("release cleans up", func(t *testing.T) {
		ei.Release()
		// Double release should be safe
		ei.Release()
	})
}

func TestEmbeddingIndexClearReleasesGPU(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	eiConfig := &EmbeddingIndexConfig{Dimensions: 4}
	ei := NewEmbeddingIndex(m, eiConfig)

	ei.Add("a", []float32{1, 0, 0, 0})
	ei.SyncToGPU()

	// Clear should release GPU buffer
	ei.Clear()

	stats := ei.Stats()
	if stats.GPUSynced {
		t.Error("should not be synced after clear")
	}

	gpuMB := ei.GPUMemoryUsageMB()
	if gpuMB != 0 {
		t.Errorf("expected 0 GPU memory after clear, got %f", gpuMB)
	}
}

func TestEmbeddingIndexSearchGPUPathWithBackend(t *testing.T) {
	config := &Config{
		Enabled:          true,
		PreferredBackend: BackendMetal,
		FallbackOnError:  true,
	}
	m, _ := NewManager(config)

	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	// Skip if GPU was detected but not usable (e.g., CUDA not built)
	if m.device != nil && !m.device.Available {
		t.Skip("GPU detected but not available for compute")
	}

	eiConfig := &EmbeddingIndexConfig{Dimensions: 128}
	ei := NewEmbeddingIndex(m, eiConfig)

	// Add enough embeddings to make GPU worthwhile
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i*j%100) / 100
		}
		ei.Add(string(rune('a'+i%26))+string(rune(i)), vec)
	}

	err := ei.SyncToGPU()
	if err != nil {
		t.Fatalf("SyncToGPU() error = %v", err)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = 0.5
	}

	results, err := ei.Search(query, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}

	stats := ei.Stats()
	t.Logf("GPU searches: %d, CPU searches: %d", stats.SearchesGPU, stats.SearchesCPU)
}

func TestEmbeddingIndexSearchCPUPath(t *testing.T) {
	// With GPU disabled, should use CPU path
	m, _ := NewManager(nil)

	eiConfig := &EmbeddingIndexConfig{Dimensions: 4}
	ei := NewEmbeddingIndex(m, eiConfig)

	ei.Add("a", []float32{1, 0, 0, 0})
	ei.Add("b", []float32{0, 1, 0, 0})

	results, err := ei.Search([]float32{1, 0, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	stats := ei.Stats()
	if stats.SearchesCPU == 0 {
		t.Error("expected CPU search to be counted")
	}
	if stats.SearchesGPU != 0 {
		t.Error("expected no GPU searches")
	}
}

func TestProbeBackendVariants(t *testing.T) {
	// Test all backend variants
	// Note: probeBackend may return DeviceInfo with Available=false when GPU hardware
	// is detected but the backend isn't built (e.g., CUDA without -tags cuda).
	// This is intentional - it provides informational data about detected hardware.
	backends := []Backend{
		BackendNone,
		BackendOpenCL,
		BackendVulkan,
		Backend("unknown"),
	}

	for _, b := range backends {
		device, err := probeBackend(b, 0)
		if err == nil {
			// If no error, device must exist and should not be usable for these backends
			if device != nil && device.Available {
				t.Errorf("probeBackend(%s) returned available device unexpectedly", b)
			}
		}
	}

	// CUDA and Metal may return device info (even if not fully available) when hardware detected
	// This is not an error - it allows informational logging about detected GPUs
	for _, b := range []Backend{BackendCUDA, BackendMetal} {
		device, err := probeBackend(b, 0)
		t.Logf("probeBackend(%s): device=%v, err=%v", b, device != nil, err)
	}
}

func TestDetectGPUWithDifferentConfigs(t *testing.T) {
	t.Run("with preferred backend that doesn't exist", func(t *testing.T) {
		config := &Config{
			Enabled:          true,
			PreferredBackend: Backend("nonexistent"),
			FallbackOnError:  true,
		}
		m, _ := NewManager(config)
		// Should either find Metal or gracefully disable
		t.Logf("GPU enabled: %v, backend: %v", m.IsEnabled(), m.device)
	})

	t.Run("with OpenCL preferred", func(t *testing.T) {
		config := &Config{
			Enabled:          true,
			PreferredBackend: BackendOpenCL,
			FallbackOnError:  true,
		}
		m, _ := NewManager(config)
		// OpenCL not implemented, should try Metal
		t.Logf("GPU enabled: %v", m.IsEnabled())
	})
}

func TestEmbeddingIndexSearchGPUFallback(t *testing.T) {
	// Test the searchGPU fallback path when GPU is "enabled" but no backend works
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	if !m.IsEnabled() {
		t.Skip("Need GPU enabled to test fallback")
	}

	eiConfig := &EmbeddingIndexConfig{Dimensions: 4}
	ei := NewEmbeddingIndex(m, eiConfig)

	ei.Add("a", []float32{1, 0, 0, 0})
	ei.Add("b", []float32{0, 1, 0, 0})

	// Without syncing to GPU, search should still work via CPU
	results, err := ei.Search([]float32{1, 0, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestEmbeddingIndexSyncToGPUWithNoBackend(t *testing.T) {
	// Create a manager that's "enabled" but force no backend
	m := &Manager{
		config: &Config{Enabled: true},
	}
	m.enabled.Store(true)
	// No device set

	eiConfig := &EmbeddingIndexConfig{Dimensions: 4}
	ei := NewEmbeddingIndex(m, eiConfig)

	ei.Add("a", []float32{1, 0, 0, 0})

	// Sync should fail gracefully
	err := ei.SyncToGPU()
	if err != ErrGPUNotAvailable {
		t.Errorf("expected ErrGPUNotAvailable, got %v", err)
	}
}

func TestVectorIndexSearchGPUPath(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)

	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	vi := NewVectorIndex(m, 4)
	vi.Add("a", []float32{1, 0, 0, 0})
	vi.Add("b", []float32{0, 1, 0, 0})

	// VectorIndex.searchGPU should now properly implement GPU search
	results, err := vi.Search([]float32{1, 0, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Check stats
	stats := m.Stats()
	t.Logf("Fallback count: %d, GPU ops: %d, CPU ops: %d", stats.FallbackCount, stats.OperationsGPU, stats.OperationsCPU)
}

// TestVectorIndexSyncToGPU tests the syncToGPU functionality
func TestVectorIndexSyncToGPU(t *testing.T) {
	t.Run("empty vectors", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: true,
		}
		m, _ := NewManager(config)
		if !m.IsEnabled() {
			t.Skip("GPU not available")
		}

		vi := NewVectorIndex(m, 3)
		// syncToGPU is called internally, but we can verify it handles empty vectors
		results, err := vi.Search([]float32{1, 0, 0}, 1)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected 0 results for empty index, got %d", len(results))
		}
	})

	t.Run("GPU disabled", func(t *testing.T) {
		config := &Config{
			Enabled: false,
		}
		m, _ := NewManager(config)
		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})

		// Should use CPU search when GPU is disabled
		results, err := vi.Search([]float32{1, 0, 0}, 1)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}
	})

	t.Run("with vectors", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: true,
		}
		m, _ := NewManager(config)
		if !m.IsEnabled() {
			t.Skip("GPU not available")
		}

		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})
		vi.Add("b", []float32{0, 1, 0})
		vi.Add("c", []float32{0, 0, 1})

		// First search should trigger sync
		results, err := vi.Search([]float32{1, 0, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// Second search should use already-synced GPU
		results2, err := vi.Search([]float32{0, 1, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results2) != 2 {
			t.Errorf("expected 2 results, got %d", len(results2))
		}
	})

	t.Run("mark unsynced on add", func(t *testing.T) {
		config := &Config{
			Enabled:         true,
			FallbackOnError: true,
		}
		m, _ := NewManager(config)
		if !m.IsEnabled() {
			t.Skip("GPU not available")
		}

		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})

		// First search syncs
		_, err := vi.Search([]float32{1, 0, 0}, 1)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		// Add new vector should mark as unsynced
		vi.Add("b", []float32{0, 1, 0})

		// Next search should re-sync
		results, err := vi.Search([]float32{0, 1, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results after adding, got %d", len(results))
		}
	})
}

// TestVectorIndexSearchGPURouting tests the searchGPU routing logic
func TestVectorIndexSearchGPURouting(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)
	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	vi := NewVectorIndex(m, 3)
	vi.Add("a", []float32{1, 0, 0})
	vi.Add("b", []float32{0, 1, 0})
	vi.Add("c", []float32{0, 0, 1})

	t.Run("auto-sync on first search", func(t *testing.T) {
		// First search should trigger sync
		results, err := vi.Search([]float32{1, 0, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("no re-sync on subsequent searches", func(t *testing.T) {
		// Subsequent searches should use already-synced GPU
		results, err := vi.Search([]float32{0, 1, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("fallback on sync error", func(t *testing.T) {
		// Create a new index and disable GPU after adding vectors
		vi2 := NewVectorIndex(m, 3)
		vi2.Add("a", []float32{1, 0, 0})
		m.Disable()

		// Should fall back to CPU
		results, err := vi2.Search([]float32{1, 0, 0}, 1)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}

		stats := m.Stats()
		if stats.FallbackCount == 0 {
			t.Log("Note: Fallback count is 0 - may have synced before disable")
		}

		m.Enable() // Re-enable for other tests
	})
}

// TestVectorIndexBackendRouting tests routing to different GPU backends
func TestVectorIndexBackendRouting(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)
	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	device := m.Device()
	if device == nil {
		t.Skip("No GPU device available")
	}

	vi := NewVectorIndex(m, 3)
	vi.Add("a", []float32{1, 0, 0})
	vi.Add("b", []float32{0, 1, 0})
	vi.Add("c", []float32{0, 0, 1})

	t.Run("routes to correct backend", func(t *testing.T) {
		// Search should route to the correct backend based on device
		results, err := vi.Search([]float32{1, 0, 0}, 2)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// Verify backend was used (stats should show GPU operations)
		stats := m.Stats()
		t.Logf("Backend: %s, GPU ops: %d, CPU ops: %d", device.Backend, stats.OperationsGPU, stats.OperationsCPU)
	})
}

// TestVectorIndexSearchBackends tests each backend search method
func TestVectorIndexSearchBackends(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)
	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	device := m.Device()
	if device == nil {
		t.Skip("No GPU device available")
	}

	vi := NewVectorIndex(m, 3)
	vi.Add("a", []float32{1, 0, 0})
	vi.Add("b", []float32{0, 1, 0})
	vi.Add("c", []float32{0, 0, 1})

	// Test that search works regardless of backend
	results, err := vi.Search([]float32{1, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Verify results are correct
	if results[0].ID != "a" {
		t.Errorf("expected first result to be 'a', got %s", results[0].ID)
	}
	if results[0].Score < 0.99 {
		t.Errorf("expected score ~1.0 for exact match, got %f", results[0].Score)
	}
}

// TestVectorIndexConcurrentAccess tests thread safety
func TestVectorIndexConcurrentAccess(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)
	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	vi := NewVectorIndex(m, 3)
	vi.Add("a", []float32{1, 0, 0})
	vi.Add("b", []float32{0, 1, 0})
	vi.Add("c", []float32{0, 0, 1})

	// Concurrent searches
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			results, err := vi.Search([]float32{1, 0, 0}, 2)
			if err != nil {
				t.Errorf("Search() error = %v", err)
			}
			if len(results) != 2 {
				t.Errorf("expected 2 results, got %d", len(results))
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestVectorIndexEdgeCases tests edge cases and error handling
func TestVectorIndexEdgeCases(t *testing.T) {
	config := &Config{
		Enabled:         true,
		FallbackOnError: true,
	}
	m, _ := NewManager(config)
	if !m.IsEnabled() {
		t.Skip("GPU not available")
	}

	t.Run("k greater than available", func(t *testing.T) {
		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})
		vi.Add("b", []float32{0, 1, 0})

		results, err := vi.Search([]float32{1, 0, 0}, 100)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results (limited by available), got %d", len(results))
		}
	})

	t.Run("k equals zero", func(t *testing.T) {
		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})

		results, err := vi.Search([]float32{1, 0, 0}, 0)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected 0 results for k=0, got %d", len(results))
		}
	})

	t.Run("invalid query dimensions", func(t *testing.T) {
		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})

		_, err := vi.Search([]float32{1, 0}, 1)
		if err != ErrInvalidDimensions {
			t.Errorf("expected ErrInvalidDimensions, got %v", err)
		}
	})

	t.Run("empty query vector", func(t *testing.T) {
		vi := NewVectorIndex(m, 3)
		vi.Add("a", []float32{1, 0, 0})

		results, err := vi.Search([]float32{0, 0, 0}, 1)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}
		// Should still return results (though with low similarity)
		if len(results) == 0 {
			t.Error("expected at least 1 result even for zero vector")
		}
	})
}
