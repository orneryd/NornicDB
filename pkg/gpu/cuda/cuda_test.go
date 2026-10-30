//go:build cuda && (linux || windows)
// +build cuda
// +build linux windows

package cuda

import (
	"testing"
)

func TestIsAvailable(t *testing.T) {
	// This test runs on systems with CUDA
	// IsAvailable should return true or false based on actual hardware
	available := IsAvailable()
	t.Logf("CUDA available: %v", available)
}

func TestDeviceCount(t *testing.T) {
	count := DeviceCount()
	t.Logf("CUDA device count: %d", count)

	if IsAvailable() && count == 0 {
		t.Error("CUDA is available but device count is 0")
	}
	if !IsAvailable() && count > 0 {
		t.Error("CUDA not available but device count > 0")
	}
}

func TestNewDevice(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice(0) failed: %v", err)
	}
	defer device.Release()

	// Verify device properties
	if device.ID() != 0 {
		t.Errorf("Device ID = %d, want 0", device.ID())
	}

	if device.Name() == "" {
		t.Error("Device name is empty")
	}
	t.Logf("Device name: %s", device.Name())

	if device.MemoryBytes() == 0 {
		t.Error("Device memory is 0")
	}
	t.Logf("Device memory: %d MB", device.MemoryMB())

	major, minor := device.ComputeCapability()
	if major == 0 {
		t.Error("Compute capability major is 0")
	}
	t.Logf("Compute capability: %d.%d", major, minor)
}

func TestNewDeviceInvalidID(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	_, err := NewDevice(999)
	if err == nil {
		t.Error("NewDevice(999) should fail")
	}
}

func TestDeviceDoubleRelease(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}

	// First release should work
	device.Release()

	// Second release should not panic
	device.Release()
}

func TestNewBuffer(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Create buffer with data
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	buffer, err := device.NewBuffer(data, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	// Verify size
	expectedSize := uint64(len(data) * 4) // 4 bytes per float32
	if buffer.Size() != expectedSize {
		t.Errorf("Buffer size = %d, want %d", buffer.Size(), expectedSize)
	}

	// Read back data
	result := buffer.ReadFloat32(len(data))
	if len(result) != len(data) {
		t.Fatalf("ReadFloat32 returned %d elements, want %d", len(result), len(data))
	}

	for i, v := range result {
		if v != data[i] {
			t.Errorf("ReadFloat32[%d] = %f, want %f", i, v, data[i])
		}
	}
}

func TestNewBufferPinned(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	data := []float32{1.0, 2.0, 3.0}
	buffer, err := device.NewBuffer(data, MemoryPinned)
	if err != nil {
		t.Fatalf("NewBuffer(MemoryPinned) failed: %v", err)
	}
	defer buffer.Release()

	result := buffer.ReadFloat32(len(data))
	for i, v := range result {
		if v != data[i] {
			t.Errorf("ReadFloat32[%d] = %f, want %f", i, v, data[i])
		}
	}
}

func TestNewBufferEmpty(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Empty slice should fail
	_, err = device.NewBuffer([]float32{}, MemoryDevice)
	if err == nil {
		t.Error("NewBuffer with empty slice should fail")
	}
}

func TestNewEmptyBuffer(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	buffer, err := device.NewEmptyBuffer(100, MemoryDevice)
	if err != nil {
		t.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer buffer.Release()

	if buffer.Size() != 400 { // 100 * 4 bytes
		t.Errorf("Buffer size = %d, want 400", buffer.Size())
	}
}

func TestBufferDoubleRelease(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	buffer, err := device.NewBuffer([]float32{1.0}, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}

	buffer.Release()
	buffer.Release() // Should not panic
}

func TestNormalizeVectors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Create 2 vectors of dimension 3
	// Vector 1: [3, 4, 0] -> norm = 5 -> normalized = [0.6, 0.8, 0]
	// Vector 2: [1, 0, 0] -> already normalized
	data := []float32{3.0, 4.0, 0.0, 1.0, 0.0, 0.0}
	buffer, err := device.NewBuffer(data, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	err = device.NormalizeVectors(buffer, 2, 3)
	if err != nil {
		t.Fatalf("NormalizeVectors failed: %v", err)
	}

	result := buffer.ReadFloat32(6)

	// Check first vector: [0.6, 0.8, 0]
	tolerance := float32(0.001)
	if abs(result[0]-0.6) > tolerance || abs(result[1]-0.8) > tolerance || abs(result[2]) > tolerance {
		t.Errorf("Vector 1 = [%f, %f, %f], want [0.6, 0.8, 0]", result[0], result[1], result[2])
	}

	// Check second vector: [1, 0, 0]
	if abs(result[3]-1.0) > tolerance || abs(result[4]) > tolerance || abs(result[5]) > tolerance {
		t.Errorf("Vector 2 = [%f, %f, %f], want [1, 0, 0]", result[3], result[4], result[5])
	}
}

func TestCosineSimilarity(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 3 normalized vectors of dimension 3
	embeddings := []float32{
		1.0, 0.0, 0.0, // Vector 0: pointing in x direction
		0.0, 1.0, 0.0, // Vector 1: pointing in y direction
		0.6, 0.8, 0.0, // Vector 2: mix of x and y
	}

	// Query: pointing in x direction
	query := []float32{1.0, 0.0, 0.0}

	embBuf, err := device.NewBuffer(embeddings, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer(embeddings) failed: %v", err)
	}
	defer embBuf.Release()

	queryBuf, err := device.NewBuffer(query, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer(query) failed: %v", err)
	}
	defer queryBuf.Release()

	scoresBuf, err := device.NewEmptyBuffer(3, MemoryDevice)
	if err != nil {
		t.Fatalf("NewEmptyBuffer(scores) failed: %v", err)
	}
	defer scoresBuf.Release()

	err = device.CosineSimilarity(embBuf, queryBuf, scoresBuf, 3, 3, true)
	if err != nil {
		t.Fatalf("CosineSimilarity failed: %v", err)
	}

	scores := scoresBuf.ReadFloat32(3)

	// Expected: [1.0, 0.0, 0.6]
	tolerance := float32(0.001)
	if abs(scores[0]-1.0) > tolerance {
		t.Errorf("Score[0] = %f, want 1.0", scores[0])
	}
	if abs(scores[1]-0.0) > tolerance {
		t.Errorf("Score[1] = %f, want 0.0", scores[1])
	}
	if abs(scores[2]-0.6) > tolerance {
		t.Errorf("Score[2] = %f, want 0.6", scores[2])
	}
}

func TestTopK(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Scores: highest at index 3, second at index 1
	scores := []float32{0.1, 0.8, 0.3, 0.9, 0.2}
	scoresBuf, err := device.NewBuffer(scores, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer scoresBuf.Release()

	indices, topScores, err := device.TopK(scoresBuf, 5, 3)
	if err != nil {
		t.Fatalf("TopK failed: %v", err)
	}

	// Top 3 should be indices [3, 1, 2] with scores [0.9, 0.8, 0.3]
	if len(indices) != 3 || len(topScores) != 3 {
		t.Fatalf("TopK returned %d indices and %d scores, want 3 each", len(indices), len(topScores))
	}

	// Check order (highest first)
	if indices[0] != 3 {
		t.Errorf("TopK[0] index = %d, want 3", indices[0])
	}
	if indices[1] != 1 {
		t.Errorf("TopK[1] index = %d, want 1", indices[1])
	}
	if indices[2] != 2 {
		t.Errorf("TopK[2] index = %d, want 2", indices[2])
	}
}

func TestSearch(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 5 normalized vectors of dimension 3
	embeddings := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
		0.0, 0.0, 1.0,
		0.6, 0.8, 0.0, // This should be closest to query
		0.7, 0.7, 0.14,
	}

	embBuf, err := device.NewBuffer(embeddings, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuf.Release()

	// Query similar to embedding 3
	query := []float32{0.6, 0.8, 0.0}

	results, err := device.Search(embBuf, query, 5, 3, 2, true)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Search returned %d results, want 2", len(results))
	}

	// First result should be index 3 (exact match)
	if results[0].Index != 3 {
		t.Errorf("Search[0].Index = %d, want 3", results[0].Index)
	}
	if abs(results[0].Score-1.0) > 0.001 {
		t.Errorf("Search[0].Score = %f, want 1.0", results[0].Score)
	}
}

func TestSearchZeroK(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	embeddings := []float32{1.0, 0.0, 0.0}
	embBuf, err := device.NewBuffer(embeddings, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuf.Release()

	query := []float32{1.0, 0.0, 0.0}

	results, err := device.Search(embBuf, query, 1, 3, 0, true)
	if err != nil {
		t.Fatalf("Search with k=0 failed: %v", err)
	}

	if results != nil {
		t.Errorf("Search with k=0 should return nil, got %v", results)
	}
}

func TestSearchKGreaterThanN(t *testing.T) {
	if !IsAvailable() {
		t.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	embeddings := []float32{1.0, 0.0, 0.0, 0.0, 1.0, 0.0}
	embBuf, err := device.NewBuffer(embeddings, MemoryDevice)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuf.Release()

	query := []float32{1.0, 0.0, 0.0}

	// Request 10 results from 2 vectors
	results, err := device.Search(embBuf, query, 2, 3, 10, true)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Should return only 2 (capped at n)
	if len(results) != 2 {
		t.Errorf("Search returned %d results, want 2", len(results))
	}
}

func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}

// Benchmarks

func BenchmarkCosineSimilarity(b *testing.B) {
	if !IsAvailable() {
		b.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 10000 vectors of 384 dimensions
	n := 10000
	dims := 384
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%100) / 100.0
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}

	embBuf, _ := device.NewBuffer(embeddings, MemoryDevice)
	queryBuf, _ := device.NewBuffer(query, MemoryDevice)
	scoresBuf, _ := device.NewEmptyBuffer(uint64(n), MemoryDevice)

	defer embBuf.Release()
	defer queryBuf.Release()
	defer scoresBuf.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.CosineSimilarity(embBuf, queryBuf, scoresBuf, uint32(n), uint32(dims), true)
	}
}

func BenchmarkSearch(b *testing.B) {
	if !IsAvailable() {
		b.Skip("CUDA not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 10000 vectors of 384 dimensions
	n := 10000
	dims := 384
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%100) / 100.0
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}

	embBuf, _ := device.NewBuffer(embeddings, MemoryDevice)
	defer embBuf.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.Search(embBuf, query, uint32(n), uint32(dims), 10, true)
	}
}
