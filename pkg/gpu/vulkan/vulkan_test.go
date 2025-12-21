//go:build vulkan && (linux || windows || darwin)
// +build vulkan
// +build linux windows darwin

package vulkan

import (
	"testing"
)

// absF32 returns absolute value of float32
func absF32(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}

func TestIsAvailable(t *testing.T) {
	available := IsAvailable()
	t.Logf("Vulkan available: %v", available)
}

func TestDeviceCount(t *testing.T) {
	count := DeviceCount()
	t.Logf("Vulkan device count: %d", count)

	if IsAvailable() && count == 0 {
		t.Error("Vulkan is available but device count is 0")
	}
	if !IsAvailable() && count > 0 {
		t.Error("Vulkan not available but device count > 0")
	}
}

func TestNewDevice(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice(0) failed: %v", err)
	}
	defer device.Release()

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
}

func TestNewDeviceInvalidID(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	_, err := NewDevice(999)
	if err == nil {
		t.Error("NewDevice(999) should fail")
	}
}

func TestDeviceDoubleRelease(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}

	device.Release()
	device.Release() // Should not panic
}

func TestNewBuffer(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	expectedSize := uint64(len(data) * 4)
	if buffer.Size() != expectedSize {
		t.Errorf("Buffer size = %d, want %d", buffer.Size(), expectedSize)
	}

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

func TestNewBufferEmpty(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	_, err = device.NewBuffer([]float32{})
	if err == nil {
		t.Error("NewBuffer with empty slice should fail")
	}
}

func TestNewEmptyBuffer(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	buffer, err := device.NewEmptyBuffer(100)
	if err != nil {
		t.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer buffer.Release()

	if buffer.Size() != 400 {
		t.Errorf("Buffer size = %d, want 400", buffer.Size())
	}
}

func TestBufferDoubleRelease(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	buffer, err := device.NewBuffer([]float32{1.0})
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}

	buffer.Release()
	buffer.Release() // Should not panic
}

func TestNormalizeVectors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Vector: [3, 4, 0] -> norm = 5 -> normalized = [0.6, 0.8, 0]
	data := []float32{3.0, 4.0, 0.0}
	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	err = device.NormalizeVectors(buffer, 1, 3)
	if err != nil {
		t.Fatalf("NormalizeVectors failed: %v", err)
	}

	result := buffer.ReadFloat32(3)

	tolerance := float32(0.001)
	if abs(result[0]-0.6) > tolerance || abs(result[1]-0.8) > tolerance || abs(result[2]) > tolerance {
		t.Errorf("Normalized = [%f, %f, %f], want [0.6, 0.8, 0]", result[0], result[1], result[2])
	}
}

func TestCosineSimilarity(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 3 normalized vectors of dimension 3
	embeddings := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
		0.6, 0.8, 0.0,
	}

	query := []float32{1.0, 0.0, 0.0}

	embBuf, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer(embeddings) failed: %v", err)
	}
	defer embBuf.Release()

	queryBuf, err := device.NewBuffer(query)
	if err != nil {
		t.Fatalf("NewBuffer(query) failed: %v", err)
	}
	defer queryBuf.Release()

	scoresBuf, err := device.NewEmptyBuffer(3)
	if err != nil {
		t.Fatalf("NewEmptyBuffer(scores) failed: %v", err)
	}
	defer scoresBuf.Release()

	err = device.CosineSimilarity(embBuf, queryBuf, scoresBuf, 3, 3, true)
	if err != nil {
		t.Fatalf("CosineSimilarity failed: %v", err)
	}

	scores := scoresBuf.ReadFloat32(3)

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

func TestNormalizeVectors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Test vector: [3, 4, 0] -> norm = 5 -> normalized = [0.6, 0.8, 0]
	data := []float32{3.0, 4.0, 0.0}
	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	// Call normalize through C bridge
	ret := C.vulkan_normalize_vectors(device.ptr, buffer.ptr, 1, 3)
	if ret != 0 {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
		t.Fatalf("vulkan_normalize_vectors failed: %s", errMsg)
	}

	// Read back results
	result := buffer.ReadFloat32(3)
	if result == nil {
		t.Fatal("Failed to read buffer")
	}

	tolerance := float32(0.01)
	expected := []float32{0.6, 0.8, 0.0}
	for i, exp := range expected {
		if absF32(result[i]-exp) > tolerance {
			t.Errorf("result[%d] = %f, want %f", i, result[i], exp)
		}
	}
}

func TestNormalizeVectorsCBridgeMultiple(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Test multiple vectors: [3,4,0] and [1,1,1]
	// [3,4,0] -> [0.6, 0.8, 0]
	// [1,1,1] -> [0.577, 0.577, 0.577] (1/sqrt(3))
	data := []float32{
		3.0, 4.0, 0.0,
		1.0, 1.0, 1.0,
	}
	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	ret := C.vulkan_normalize_vectors(device.ptr, buffer.ptr, 2, 3)
	if ret != 0 {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
		t.Fatalf("vulkan_normalize_vectors failed: %s", errMsg)
	}

	result := buffer.ReadFloat32(6)
	if result == nil {
		t.Fatal("Failed to read buffer")
	}

	tolerance := float32(0.01)
	expected := []float32{
		0.6, 0.8, 0.0,
		0.57735026, 0.57735026, 0.57735026, // 1/sqrt(3)
	}
	for i, exp := range expected {
		if absF32(result[i]-exp) > tolerance {
			t.Errorf("result[%d] = %f, want %f", i, result[i], exp)
		}
	}
}

func TestNormalizeVectorsCBridgeZeroVector(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// Test zero vector: [0, 0, 0] -> should remain [0, 0, 0]
	data := []float32{0.0, 0.0, 0.0}
	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	ret := C.vulkan_normalize_vectors(device.ptr, buffer.ptr, 1, 3)
	if ret != 0 {
		errMsg := C.GoString(C.vulkan_get_last_error())
		C.vulkan_clear_error()
		t.Fatalf("vulkan_normalize_vectors failed: %s", errMsg)
	}

	result := buffer.ReadFloat32(3)
	if result == nil {
		t.Fatal("Failed to read buffer")
	}

	// Zero vector should remain zero
	for i, val := range result {
		if val != 0.0 {
			t.Errorf("result[%d] = %f, want 0.0", i, val)
		}
	}
}

func TestTopK(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	scores := []float32{0.1, 0.8, 0.3, 0.9, 0.2}
	scoresBuf, err := device.NewBuffer(scores)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer scoresBuf.Release()

	indices, topScores, err := device.TopK(scoresBuf, 5, 3)
	if err != nil {
		t.Fatalf("TopK failed: %v", err)
	}

	if len(indices) != 3 || len(topScores) != 3 {
		t.Fatalf("TopK returned %d indices and %d scores, want 3 each", len(indices), len(topScores))
	}

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
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	embeddings := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
		0.0, 0.0, 1.0,
		0.6, 0.8, 0.0,
		0.7, 0.7, 0.14,
	}

	embBuf, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuf.Release()

	query := []float32{0.6, 0.8, 0.0}

	results, err := device.Search(embBuf, query, 5, 3, 2, true)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Search returned %d results, want 2", len(results))
	}

	if results[0].Index != 3 {
		t.Errorf("Search[0].Index = %d, want 3", results[0].Index)
	}
	if abs(results[0].Score-1.0) > 0.001 {
		t.Errorf("Search[0].Score = %f, want 1.0", results[0].Score)
	}
}

func TestSearchZeroK(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	embeddings := []float32{1.0, 0.0, 0.0}
	embBuf, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuf.Release()

	results, err := device.Search(embBuf, []float32{1.0, 0.0, 0.0}, 1, 3, 0, true)
	if err != nil {
		t.Fatalf("Search with k=0 failed: %v", err)
	}

	if results != nil {
		t.Errorf("Search with k=0 should return nil")
	}
}

func TestSearchKGreaterThanN(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	embeddings := []float32{1.0, 0.0, 0.0, 0.0, 1.0, 0.0}
	embBuf, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuf.Release()

	results, err := device.Search(embBuf, []float32{1.0, 0.0, 0.0}, 2, 3, 10, true)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

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
		b.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

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

	embBuf, _ := device.NewBuffer(embeddings)
	queryBuf, _ := device.NewBuffer(query)
	scoresBuf, _ := device.NewEmptyBuffer(uint64(n))

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
		b.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

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

	embBuf, _ := device.NewBuffer(embeddings)
	defer embBuf.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.Search(embBuf, query, uint32(n), uint32(dims), 10, true)
	}
}
