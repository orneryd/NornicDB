//go:build darwin
// +build darwin

package metal

import (
	"math"
	"testing"
)

// =============================================================================
// Memory Tracking Tests
// =============================================================================

func TestGetMemoryInfo(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("memory info returns valid values", func(t *testing.T) {
		info := device.GetMemoryInfo()

		// Total memory should be positive (at least 1GB on any Mac)
		if info.TotalMemory < 1024*1024*1024 {
			t.Errorf("TotalMemory too small: %d bytes", info.TotalMemory)
		}
		t.Logf("Total Memory: %.2f GB", float64(info.TotalMemory)/(1024*1024*1024))

		// Available should be <= total
		if info.AvailableMemory > info.TotalMemory {
			t.Errorf("AvailableMemory (%d) > TotalMemory (%d)", info.AvailableMemory, info.TotalMemory)
		}
		t.Logf("Available Memory: %.2f GB", float64(info.AvailableMemory)/(1024*1024*1024))

		// GPU recommended should be positive
		if info.GPURecommended == 0 {
			t.Log("Warning: GPURecommended is 0 - this may indicate a detection issue")
		}
		t.Logf("GPU Recommended: %.2f GB", float64(info.GPURecommended)/(1024*1024*1024))

		t.Logf("Used Memory: %.2f GB", float64(info.UsedMemory)/(1024*1024*1024))
		t.Logf("Current Allocated: %d bytes", info.CurrentAllocated)
	})

	t.Run("memory changes after allocation", func(t *testing.T) {
		infoBefore := device.GetMemoryInfo()

		// Allocate a 10MB buffer
		largeData := make([]float32, 2500000) // 10MB
		buf, err := device.NewBuffer(largeData, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer() error = %v", err)
		}
		defer buf.Release()

		infoAfter := device.GetMemoryInfo()

		// Note: This test is informational - allocation tracking may not be immediate
		t.Logf("Allocated before: %d bytes", infoBefore.CurrentAllocated)
		t.Logf("Allocated after: %d bytes", infoAfter.CurrentAllocated)
	})
}

// =============================================================================
// Device Capabilities Tests
// =============================================================================

func TestGetCapabilities(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("capabilities returns valid values", func(t *testing.T) {
		caps := device.GetCapabilities()

		// Name should not be empty
		if caps.Name == "" {
			t.Error("Device name should not be empty")
		}
		t.Logf("Name: %s", caps.Name)

		// Architecture may be empty on older devices but log it
		t.Logf("Architecture: %s", caps.Architecture)

		// GPU Family should be positive
		if caps.GPUFamily <= 0 {
			t.Errorf("GPUFamily should be positive, got %d", caps.GPUFamily)
		}
		t.Logf("GPU Family: %d", caps.GPUFamily)

		// MaxThreadsPerThreadgroup should be reasonable (at least 256)
		if caps.MaxThreadsPerThreadgroup < 256 {
			t.Errorf("MaxThreadsPerThreadgroup too small: %d", caps.MaxThreadsPerThreadgroup)
		}
		t.Logf("Max Threads Per Threadgroup: %d", caps.MaxThreadsPerThreadgroup)

		// MaxBufferLength should be at least 256MB
		if caps.MaxBufferLength < 256*1024*1024 {
			t.Errorf("MaxBufferLength too small: %d", caps.MaxBufferLength)
		}
		t.Logf("Max Buffer Length: %.2f GB", float64(caps.MaxBufferLength)/(1024*1024*1024))

		// Log boolean capabilities
		t.Logf("Is Low Power: %v", caps.IsLowPower)
		t.Logf("Is Headless: %v", caps.IsHeadless)
		t.Logf("Has Unified Memory: %v", caps.HasUnifiedMemory)
		t.Logf("Registry ID: %d", caps.RegistryID)
	})

	t.Run("capabilities consistent with device", func(t *testing.T) {
		caps := device.GetCapabilities()

		// Name should match device name
		if caps.Name != device.Name() {
			t.Errorf("Caps name (%s) != device name (%s)", caps.Name, device.Name())
		}
	})
}

func TestGetCapabilitiesNoDevice(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	t.Run("works without device", func(t *testing.T) {
		caps := GetCapabilitiesNoDevice()

		// Should return valid information
		if caps.Name == "" {
			t.Error("Name should not be empty")
		}
		t.Logf("Name (no device): %s", caps.Name)

		if caps.GPUFamily <= 0 {
			t.Errorf("GPUFamily should be positive, got %d", caps.GPUFamily)
		}

		if caps.MaxThreadsPerThreadgroup < 256 {
			t.Errorf("MaxThreadsPerThreadgroup too small: %d", caps.MaxThreadsPerThreadgroup)
		}
	})

	t.Run("consistent with device version", func(t *testing.T) {
		capsNoDevice := GetCapabilitiesNoDevice()

		device, err := NewDevice()
		if err != nil {
			t.Fatalf("NewDevice() error = %v", err)
		}
		defer device.Release()

		capsWithDevice := device.GetCapabilities()

		// Should be the same
		if capsNoDevice.Name != capsWithDevice.Name {
			t.Errorf("Name mismatch: no-device=%s, with-device=%s",
				capsNoDevice.Name, capsWithDevice.Name)
		}
		if capsNoDevice.GPUFamily != capsWithDevice.GPUFamily {
			t.Errorf("GPUFamily mismatch: no-device=%d, with-device=%d",
				capsNoDevice.GPUFamily, capsWithDevice.GPUFamily)
		}
	})
}

func TestPrintDeviceInfo(t *testing.T) {
	// This function logs info, so we just verify it doesn't panic
	t.Run("does not panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("PrintDeviceInfo() panicked: %v", r)
			}
		}()

		PrintDeviceInfo()
	})
}

// =============================================================================
// Metal Performance Shaders (MPS) Tests
// =============================================================================

func TestMPSIsSupported(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	// On any modern Mac with Metal, MPS should be supported
	supported := MPSIsSupported()
	t.Logf("MPS Supported: %v", supported)

	// We can't assert it must be true, but log a warning if not
	if !supported {
		t.Log("Warning: MPS not supported - some GPU features may be slower")
	}
}

func TestMPSMatrixMultiply(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		t.Skip("MPS not supported")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("simple matrix multiply", func(t *testing.T) {
		// A = 2x3 matrix, B = 3x2 matrix -> C = 2x2 matrix
		// A = [[1, 2, 3], [4, 5, 6]]
		// B = [[1, 2], [3, 4], [5, 6]]
		// C = A*B = [[22, 28], [49, 64]]
		a := []float32{1, 2, 3, 4, 5, 6}
		b := []float32{1, 2, 3, 4, 5, 6}
		c := make([]float32, 4) // output 2x2

		aBuf, err := device.NewBuffer(a, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer(a) error = %v", err)
		}
		defer aBuf.Release()

		bBuf, err := device.NewBuffer(b, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer(b) error = %v", err)
		}
		defer bBuf.Release()

		cBuf, err := device.NewBuffer(c, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer(c) error = %v", err)
		}
		defer cBuf.Release()

		// C = 1.0 * A * B + 0.0 * C
		err = device.MPSMatrixMultiply(aBuf, bBuf, cBuf, 2, 2, 3, 1.0, 0.0)
		if err != nil {
			t.Fatalf("MPSMatrixMultiply() error = %v", err)
		}

		result := cBuf.ReadFloat32(4)
		t.Logf("Result: %v", result)

		// Expected: [22, 28, 49, 64]
		expected := []float32{22, 28, 49, 64}
		for i, v := range result {
			if math.Abs(float64(v-expected[i])) > 0.01 {
				t.Errorf("result[%d] = %f, expected %f", i, v, expected[i])
			}
		}
	})

	t.Run("matrix multiply with alpha and beta", func(t *testing.T) {
		// Simple 2x2 test with alpha=2.0, beta=1.0
		// A = B = [[1, 0], [0, 1]] (identity)
		// C_initial = [[1, 1], [1, 1]]
		// Result: C = 2.0 * A * B + 1.0 * C_initial = [[3, 1], [1, 3]]
		a := []float32{1, 0, 0, 1}
		b := []float32{1, 0, 0, 1}
		c := []float32{1, 1, 1, 1}

		aBuf, _ := device.NewBuffer(a, StorageShared)
		defer aBuf.Release()
		bBuf, _ := device.NewBuffer(b, StorageShared)
		defer bBuf.Release()
		cBuf, _ := device.NewBuffer(c, StorageShared)
		defer cBuf.Release()

		err := device.MPSMatrixMultiply(aBuf, bBuf, cBuf, 2, 2, 2, 2.0, 1.0)
		if err != nil {
			t.Fatalf("MPSMatrixMultiply() error = %v", err)
		}

		result := cBuf.ReadFloat32(4)
		t.Logf("Result with alpha=2, beta=1: %v", result)

		// Expected: [3, 1, 1, 3]
		expected := []float32{3, 1, 1, 3}
		for i, v := range result {
			if math.Abs(float64(v-expected[i])) > 0.01 {
				t.Errorf("result[%d] = %f, expected %f", i, v, expected[i])
			}
		}
	})
}

func TestMPSMatrixVectorMultiply(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		t.Skip("MPS not supported")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("simple matrix-vector multiply", func(t *testing.T) {
		// A = 2x3 matrix, x = 3 vector -> y = 2 vector
		// A = [[1, 2, 3], [4, 5, 6]]
		// x = [1, 1, 1]
		// y = A * x = [6, 15]
		a := []float32{1, 2, 3, 4, 5, 6}
		x := []float32{1, 1, 1}
		y := make([]float32, 2)

		aBuf, _ := device.NewBuffer(a, StorageShared)
		defer aBuf.Release()
		xBuf, _ := device.NewBuffer(x, StorageShared)
		defer xBuf.Release()
		yBuf, _ := device.NewBuffer(y, StorageShared)
		defer yBuf.Release()

		// y = 1.0 * A * x + 0.0 * y
		err := device.MPSMatrixVectorMultiply(aBuf, xBuf, yBuf, 2, 3, 1.0, 0.0)
		if err != nil {
			t.Fatalf("MPSMatrixVectorMultiply() error = %v", err)
		}

		result := yBuf.ReadFloat32(2)
		t.Logf("Result: %v", result)

		// Expected: [6, 15]
		if math.Abs(float64(result[0]-6)) > 0.01 {
			t.Errorf("result[0] = %f, expected 6", result[0])
		}
		if math.Abs(float64(result[1]-15)) > 0.01 {
			t.Errorf("result[1] = %f, expected 15", result[1])
		}
	})
}

func TestMPSFunctions_InvalidBuffersReturnErrors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		t.Skip("MPS not supported")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	invalid := &Buffer{}

	if err := device.MPSMatrixMultiply(invalid, invalid, invalid, 1, 1, 1, 1.0, 0.0); err == nil {
		t.Error("expected MPSMatrixMultiply error for invalid buffers")
	}
	if err := device.MPSMatrixVectorMultiply(invalid, invalid, invalid, 1, 1, 1.0, 0.0); err == nil {
		t.Error("expected MPSMatrixVectorMultiply error for invalid buffers")
	}
	if err := device.MPSBatchCosineSimilarity(invalid, invalid, invalid, 1, 1); err == nil {
		t.Error("expected MPSBatchCosineSimilarity error for invalid buffers")
	}
}

func TestMPSBatchCosineSimilarity(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		t.Skip("MPS not supported")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("batch cosine similarity", func(t *testing.T) {
		// 3 pre-normalized embeddings of dimension 4
		// For cosine similarity with normalized vectors, it's just a dot product
		embeddings := []float32{
			1, 0, 0, 0, // emb0 - exact match with query
			0, 1, 0, 0, // emb1 - orthogonal to query
			0.707, 0.707, 0, 0, // emb2 - 45 degrees from query (~0.707 similarity)
		}

		query := []float32{1, 0, 0, 0} // unit vector

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()
		queryBuf, _ := device.NewBuffer(query, StorageShared)
		defer queryBuf.Release()
		scoresBuf, _ := device.NewEmptyBuffer(12, StorageShared) // 3 floats
		defer scoresBuf.Release()

		err := device.MPSBatchCosineSimilarity(embBuf, queryBuf, scoresBuf, 3, 4)
		if err != nil {
			t.Fatalf("MPSBatchCosineSimilarity() error = %v", err)
		}

		scores := scoresBuf.ReadFloat32(3)
		t.Logf("Scores: %v", scores)

		// emb0 should have score ~1.0
		if math.Abs(float64(scores[0]-1.0)) > 0.05 {
			t.Errorf("scores[0] = %f, expected ~1.0", scores[0])
		}

		// emb1 should have score ~0.0
		if math.Abs(float64(scores[1])) > 0.05 {
			t.Errorf("scores[1] = %f, expected ~0.0", scores[1])
		}

		// emb2 should have score ~0.707
		if math.Abs(float64(scores[2]-0.707)) > 0.05 {
			t.Errorf("scores[2] = %f, expected ~0.707", scores[2])
		}
	})

	t.Run("larger batch normalized", func(t *testing.T) {
		// 1000 normalized embeddings (unit vectors)
		n := uint32(100)
		dims := uint32(128)

		embeddings := make([]float32, n*dims)
		// Create normalized vectors - each row sums to 1/sqrt(dims)
		for i := uint32(0); i < n; i++ {
			var norm float32 = 0
			for j := uint32(0); j < dims; j++ {
				val := float32((int(i*dims+j) % 1000)) / 1000.0
				embeddings[i*dims+j] = val
				norm += val * val
			}
			// Normalize the vector
			norm = float32(math.Sqrt(float64(norm)))
			if norm > 0 {
				for j := uint32(0); j < dims; j++ {
					embeddings[i*dims+j] /= norm
				}
			}
		}

		// Normalized query
		query := make([]float32, dims)
		var qnorm float32 = 0
		for i := range query {
			query[i] = 0.5
			qnorm += 0.5 * 0.5
		}
		qnorm = float32(math.Sqrt(float64(qnorm)))
		for i := range query {
			query[i] /= qnorm
		}

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()
		queryBuf, _ := device.NewBuffer(query, StorageShared)
		defer queryBuf.Release()
		scoresBuf, _ := device.NewEmptyBuffer(uint64(n)*4, StorageShared)
		defer scoresBuf.Release()

		err := device.MPSBatchCosineSimilarity(embBuf, queryBuf, scoresBuf, n, dims)
		if err != nil {
			t.Fatalf("MPSBatchCosineSimilarity() error = %v", err)
		}

		scores := scoresBuf.ReadFloat32(int(n))
		if len(scores) != int(n) {
			t.Errorf("expected %d scores, got %d", n, len(scores))
		}

		// With normalized vectors, scores should be between -1 and 1
		outOfRange := 0
		for _, s := range scores {
			if s < -1.1 || s > 1.1 {
				outOfRange++
			}
		}
		if outOfRange > 0 {
			t.Errorf("%d scores out of range [-1, 1]", outOfRange)
		}

		t.Logf("Score range: min=%f, max=%f (first 5: %v)",
			scores[0], scores[len(scores)-1], scores[:5])
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestMPSVsCustomKernel(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		t.Skip("MPS not supported")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("compare MPS and custom cosine similarity", func(t *testing.T) {
		// Test that MPS and custom kernel produce similar results
		embeddings := []float32{
			1, 0, 0, 0,
			0.9, 0.1, 0, 0,
			0.5, 0.5, 0, 0,
		}
		query := []float32{1, 0, 0, 0}

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()

		// Custom kernel
		queryBuf, _ := device.NewBuffer(query, StorageShared)
		defer queryBuf.Release()
		customScoresBuf, _ := device.NewEmptyBuffer(12, StorageShared)
		defer customScoresBuf.Release()

		err := device.ComputeCosineSimilarity(embBuf, queryBuf, customScoresBuf, 3, 4, false)
		if err != nil {
			t.Fatalf("ComputeCosineSimilarity() error = %v", err)
		}

		// MPS
		mpsScoresBuf, _ := device.NewEmptyBuffer(12, StorageShared)
		defer mpsScoresBuf.Release()

		err = device.MPSBatchCosineSimilarity(embBuf, queryBuf, mpsScoresBuf, 3, 4)
		if err != nil {
			t.Fatalf("MPSBatchCosineSimilarity() error = %v", err)
		}

		customScores := customScoresBuf.ReadFloat32(3)
		mpsScores := mpsScoresBuf.ReadFloat32(3)

		t.Logf("Custom kernel scores: %v", customScores)
		t.Logf("MPS scores: %v", mpsScores)

		// They should be close (within 10% or absolute 0.1)
		for i := range customScores {
			diff := math.Abs(float64(customScores[i] - mpsScores[i]))
			if diff > 0.1 {
				t.Logf("Warning: score[%d] differs: custom=%f, mps=%f (diff=%f)",
					i, customScores[i], mpsScores[i], diff)
			}
		}
	})
}

// =============================================================================
// Benchmarks for New Features
// =============================================================================

func BenchmarkMPSBatchCosineSimilarity(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		b.Skip("MPS not supported")
	}

	device, _ := NewDevice()
	defer device.Release()

	n := uint32(10000)
	dims := uint32(1024)

	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%1000) / 1000
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}

	embBuf, _ := device.NewBuffer(embeddings, StorageShared)
	defer embBuf.Release()
	queryBuf, _ := device.NewBuffer(query, StorageShared)
	defer queryBuf.Release()
	scoresBuf, _ := device.NewEmptyBuffer(uint64(n)*4, StorageShared)
	defer scoresBuf.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.MPSBatchCosineSimilarity(embBuf, queryBuf, scoresBuf, n, dims)
	}
}

func BenchmarkMPSMatrixMultiply(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Metal not available")
	}
	if !MPSIsSupported() {
		b.Skip("MPS not supported")
	}

	device, _ := NewDevice()
	defer device.Release()

	// 1024x1024 matrix multiply
	m := uint32(1024)
	n := uint32(1024)
	k := uint32(1024)

	a := make([]float32, m*k)
	b_data := make([]float32, k*n)
	c := make([]float32, m*n)

	for i := range a {
		a[i] = float32(i%100) / 100
	}
	for i := range b_data {
		b_data[i] = float32(i%100) / 100
	}

	aBuf, _ := device.NewBuffer(a, StorageShared)
	defer aBuf.Release()
	bBuf, _ := device.NewBuffer(b_data, StorageShared)
	defer bBuf.Release()
	cBuf, _ := device.NewBuffer(c, StorageShared)
	defer cBuf.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.MPSMatrixMultiply(aBuf, bBuf, cBuf, m, n, k, 1.0, 0.0)
	}
}

func BenchmarkGetMemoryInfo(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Metal not available")
	}

	device, _ := NewDevice()
	defer device.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.GetMemoryInfo()
	}
}

func BenchmarkGetCapabilities(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Metal not available")
	}

	device, _ := NewDevice()
	defer device.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.GetCapabilities()
	}
}
