//go:build darwin
// +build darwin

package metal

import (
	"testing"
)

func TestIsAvailable(t *testing.T) {
	// On macOS, Metal should be available
	available := IsAvailable()
	if !available {
		t.Skip("Metal not available on this system")
	}
	t.Log("Metal is available")
}

func TestNewDevice(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("device properties", func(t *testing.T) {
		name := device.Name()
		if name == "" {
			t.Error("device name should not be empty")
		}
		t.Logf("Device name: %s", name)

		memMB := device.MemoryMB()
		if memMB <= 0 {
			t.Error("device memory should be positive")
		}
		t.Logf("Device memory: %d MB", memMB)

		memBytes := device.MemoryBytes()
		if memBytes <= 0 {
			t.Error("device memory bytes should be positive")
		}
		t.Logf("Device memory bytes: %d", memBytes)
	})

	t.Run("double release is safe", func(t *testing.T) {
		d2, _ := NewDevice()
		d2.Release()
		d2.Release() // Should not panic
	})

	t.Run("released device rejects new buffers", func(t *testing.T) {
		d2, err := NewDevice()
		if err != nil {
			t.Fatalf("NewDevice() error = %v", err)
		}
		d2.Release()

		_, err = d2.NewBuffer([]float32{1, 2, 3}, StorageShared)
		if err == nil {
			t.Error("expected error creating buffer on released device")
		}

		_, err = d2.NewBufferNoCopy([]float32{1, 2, 3}, StorageShared)
		if err == nil {
			t.Error("expected error creating no-copy buffer on released device")
		}

		_, err = d2.NewEmptyBuffer(16, StorageShared)
		if err == nil {
			t.Error("expected error creating empty buffer on released device")
		}
	})
}

func TestNewBuffer(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("create buffer with data", func(t *testing.T) {
		data := []float32{1.0, 2.0, 3.0, 4.0}
		buf, err := device.NewBuffer(data, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer() error = %v", err)
		}
		defer buf.Release()

		if buf.Size() != 16 { // 4 floats * 4 bytes
			t.Errorf("expected size 16, got %d", buf.Size())
		}
	})

	t.Run("create buffer StorageManaged", func(t *testing.T) {
		data := []float32{1.0, 2.0, 3.0, 4.0}
		buf, err := device.NewBuffer(data, StorageManaged)
		if err != nil {
			t.Fatalf("NewBuffer(StorageManaged) error = %v", err)
		}
		defer buf.Release()

		if buf.Size() != 16 {
			t.Errorf("expected size 16, got %d", buf.Size())
		}
	})

	t.Run("create buffer StoragePrivate", func(t *testing.T) {
		data := []float32{1.0, 2.0, 3.0, 4.0}
		buf, err := device.NewBuffer(data, StoragePrivate)
		if err != nil {
			t.Fatalf("NewBuffer(StoragePrivate) error = %v", err)
		}
		defer buf.Release()

		if buf.Size() != 16 {
			t.Errorf("expected size 16, got %d", buf.Size())
		}
	})

	t.Run("empty data returns error", func(t *testing.T) {
		_, err := device.NewBuffer([]float32{}, StorageShared)
		if err == nil {
			t.Error("expected error for empty data")
		}
	})

	t.Run("buffer contents readable", func(t *testing.T) {
		data := []float32{1.5, 2.5, 3.5, 4.5}
		buf, err := device.NewBuffer(data, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer() error = %v", err)
		}
		defer buf.Release()

		contents := buf.Contents()
		if contents == nil {
			t.Fatal("Contents() returned nil")
		}

		readBack := buf.ReadFloat32(4)
		if readBack == nil {
			t.Fatal("ReadFloat32() returned nil")
		}
		for i, v := range readBack {
			if v != data[i] {
				t.Errorf("readBack[%d] = %f, expected %f", i, v, data[i])
			}
		}
	})

	t.Run("read with invalid count", func(t *testing.T) {
		data := []float32{1.0, 2.0}
		buf, _ := device.NewBuffer(data, StorageShared)
		defer buf.Release()

		// Read more than available
		result := buf.ReadFloat32(100)
		if result != nil {
			t.Error("expected nil for invalid count")
		}

		// Read zero
		result = buf.ReadFloat32(0)
		if result != nil {
			t.Error("expected nil for zero count")
		}

		// Read negative
		result = buf.ReadFloat32(-1)
		if result != nil {
			t.Error("expected nil for negative count")
		}
	})
}

func TestNewBufferNoCopy(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("create no-copy buffer", func(t *testing.T) {
		data := []float32{1.0, 2.0, 3.0, 4.0}
		buf, err := device.NewBufferNoCopy(data, StorageShared)
		if err != nil {
			t.Fatalf("NewBufferNoCopy() error = %v", err)
		}
		defer buf.Release()

		if buf.Size() != 16 {
			t.Errorf("expected size 16, got %d", buf.Size())
		}
	})

	t.Run("empty data returns error", func(t *testing.T) {
		_, err := device.NewBufferNoCopy([]float32{}, StorageShared)
		if err == nil {
			t.Error("expected error for empty data")
		}
	})
}

func TestNewEmptyBuffer(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("create empty buffer", func(t *testing.T) {
		buf, err := device.NewEmptyBuffer(1024, StorageShared)
		if err != nil {
			t.Fatalf("NewEmptyBuffer() error = %v", err)
		}
		defer buf.Release()

		if buf.Size() != 1024 {
			t.Errorf("expected size 1024, got %d", buf.Size())
		}
	})

	t.Run("buffer release is safe twice", func(t *testing.T) {
		buf, err := device.NewEmptyBuffer(16, StorageShared)
		if err != nil {
			t.Fatalf("NewEmptyBuffer() error = %v", err)
		}
		buf.Release()
		buf.Release()
	})
}

func TestBufferReadUint32(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	// Create buffer with uint32 data (as float32 reinterpreted)
	buf, err := device.NewEmptyBuffer(16, StorageShared) // 4 uint32s
	if err != nil {
		t.Fatalf("NewEmptyBuffer() error = %v", err)
	}
	defer buf.Release()

	result := buf.ReadUint32(4)
	if result == nil {
		t.Fatal("ReadUint32() returned nil")
	}
	if len(result) != 4 {
		t.Errorf("expected 4 values, got %d", len(result))
	}

	// Test invalid reads
	result = buf.ReadUint32(100)
	if result != nil {
		t.Error("expected nil for over-read")
	}

	result = buf.ReadUint32(0)
	if result != nil {
		t.Error("expected nil for zero count")
	}
}

func TestBufferWriteFloat32(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("write and read back", func(t *testing.T) {
		buf, err := device.NewEmptyBuffer(32, StorageShared) // 8 floats
		if err != nil {
			t.Fatalf("NewEmptyBuffer() error = %v", err)
		}
		defer buf.Release()

		data := []float32{1.1, 2.2, 3.3, 4.4}
		err = buf.WriteFloat32(data, 0)
		if err != nil {
			t.Fatalf("WriteFloat32() error = %v", err)
		}

		readBack := buf.ReadFloat32(4)
		for i, v := range readBack {
			if v != data[i] {
				t.Errorf("readBack[%d] = %f, expected %f", i, v, data[i])
			}
		}
	})

	t.Run("write at offset", func(t *testing.T) {
		buf, _ := device.NewEmptyBuffer(32, StorageShared)
		defer buf.Release()

		data := []float32{9.9, 8.8}
		err := buf.WriteFloat32(data, 4) // Write at offset 4
		if err != nil {
			t.Fatalf("WriteFloat32() error = %v", err)
		}

		readBack := buf.ReadFloat32(8)
		if readBack[4] != 9.9 || readBack[5] != 8.8 {
			t.Errorf("write at offset failed: %v", readBack)
		}
	})

	t.Run("write exceeds size", func(t *testing.T) {
		buf, _ := device.NewEmptyBuffer(8, StorageShared) // 2 floats
		defer buf.Release()

		data := []float32{1.0, 2.0, 3.0, 4.0} // 4 floats - too many
		err := buf.WriteFloat32(data, 0)
		if err == nil {
			t.Error("expected error for write exceeding size")
		}
	})

	t.Run("write empty data", func(t *testing.T) {
		buf, _ := device.NewEmptyBuffer(16, StorageShared)
		defer buf.Release()

		err := buf.WriteFloat32([]float32{}, 0)
		if err != nil {
			t.Errorf("empty write should succeed, got error: %v", err)
		}
	})

	t.Run("invalid buffer returns error or nil reads", func(t *testing.T) {
		buf := Buffer{size: 4}
		if got := buf.ReadFloat32(1); got != nil {
			t.Errorf("expected nil ReadFloat32 on invalid buffer, got %v", got)
		}
		if got := buf.ReadUint32(1); got != nil {
			t.Errorf("expected nil ReadUint32 on invalid buffer, got %v", got)
		}
		if err := buf.WriteFloat32([]float32{1}, 0); err != ErrInvalidBuffer {
			t.Errorf("expected ErrInvalidBuffer, got %v", err)
		}
	})
}

func TestComputeCosineSimilarity(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("compute similarity", func(t *testing.T) {
		// 3 embeddings of dimension 4
		embeddings := []float32{
			1, 0, 0, 0, // vec0 - should match query perfectly
			0, 1, 0, 0, // vec1 - orthogonal
			0.9, 0.1, 0, 0, // vec2 - similar to query
		}

		query := []float32{1, 0, 0, 0}

		embBuf, err := device.NewBuffer(embeddings, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer(embeddings) error = %v", err)
		}
		defer embBuf.Release()

		queryBuf, err := device.NewBuffer(query, StorageShared)
		if err != nil {
			t.Fatalf("NewBuffer(query) error = %v", err)
		}
		defer queryBuf.Release()

		scoresBuf, err := device.NewEmptyBuffer(12, StorageShared) // 3 floats
		if err != nil {
			t.Fatalf("NewEmptyBuffer(scores) error = %v", err)
		}
		defer scoresBuf.Release()

		err = device.ComputeCosineSimilarity(embBuf, queryBuf, scoresBuf, 3, 4, false)
		if err != nil {
			t.Fatalf("ComputeCosineSimilarity() error = %v", err)
		}

		scores := scoresBuf.ReadFloat32(3)
		t.Logf("Scores: %v", scores)

		// vec0 should have score ~1.0 (exact match)
		if scores[0] < 0.99 {
			t.Errorf("expected score[0] ~1.0, got %f", scores[0])
		}

		// vec1 should have score ~0.0 (orthogonal)
		if scores[1] > 0.01 || scores[1] < -0.01 {
			t.Errorf("expected score[1] ~0.0, got %f", scores[1])
		}

		// vec2 should have score > 0.9 (similar)
		if scores[2] < 0.9 {
			t.Errorf("expected score[2] > 0.9, got %f", scores[2])
		}
	})

	t.Run("normalized vectors", func(t *testing.T) {
		// Pre-normalized vectors
		embeddings := []float32{
			1, 0, 0, 0, // normalized
			0, 1, 0, 0, // normalized
		}

		query := []float32{1, 0, 0, 0}

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()
		queryBuf, _ := device.NewBuffer(query, StorageShared)
		defer queryBuf.Release()
		scoresBuf, _ := device.NewEmptyBuffer(8, StorageShared)
		defer scoresBuf.Release()

		err := device.ComputeCosineSimilarity(embBuf, queryBuf, scoresBuf, 2, 4, true)
		if err != nil {
			t.Fatalf("ComputeCosineSimilarity(normalized) error = %v", err)
		}

		scores := scoresBuf.ReadFloat32(2)
		if scores[0] < 0.99 {
			t.Errorf("expected normalized score[0] ~1.0, got %f", scores[0])
		}
	})
}

func TestComputeTopK(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("find top-k", func(t *testing.T) {
		scores := []float32{0.1, 0.9, 0.5, 0.3, 0.7} // 5 scores

		scoresBuf, _ := device.NewBuffer(scores, StorageShared)
		defer scoresBuf.Release()

		indicesBuf, _ := device.NewEmptyBuffer(12, StorageShared) // 3 uint32
		defer indicesBuf.Release()

		topkScoresBuf, _ := device.NewEmptyBuffer(12, StorageShared) // 3 float32
		defer topkScoresBuf.Release()

		err := device.ComputeTopK(scoresBuf, indicesBuf, topkScoresBuf, 5, 3)
		if err != nil {
			t.Fatalf("ComputeTopK() error = %v", err)
		}

		indices := indicesBuf.ReadUint32(3)
		topkScores := topkScoresBuf.ReadFloat32(3)

		t.Logf("Top-3 indices: %v", indices)
		t.Logf("Top-3 scores: %v", topkScores)

		// Highest score is 0.9 at index 1
		if topkScores[0] < 0.89 {
			t.Errorf("expected top score ~0.9, got %f", topkScores[0])
		}
		if indices[0] != 1 {
			t.Errorf("expected top index 1, got %d", indices[0])
		}

		// Second highest is 0.7 at index 4
		if topkScores[1] < 0.69 {
			t.Errorf("expected second score ~0.7, got %f", topkScores[1])
		}

		// Third highest is 0.5 at index 2
		if topkScores[2] < 0.49 {
			t.Errorf("expected third score ~0.5, got %f", topkScores[2])
		}
	})
}

func TestNormalizeVectors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("normalize vectors", func(t *testing.T) {
		// 2 vectors of dimension 3, not normalized
		vectors := []float32{
			3, 4, 0, // length = 5
			0, 0, 5, // length = 5
		}

		buf, _ := device.NewBuffer(vectors, StorageShared)
		defer buf.Release()

		err := device.NormalizeVectors(buf, 2, 3)
		if err != nil {
			t.Fatalf("NormalizeVectors() error = %v", err)
		}

		normalized := buf.ReadFloat32(6)
		t.Logf("Normalized: %v", normalized)

		// First vector should be (0.6, 0.8, 0) - (3/5, 4/5, 0)
		if normalized[0] < 0.59 || normalized[0] > 0.61 {
			t.Errorf("expected normalized[0] ~0.6, got %f", normalized[0])
		}
		if normalized[1] < 0.79 || normalized[1] > 0.81 {
			t.Errorf("expected normalized[1] ~0.8, got %f", normalized[1])
		}

		// Second vector should be (0, 0, 1) - (0, 0, 5/5)
		if normalized[5] < 0.99 {
			t.Errorf("expected normalized[5] ~1.0, got %f", normalized[5])
		}
	})
}

func TestSearch(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	t.Run("full search pipeline", func(t *testing.T) {
		// 5 embeddings of dimension 4
		embeddings := []float32{
			1, 0, 0, 0, // idx 0 - exact match
			0, 1, 0, 0, // idx 1 - orthogonal
			0.9, 0.1, 0, 0, // idx 2 - very similar
			0.5, 0.5, 0, 0, // idx 3 - somewhat similar
			0, 0, 1, 0, // idx 4 - orthogonal
		}

		query := []float32{1, 0, 0, 0}

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()

		results, err := device.Search(embBuf, query, 5, 4, 3, false)
		if err != nil {
			t.Fatalf("Search() error = %v", err)
		}

		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		t.Logf("Results: %+v", results)

		// First result should be idx 0 (exact match)
		if results[0].Index != 0 {
			t.Errorf("expected first result index 0, got %d", results[0].Index)
		}
		if results[0].Score < 0.99 {
			t.Errorf("expected first score ~1.0, got %f", results[0].Score)
		}

		// Second result should be idx 2 (very similar)
		if results[1].Index != 2 {
			t.Errorf("expected second result index 2, got %d", results[1].Index)
		}
	})

	t.Run("search with k=0", func(t *testing.T) {
		embeddings := []float32{1, 0, 0, 0}
		query := []float32{1, 0, 0, 0}

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()

		results, err := device.Search(embBuf, query, 1, 4, 0, false)
		if err != nil {
			t.Fatalf("Search(k=0) error = %v", err)
		}
		if results != nil {
			t.Errorf("expected nil results for k=0, got %v", results)
		}
	})

	t.Run("search with k > n", func(t *testing.T) {
		embeddings := []float32{
			1, 0, 0, 0,
			0, 1, 0, 0,
		}
		query := []float32{1, 0, 0, 0}

		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()

		results, err := device.Search(embBuf, query, 2, 4, 10, false) // k=10 > n=2
		if err != nil {
			t.Fatalf("Search(k>n) error = %v", err)
		}
		if len(results) != 2 { // Should return only 2
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("search fails for invalid query buffer creation", func(t *testing.T) {
		embeddings := []float32{1, 0, 0, 0}
		embBuf, _ := device.NewBuffer(embeddings, StorageShared)
		defer embBuf.Release()

		_, err := device.Search(embBuf, []float32{}, 1, 4, 1, false)
		if err == nil {
			t.Error("expected error for empty query")
		}
	})

	t.Run("search fails when compute kernel receives invalid embeddings", func(t *testing.T) {
		_, err := device.Search(&Buffer{}, []float32{1, 0, 0, 0}, 1, 4, 1, false)
		if err == nil {
			t.Error("expected error for invalid embeddings buffer")
		}
	})
}

func TestStorageModeConstants(t *testing.T) {
	if StorageShared != 0 {
		t.Errorf("StorageShared should be 0, got %d", StorageShared)
	}
	if StorageManaged != 1 {
		t.Errorf("StorageManaged should be 1, got %d", StorageManaged)
	}
	if StoragePrivate != 2 {
		t.Errorf("StoragePrivate should be 2, got %d", StoragePrivate)
	}
}

func TestErrors(t *testing.T) {
	errors := []error{
		ErrMetalNotAvailable,
		ErrDeviceCreation,
		ErrBufferCreation,
		ErrKernelExecution,
		ErrInvalidBuffer,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("error should not be nil")
		}
		if err.Error() == "" {
			t.Errorf("error message should not be empty: %v", err)
		}
	}
}

func TestKernelOperations_InvalidBuffersReturnErrors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	invalid := &Buffer{}

	if err := device.ComputeCosineSimilarity(invalid, invalid, invalid, 1, 4, false); err == nil {
		t.Error("expected ComputeCosineSimilarity error for invalid buffers")
	}
	if err := device.ComputeTopK(invalid, invalid, invalid, 1, 1); err == nil {
		t.Error("expected ComputeTopK error for invalid buffers")
	}
	if err := device.NormalizeVectors(invalid, 1, 4); err == nil {
		t.Error("expected NormalizeVectors error for invalid buffers")
	}
}

// Benchmarks

func BenchmarkCosineSimilarity(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Metal not available")
	}

	device, _ := NewDevice()
	defer device.Release()

	// 10K embeddings of dimension 1024
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
		device.ComputeCosineSimilarity(embBuf, queryBuf, scoresBuf, n, dims, false)
	}
}

func BenchmarkSearch(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Metal not available")
	}

	device, _ := NewDevice()
	defer device.Release()

	// 10K embeddings of dimension 1024
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		device.Search(embBuf, query, n, dims, 10, false)
	}
}

// Regression: buffer reads and writes viewed memory through a *[1 << 30]
// array type, so any access past 2^30 elements (4 GiB of float32) panicked
// with "slice bounds out of range" even though the buffer was large enough.
// Resident HNSW build buffers for corpora above ~1M 1024-dim vectors exceed it.
func TestBufferAccessBeyondFourGiB(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a buffer larger than 4 GiB")
	}
	if !IsAvailable() {
		t.Skip("Metal not available")
	}

	device, err := NewDevice()
	if err != nil {
		t.Fatalf("NewDevice() error = %v", err)
	}
	defer device.Release()

	const offset = 1 << 30 // first element past the old array-type bound
	const tail = 16
	buf, err := device.NewEmptyBuffer(uint64(offset+tail)*4, StorageShared)
	if err != nil {
		t.Skipf("device cannot allocate a %d-byte buffer: %v", uint64(offset+tail)*4, err)
	}
	defer buf.Release()

	data := []float32{1.5, 2.5, 3.5, 4.5}
	if err := buf.WriteFloat32(data, offset); err != nil {
		t.Fatalf("WriteFloat32() at offset 2^30 error = %v", err)
	}
	view := buf.Float32View(offset + tail)
	for i, want := range data {
		if got := view[offset+i]; got != want {
			t.Errorf("view[2^30+%d] = %v, want %v", i, got, want)
		}
	}
	if err := buf.WriteFloat32(data, offset+tail-len(data)+1); err == nil {
		t.Error("WriteFloat32() past the buffer end succeeded, want error")
	}
	if err := buf.WriteFloat32(data, -1); err == nil {
		t.Error("WriteFloat32() at a negative offset succeeded, want error")
	}
}
