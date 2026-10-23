//go:build !cgovulkan

package vulkan

import (
	"math"
	"testing"
)

// TestComputeContextCreation tests that we can create a compute context with shader pipelines
func TestComputeContextCreation(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// Verify all pipelines were created
	if ctx.pipelines[PipelineCosineSimilarity] == nil {
		t.Error("Cosine similarity pipeline not created")
	}
	if ctx.pipelines[PipelineNormalize] == nil {
		t.Error("Normalize pipeline not created")
	}
	if ctx.pipelines[PipelineTopK] == nil {
		t.Error("TopK pipeline not created")
	}
}

// TestComputeContextNormalize tests GPU-accelerated vector normalization
func TestComputeContextNormalize(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// Test vector: [3, 4, 0] -> norm = 5 -> normalized = [0.6, 0.8, 0]
	data := []float32{3.0, 4.0, 0.0}
	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	err = ctx.NormalizeVectorsGPU(buffer, 1, 3)
	if err != nil {
		t.Fatalf("NormalizeVectorsGPU failed: %v", err)
	}

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

// TestComputeContextCosineSimilarity tests GPU-accelerated cosine similarity
func TestComputeContextCosineSimilarity(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 3 normalized vectors of dimension 4
	// v0 = [1, 0, 0, 0]
	// v1 = [0, 1, 0, 0]
	// v2 = [0.707, 0.707, 0, 0]  (45 degrees from v0)
	embeddings := []float32{
		1, 0, 0, 0,
		0, 1, 0, 0,
		0.707, 0.707, 0, 0,
	}

	// Query: [1, 0, 0, 0] - should have similarity 1.0 with v0, 0.0 with v1, 0.707 with v2
	query := []float32{1, 0, 0, 0}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer for embeddings failed: %v", err)
	}
	defer embBuffer.Release()

	queryBuffer, err := device.NewBuffer(query)
	if err != nil {
		t.Fatalf("NewBuffer for query failed: %v", err)
	}
	defer queryBuffer.Release()

	scoresBuffer, err := device.NewEmptyBuffer(3)
	if err != nil {
		t.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer scoresBuffer.Release()

	err = ctx.CosineSimilarityGPU(embBuffer, queryBuffer, scoresBuffer, 3, 4, true)
	if err != nil {
		t.Fatalf("CosineSimilarityGPU failed: %v", err)
	}

	scores := scoresBuffer.ReadFloat32(3)
	if scores == nil {
		t.Fatal("Failed to read scores")
	}

	tolerance := float32(0.01)
	expectedScores := []float32{1.0, 0.0, 0.707}
	for i, exp := range expectedScores {
		if absF32(scores[i]-exp) > tolerance {
			t.Errorf("scores[%d] = %f, want %f", i, scores[i], exp)
		}
	}
}

// TestComputeContextSearchGPU tests end-to-end GPU search
func TestComputeContextSearchGPU(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// Create 100 random-ish embeddings of dimension 128
	n := 100
	dims := 128
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%dims) / float32(dims)
	}

	// Normalize embeddings
	for i := 0; i < n; i++ {
		var norm float32
		for d := 0; d < dims; d++ {
			v := embeddings[i*dims+d]
			norm += v * v
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 0 {
			for d := 0; d < dims; d++ {
				embeddings[i*dims+d] /= norm
			}
		}
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	// Query: same as first embedding
	query := make([]float32, dims)
	copy(query, embeddings[:dims])

	results, err := ctx.SearchGPU(embBuffer, query, uint32(n), uint32(dims), 5, true)
	if err != nil {
		t.Fatalf("SearchGPU failed: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Got %d results, want 5", len(results))
	}

	// First result should be the same vector (index 0, score ~1.0)
	if len(results) > 0 {
		t.Logf("Top result: index=%d, score=%f", results[0].Index, results[0].Score)
		if results[0].Score < 0.99 {
			t.Errorf("Expected top score > 0.99, got %f", results[0].Score)
		}
	}
}

// TestComputeContextLargeScale tests GPU performance with larger data
func TestComputeContextLargeScale(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}
	if testing.Short() {
		t.Skip("Skipping large scale test in short mode")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 10000 embeddings of dimension 1024
	n := 10000
	dims := 1024
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%1000) / 1000.0
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}

	results, err := ctx.SearchGPU(embBuffer, query, uint32(n), uint32(dims), 10, false)
	if err != nil {
		t.Fatalf("SearchGPU failed: %v", err)
	}

	t.Logf("Large scale search completed: %d results", len(results))
	for i, r := range results {
		t.Logf("  %d: index=%d score=%f", i, r.Index, r.Score)
	}
}

func TestComputeContextTopKDistinctScores(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// Create embeddings where each one has a predictable similarity to query
	// v[i] = [1-i/n, i/n, 0, 0, ...] for varying angles
	n := 50
	dims := 32
	embeddings := make([]float32, n*dims)

	for i := 0; i < n; i++ {
		// Create vectors at different angles from the query
		angle := float64(i) / float64(n) * math.Pi / 2 // 0 to 90 degrees
		embeddings[i*dims+0] = float32(math.Cos(angle))
		embeddings[i*dims+1] = float32(math.Sin(angle))
		// Rest are zeros
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	// Query: [1, 0, 0, ...] - aligned with first vector
	query := make([]float32, dims)
	query[0] = 1.0

	results, err := ctx.SearchGPU(embBuffer, query, uint32(n), uint32(dims), 10, true)
	if err != nil {
		t.Fatalf("SearchGPU failed: %v", err)
	}

	// Results should be in descending order of similarity
	// Lower indices should have higher scores (closer to query)
	t.Logf("Top-K with distinct scores:")
	for i, r := range results {
		t.Logf("  %d: index=%d score=%f", i, r.Index, r.Score)
	}

	// First result should be index 0 (identical direction)
	if results[0].Index != 0 {
		t.Errorf("Expected first result index=0, got %d", results[0].Index)
	}
	if results[0].Score < 0.99 {
		t.Errorf("Expected first score ~1.0, got %f", results[0].Score)
	}

	// Scores should be in descending order
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score+0.001 {
			t.Errorf("Scores not in descending order: [%d]=%f > [%d]=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}

	// Indices should be in ascending order (lower indices = more similar)
	for i := 1; i < len(results); i++ {
		if results[i].Index < results[i-1].Index {
			// This is OK, indices don't have to be ordered, but scores should be
		}
	}
}

// TestComputeContextOrthogonalVectors tests cosine similarity with orthogonal vectors
func TestComputeContextOrthogonalVectors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 4 orthogonal unit vectors in 4D
	embeddings := []float32{
		1, 0, 0, 0, // v0
		0, 1, 0, 0, // v1
		0, 0, 1, 0, // v2
		0, 0, 0, 1, // v3
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	queryBuffer, err := device.NewBuffer([]float32{1, 0, 0, 0})
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer queryBuffer.Release()

	scoresBuffer, err := device.NewEmptyBuffer(4)
	if err != nil {
		t.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer scoresBuffer.Release()

	err = ctx.CosineSimilarityGPU(embBuffer, queryBuffer, scoresBuffer, 4, 4, true)
	if err != nil {
		t.Fatalf("CosineSimilarityGPU failed: %v", err)
	}

	scores := scoresBuffer.ReadFloat32(4)
	t.Logf("Orthogonal scores: %v", scores)

	tolerance := float32(0.001)
	expected := []float32{1.0, 0.0, 0.0, 0.0}
	for i, exp := range expected {
		if absF32(scores[i]-exp) > tolerance {
			t.Errorf("scores[%d] = %f, want %f", i, scores[i], exp)
		}
	}
}

// TestComputeContextNegativeSimilarity tests vectors pointing in opposite directions
func TestComputeContextNegativeSimilarity(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// v0 = [1, 0], v1 = [-1, 0] (opposite), v2 = [0.707, 0.707]
	embeddings := []float32{
		1, 0,
		-1, 0,
		0.707, 0.707,
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	queryBuffer, err := device.NewBuffer([]float32{1, 0})
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer queryBuffer.Release()

	scoresBuffer, err := device.NewEmptyBuffer(3)
	if err != nil {
		t.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer scoresBuffer.Release()

	err = ctx.CosineSimilarityGPU(embBuffer, queryBuffer, scoresBuffer, 3, 2, true)
	if err != nil {
		t.Fatalf("CosineSimilarityGPU failed: %v", err)
	}

	scores := scoresBuffer.ReadFloat32(3)
	t.Logf("Negative similarity scores: %v", scores)

	tolerance := float32(0.01)
	expected := []float32{1.0, -1.0, 0.707}
	for i, exp := range expected {
		if absF32(scores[i]-exp) > tolerance {
			t.Errorf("scores[%d] = %f, want %f", i, scores[i], exp)
		}
	}
}

// TestComputeContextHighDimensions tests with high-dimensional vectors (like real embeddings)
func TestComputeContextHighDimensions(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// Test with 1024 dimensions (typical embedding size)
	dims := 1024
	n := 100

	// Create n vectors where vector i has more weight in dimension i%dims
	embeddings := make([]float32, n*dims)
	for i := 0; i < n; i++ {
		// Base uniform values
		for d := 0; d < dims; d++ {
			embeddings[i*dims+d] = 0.1
		}
		// Add spike at dimension i%dims
		embeddings[i*dims+(i%dims)] = 1.0

		// Normalize
		var norm float32
		for d := 0; d < dims; d++ {
			v := embeddings[i*dims+d]
			norm += v * v
		}
		norm = float32(math.Sqrt(float64(norm)))
		for d := 0; d < dims; d++ {
			embeddings[i*dims+d] /= norm
		}
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	// Query: spike at dimension 0
	query := make([]float32, dims)
	for d := 0; d < dims; d++ {
		query[d] = 0.1
	}
	query[0] = 1.0
	var qnorm float32
	for _, v := range query {
		qnorm += v * v
	}
	qnorm = float32(math.Sqrt(float64(qnorm)))
	for d := range query {
		query[d] /= qnorm
	}

	results, err := ctx.SearchGPU(embBuffer, query, uint32(n), uint32(dims), 5, true)
	if err != nil {
		t.Fatalf("SearchGPU failed: %v", err)
	}

	t.Logf("High-dimensional search results:")
	for i, r := range results {
		t.Logf("  %d: index=%d score=%f", i, r.Index, r.Score)
	}

	// Index 0 should be the best match (same spike pattern)
	if results[0].Index != 0 {
		t.Errorf("Expected best match at index 0, got %d", results[0].Index)
	}
}

// TestComputeContextNormalizeBatch tests normalizing multiple vectors
func TestComputeContextNormalizeBatch(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 3 vectors of dimension 4
	data := []float32{
		3, 4, 0, 0, // norm=5 -> [0.6, 0.8, 0, 0]
		0, 0, 1, 0, // norm=1 -> [0, 0, 1, 0]
		1, 1, 1, 1, // norm=2 -> [0.5, 0.5, 0.5, 0.5]
	}

	buffer, err := device.NewBuffer(data)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer buffer.Release()

	err = ctx.NormalizeVectorsGPU(buffer, 3, 4)
	if err != nil {
		t.Fatalf("NormalizeVectorsGPU failed: %v", err)
	}

	result := buffer.ReadFloat32(12)
	t.Logf("Normalized vectors: %v", result)

	tolerance := float32(0.01)

	// Check each vector is unit length
	for i := 0; i < 3; i++ {
		var norm float32
		for d := 0; d < 4; d++ {
			v := result[i*4+d]
			norm += v * v
		}
		norm = float32(math.Sqrt(float64(norm)))
		if absF32(norm-1.0) > tolerance {
			t.Errorf("Vector %d norm = %f, want 1.0", i, norm)
		}
	}

	// Check specific values
	expected := []float32{
		0.6, 0.8, 0, 0,
		0, 0, 1, 0,
		0.5, 0.5, 0.5, 0.5,
	}
	for i, exp := range expected {
		if absF32(result[i]-exp) > tolerance {
			t.Errorf("result[%d] = %f, want %f", i, result[i], exp)
		}
	}
}

// TestComputeContextSearchK1 tests search with k=1
func TestComputeContextSearchK1(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 10 vectors, where index 7 is identical to query
	dims := 8
	n := 10
	embeddings := make([]float32, n*dims)
	for i := 0; i < n; i++ {
		for d := 0; d < dims; d++ {
			embeddings[i*dims+d] = float32(i*dims + d)
		}
	}
	// Make index 7 special
	for d := 0; d < dims; d++ {
		embeddings[7*dims+d] = float32(d + 100)
	}

	// Normalize
	for i := 0; i < n; i++ {
		var norm float32
		for d := 0; d < dims; d++ {
			v := embeddings[i*dims+d]
			norm += v * v
		}
		norm = float32(math.Sqrt(float64(norm)))
		for d := 0; d < dims; d++ {
			embeddings[i*dims+d] /= norm
		}
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	// Query: same as index 7
	query := make([]float32, dims)
	copy(query, embeddings[7*dims:(7+1)*dims])

	results, err := ctx.SearchGPU(embBuffer, query, uint32(n), uint32(dims), 1, true)
	if err != nil {
		t.Fatalf("SearchGPU failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	t.Logf("K=1 result: index=%d score=%f", results[0].Index, results[0].Score)

	if results[0].Index != 7 {
		t.Errorf("Expected index=7, got %d", results[0].Index)
	}
	if results[0].Score < 0.99 {
		t.Errorf("Expected score~1.0, got %f", results[0].Score)
	}
}

// TestComputeContextUnnormalizedVectors tests with unnormalized input
func TestComputeContextUnnormalizedVectors(t *testing.T) {
	if !IsAvailable() {
		t.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		t.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		t.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// Unnormalized vectors of varying magnitudes but same direction
	embeddings := []float32{
		1, 0, 0, 0, // unit
		2, 0, 0, 0, // 2x magnitude
		0.5, 0, 0, 0, // 0.5x magnitude
		0, 1, 0, 0, // orthogonal
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	queryBuffer, err := device.NewBuffer([]float32{3, 0, 0, 0}) // 3x magnitude
	if err != nil {
		t.Fatalf("NewBuffer failed: %v", err)
	}
	defer queryBuffer.Release()

	scoresBuffer, err := device.NewEmptyBuffer(4)
	if err != nil {
		t.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer scoresBuffer.Release()

	// With normalized=false, cosine similarity should normalize internally
	err = ctx.CosineSimilarityGPU(embBuffer, queryBuffer, scoresBuffer, 4, 4, false)
	if err != nil {
		t.Fatalf("CosineSimilarityGPU failed: %v", err)
	}

	scores := scoresBuffer.ReadFloat32(4)
	t.Logf("Unnormalized vector scores: %v", scores)

	tolerance := float32(0.01)
	// First three vectors should all have similarity 1.0 (same direction)
	// Fourth vector should have similarity 0.0 (orthogonal)
	expected := []float32{1.0, 1.0, 1.0, 0.0}
	for i, exp := range expected {
		if absF32(scores[i]-exp) > tolerance {
			t.Errorf("scores[%d] = %f, want %f", i, scores[i], exp)
		}
	}
}

// Benchmarks to compare GPU vs CPU performance

// BenchmarkGPUCosineSimilarity benchmarks GPU cosine similarity
func BenchmarkGPUCosineSimilarity(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		b.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 10,000 embeddings of 1024 dimensions
	n := 10000
	dims := 1024
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%1000) / 1000.0
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		b.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}
	queryBuffer, err := device.NewBuffer(query)
	if err != nil {
		b.Fatalf("NewBuffer failed: %v", err)
	}
	defer queryBuffer.Release()

	scoresBuffer, err := device.NewEmptyBuffer(uint64(n))
	if err != nil {
		b.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer scoresBuffer.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := ctx.CosineSimilarityGPU(embBuffer, queryBuffer, scoresBuffer, uint32(n), uint32(dims), false)
		if err != nil {
			b.Fatalf("CosineSimilarityGPU failed: %v", err)
		}
	}
}

// BenchmarkCPUCosineSimilarity benchmarks CPU cosine similarity for comparison
func BenchmarkCPUCosineSimilarity(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 10,000 embeddings of 1024 dimensions
	n := 10000
	dims := 1024
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%1000) / 1000.0
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		b.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}
	queryBuffer, err := device.NewBuffer(query)
	if err != nil {
		b.Fatalf("NewBuffer failed: %v", err)
	}
	defer queryBuffer.Release()

	scoresBuffer, err := device.NewEmptyBuffer(uint64(n))
	if err != nil {
		b.Fatalf("NewEmptyBuffer failed: %v", err)
	}
	defer scoresBuffer.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use the CPU fallback method
		err := device.CosineSimilarity(embBuffer, queryBuffer, scoresBuffer, uint32(n), uint32(dims), false)
		if err != nil {
			b.Fatalf("CosineSimilarity failed: %v", err)
		}
	}
}

// BenchmarkGPUSearchComplete benchmarks complete GPU search pipeline
func BenchmarkGPUSearchComplete(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	ctx, err := device.NewComputeContext()
	if err != nil {
		b.Fatalf("NewComputeContext failed: %v", err)
	}
	defer ctx.Release()

	// 10,000 embeddings of 1024 dimensions
	n := 10000
	dims := 1024
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%1000) / 1000.0
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		b.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ctx.SearchGPU(embBuffer, query, uint32(n), uint32(dims), 10, false)
		if err != nil {
			b.Fatalf("SearchGPU failed: %v", err)
		}
	}
}

// BenchmarkCPUSearchComplete benchmarks complete CPU search for comparison
func BenchmarkCPUSearchComplete(b *testing.B) {
	if !IsAvailable() {
		b.Skip("Vulkan not available")
	}

	device, err := NewDevice(0)
	if err != nil {
		b.Fatalf("NewDevice failed: %v", err)
	}
	defer device.Release()

	// 10,000 embeddings of 1024 dimensions
	n := 10000
	dims := 1024
	embeddings := make([]float32, n*dims)
	for i := range embeddings {
		embeddings[i] = float32(i%1000) / 1000.0
	}

	embBuffer, err := device.NewBuffer(embeddings)
	if err != nil {
		b.Fatalf("NewBuffer failed: %v", err)
	}
	defer embBuffer.Release()

	query := make([]float32, dims)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := device.Search(embBuffer, query, uint32(n), uint32(dims), 10, false)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
	}
}
