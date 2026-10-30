package simd

import (
	"math"
	"testing"
)

// TestBatchCosineSimilarity verifies batch cosine similarity against scalar results.
func TestBatchCosineSimilarity(t *testing.T) {
	dims := 4
	// 3 vectors: [1,0,0,0], [0,1,0,0], [1,1,0,0]/sqrt(2)
	v0 := []float32{1, 0, 0, 0}
	v1 := []float32{0, 1, 0, 0}
	v2 := []float32{0.70710678, 0.70710678, 0, 0}

	embeddings := make([]float32, 0, 3*dims)
	embeddings = append(embeddings, v0...)
	embeddings = append(embeddings, v1...)
	embeddings = append(embeddings, v2...)

	query := []float32{1, 0, 0, 0}
	scores := make([]float32, 3)

	BatchCosineSimilarity(embeddings, query, scores)

	// v0 · query = 1.0 (identical direction)
	if !approxEqual(scores[0], 1.0, 1e-4) {
		t.Errorf("scores[0] = %v, want ~1.0", scores[0])
	}
	// v1 · query = 0.0 (orthogonal)
	if !approxEqual(scores[1], 0.0, 1e-4) {
		t.Errorf("scores[1] = %v, want ~0.0", scores[1])
	}
	// v2 · query ≈ 0.707
	if !approxEqual(scores[2], 0.70710678, 1e-3) {
		t.Errorf("scores[2] = %v, want ~0.707", scores[2])
	}
}

func TestBatchCosineSimilarity_EmptyQuery(t *testing.T) {
	embeddings := []float32{1, 2, 3}
	query := []float32{}
	scores := make([]float32, 1)
	// Should be a no-op (dimensions == 0)
	BatchCosineSimilarity(embeddings, query, scores)
	if scores[0] != 0 {
		t.Errorf("expected 0 for empty query, got %v", scores[0])
	}
}

func TestBatchCosineSimilarity_EmptyEmbeddings(t *testing.T) {
	embeddings := []float32{}
	query := []float32{1, 0}
	scores := make([]float32, 0)
	// numVectors = 0, should be no-op
	BatchCosineSimilarity(embeddings, query, scores)
}

func TestBatchCosineSimilarity_ScoresBufTooSmall(t *testing.T) {
	// 2 vectors of dim=3, but scores only holds 1 → numVectors guard triggers
	embeddings := []float32{1, 0, 0, 0, 1, 0}
	query := []float32{1, 0, 0}
	scores := make([]float32, 1) // smaller than numVectors (2)
	BatchCosineSimilarity(embeddings, query, scores)
}

func TestBatchCosineSimilarity_LargeBatch(t *testing.T) {
	dims := 128
	numVectors := 100

	embeddings := make([]float32, numVectors*dims)
	query := make([]float32, dims)
	scores := make([]float32, numVectors)

	// Fill with all-ones vectors (normalized) — cosine similarity should be 1
	for i := range embeddings {
		embeddings[i] = 1.0 / float32(math.Sqrt(float64(dims)))
	}
	for i := range query {
		query[i] = 1.0 / float32(math.Sqrt(float64(dims)))
	}

	BatchCosineSimilarity(embeddings, query, scores)

	for i, s := range scores {
		if !approxEqual(s, 1.0, 1e-3) {
			t.Errorf("scores[%d] = %v, want ~1.0", i, s)
		}
	}
}

// TestBatchDotProduct verifies batch dot product against scalar results.
func TestBatchDotProduct(t *testing.T) {
	dims := 3
	v0 := []float32{1, 2, 3}
	v1 := []float32{4, 5, 6}

	embeddings := make([]float32, 0, 2*dims)
	embeddings = append(embeddings, v0...)
	embeddings = append(embeddings, v1...)

	query := []float32{1, 1, 1}
	results := make([]float32, 2)

	BatchDotProduct(embeddings, query, results)

	// v0 · query = 1+2+3 = 6
	if !approxEqual(results[0], 6.0, 1e-4) {
		t.Errorf("results[0] = %v, want 6.0", results[0])
	}
	// v1 · query = 4+5+6 = 15
	if !approxEqual(results[1], 15.0, 1e-4) {
		t.Errorf("results[1] = %v, want 15.0", results[1])
	}
}

func TestBatchDotProduct_EmptyQuery(t *testing.T) {
	results := make([]float32, 1)
	BatchDotProduct([]float32{1, 2, 3}, []float32{}, results)
	if results[0] != 0 {
		t.Errorf("expected 0 for empty query, got %v", results[0])
	}
}

func TestBatchDotProduct_EmptyEmbeddings(t *testing.T) {
	BatchDotProduct([]float32{}, []float32{1, 2}, []float32{})
}

func TestBatchDotProduct_ResultsBufTooSmall(t *testing.T) {
	embeddings := []float32{1, 0, 0, 1} // 2 vectors of dim=2
	query := []float32{1, 1}
	results := make([]float32, 1) // too small
	BatchDotProduct(embeddings, query, results)
}

// TestBatchEuclideanDistance verifies batch Euclidean distance against scalar results.
func TestBatchEuclideanDistance(t *testing.T) {
	dims := 2
	v0 := []float32{0, 0}
	v1 := []float32{3, 4} // dist to origin = 5

	embeddings := make([]float32, 0, 2*dims)
	embeddings = append(embeddings, v0...)
	embeddings = append(embeddings, v1...)

	query := []float32{0, 0}
	distances := make([]float32, 2)

	BatchEuclideanDistance(embeddings, query, distances)

	if !approxEqual(distances[0], 0.0, 1e-4) {
		t.Errorf("distances[0] = %v, want 0.0", distances[0])
	}
	if !approxEqual(distances[1], 5.0, 1e-3) {
		t.Errorf("distances[1] = %v, want 5.0", distances[1])
	}
}

func TestBatchEuclideanDistance_EmptyQuery(t *testing.T) {
	BatchEuclideanDistance([]float32{1, 2}, []float32{}, []float32{1})
}

func TestBatchEuclideanDistance_EmptyEmbeddings(t *testing.T) {
	BatchEuclideanDistance([]float32{}, []float32{1, 2}, []float32{})
}

func TestBatchEuclideanDistance_DistancesBufTooSmall(t *testing.T) {
	embeddings := []float32{0, 0, 1, 0} // 2 vectors of dim=2
	query := []float32{0, 0}
	distances := make([]float32, 1) // too small
	BatchEuclideanDistance(embeddings, query, distances)
}

// TestBatchNormalize verifies batch normalization produces unit vectors.
func TestBatchNormalize(t *testing.T) {
	dims := 2
	numVectors := 3

	// [3,4] → norm=5; [1,0] → norm=1; [0,0] → stays zero
	vectors := []float32{3, 4, 1, 0, 0, 0}

	BatchNormalize(vectors, numVectors, dims)

	// First vector [3,4] → [0.6, 0.8]
	if !approxEqual(vectors[0], 0.6, 1e-4) {
		t.Errorf("vectors[0] = %v, want 0.6", vectors[0])
	}
	if !approxEqual(vectors[1], 0.8, 1e-4) {
		t.Errorf("vectors[1] = %v, want 0.8", vectors[1])
	}

	// Second vector [1,0] stays [1,0]
	if !approxEqual(vectors[2], 1.0, 1e-4) {
		t.Errorf("vectors[2] = %v, want 1.0", vectors[2])
	}
	if !approxEqual(vectors[3], 0.0, 1e-4) {
		t.Errorf("vectors[3] = %v, want 0.0", vectors[3])
	}

	// Third vector [0,0] stays [0,0]
	if vectors[4] != 0 || vectors[5] != 0 {
		t.Errorf("zero vector should remain zero, got [%v, %v]", vectors[4], vectors[5])
	}
}

func TestBatchNormalize_ZeroNumVectors(t *testing.T) {
	vectors := []float32{1, 2, 3, 4}
	BatchNormalize(vectors, 0, 2)
}

func TestBatchNormalize_ZeroDimensions(t *testing.T) {
	vectors := []float32{1, 2, 3, 4}
	BatchNormalize(vectors, 2, 0)
}

func TestBatchNormalize_BufferTooSmall(t *testing.T) {
	// len(vectors) < numVectors*dimensions → should be no-op
	vectors := []float32{1, 2}    // only 2 elements
	BatchNormalize(vectors, 3, 2) // needs 6 elements
}

func TestBatchNormalize_SingleVector(t *testing.T) {
	vectors := []float32{3, 4}
	BatchNormalize(vectors, 1, 2)
	norm := float32(math.Sqrt(float64(vectors[0]*vectors[0] + vectors[1]*vectors[1])))
	if !approxEqual(norm, 1.0, 1e-4) {
		t.Errorf("normalized norm = %v, want 1.0", norm)
	}
}

func TestBatchNormalize_ResultsConsistentWithNormalizeInPlace(t *testing.T) {
	dims := 3
	numVectors := 4

	original := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 0, 0}

	// BatchNormalize copy
	batch := make([]float32, len(original))
	copy(batch, original)
	BatchNormalize(batch, numVectors, dims)

	// NormalizeInPlace copy, vector by vector
	scalar := make([]float32, len(original))
	copy(scalar, original)
	for i := 0; i < numVectors; i++ {
		start := i * dims
		end := start + dims
		NormalizeInPlace(scalar[start:end])
	}

	for i, b := range batch {
		if !approxEqual(b, scalar[i], 1e-4) {
			t.Errorf("batch[%d] = %v, scalar[%d] = %v", i, b, i, scalar[i])
		}
	}
}
