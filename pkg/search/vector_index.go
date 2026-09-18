// Package search provides vector indexing with cosine similarity search.
//
// This file implements a simple but effective vector similarity search index
// using brute-force cosine similarity calculation. While not as sophisticated
// as HNSW or other approximate methods, it provides exact results and is
// suitable for moderate-sized datasets.
//
// Key Features:
//   - Exact cosine similarity search
//   - Automatic vector normalization for performance
//   - Thread-safe concurrent operations
//   - Context-aware search with cancellation
//   - Configurable similarity thresholds
//
// Example Usage:
//
//	// Create vector index for 1024-dimensional embeddings
//	index := search.NewVectorIndex(1024)
//
//	// Add vectors to the index
//	embedding1 := make([]float32, 1024)
//	// ... populate embedding1 ...
//	index.Add("doc-1", embedding1)
//
//	embedding2 := make([]float32, 1024)
//	// ... populate embedding2 ...
//	index.Add("doc-2", embedding2)
//
//	// Search for similar vectors
//	query := make([]float32, 1024)
//	// ... populate query vector ...
//	results, err := index.Search(ctx, query, 10, 0.7)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Process results (sorted by similarity)
//	for _, result := range results {
//		fmt.Printf("ID: %s, Similarity: %.3f\n", result.ID, result.Score)
//	}
//
// Algorithm Details:
//
// The index uses cosine similarity, which measures the cosine of the angle
// between two vectors. It's particularly effective for high-dimensional
// embeddings where magnitude is less important than direction.
//
// Cosine Similarity Formula:
//
//	similarity = (A · B) / (||A|| × ||B||)
//
// Where:
//   - A · B is the dot product of vectors A and B
//   - ||A|| is the magnitude (L2 norm) of vector A
//   - ||B|| is the magnitude (L2 norm) of vector B
//
// Optimization:
//
//	Vectors are normalized when added to the index, so cosine similarity
//	becomes just the dot product, making search much faster.
//
// Performance Characteristics:
//   - Add: O(d) where d is vector dimensions
//   - Search: O(n×d) where n is number of vectors
//   - Memory: O(n×d) for storing normalized vectors
//   - Thread-safe: Uses RWMutex for concurrent access
//
// When to Use:
//
//	✅ Exact similarity search required
//	✅ Moderate dataset sizes (<100K vectors)
//	✅ High-dimensional embeddings (>100 dimensions)
//	✅ Simplicity and reliability preferred
//	❌ Very large datasets (>1M vectors)
//	❌ Approximate search acceptable
//	❌ Sub-linear search time required
//
// Future Improvements:
//   - HNSW implementation for O(log n) search
//   - GPU acceleration for large batches
//   - Quantization for memory efficiency
//   - Incremental index updates
//
// ELI12 (Explain Like I'm 12):
//
// Think of vector similarity like comparing the "direction" things are pointing:
//
//  1. **Vectors**: Like arrows pointing in different directions in space.
//     Each arrow represents a document, image, or piece of text.
//
//  2. **Cosine similarity**: Measures how much two arrows point in the
//     same direction. If they point exactly the same way, similarity = 1.
//     If they point opposite ways, similarity = -1.
//
//  3. **Normalization**: We make all arrows the same length so we only
//     care about direction, not how "strong" they are.
//
//  4. **Search**: When you give us a query arrow, we check how similar
//     its direction is to all the arrows we've stored, then give you
//     the most similar ones.
//
// It's like finding documents that "point in the same direction" as your search!
package search

import (
	"context"
	"errors"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/util"
)

var (
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
)

// VectorIndex provides exact vector similarity search using cosine similarity.
//
// The index stores normalized vectors for efficient similarity computation
// and supports concurrent read/write operations. It uses brute-force search
// which provides exact results but has O(n) time complexity.
//
// Key features:
//   - Exact cosine similarity computation
//   - Automatic vector normalization
//   - Thread-safe concurrent operations
//   - Context-aware search with cancellation
//   - Configurable similarity thresholds
//
// Example:
//
//	// Create index for 512-dimensional vectors
//	index := search.NewVectorIndex(512)
//
//	// Add vectors
//	for id, vector := range vectors {
//		index.Add(id, vector)
//	}
//
//	// Search with minimum similarity threshold
//	results, _ := index.Search(ctx, queryVector, 5, 0.8)
//	for _, result := range results {
//		fmt.Printf("%s: %.3f\n", result.ID, result.Score)
//	}
//
// Performance:
//   - Add: O(d) where d = dimensions
//   - Search: O(n×d) where n = number of vectors
//   - Memory: O(n×d) for normalized vectors
//
// Thread Safety:
//
//	All methods are thread-safe using RWMutex for concurrent access.
type VectorIndex struct {
	dimensions int
	mu         sync.RWMutex
	// vectors holds normalized vectors (unit length) for cosine similarity and HNSW.
	vectors map[string][]float32
	// rawVectors is sparse: it only retains non-unit inputs whose original
	// magnitude is required by Cypher dot/euclidean similarity semantics.
	rawVectors map[string][]float32
}

// NewVectorIndex creates a new vector similarity index for the given dimensions.
//
// The index is initialized empty and ready to accept vectors of the specified
// dimensionality. All vectors added to the index must have exactly this
// number of dimensions.
//
// Parameters:
//   - dimensions: Number of dimensions for vectors (must be > 0)
//
// Returns:
//   - VectorIndex ready for use
//
// Example:
//
//	// Create index for OpenAI embeddings (1536 dimensions)
//	index := search.NewVectorIndex(1536)
//
//	// Create index for sentence transformers (384 dimensions)
//	index = search.NewVectorIndex(384)
//
//	// Index is ready to accept vectors
//	index.Add("doc1", embedding1)
func NewVectorIndex(dimensions int) *VectorIndex {
	return &VectorIndex{
		dimensions: dimensions,
		vectors:    make(map[string][]float32),
		rawVectors: make(map[string][]float32),
	}
}

// Add adds or updates a vector in the index with automatic normalization.
//
// The vector is normalized to unit length for efficient cosine similarity
// computation. If a vector with the same ID already exists, it is replaced.
//
// Parameters:
//   - id: Unique identifier for the vector
//   - vector: Vector to add (must match index dimensions)
//
// Returns:
//   - ErrDimensionMismatch if vector dimensions don't match index
//
// Example:
//
//	// Add document embeddings
//	for docID, embedding := range documentEmbeddings {
//		err := index.Add(docID, embedding)
//		if err != nil {
//			log.Printf("Failed to add %s: %v", docID, err)
//		}
//	}
//
//	// Update existing vector
//	newEmbedding := getUpdatedEmbedding("doc1")
//	index.Add("doc1", newEmbedding) // Replaces previous
//
// Performance:
//   - Time: O(d) where d is vector dimensions
//   - Space: O(d) additional storage per vector
//   - Thread-safe: Uses write lock during update
//
// Normalization:
//
//	Vectors are automatically normalized to unit length, so cosine
//	similarity becomes a simple dot product during search.
func (v *VectorIndex) Add(id string, vec []float32) error {
	if len(vec) != v.dimensions {
		return ErrDimensionMismatch
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	// Unit-length embeddings need only one private copy. Preserve a sparse raw
	// copy for non-unit vectors so Cypher dot/euclidean behavior does not change.
	private := append([]float32(nil), vec...)
	if !rawVectorNeeded(private) {
		vector.NormalizeInPlace(private)
		delete(v.rawVectors, id)
		v.vectors[id] = private
		return nil
	}
	v.rawVectors[id] = private
	normalized := append([]float32(nil), private...)
	vector.NormalizeInPlace(normalized)
	v.vectors[id] = normalized
	return nil
}

func rawVectorNeeded(vec []float32) bool {
	normSquared := float64(0)
	for _, value := range vec {
		normSquared += float64(value) * float64(value)
	}
	return normSquared != 0 && math.Abs(math.Sqrt(normSquared)-1) > 1e-5
}

// Remove removes a vector from the index by its ID.
//
// If the vector doesn't exist, this operation is a no-op.
//
// Parameters:
//   - id: Identifier of the vector to remove
//
// Example:
//
//	// Remove a document that was deleted
//	index.Remove("doc-123")
//
//	// Remove multiple vectors
//	for _, docID := range deletedDocs {
//		index.Remove(docID)
//	}
//
// Performance:
//   - Time: O(1) hash map deletion
//   - Thread-safe: Uses write lock during removal
func (v *VectorIndex) Remove(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.vectors, id)
	delete(v.rawVectors, id)
}

// Search finds vectors similar to the query vector using cosine similarity.
//
// The search computes cosine similarity between the query and all indexed
// vectors, filters by minimum similarity threshold, and returns the top
// results sorted by similarity score (highest first).
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - query: Query vector (must match index dimensions)
//   - limit: Maximum number of results to return
//   - minSimilarity: Minimum similarity threshold (0.0 to 1.0)
//
// Returns:
//   - Slice of indexResult sorted by similarity (descending)
//   - ErrDimensionMismatch if query dimensions don't match
//
// Example:
//
//	// Search for top 10 most similar vectors
//	results, err := index.Search(ctx, queryVector, 10, 0.0)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Search with high similarity threshold
//	results, err = index.Search(ctx, queryVector, 5, 0.8)
//	for _, result := range results {
//		fmt.Printf("ID: %s, Similarity: %.3f\n", result.ID, result.Score)
//	}
//
//	// Search with timeout
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	results, err = index.Search(ctx, queryVector, 10, 0.7)
//
// Performance:
//   - Time: O(n×d) where n=vectors, d=dimensions
//   - Space: O(k) where k=number of results
//   - Cancellation: Respects context cancellation during search
//
// Similarity Range:
//   - 1.0: Identical vectors (same direction)
//   - 0.0: Orthogonal vectors (perpendicular)
//   - -1.0: Opposite vectors (opposite directions)
//
// Thread Safety:
//
//	Uses read lock during search, allowing concurrent searches.
func (v *VectorIndex) Search(ctx context.Context, query []float32, limit int, minSimilarity float64) ([]indexResult, error) {
	if len(query) != v.dimensions {
		return nil, ErrDimensionMismatch
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	// Normalize query
	normalizedQuery := vector.Normalize(query)

	// Calculate similarities (brute force)
	type scored struct {
		id    string
		score float64
	}
	var results []scored

	for id, vec := range v.vectors {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Dot product of normalized vectors = cosine similarity
		sim := vector.DotProduct(normalizedQuery, vec)
		if sim >= minSimilarity {
			results = append(results, scored{id: id, score: sim})
		}
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	// Limit results
	if len(results) > limit {
		results = results[:limit]
	}

	// Convert to indexResult
	output := make([]indexResult, len(results))
	for i, r := range results {
		output[i] = indexResult{ID: r.id, Score: r.score}
	}

	return output, nil
}

// Count returns the number of vectors in the index.
func (v *VectorIndex) Count() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.vectors)
}

// Clear removes all vectors from the index.
// This is useful when regenerating all embeddings to reset the index state.
func (v *VectorIndex) Clear() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.vectors = make(map[string][]float32)
	v.rawVectors = make(map[string][]float32)
}

// HasVector checks if a vector exists for the given ID.
func (v *VectorIndex) HasVector(id string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, exists := v.vectors[id]
	return exists
}

// GetVector returns a copy of the normalized vector for id, or (nil, false) if not found.
// Used when loading a graph-only HNSW index to populate vectors from the vector index.
func (v *VectorIndex) GetVector(id string) ([]float32, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	vec, exists := v.vectors[id]
	if !exists || len(vec) == 0 {
		return nil, false
	}
	out := make([]float32, len(vec))
	copy(out, vec)
	return out, true
}

// getVectorRef returns an immutable internal vector reference for in-package
// search indexes. Add replaces whole slices and never mutates a published
// vector, so a reference remains safe after the read lock is released.
func (v *VectorIndex) getVectorRef(id string) ([]float32, bool) {
	v.mu.RLock()
	vec, exists := v.vectors[id]
	v.mu.RUnlock()
	return vec, exists && len(vec) > 0
}

// GetDimensions returns the vector dimensions.
func (v *VectorIndex) GetDimensions() int {
	return v.dimensions
}

// vectorIndexFormatVersion is the semver written into saved index files (Qdrant-style).
// On load, if the file's version is not equal to this, the file is not loaded (caller rebuilds).
const vectorIndexFormatVersion = "1.0.0"

// vectorIndexSnapshot is the serializable form of the vector index (no mutex).
type vectorIndexSnapshot struct {
	Version    string
	Dimensions int
	Vectors    map[string][]float32
	// RawVectors is retained only to decode snapshots written by older builds.
	RawVectors map[string][]float32 `msgpack:"RawVectors,omitempty"`
}

// Save writes the vector index to path (msgpack format). Dir is created if needed.
// Copies index data under a short read lock so I/O does not block Search/IndexNode.
func (v *VectorIndex) Save(path string) error {
	v.mu.RLock()
	dimensions := v.dimensions
	vectors := make(map[string][]float32, len(v.vectors))
	for id, vec := range v.vectors {
		if len(vec) > 0 {
			vectors[id] = append([]float32(nil), vec...)
		}
	}
	rawVectors := make(map[string][]float32, len(v.rawVectors))
	for id, vec := range v.rawVectors {
		rawVectors[id] = append([]float32(nil), vec...)
	}
	v.mu.RUnlock()

	snap := vectorIndexSnapshot{
		Version:    vectorIndexFormatVersion,
		Dimensions: dimensions,
		Vectors:    vectors,
		RawVectors: rawVectors,
	}
	return writeMsgpackSnapshot(path, &snap)
}

// Load replaces the vector index with the one stored at path (msgpack format).
// If the file does not exist or decode fails, the index is left empty (or unchanged on
// missing file) and no error is returned, so the caller can proceed with a full rebuild.
// Returns an error only for unexpected I/O (e.g. permission denied).
func (v *VectorIndex) Load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // No saved index; caller will rebuild
		}
		return err
	}
	defer file.Close()

	var snap vectorIndexSnapshot
	if err := util.DecodeMsgpackFile(file, &snap); err != nil {
		// Corrupt or wrong format; clear index so caller rebuilds
		v.mu.Lock()
		v.vectors = make(map[string][]float32)
		v.rawVectors = make(map[string][]float32)
		v.mu.Unlock()
		return nil
	}
	if snap.Vectors == nil {
		snap.Vectors = make(map[string][]float32)
	}
	if !searchIndexVersionCompatible(snap.Version, vectorIndexFormatVersion, "vector") {
		v.mu.Lock()
		v.vectors = make(map[string][]float32)
		v.rawVectors = make(map[string][]float32)
		v.mu.Unlock()
		return nil
	}
	// Require dimensions to match the existing index (service was created with fixed dimensions).
	if snap.Dimensions <= 0 || snap.Dimensions != v.dimensions {
		v.mu.Lock()
		v.vectors = make(map[string][]float32)
		v.rawVectors = make(map[string][]float32)
		v.mu.Unlock()
		return nil // Mismatch or invalid; caller will rebuild
	}

	v.mu.Lock()
	v.vectors = snap.Vectors
	v.rawVectors = snap.RawVectors
	if v.rawVectors == nil {
		v.rawVectors = make(map[string][]float32)
	} else {
		for id, raw := range v.rawVectors {
			if !rawVectorNeeded(raw) {
				delete(v.rawVectors, id)
			}
		}
	}
	v.mu.Unlock()
	return nil
}
