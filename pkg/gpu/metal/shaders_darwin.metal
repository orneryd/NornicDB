// Metal Compute Shaders for NornicDB GPU-Accelerated Vector Search
//
// This file contains Metal Shading Language (MSL) compute kernels for:
// 1. Cosine similarity computation (parallel across all embeddings)
// 2. Top-k selection using parallel reduction
//
// Memory Layout:
//   - embeddings: Contiguous float array [n × dimensions]
//   - query: Single query vector [dimensions]
//   - scores: Output similarity scores [n]
//   - indices: Top-k indices output [k]
//
// Performance Characteristics:
//   - M1/M2/M3 GPUs can process millions of embeddings/second
//   - Memory bandwidth: ~200-400 GB/s on Apple Silicon
//   - Optimal for embedding dimensions 128-4096

#include <metal_stdlib>
using namespace metal;

// =============================================================================
// Constants
// =============================================================================

constant uint THREADS_PER_THREADGROUP = 256;
constant uint MAX_K = 100;  // Maximum top-k supported

// =============================================================================
// Kernel: Cosine Similarity (Normalized Vectors)
// =============================================================================
// For pre-normalized vectors, cosine similarity = dot product
// This is the fast path - vectors are normalized on CPU before upload
//
// Input:
//   embeddings: [n × dimensions] contiguous float array
//   query: [dimensions] query vector (normalized)
//   n: number of embeddings
//   dimensions: embedding dimensions
//
// Output:
//   scores: [n] similarity scores

kernel void cosine_similarity_normalized(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* scores [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    
    // Compute dot product for this embedding
    float dot = 0.0f;
    ulong base = (ulong)gid * dimensions;
    
    // Unrolled loop for common dimensions (1024)
    // Metal compiler will optimize this further
    for (uint i = 0; i < dimensions; i += 4) {
        if (i + 3 < dimensions) {
            dot += embeddings[base + i] * query[i];
            dot += embeddings[base + i + 1] * query[i + 1];
            dot += embeddings[base + i + 2] * query[i + 2];
            dot += embeddings[base + i + 3] * query[i + 3];
        } else {
            for (uint j = i; j < dimensions; j++) {
                dot += embeddings[base + j] * query[j];
            }
        }
    }
    
    scores[gid] = dot;
}

// =============================================================================
// Kernel: Cosine Similarity (Unnormalized Vectors)
// =============================================================================
// Full cosine similarity for vectors that aren't pre-normalized
// Slower due to additional norm computations
//
// cosine_sim = dot(a, b) / (||a|| * ||b||)

kernel void cosine_similarity_full(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* scores [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    
    float dot = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;
    
    ulong base = (ulong)gid * dimensions;
    
    for (uint i = 0; i < dimensions; i++) {
        float a = embeddings[base + i];
        float b = query[i];
        dot += a * b;
        normA += a * a;
        normB += b * b;
    }
    
    // Handle zero vectors
    if (normA == 0.0f || normB == 0.0f) {
        scores[gid] = 0.0f;
        return;
    }
    
    scores[gid] = dot / (sqrt(normA) * sqrt(normB));
}

// =============================================================================
// Kernel: Top-K Selection via Parallel Bitonic Sort
// =============================================================================
// Finds the k highest scoring indices using parallel sorting
// Uses bitonic sort which is well-suited for GPU parallelization
//
// For large n with small k, we first filter to candidates,
// then sort to find exact top-k

// Structure to hold score-index pairs for sorting
struct ScoreIndex {
    float score;
    uint index;
};

// Threadgroup shared memory for parallel reduction
kernel void topk_select(
    device const float* scores [[buffer(0)]],
    device uint* topk_indices [[buffer(1)]],
    device float* topk_scores [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant uint& k [[buffer(4)]],
    uint gid [[thread_position_in_grid]],
    uint lid [[thread_position_in_threadgroup]],
    uint tgid [[threadgroup_position_in_grid]],
    threadgroup ScoreIndex* shared [[threadgroup(0)]])
{
    // Each threadgroup processes a chunk of scores
    // and maintains a local top-k
    
    // Initialize shared memory with worst scores
    if (lid < k) {
        shared[lid].score = -2.0f;  // Worse than any cosine similarity
        shared[lid].index = UINT_MAX;
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    
    // Each thread checks its assigned scores
    uint chunk_size = (n + THREADS_PER_THREADGROUP - 1) / THREADS_PER_THREADGROUP;
    uint start = gid * chunk_size;
    uint end = min(start + chunk_size, n);
    
    // Find local top-k for this thread
    ScoreIndex local_topk[MAX_K];
    uint local_count = 0;
    
    for (uint i = start; i < end; i++) {
        float score = scores[i];
        
        // Insert into local top-k if better than worst
        if (local_count < k) {
            // Still filling up
            local_topk[local_count].score = score;
            local_topk[local_count].index = i;
            local_count++;
            
            // Bubble up to maintain sorted order
            for (uint j = local_count - 1; j > 0; j--) {
                if (local_topk[j].score > local_topk[j-1].score) {
                    ScoreIndex tmp = local_topk[j];
                    local_topk[j] = local_topk[j-1];
                    local_topk[j-1] = tmp;
                } else {
                    break;
                }
            }
        } else if (score > local_topk[k-1].score) {
            // Replace worst and re-sort
            local_topk[k-1].score = score;
            local_topk[k-1].index = i;
            
            for (uint j = k - 1; j > 0; j--) {
                if (local_topk[j].score > local_topk[j-1].score) {
                    ScoreIndex tmp = local_topk[j];
                    local_topk[j] = local_topk[j-1];
                    local_topk[j-1] = tmp;
                } else {
                    break;
                }
            }
        }
    }
    
    // Merge local results into shared memory (atomic-like operations)
    threadgroup_barrier(mem_flags::mem_threadgroup);
    
    // Simple merge: each thread contributes its findings
    // This is a simplified version - production code would use proper reduction
    for (uint i = 0; i < local_count && i < k; i++) {
        // Try to insert into shared top-k
        for (uint j = 0; j < k; j++) {
            if (local_topk[i].score > shared[j].score) {
                // Shift down and insert
                for (uint m = k - 1; m > j; m--) {
                    shared[m] = shared[m-1];
                }
                shared[j] = local_topk[i];
                break;
            }
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    
    // First thread writes results
    if (lid == 0) {
        for (uint i = 0; i < k; i++) {
            topk_indices[tgid * k + i] = shared[i].index;
            topk_scores[tgid * k + i] = shared[i].score;
        }
    }
}

// =============================================================================
// Kernel: Simple Top-K for Small N
// =============================================================================
// For small number of embeddings (< 10K), simpler approach is faster

kernel void topk_simple(
    device const float* scores [[buffer(0)]],
    device uint* topk_indices [[buffer(1)]],
    device float* topk_scores [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant uint& k [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    // Single thread finds top-k (only use for small n)
    if (gid != 0) return;
    
    // Initialize with worst scores
    for (uint i = 0; i < k; i++) {
        topk_scores[i] = -2.0f;
        topk_indices[i] = UINT_MAX;
    }
    
    // Linear scan to find top-k
    for (uint i = 0; i < n; i++) {
        float score = scores[i];
        
        // Check if this score makes it into top-k
        if (score > topk_scores[k-1]) {
            // Find insertion point
            uint pos = k - 1;
            while (pos > 0 && score > topk_scores[pos-1]) {
                topk_scores[pos] = topk_scores[pos-1];
                topk_indices[pos] = topk_indices[pos-1];
                pos--;
            }
            topk_scores[pos] = score;
            topk_indices[pos] = i;
        }
    }
}

// =============================================================================
// Kernel: Vector Normalization
// =============================================================================
// Normalize vectors in-place for faster future similarity computations

kernel void normalize_vectors(
    device float* vectors [[buffer(0)]],
    constant uint& n [[buffer(1)]],
    constant uint& dimensions [[buffer(2)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    
    ulong base = (ulong)gid * dimensions;
    
    // Compute L2 norm
    float sum_sq = 0.0f;
    for (uint i = 0; i < dimensions; i++) {
        float v = vectors[base + i];
        sum_sq += v * v;
    }
    
    if (sum_sq == 0.0f) return;
    
    float inv_norm = rsqrt(sum_sq);  // Fast reciprocal square root
    
    // Normalize in-place
    for (uint i = 0; i < dimensions; i++) {
        vectors[base + i] *= inv_norm;
    }
}

// =============================================================================
// Kernel: Batch Dot Product
// =============================================================================
// Compute dot products between query and multiple embeddings
// Returns raw dot products (use for already-normalized vectors)

kernel void batch_dot_product(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* results [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    
    float dot = 0.0f;
    ulong base = (ulong)gid * dimensions;
    
    // SIMD-friendly loop
    for (uint i = 0; i < dimensions; i++) {
        dot = fma(embeddings[base + i], query[i], dot);
    }
    
    results[gid] = dot;
}

// =============================================================================
// Kernel: Euclidean Distance (L2)
// =============================================================================
// For completeness - some use cases prefer euclidean distance

kernel void euclidean_distance(
    device const float* embeddings [[buffer(0)]],
    device const float* query [[buffer(1)]],
    device float* distances [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant uint& dimensions [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    
    float sum_sq = 0.0f;
    ulong base = (ulong)gid * dimensions;
    
    for (uint i = 0; i < dimensions; i++) {
        float diff = embeddings[base + i] - query[i];
        sum_sq += diff * diff;
    }
    
    distances[gid] = sqrt(sum_sq);
}

// =============================================================================
// Kernel: Minimum Similarity Filter
// =============================================================================
// Filter embeddings by minimum similarity threshold
// Returns count of embeddings above threshold

kernel void filter_by_similarity(
    device const float* scores [[buffer(0)]],
    device uint* filtered_indices [[buffer(1)]],
    device atomic_uint* count [[buffer(2)]],
    constant uint& n [[buffer(3)]],
    constant float& min_similarity [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    
    if (scores[gid] >= min_similarity) {
        uint idx = atomic_fetch_add_explicit(count, 1, memory_order_relaxed);
        filtered_indices[idx] = gid;
    }
}

// =============================================================================
// Kernel: HNSW Build Batched Cosine Matrix
// =============================================================================
// Computes scores for a query batch against a frontier shard. Both inputs are
// normalized, so cosine similarity is a dot product.

kernel void hnsw_build_cosine_matrix(
    device const float* frontier [[buffer(0)]],
    device const float* queries [[buffer(1)]],
    device float* scores [[buffer(2)]],
    constant uint& frontier_n [[buffer(3)]],
    constant uint& query_n [[buffer(4)]],
    constant uint& dimensions [[buffer(5)]],
    uint2 gid [[thread_position_in_grid]])
{
    uint fid = gid.x;
    uint qid = gid.y;
    if (fid >= frontier_n || qid >= query_n) return;

    float dot = 0.0f;
    ulong fbase = (ulong)fid * dimensions;
    ulong qbase = (ulong)qid * dimensions;
    for (uint d = 0; d < dimensions; d++) {
        dot = fma(frontier[fbase + d], queries[qbase + d], dot);
    }
    scores[(ulong)qid * frontier_n + fid] = dot;
}

// =============================================================================
// Kernel: HNSW Build Top-K Rows
// =============================================================================
// Selects top-k candidates for each query row from the score matrix.

kernel void hnsw_build_topk_rows(
    device const float* scores [[buffer(0)]],
    device uint* topk_indices [[buffer(1)]],
    device float* topk_scores [[buffer(2)]],
    constant uint& frontier_n [[buffer(3)]],
    constant uint& query_n [[buffer(4)]],
    constant uint& k [[buffer(5)]],
    uint qid [[thread_position_in_grid]])
{
    if (qid >= query_n || k == 0 || k > 256) return;

    float best_scores[256];
    uint best_indices[256];
    for (uint i = 0; i < k; i++) {
        best_scores[i] = -2.0f;
        best_indices[i] = UINT_MAX;
    }

    ulong row = (ulong)qid * frontier_n;
    for (uint fid = 0; fid < frontier_n; fid++) {
        float score = scores[row + fid];
        if (score > best_scores[k - 1] || (score == best_scores[k - 1] && fid < best_indices[k - 1])) {
            uint pos = k - 1;
            while (pos > 0 && (score > best_scores[pos - 1] || (score == best_scores[pos - 1] && fid < best_indices[pos - 1]))) {
                best_scores[pos] = best_scores[pos - 1];
                best_indices[pos] = best_indices[pos - 1];
                pos--;
            }
            best_scores[pos] = score;
            best_indices[pos] = fid;
        }
    }

    ulong out = (ulong)qid * k;
    for (uint i = 0; i < k; i++) {
        topk_scores[out + i] = best_scores[i];
        topk_indices[out + i] = best_indices[i];
    }
}
