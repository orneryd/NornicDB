// Metal K-Means Clustering Kernels for NornicDB GPU-Accelerated Clustering
//
// This file contains Metal Shading Language (MSL) compute kernels for:
// 1. Distance computation (embeddings to centroids)
// 2. Cluster assignment (find nearest centroid)
// 3. Centroid updates (parallel atomic accumulation)
// 4. Convergence checking
//
// ATOMIC FLOAT WORKAROUND:
// Metal does not have native atomic_float operations until Metal 3.0 (macOS 13+).
// We use atomic compare-exchange on atomic_uint to emulate atomic float addition.
//
// Memory Layout:
//   - embeddings: [N × D] contiguous float array
//   - centroids: [K × D] centroid vectors
//   - assignments: [N] cluster assignment per embedding
//   - distances: [N × K] distance matrix (working buffer)
//
// Performance Characteristics:
//   - Optimized for 768-1536 dimensional embeddings
//   - Handles 10K-100K embeddings efficiently on M1/M2/M3
//   - Memory bandwidth: ~200-400 GB/s on Apple Silicon

#include <metal_stdlib>
using namespace metal;

// =============================================================================
// Constants
// =============================================================================

constant uint THREADS_PER_THREADGROUP = 256;
constant uint MAX_DIMENSIONS = 2048;
constant uint MAX_CLUSTERS = 1024;

// =============================================================================
// Helper: Atomic Float Addition via Compare-Exchange
// =============================================================================
// Metal doesn't support atomic_float natively (until Metal 3.0)
// We emulate it using atomic_uint with compare-exchange loops
//
// This is safe but slower than native atomic_float would be.
// Performance: ~100-200 atomic adds per microsecond per core on M1/M2

inline void atomicAddFloat(device atomic_uint* addr, float val) {
    // Load current value
    uint expected = atomic_load_explicit(addr, memory_order_relaxed);
    uint desired;
    
    // Compare-exchange loop until successful
    do {
        // Interpret uint bits as float, add value, reinterpret as uint
        float current = as_type<float>(expected);
        float new_val = current + val;
        desired = as_type<uint>(new_val);
    } while (!atomic_compare_exchange_weak_explicit(
        addr, &expected, desired,
        memory_order_relaxed, memory_order_relaxed
    ));
}

// =============================================================================
// Kernel 1: Compute Squared Euclidean Distances
// =============================================================================
// For each embedding, compute squared distance to each centroid.
// We use squared distance to avoid expensive sqrt operations.
//
// Distance formula: ||a - b||^2 = sum((a_i - b_i)^2)
//
// Dispatched as: MTLSize(N, K, 1) - one thread per (embedding, centroid) pair

kernel void kmeans_compute_distances(
    device const float* embeddings [[buffer(0)]],  // [N × D]
    device const float* centroids [[buffer(1)]],   // [K × D]
    device float* distances [[buffer(2)]],         // [N × K] output
    constant uint& N [[buffer(3)]],                // Number of embeddings
    constant uint& K [[buffer(4)]],                // Number of clusters
    constant uint& D [[buffer(5)]],                // Dimensions
    uint2 gid [[thread_position_in_grid]])         // (embedding_idx, centroid_idx)
{
    uint n = gid.x;  // embedding index
    uint k = gid.y;  // centroid index
    
    if (n >= N || k >= K) return;
    
    // Compute squared euclidean distance
    float dist_sq = 0.0f;
    uint emb_base = n * D;
    uint cent_base = k * D;
    
    // Unrolled loop for better performance (Metal compiler will optimize further)
    for (uint d = 0; d < D; d += 4) {
        if (d + 3 < D) {
            // Process 4 dimensions at once (SIMD friendly)
            float diff0 = embeddings[emb_base + d] - centroids[cent_base + d];
            float diff1 = embeddings[emb_base + d + 1] - centroids[cent_base + d + 1];
            float diff2 = embeddings[emb_base + d + 2] - centroids[cent_base + d + 2];
            float diff3 = embeddings[emb_base + d + 3] - centroids[cent_base + d + 3];
            
            dist_sq += diff0 * diff0;
            dist_sq += diff1 * diff1;
            dist_sq += diff2 * diff2;
            dist_sq += diff3 * diff3;
        } else {
            // Handle remaining dimensions
            for (uint i = d; i < D; i++) {
                float diff = embeddings[emb_base + i] - centroids[cent_base + i];
                dist_sq += diff * diff;
            }
        }
    }
    
    // Store distance (row-major: distances[n * K + k])
    distances[n * K + k] = dist_sq;
}

// =============================================================================
// Kernel 2: Assign Embeddings to Nearest Centroids
// =============================================================================
// For each embedding, find the centroid with minimum distance.
// Track if any assignments changed (for convergence detection).
//
// Dispatched as: MTLSize(N, 1, 1) - one thread per embedding

kernel void kmeans_assign_clusters(
    device const float* distances [[buffer(0)]],     // [N × K]
    device int* assignments [[buffer(1)]],           // [N] output
    device const int* old_assignments [[buffer(2)]], // [N] previous assignments
    device atomic_int* changed_count [[buffer(3)]], // [1] count of changed assignments
    constant uint& N [[buffer(4)]],
    constant uint& K [[buffer(5)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= N) return;
    
    int old_cluster = old_assignments[gid];
    
    // Find minimum distance centroid
    float min_dist = distances[gid * K];
    int nearest = 0;
    
    for (uint k = 1; k < K; k++) {
        float d = distances[gid * K + k];
        if (d < min_dist) {
            min_dist = d;
            nearest = int(k);
        }
    }
    
    // Update assignment
    assignments[gid] = nearest;
    
    // Track changes for convergence
    if (nearest != old_cluster) {
        atomic_fetch_add_explicit(changed_count, 1, memory_order_relaxed);
    }
}

// =============================================================================
// Kernel 3: Zero Centroid Accumulators
// =============================================================================
// Clear centroid sum and count buffers before accumulation.
//
// Dispatched as: MTLSize(K * D, 1, 1)

kernel void kmeans_zero_centroids(
    device atomic_uint* centroid_sums [[buffer(0)]],  // [K × D] to clear
    device atomic_int* cluster_counts [[buffer(1)]],  // [K] to clear
    constant uint& K [[buffer(2)]],
    constant uint& D [[buffer(3)]],
    uint gid [[thread_position_in_grid]])
{
    uint total = K * D;
    if (gid < total) {
        atomic_store_explicit(&centroid_sums[gid], 0, memory_order_relaxed);
    }
    
    if (gid < K) {
        atomic_store_explicit(&cluster_counts[gid], 0, memory_order_relaxed);
    }
}

// =============================================================================
// Kernel 4: Accumulate Points into Centroids
// =============================================================================
// For each embedding, atomically add its values to its assigned centroid.
// Also increment cluster count.
//
// Uses atomic float workaround for accumulation.
//
// Dispatched as: MTLSize(N, 1, 1) - one thread per embedding

kernel void kmeans_accumulate_centroids(
    device const float* embeddings [[buffer(0)]],     // [N × D]
    device const int* assignments [[buffer(1)]],      // [N] cluster assignments
    device atomic_uint* centroid_sums [[buffer(2)]], // [K × D] accumulator (as uint)
    device atomic_int* cluster_counts [[buffer(3)]], // [K] cluster counts
    constant uint& N [[buffer(4)]],
    constant uint& D [[buffer(5)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= N) return;
    
    int cluster = assignments[gid];
    uint emb_base = gid * D;
    uint cent_base = cluster * D;
    
    // Atomically add this embedding to its cluster's centroid sum
    for (uint d = 0; d < D; d++) {
        atomicAddFloat(&centroid_sums[cent_base + d], embeddings[emb_base + d]);
    }
    
    // Atomically increment cluster count (thread 0 only per embedding to avoid double-count)
    if (gid == 0 || assignments[gid] != assignments[gid - 1]) {
        atomic_fetch_add_explicit(&cluster_counts[cluster], 1, memory_order_relaxed);
    }
}

// =============================================================================
// Kernel 5: Finalize Centroids (Divide by Count)
// =============================================================================
// Divide accumulated centroid sums by cluster counts to get final centroids.
// Handle empty clusters by keeping previous centroid position.
//
// Dispatched as: MTLSize(K, D, 1) or MTLSize(K*D, 1, 1)

kernel void kmeans_finalize_centroids(
    device float* centroids [[buffer(0)]],           // [K × D] output
    device const atomic_uint* centroid_sums [[buffer(1)]], // [K × D] accumulated sums
    device const atomic_int* cluster_counts [[buffer(2)]], // [K] cluster sizes
    constant uint& K [[buffer(3)]],
    constant uint& D [[buffer(4)]],
    uint2 gid [[thread_position_in_grid]])
{
    uint k = gid.x;  // cluster index
    uint d = gid.y;  // dimension index
    
    if (k >= K || d >= D) return;
    
    int count = atomic_load_explicit(&cluster_counts[k], memory_order_relaxed);
    uint idx = k * D + d;
    
    if (count > 0) {
        // Average the accumulated sum
        uint sum_bits = atomic_load_explicit(&centroid_sums[idx], memory_order_relaxed);
        float sum = as_type<float>(sum_bits);
        centroids[idx] = sum / float(count);
    }
    // If count == 0, keep previous centroid (don't update)
}

// =============================================================================
// Kernel 6: Compute Centroid Drift
// =============================================================================
// Compute how much centroids moved in this iteration (for convergence).
// Returns sum of squared differences: sum(||new - old||^2)
//
// Dispatched as: MTLSize(K, 1, 1) with reduction

kernel void kmeans_compute_drift(
    device const float* new_centroids [[buffer(0)]],  // [K × D]
    device const float* old_centroids [[buffer(1)]],  // [K × D]
    device atomic_uint* drift_sum [[buffer(2)]],     // [1] accumulator (as uint)
    constant uint& K [[buffer(3)]],
    constant uint& D [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= K) return;
    
    float local_drift = 0.0f;
    uint base = gid * D;
    
    // Compute drift for this centroid
    for (uint d = 0; d < D; d++) {
        float diff = new_centroids[base + d] - old_centroids[base + d];
        local_drift += diff * diff;
    }
    
    // Atomically accumulate total drift
    atomicAddFloat(drift_sum, local_drift);
}

// =============================================================================
// Kernel 7: Single-Node Reassignment (Real-Time Tier 1)
// =============================================================================
// Fast path for reassigning a single embedding to nearest centroid.
// Used for real-time updates in Tier 1 of the 3-tier system.
//
// Dispatched as: MTLSize(1, 1, 1) - single-threaded

kernel void kmeans_reassign_single(
    device const float* embedding [[buffer(0)]],     // [D] single embedding
    device const float* centroids [[buffer(1)]],     // [K × D]
    device int* assignment [[buffer(2)]],            // [1] output cluster
    constant uint& K [[buffer(3)]],
    constant uint& D [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid != 0) return;  // Single thread only
    
    float min_dist = INFINITY;
    int nearest = 0;
    
    // Check distance to each centroid
    for (uint k = 0; k < K; k++) {
        float dist_sq = 0.0f;
        uint cent_base = k * D;
        
        for (uint d = 0; d < D; d++) {
            float diff = embedding[d] - centroids[cent_base + d];
            dist_sq += diff * diff;
        }
        
        if (dist_sq < min_dist) {
            min_dist = dist_sq;
            nearest = int(k);
        }
    }
    
    *assignment = nearest;
}

// =============================================================================
// Kernel 8: K-Means++ Initialization - Distance to Nearest Center
// =============================================================================
// For k-means++ initialization, compute distance from each point to
// the nearest already-chosen centroid.
//
// Dispatched as: MTLSize(N, 1, 1)

kernel void kmeans_pp_distances(
    device const float* embeddings [[buffer(0)]],      // [N × D]
    device const float* chosen_centroids [[buffer(1)]], // [num_chosen × D]
    device float* min_distances [[buffer(2)]],         // [N] output
    constant uint& N [[buffer(3)]],
    constant uint& D [[buffer(4)]],
    constant uint& num_chosen [[buffer(5)]],           // Number of centroids chosen so far
    uint gid [[thread_position_in_grid]])
{
    if (gid >= N) return;
    
    float min_dist_sq = INFINITY;
    uint emb_base = gid * D;
    
    // Find distance to nearest chosen centroid
    for (uint c = 0; c < num_chosen; c++) {
        float dist_sq = 0.0f;
        uint cent_base = c * D;
        
        for (uint d = 0; d < D; d++) {
            float diff = embeddings[emb_base + d] - chosen_centroids[cent_base + d];
            dist_sq += diff * diff;
        }
        
        if (dist_sq < min_dist_sq) {
            min_dist_sq = dist_sq;
        }
    }
    
    min_distances[gid] = min_dist_sq;
}

// =============================================================================
// Kernel 9: Batch Centroid Update (Tier 2 Real-Time)
// =============================================================================
// Update specific centroids affected by node movements.
// More efficient than full re-clustering for small changes.
//
// Dispatched as: MTLSize(num_affected_clusters, 1, 1)

kernel void kmeans_update_affected_centroids(
    device const float* embeddings [[buffer(0)]],      // [N × D]
    device const int* assignments [[buffer(1)]],       // [N]
    device float* centroids [[buffer(2)]],            // [K × D]
    device const uint* affected_clusters [[buffer(3)]], // [num_affected] cluster IDs
    constant uint& N [[buffer(4)]],
    constant uint& D [[buffer(5)]],
    constant uint& num_affected [[buffer(6)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= num_affected) return;
    
    uint cluster = affected_clusters[gid];
    uint cent_base = cluster * D;
    
    // Accumulate sum and count for this cluster
    float sum[MAX_DIMENSIONS];
    for (uint d = 0; d < D; d++) {
        sum[d] = 0.0f;
    }
    int count = 0;
    
    // Sum all embeddings assigned to this cluster
    for (uint n = 0; n < N; n++) {
        if (assignments[n] == int(cluster)) {
            uint emb_base = n * D;
            for (uint d = 0; d < D; d++) {
                sum[d] += embeddings[emb_base + d];
            }
            count++;
        }
    }
    
    // Update centroid (average)
    if (count > 0) {
        for (uint d = 0; d < D; d++) {
            centroids[cent_base + d] = sum[d] / float(count);
        }
    }
}

// =============================================================================
// Performance Notes
// =============================================================================
//
// Kernel Performance on M1/M2/M3 (estimated):
// 1. kmeans_compute_distances: ~0.5-2ms for 10K embeddings, 500 clusters, 1024D
// 2. kmeans_assign_clusters: ~0.1-0.5ms for 10K embeddings
// 3. kmeans_accumulate_centroids: ~1-3ms for 10K embeddings (bottleneck: atomic ops)
// 4. kmeans_finalize_centroids: ~0.05-0.1ms
// 5. kmeans_reassign_single: ~0.05-0.1ms (Tier 1 real-time)
//
// Total per iteration: ~2-6ms for 10K embeddings
// Expected iterations: 10-50 for convergence
// Total clustering time: 20-300ms (GPU) vs 5-30s (CPU)
//
// Speedup: 50-150x vs CPU for typical workloads
//
// Memory Requirements:
// - Embeddings: N × D × 4 bytes
// - Centroids: K × D × 4 bytes
// - Distances: N × K × 4 bytes (working buffer)
// - Assignments: N × 4 bytes
// - Atomic buffers: K × D × 4 bytes + K × 4 bytes
//
// Example (10K embeddings, 500 clusters, 1024D):
// - 40MB embeddings
// - 2MB centroids
// - 20MB distances (can be deallocated after assignment)
// - 40KB assignments
// - 2MB atomic buffers
// Total: ~64MB peak, ~44MB steady-state
//
