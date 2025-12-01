# GPU K-Means Clustering for High-Dimensional Embeddings

## Overview

This document describes how to implement GPU-accelerated k-means clustering for 1024-dimensional embeddings in NornicDB, enabling instant cross-chunk concept discovery and related document finding.

## Feasibility Analysis

### ✅ Can GPUs Do K-Means Clustering?

**Yes, extremely well.** K-means is highly parallelizable:

- **Distance calculations** (Euclidean/cosine): Embarrassingly parallel across all points
- **Centroid updates**: Parallel reduction operations
- **Assignment step**: Each point evaluated independently
- **Memory access patterns**: Coalesced reads perfect for GPU architecture

### ✅ Math Functions Available

All required operations are GPU-native in CUDA/Metal/Vulkan:

**Distance Metrics:**
- Euclidean: `sqrt(sum((a[i] - b[i])^2))`
- Cosine similarity: `dot(a,b) / (norm(a) * norm(b))`

**Operations:**
- Vector addition/subtraction (SIMD)
- Dot products (highly optimized)
- Reductions: sum, mean, argmin
- Square root (hardware accelerated)

### 📊 Performance on 1024-Dimensional Embeddings

**Back-of-envelope calculation:**
- 10,000 embeddings × 1024 dimensions
- K=100 clusters
- Per iteration: 10,000 × 100 × 1024 = ~1 billion operations

**Performance comparison:**
- **GPU**: ~0.5-2ms per iteration
- **CPU**: ~50-200ms per iteration
- **Speedup**: 100-400x

## Implementation Options

### Option 1: Use Existing GPU Libraries (Recommended)

#### RAPIDS cuML (NVIDIA GPUs)
```bash
pip install cuml-cu11
```

```python
import cuml
from cuml.cluster import KMeans

# 10,000 documents, 1024-dim embeddings
embeddings = load_embeddings()  # shape: (10000, 1024)

# Cluster into 100 topic groups
kmeans = KMeans(n_clusters=100, max_iter=300)
labels = kmeans.fit_predict(embeddings)

# Find related documents
topic_42_docs = np.where(labels == 42)[0]
```

#### FAISS (Meta, Multi-GPU Support)
```bash
pip install faiss-gpu
```

```python
import faiss

# Create GPU index
d = 1024  # dimensions
nlist = 100  # number of clusters
quantizer = faiss.IndexFlatL2(d)
index = faiss.IndexIVFFlat(quantizer, d, nlist)

# Train on GPU
res = faiss.StandardGpuResources()
gpu_index = faiss.index_cpu_to_gpu(res, 0, index)
gpu_index.train(embeddings)

# Search
D, I = gpu_index.search(query_vectors, k=10)
```

#### PyTorch K-Means (Cross-Platform)
```bash
pip install torch torchvision
```

```python
import torch
from kmeans_pytorch import kmeans

# Move to GPU
embeddings_gpu = torch.from_numpy(embeddings).cuda()

# Run k-means
cluster_ids, cluster_centers = kmeans(
    X=embeddings_gpu,
    num_clusters=100,
    distance='euclidean',
    device=torch.device('cuda:0')
)
```

### Option 2: Custom CUDA Kernel

For maximum control and performance:

```cuda
// k-means distance kernel (simplified)
__global__ void compute_distances(
    float* embeddings,        // [N, D] embeddings
    float* centroids,         // [K, D] centroids
    int* assignments,         // [N] closest cluster
    float* distances,         // [N] min distance
    int N, int K, int D
) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= N) return;
    
    float min_dist = INFINITY;
    int closest = -1;
    
    // For each centroid
    for (int k = 0; k < K; k++) {
        float dist = 0.0f;
        
        // Compute Euclidean distance (unrolled for 1024-dim)
        #pragma unroll 32
        for (int d = 0; d < D; d++) {
            float diff = embeddings[idx * D + d] - centroids[k * D + d];
            dist += diff * diff;
        }
        
        if (dist < min_dist) {
            min_dist = dist;
            closest = k;
        }
    }
    
    assignments[idx] = closest;
    distances[idx] = sqrtf(min_dist);
}

// Centroid update kernel
__global__ void update_centroids(
    float* embeddings,        // [N, D] embeddings
    int* assignments,         // [N] cluster assignments
    float* new_centroids,     // [K, D] output centroids
    int* counts,              // [K] points per cluster
    int N, int K, int D
) {
    int k = blockIdx.x;  // One block per cluster
    int d = threadIdx.x; // One thread per dimension
    
    if (k >= K || d >= D) return;
    
    float sum = 0.0f;
    int count = 0;
    
    // Sum all points assigned to this cluster
    for (int n = 0; n < N; n++) {
        if (assignments[n] == k) {
            sum += embeddings[n * D + d];
            if (d == 0) count++;
        }
    }
    
    // Average to get new centroid
    new_centroids[k * D + d] = (count > 0) ? sum / count : 0.0f;
    if (d == 0) counts[k] = count;
}
```

### Option 3: Metal Shaders (Apple Silicon)

```metal
kernel void compute_distances(
    device const float* embeddings [[buffer(0)]],
    device const float* centroids [[buffer(1)]],
    device int* assignments [[buffer(2)]],
    device float* distances [[buffer(3)]],
    constant int& N [[buffer(4)]],
    constant int& K [[buffer(5)]],
    constant int& D [[buffer(6)]],
    uint idx [[thread_position_in_grid]]
) {
    if (idx >= N) return;
    
    float min_dist = INFINITY;
    int closest = -1;
    
    for (int k = 0; k < K; k++) {
        float dist = 0.0f;
        
        for (int d = 0; d < D; d++) {
            float diff = embeddings[idx * D + d] - centroids[k * D + d];
            dist += diff * diff;
        }
        
        if (dist < min_dist) {
            min_dist = dist;
            closest = k;
        }
    }
    
    assignments[idx] = closest;
    distances[idx] = sqrt(min_dist);
}
```

## Use Cases

### 1. Cluster N Embeddings into K Groups

**Goal**: Group similar documents together for topic discovery

```python
import cuml
from cuml.cluster import KMeans

# Load all document embeddings
embeddings = load_embeddings()  # shape: (10000, 1024)

# Cluster into 100 topic groups
kmeans = KMeans(
    n_clusters=100,
    max_iter=300,
    tol=1e-4,
    init='k-means++',
    random_state=42
)
labels = kmeans.fit_predict(embeddings)

# Find all documents in topic 42
topic_42_docs = np.where(labels == 42)[0]

# Get centroid for topic 42
topic_centroid = kmeans.cluster_centers_[42]
```

**Performance**: ~10-50ms for 10K embeddings on GPU vs 1-5 seconds on CPU

### 2. Hierarchical Concept Extraction

**Goal**: Extract multi-level concepts from embeddings

```python
# 1. Top-level clustering
top_level_clusters = kmeans_gpu.fit_predict(all_embeddings)

# 2. For each cluster, extract representative features
for cluster_id in range(K):
    cluster_embeddings = embeddings[labels == cluster_id]
    
    # Find dimensions with high variance = discriminative features
    variance = np.var(cluster_embeddings, axis=0)
    top_dims = np.argsort(variance)[-50:]  # Top 50 features
    
    # Sub-cluster using only discriminative dimensions
    sub_clusters = kmeans_gpu.fit_predict(
        cluster_embeddings[:, top_dims]
    )
```

### 3. Semantic Concept Mining (Instant Cross-Chunk Discovery)

**Goal**: Find related concepts across ALL chunks instantly

```python
# 1. Load all embeddings to GPU (keep in memory)
gpu_embeddings = cupy.array(embeddings)

# 2. K-means in GPU memory
kmeans = KMeans(n_clusters=500)  # 500 topics
cluster_ids = kmeans.fit_predict(gpu_embeddings)

# 3. Instant lookup: "Show me all chunks related to cluster 42"
related_chunks = chunk_ids[cluster_ids == 42]

# 4. Cross-cluster relationships
cluster_centroids = kmeans.cluster_centers_
similarity_matrix = cosine_similarity(cluster_centroids)

# Find which topics are related to topic 42
similar_topics = np.argsort(similarity_matrix[42])[-10:]
```

## NornicDB Integration

### Architecture

```
pkg/gpu/kmeans/
├── kmeans.go              # Go interface
├── cuda_kmeans.cu         # CUDA implementation
├── metal_kmeans.metal     # Metal implementation
└── kmeans_test.go         # Tests

pkg/inference/
└── concept_clustering.go  # Integration with inference engine
```

### Go Interface

```go
// pkg/gpu/kmeans/kmeans.go
package kmeans

import (
    "context"
    "github.com/orneryd/nornicdb/pkg/storage"
)

type GPUKMeans struct {
    device      *Device           // CUDA/Metal/Vulkan device
    embeddings  *DeviceMemory     // Keep embeddings in GPU
    centroids   *DeviceMemory     // Cluster centroids
    assignments *DeviceMemory     // Cluster assignments
    numClusters int
    dimensions  int
    maxIter     int
}

type Config struct {
    NumClusters int
    Dimensions  int
    MaxIter     int
    Tolerance   float32
    Device      string  // "cuda", "metal", "vulkan"
}

// NewGPUKMeans creates a new GPU k-means clusterer
func NewGPUKMeans(config Config) (*GPUKMeans, error) {
    km := &GPUKMeans{
        numClusters: config.NumClusters,
        dimensions:  config.Dimensions,
        maxIter:     config.MaxIter,
    }
    
    // Initialize GPU device
    device, err := InitDevice(config.Device)
    if err != nil {
        return nil, err
    }
    km.device = device
    
    // Allocate GPU memory
    km.embeddings = device.Alloc(maxEmbeddings * config.Dimensions * 4)
    km.centroids = device.Alloc(config.NumClusters * config.Dimensions * 4)
    km.assignments = device.Alloc(maxEmbeddings * 4)
    
    return km, nil
}

// Fit runs k-means clustering on the embeddings
func (km *GPUKMeans) Fit(embeddings [][]float32) error {
    n := len(embeddings)
    
    // 1. Copy embeddings to GPU (once)
    flatEmbeddings := flatten(embeddings)
    km.embeddings.CopyFrom(flatEmbeddings)
    
    // 2. Initialize centroids (k-means++)
    km.initCentroids(embeddings)
    
    // 3. Iterate until convergence
    for iter := 0; iter < km.maxIter; iter++ {
        // Compute distances and assignments (GPU kernel)
        km.computeAssignments()
        
        // Update centroids (GPU reduction)
        changed := km.updateCentroids()
        
        if !changed {
            break
        }
    }
    
    return nil
}

// Predict finds the closest centroid for a single embedding
func (km *GPUKMeans) Predict(embedding []float32) int {
    // Copy single embedding to GPU
    // Run distance computation
    // Return closest centroid ID
    return km.findClosestCentroid(embedding)
}

// GetClusterAssignments returns cluster ID for each embedding
func (km *GPUKMeans) GetClusterAssignments() []int {
    assignments := make([]int, km.numEmbeddings)
    km.assignments.CopyTo(assignments)
    return assignments
}

// GetClusterCentroids returns the centroid vectors
func (km *GPUKMeans) GetClusterCentroids() [][]float32 {
    centroids := make([]float32, km.numClusters * km.dimensions)
    km.centroids.CopyTo(centroids)
    return unflatten(centroids, km.dimensions)
}

// FindSimilarClusters returns cluster IDs similar to the given cluster
func (km *GPUKMeans) FindSimilarClusters(clusterID int, topK int) []int {
    centroids := km.GetClusterCentroids()
    targetCentroid := centroids[clusterID]
    
    // Compute cosine similarity between target and all centroids
    similarities := make([]float32, km.numClusters)
    for i, centroid := range centroids {
        similarities[i] = cosineSimilarity(targetCentroid, centroid)
    }
    
    // Return top-K most similar
    return argsort(similarities)[:topK]
}

// Cleanup releases GPU resources
func (km *GPUKMeans) Cleanup() {
    km.embeddings.Free()
    km.centroids.Free()
    km.assignments.Free()
    km.device.Release()
}
```

### Inference Engine Integration

```go
// pkg/inference/concept_clustering.go
package inference

import (
    "context"
    "sync"
    
    "github.com/orneryd/nornicdb/pkg/gpu/kmeans"
    "github.com/orneryd/nornicdb/pkg/storage"
)

type ConceptClusteringEngine struct {
    mu          sync.RWMutex
    storage     storage.Engine
    kmeans      *kmeans.GPUKMeans
    clusterMap  map[int][]storage.NodeID  // cluster_id -> node_ids
    nodeCluster map[storage.NodeID]int    // node_id -> cluster_id
    centroids   [][]float32
}

type ConceptClusteringConfig struct {
    NumClusters int
    Dimensions  int
    MaxIter     int
    Device      string  // "cuda", "metal", "auto"
}

func NewConceptClusteringEngine(
    storage storage.Engine,
    config ConceptClusteringConfig,
) (*ConceptClusteringEngine, error) {
    km, err := kmeans.NewGPUKMeans(kmeans.Config{
        NumClusters: config.NumClusters,
        Dimensions:  config.Dimensions,
        MaxIter:     config.MaxIter,
        Device:      config.Device,
    })
    if err != nil {
        return nil, err
    }
    
    return &ConceptClusteringEngine{
        storage:     storage,
        kmeans:      km,
        clusterMap:  make(map[int][]storage.NodeID),
        nodeCluster: make(map[storage.NodeID]int),
    }, nil
}

// OnIndexComplete runs after indexing finishes
func (e *ConceptClusteringEngine) OnIndexComplete(ctx context.Context) error {
    // 1. Get all embeddings from storage
    nodes, err := e.storage.GetAllNodesWithEmbeddings(ctx)
    if err != nil {
        return err
    }
    
    embeddings := make([][]float32, len(nodes))
    nodeIDs := make([]storage.NodeID, len(nodes))
    
    for i, node := range nodes {
        embeddings[i] = node.Embedding
        nodeIDs[i] = node.ID
    }
    
    // 2. Cluster on GPU
    if err := e.kmeans.Fit(embeddings); err != nil {
        return err
    }
    
    // 3. Build reverse index
    assignments := e.kmeans.GetClusterAssignments()
    
    e.mu.Lock()
    defer e.mu.Unlock()
    
    e.clusterMap = make(map[int][]storage.NodeID)
    e.nodeCluster = make(map[storage.NodeID]int)
    
    for i, nodeID := range nodeIDs {
        clusterID := assignments[i]
        e.clusterMap[clusterID] = append(e.clusterMap[clusterID], nodeID)
        e.nodeCluster[nodeID] = clusterID
    }
    
    // 4. Store centroids for similarity queries
    e.centroids = e.kmeans.GetClusterCentroids()
    
    return nil
}

// FindRelatedConcepts - instant lookup using pre-computed clusters
func (e *ConceptClusteringEngine) FindRelatedConcepts(
    nodeID storage.NodeID,
) []storage.NodeID {
    e.mu.RLock()
    defer e.mu.RUnlock()
    
    clusterID, exists := e.nodeCluster[nodeID]
    if !exists {
        return nil
    }
    
    // All nodes in same cluster = related concepts
    return e.clusterMap[clusterID]
}

// FindCrossClusterConcepts finds related concepts across multiple clusters
func (e *ConceptClusteringEngine) FindCrossClusterConcepts(
    nodeID storage.NodeID,
    topKClusters int,
) []storage.NodeID {
    e.mu.RLock()
    defer e.mu.RUnlock()
    
    clusterID, exists := e.nodeCluster[nodeID]
    if !exists {
        return nil
    }
    
    // Find similar clusters
    similarClusters := e.kmeans.FindSimilarClusters(clusterID, topKClusters)
    
    // Collect nodes from similar clusters
    var relatedNodes []storage.NodeID
    for _, similarCluster := range similarClusters {
        relatedNodes = append(relatedNodes, e.clusterMap[similarCluster]...)
    }
    
    return relatedNodes
}

// AddNode adds a new node to the cluster index
func (e *ConceptClusteringEngine) AddNode(
    ctx context.Context,
    nodeID storage.NodeID,
    embedding []float32,
) error {
    // Assign to nearest centroid (no re-clustering)
    clusterID := e.kmeans.Predict(embedding)
    
    e.mu.Lock()
    defer e.mu.Unlock()
    
    e.clusterMap[clusterID] = append(e.clusterMap[clusterID], nodeID)
    e.nodeCluster[nodeID] = clusterID
    
    return nil
}

// ReclusterIfNeeded re-runs k-means if drift is detected
func (e *ConceptClusteringEngine) ReclusterIfNeeded(
    ctx context.Context,
    threshold int,
) error {
    e.mu.RLock()
    totalNodes := 0
    for _, nodes := range e.clusterMap {
        totalNodes += len(nodes)
    }
    e.mu.RUnlock()
    
    // Re-cluster if we've added >threshold new nodes
    if totalNodes > threshold {
        return e.OnIndexComplete(ctx)
    }
    
    return nil
}

// GetClusterStats returns statistics about clusters
func (e *ConceptClusteringEngine) GetClusterStats() map[string]interface{} {
    e.mu.RLock()
    defer e.mu.RUnlock()
    
    sizes := make([]int, 0, len(e.clusterMap))
    for _, nodes := range e.clusterMap {
        sizes = append(sizes, len(nodes))
    }
    
    return map[string]interface{}{
        "num_clusters":   len(e.clusterMap),
        "total_nodes":    len(e.nodeCluster),
        "cluster_sizes":  sizes,
        "avg_cluster_size": average(sizes),
    }
}

// Cleanup releases GPU resources
func (e *ConceptClusteringEngine) Cleanup() {
    e.kmeans.Cleanup()
}
```

## Performance Benchmarks

### Expected Performance on 1024-Dimensional Embeddings

| Dataset Size | K Clusters | GPU Time (NVIDIA A100) | CPU Time (32 cores) | Speedup |
|--------------|-----------|------------------------|---------------------|---------|
| 1K docs      | 10        | <1ms                   | ~10ms               | 10-20x  |
| 10K docs     | 100       | ~10ms                  | ~500ms              | 50x     |
| 100K docs    | 500       | ~100ms                 | ~10s                | 100x    |
| 1M docs      | 1000      | ~1s                    | ~5min               | 300x    |
| 10M docs     | 5000      | ~10s                   | ~1hr                | 360x    |

### Memory Requirements

For N embeddings of dimension D with K clusters:

- **Embeddings**: N × D × 4 bytes (float32)
- **Centroids**: K × D × 4 bytes
- **Assignments**: N × 4 bytes (int32)
- **Working memory**: ~2x embeddings size

**Example (10K documents, 1024-dim, 100 clusters):**
- Embeddings: 10,000 × 1,024 × 4 = ~40 MB
- Centroids: 100 × 1,024 × 4 = ~400 KB
- Assignments: 10,000 × 4 = ~40 KB
- Total: ~80 MB (with working memory)

**Fits easily in modern GPUs (8-24 GB VRAM)**

## Recommended Workflow

### Initial Setup

1. **Index Phase**: Collect all embeddings
2. **Clustering Phase**: Run GPU k-means (batch)
3. **Build Index**: Create reverse mapping (cluster → nodes)
4. **Store Results**: Persist cluster assignments

### Production Use

1. **Query Time**: O(1) lookup via cluster ID
2. **New Documents**: Assign to nearest centroid (no re-clustering)
3. **Periodic Re-clustering**: Every 10K-100K new documents

### Hybrid Approach

Combine GPU k-means with existing vector similarity:

```go
func (e *InferenceEngine) GetRelatedDocuments(
    ctx context.Context,
    nodeID storage.NodeID,
    topK int,
) []storage.NodeID {
    // 1. Fast cluster-based retrieval (GPU k-means)
    clusterCandidates := e.clustering.FindRelatedConcepts(nodeID)
    
    // 2. Refine with precise vector similarity
    node, _ := e.storage.GetNode(nodeID)
    embedding := node.Embedding
    
    similarities := make([]struct{
        NodeID storage.NodeID
        Score  float64
    }, 0, len(clusterCandidates))
    
    for _, candidateID := range clusterCandidates {
        candidate, _ := e.storage.GetNode(candidateID)
        score := cosineSimilarity(embedding, candidate.Embedding)
        similarities = append(similarities, struct{
            NodeID storage.NodeID
            Score  float64
        }{candidateID, score})
    }
    
    // 3. Return top-K most similar
    sort.Slice(similarities, func(i, j int) bool {
        return similarities[i].Score > similarities[j].Score
    })
    
    results := make([]storage.NodeID, min(topK, len(similarities)))
    for i := range results {
        results[i] = similarities[i].NodeID
    }
    
    return results
}
```

## Configuration Example

```yaml
# nornicdb.example.yaml

concept_clustering:
  enabled: true
  num_clusters: 500
  dimensions: 1024
  max_iterations: 300
  tolerance: 0.0001
  device: "auto"  # auto, cuda, metal, vulkan
  
  # Re-clustering policy
  recluster_threshold: 10000  # Re-cluster after N new documents
  recluster_schedule: "0 2 * * *"  # Daily at 2 AM
  
  # Memory management
  keep_embeddings_in_gpu: true
  max_gpu_memory_mb: 4096
```

## Future Enhancements

### 1. **Hierarchical K-Means**
- Multi-level clustering for better concept hierarchy
- Top level: broad topics
- Lower levels: specific concepts

### 2. **Online K-Means**
- Incremental updates without full re-clustering
- Streaming k-means for continuous indexing

### 3. **Multi-GPU Support**
- Distribute clusters across GPUs
- Scale to 100M+ documents

### 4. **Approximate K-Means**
- Product quantization for reduced memory
- Trade accuracy for speed (10-100x faster)

### 5. **Cluster Quality Metrics**
- Silhouette score
- Davies-Bouldin index
- Calinski-Harabasz index

### 6. **Auto-tuning K**
- Elbow method to find optimal cluster count
- Gap statistic for validation

## References

- **RAPIDS cuML**: https://docs.rapids.ai/api/cuml/stable/
- **FAISS**: https://github.com/facebookresearch/faiss
- **K-Means++**: Arthur & Vassilvitskii (2007)
- **GPU K-Means**: Li et al., "Fast K-Means on GPUs" (2013)
- **Product Quantization**: Jegou et al. (2011)

## Summary

**GPU k-means clustering on 1024-dimensional embeddings is:**

✅ **Feasible**: All required math operations are GPU-native  
✅ **Fast**: 100-400x speedup over CPU  
✅ **Scalable**: Handles millions of embeddings  
✅ **Memory-efficient**: Fits in modern GPU VRAM  
✅ **Practical**: Multiple proven implementations available  

**For NornicDB, this enables:**

🚀 **Instant concept discovery** across all chunks  
🚀 **O(1) related document lookup** via cluster IDs  
🚀 **Cross-chunk semantic relationships** without exhaustive search  
🚀 **Real-time clustering** for new documents  

**Recommended next steps:**

1. Prototype with FAISS-GPU (easiest integration)
2. Benchmark on real NornicDB data
3. Implement Go wrapper for production use
4. Add to inference engine pipeline
