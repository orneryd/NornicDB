// Package gpu provides GPU-accelerated k-means clustering for NornicDB.
//
// This file implements ClusterIndex, which extends EmbeddingIndex with
// k-means clustering capabilities for faster semantic search.
//
// Architecture:
//
//	ClusterIndex
//	    ├── EmbeddingIndex (inherited)      <- GPU vector search
//	    ├── centroids [][]float32           <- cluster centers
//	    └── membership                      <- cluster membership by node ID
//
// Performance (M1/M2/M3 GPU):
//   - 10K embeddings, 100 clusters: ~50-100ms
//   - 100K embeddings, 500 clusters: ~500ms-1s
//   - Search speedup: 10-50x vs brute-force
//
// Usage:
//
//	index := gpu.NewClusterIndex(manager, nil, nil)
//	for _, emb := range embeddings {
//	    index.Add(nodeID, emb)
//	}
//	index.Cluster()  // Run k-means
//	results := index.SearchWithClusters(query, 10, 3)  // Search 3 clusters
package gpu

import (
	"context"
	"errors"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Errors for k-means clustering
var (
	ErrNotClustered     = errors.New("gpu: clustering not yet performed")
	ErrTooFewEmbeddings = errors.New("gpu: too few embeddings for requested clusters")
	ErrInvalidK         = errors.New("gpu: invalid number of clusters")
)

// KMeansConfig configures k-means clustering behavior.
//
// Example:
//
//	config := &gpu.KMeansConfig{
//	    NumClusters:    100,       // Fixed K
//	    MaxIterations:  50,        // Converge faster
//	    Tolerance:      0.001,     // Stricter convergence
//	    InitMethod:     "kmeans++",
//	    DriftThreshold: 0.05,      // Recluster on 5% drift
//	}
type KMeansConfig struct {
	// NumClusters is the K value. If 0 and AutoK=true, auto-detected.
	NumClusters int

	// MaxIterations limits convergence iterations (default: 15)
	MaxIterations int

	// Tolerance is the convergence threshold (default: 0.0001)
	// Clustering stops when centroid drift < tolerance
	Tolerance float32

	// InitMethod: "kmeans++" (better) or "random" (faster)
	InitMethod string

	// AutoK enables automatic cluster count selection
	AutoK bool

	// DriftThreshold triggers re-clustering when centroids drift > this (default: 0.1)
	DriftThreshold float32

	// MinClusterSize is the minimum embeddings per cluster (default: 10)
	MinClusterSize int
}

// DefaultKMeansConfig returns sensible defaults.
// Dimensions are auto-detected from the first embedding added.
func DefaultKMeansConfig() *KMeansConfig {
	return &KMeansConfig{
		NumClusters:    0, // Auto-detect based on data size
		MaxIterations:  2,
		Tolerance:      0.0001,
		InitMethod:     "kmeans++",
		AutoK:          true,
		DriftThreshold: 0.1,
		MinClusterSize: 10,
	}
}

// ClusterStats holds clustering statistics.
type ClusterStats struct {
	EmbeddingCount  int
	NumClusters     int
	AvgClusterSize  float64
	MinClusterSize  int
	MaxClusterSize  int
	Iterations      int
	LastClusterTime time.Duration
	CentroidDrift   float32
	Clustered       bool
}

// ClusterIndex extends EmbeddingIndex with k-means clustering.
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────┐
//	│                  ClusterIndex                        │
//	├─────────────────────────────────────────────────────┤
//	│  EmbeddingIndex (embedded)                          │
//	│    ├── cpuVectors []float32   <- all embeddings     │
//	│    ├── nodeIDs []string       <- ID mapping         │
//	│    └── GPU buffers            <- Metal/CUDA         │
//	├─────────────────────────────────────────────────────┤
//	│  Clustering State                                    │
//	│    ├── centroids [][]float32  <- K cluster centers  │
//	│    └── membership             <- by node ID         │
//	└─────────────────────────────────────────────────────┘
//
// Usage:
//
//	index := gpu.NewClusterIndex(manager, embConfig, kmeansConfig)
//
//	// Add embeddings
//	for i, emb := range embeddings {
//	    index.Add(nodeIDs[i], emb)
//	}
//
//	// Run clustering
//	if err := index.Cluster(); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Fast cluster-based search
//	results, _ := index.SearchWithClusters(query, 10, 3)
//
// Thread Safety: All methods are thread-safe.
type ClusterIndex struct {
	*EmbeddingIndex

	config *KMeansConfig

	// Cluster state: the published generation's routing (centroids and
	// membership by node ID); nil until clustered. Publication replaces the
	// pointer, so a caller holding an older generation keeps routing on it.
	routing *Clustering

	// Real-time update tracking (Tier 1/2)
	pendingUpdates      []nodeUpdate
	updatesSinceCluster int64

	// State tracking
	lastClusterTime     time.Time
	lastClusterDuration time.Duration
	iterations          int

	// Stats
	clusterIterations int64
	centroidDrift     float32

	clusterMu sync.RWMutex // Separate mutex for cluster operations

	// Optional preferred seed embedding indices for k-means++ first centroid selection.
	// When set, ClusterWithContext uses one of these as the first centroid, then
	// proceeds with standard k-means++ distance-weighted selection.
	preferredSeedIndices []int
}

// nodeUpdate tracks pending node reassignments for Tier 2 updates.
type nodeUpdate struct {
	id         string
	oldCluster int
	newCluster int
}

// NewClusterIndex creates a clusterable embedding index.
//
// Parameters:
//   - manager: GPU manager (can be nil for CPU-only mode)
//   - embConfig: Embedding index config (nil uses defaults)
//   - kmeansConfig: K-means config (nil uses defaults)
//
// Example:
//
//	// Default configuration
//	index := gpu.NewClusterIndex(manager, nil, nil)
//
//	// Custom configuration
//	embConfig := &gpu.EmbeddingIndexConfig{
//	    Dimensions: 1024,
//	    InitialCap: 100000,
//	}
//	kmeansConfig := &gpu.KMeansConfig{
//	    NumClusters:   500,
//	    MaxIterations: 50,
//	}
//	index = gpu.NewClusterIndex(manager, embConfig, kmeansConfig)
func NewClusterIndex(manager *Manager, embConfig *EmbeddingIndexConfig, kmeansConfig *KMeansConfig) *ClusterIndex {
	if kmeansConfig == nil {
		kmeansConfig = DefaultKMeansConfig()
	}

	return &ClusterIndex{
		EmbeddingIndex: NewEmbeddingIndex(manager, embConfig),
		config:         kmeansConfig,
		pendingUpdates: make([]nodeUpdate, 0, 1000),
	}
}

// Config returns a copy of the k-means configuration.
func (ci *ClusterIndex) Config() KMeansConfig {
	ci.clusterMu.RLock()
	defer ci.clusterMu.RUnlock()
	if ci.config == nil {
		return *DefaultKMeansConfig()
	}
	return *ci.config
}

// Cluster performs k-means clustering on current embeddings.
//
// This method:
//  1. Determines optimal K (if AutoK enabled)
//  2. Initializes centroids (k-means++ or random)
//  3. Iterates assignment/update steps until convergence
//  4. Builds cluster membership map for fast lookup
//
// Returns error if too few embeddings or invalid configuration.
//
// Example:
//
//	if err := index.Cluster(); err != nil {
//	    log.Printf("Clustering failed: %v", err)
//	}
//
//	stats := index.ClusterStats()
//	fmt.Printf("Created %d clusters in %v\n",
//	    stats.NumClusters, stats.LastClusterTime)
//
// Cluster runs k-means clustering. For cancellable clustering (e.g. on shutdown), use ClusterWithContext.
func (ci *ClusterIndex) Cluster() error {
	return ci.ClusterWithContext(context.Background())
}

// ClusterWithContext runs k-means clustering and stops promptly if ctx is cancelled (e.g. process shutdown).
// It copies embedding data out, runs the iteration without holding clusterMu, then applies the result.
// This allows search to continue using the previous clustering state while k-means runs in the background.
func (ci *ClusterIndex) ClusterWithContext(ctx context.Context) error {
	clustering, err := ci.ComputeClustering(ctx)
	if err != nil || clustering == nil {
		return err
	}
	ci.PublishClustering(clustering)
	return nil
}

// ComputeClustering runs k-means on a copy of the current embeddings without
// changing the published clustering. It returns nil when the index is empty.
func (ci *ClusterIndex) ComputeClustering(ctx context.Context) (*Clustering, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Lock order: always clusterMu before mu (matches GetClusterMemberIDs, OnNodeUpdate).
	ci.clusterMu.Lock()
	ci.mu.Lock()

	n := len(ci.nodeIDs)
	if n == 0 {
		ci.mu.Unlock()
		ci.clusterMu.Unlock()
		return nil, nil
	}

	if err := ctx.Err(); err != nil {
		ci.mu.Unlock()
		ci.clusterMu.Unlock()
		return nil, err
	}

	// Determine K
	k := ci.config.NumClusters
	if k <= 0 || ci.config.AutoK {
		k = optimalK(n)
	}
	if k > n {
		k = n
	}
	if k < 1 {
		ci.mu.Unlock()
		ci.clusterMu.Unlock()
		return nil, ErrInvalidK
	}
	if n < k {
		ci.mu.Unlock()
		ci.clusterMu.Unlock()
		return nil, ErrTooFewEmbeddings
	}

	dims := ci.dimensions
	// Copy vectors so we can release both locks during the long-running iteration.
	// Search needs ci.mu.RLock() in GetClusterMemberIDs, so we must not hold ci.mu.
	vectorsCopy := make([]float32, len(ci.cpuVectors))
	copy(vectorsCopy, ci.cpuVectors)
	nodeIDs := append([]string(nil), ci.nodeIDs...)
	initMethod := ci.config.InitMethod
	maxIter := ci.config.MaxIterations
	preferredSeeds := append([]int(nil), ci.preferredSeedIndices...)
	ci.preferredSeedIndices = nil

	ci.mu.Unlock()
	ci.clusterMu.Unlock()

	start := time.Now()

	// Initialize centroids on the copy (no lock held)
	var centroids [][]float32
	var err error
	if initMethod == "kmeans++" {
		centroids, err = initCentroidsKMeansPlusPlusSeededFromVectorsWithContext(ctx, vectorsCopy, n, dims, k, preferredSeeds)
	} else {
		centroids, err = initCentroidsRandomFromVectors(vectorsCopy, n, dims, k)
	}
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	assignments := make([]int, n)
	centroidSums := make([][]float64, k)
	centroidCounts := make([]int, k)
	for c := 0; c < k; c++ {
		centroidSums[c] = make([]float64, dims)
	}

	iterations := 0
	for iter := 0; iter < maxIter; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changed := assignToCentroidsFromVectors(vectorsCopy, centroids, assignments, n, dims, k)
		updateCentroidsFromVectors(vectorsCopy, assignments, centroids, centroidSums, centroidCounts, n, dims, k)
		iterations++
		log.Printf("🔬 K-means iteration %d complete (reassignments: %d)", iterations, changed)
		if changed == 0 {
			break
		}
	}

	clustering := newClustering(centroids, nodeIDs, assignments)
	clustering.computed = true
	clustering.iterations = iterations
	clustering.start = start
	return clustering, nil
}

// PublishClustering makes clustering the index's routing and membership: the
// index routes through clustering itself from now on, and later admissions
// to it (AssignToClustering, Remove) are the index's. Its membership is first
// brought to the index's embeddings by node ID: an embedding the generation
// does not know joins its nearest centroid, and a member the index no longer
// holds is dropped. A previously published Clustering is left as it was.
func (ci *ClusterIndex) PublishClustering(clustering *Clustering) {
	// Brief lock to publish new cluster state (order: clusterMu, mu,
	// then the clustering's own lock).
	ci.clusterMu.Lock()
	ci.mu.Lock()
	defer ci.mu.Unlock()
	defer ci.clusterMu.Unlock()
	clustering.mu.Lock()
	present := make(map[string]struct{}, len(ci.nodeIDs))
	for _, id := range ci.nodeIDs {
		present[id] = struct{}{}
		if _, ok := clustering.membership.clusterOf(id); ok {
			continue
		}
		if cid, ok := ci.nearestCentroidForIDLocked(id, clustering.centroids); ok {
			clustering.membership.set(id, cid)
		}
	}
	for id := range clustering.membership.idToCluster {
		if _, ok := present[id]; !ok {
			clustering.membership.remove(id)
		}
	}
	clustering.mu.Unlock()
	ci.routing = clustering
	ci.updatesSinceCluster = 0
	if !clustering.computed {
		return
	}
	ci.iterations = clustering.iterations
	ci.lastClusterTime = time.Now()
	ci.lastClusterDuration = time.Since(clustering.start)

	atomic.AddInt64(&ci.clusterIterations, int64(clustering.iterations))
}

// optimalK calculates optimal cluster count using sqrt(n/2) heuristic so that
// average cluster size is about sqrt(2n). Scales with dataset (e.g. 900k → ~670 clusters).
func optimalK(n int) int {
	k := int(math.Sqrt(float64(n) / 2))
	if k < 10 {
		k = 10 // Minimum clusters
	}
	if k > 8192 {
		k = 8192 // Maximum clusters (e.g. 10M vectors → ~2236 clusters)
	}
	return k
}

// initCentroidsKMeansPlusPlusSeededFromVectors initializes centroids using k-means++.
// If preferredSeeds is non-empty, the first centroid is chosen from that subset.
func initCentroidsKMeansPlusPlusSeededFromVectors(vectors []float32, n, dims, k int, preferredSeeds []int) ([][]float32, error) {
	return initCentroidsKMeansPlusPlusSeededFromVectorsWithContext(context.Background(), vectors, n, dims, k, preferredSeeds)
}

func initCentroidsKMeansPlusPlusSeededFromVectorsWithContext(ctx context.Context, vectors []float32, n, dims, k int, preferredSeeds []int) ([][]float32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	centroids := make([][]float32, k)
	firstIdx := rand.Intn(n)
	if len(preferredSeeds) > 0 {
		valid := make([]int, 0, len(preferredSeeds))
		for _, idx := range preferredSeeds {
			if idx >= 0 && idx < n {
				valid = append(valid, idx)
			}
		}
		if len(valid) > 0 {
			firstIdx = valid[rand.Intn(len(valid))]
		}
	}
	centroids[0] = make([]float32, dims)
	start := firstIdx * dims
	copy(centroids[0], vectors[start:start+dims])

	minDistances := make([]float64, n)
	for i := 0; i < n; i++ {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		embStart := i * dims
		minDistances[i] = squaredEuclidean(vectors[embStart:embStart+dims], centroids[0])
	}

	for c := 1; c < k; c++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		totalWeight := 0.0
		for i := 0; i < n; i++ {
			if i&63 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			totalWeight += minDistances[i]
		}
		target := rand.Float64() * totalWeight
		cumWeight := 0.0
		selectedIdx := n - 1
		for i := 0; i < n; i++ {
			if i&63 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			cumWeight += minDistances[i]
			if cumWeight >= target {
				selectedIdx = i
				break
			}
		}
		centroids[c] = make([]float32, dims)
		start := selectedIdx * dims
		copy(centroids[c], vectors[start:start+dims])
		for i := 0; i < n; i++ {
			if i&63 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			embStart := i * dims
			distToNew := squaredEuclidean(vectors[embStart:embStart+dims], centroids[c])
			if distToNew < minDistances[i] {
				minDistances[i] = distToNew
			}
		}
	}
	return centroids, nil
}

// SetPreferredSeedIndices sets optional preferred indices for the next Cluster/ClusterWithContext call.
// Indices outside the current embedding range are ignored during clustering.
func (ci *ClusterIndex) SetPreferredSeedIndices(indices []int) {
	ci.clusterMu.Lock()
	ci.mu.Lock()
	defer ci.mu.Unlock()
	defer ci.clusterMu.Unlock()
	if len(indices) == 0 {
		ci.preferredSeedIndices = nil
		return
	}
	ci.preferredSeedIndices = append(ci.preferredSeedIndices[:0], indices...)
}

// GetIndicesForNodeIDs returns embedding indices for the provided node IDs.
func (ci *ClusterIndex) GetIndicesForNodeIDs(nodeIDs []string) []int {
	if len(nodeIDs) == 0 {
		return nil
	}
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	out := make([]int, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if idx, ok := ci.idToIndex[id]; ok {
			out = append(out, idx)
		}
	}
	return out
}

// initCentroidsRandomFromVectors initializes centroids by random selection from a vector slice.
func initCentroidsRandomFromVectors(vectors []float32, n, dims, k int) ([][]float32, error) {
	centroids := make([][]float32, k)
	selected := make(map[int]bool)
	for i := 0; i < k; i++ {
		var idx int
		for {
			idx = rand.Intn(n)
			if !selected[idx] {
				selected[idx] = true
				break
			}
		}
		centroids[i] = make([]float32, dims)
		start := idx * dims
		copy(centroids[i], vectors[start:start+dims])
	}
	return centroids, nil
}

// assignToCentroidsFromVectors assigns each embedding to nearest centroid; returns number changed.
func assignToCentroidsFromVectors(vectors []float32, centroids [][]float32, assignments []int, n, dims, k int) int {
	changed := 0
	for i := 0; i < n; i++ {
		embStart := i * dims
		emb := vectors[embStart : embStart+dims]
		minDist := math.MaxFloat64
		nearest := 0
		for c := 0; c < k; c++ {
			dist := squaredEuclidean(emb, centroids[c])
			if dist < minDist {
				minDist = dist
				nearest = c
			}
		}
		if assignments[i] != nearest {
			assignments[i] = nearest
			changed++
		}
	}
	return changed
}

// updateCentroidsFromVectors recomputes centroids from assignments using pre-allocated buffers.
func updateCentroidsFromVectors(vectors []float32, assignments []int, centroids [][]float32, sums [][]float64, counts []int, n, dims, k int) {
	for c := 0; c < k; c++ {
		counts[c] = 0
		for d := 0; d < dims; d++ {
			sums[c][d] = 0
		}
	}
	for i := 0; i < n; i++ {
		cluster := assignments[i]
		embStart := i * dims
		counts[cluster]++
		for d := 0; d < dims; d++ {
			sums[cluster][d] += float64(vectors[embStart+d])
		}
	}
	for c := 0; c < k; c++ {
		if counts[c] > 0 {
			for d := 0; d < dims; d++ {
				centroids[c][d] = float32(sums[c][d] / float64(counts[c]))
			}
		}
	}
}

// nearestCentroidFromList returns the index of the centroid nearest to the embedding.
func nearestCentroidFromList(embedding []float32, centroids [][]float32) int {
	if len(centroids) == 0 {
		return -1
	}
	minDist := math.MaxFloat64
	nearest := 0
	for c, centroid := range centroids {
		dist := squaredEuclidean(embedding, centroid)
		if dist < minDist {
			minDist = dist
			nearest = c
		}
	}
	return nearest
}

// initCentroidsRandom initializes centroids by random selection.
func (ci *ClusterIndex) initCentroidsRandom(k int) ([][]float32, error) {
	n := len(ci.nodeIDs)
	dims := ci.dimensions

	centroids := make([][]float32, k)
	selected := make(map[int]bool)

	for i := 0; i < k; i++ {
		// Pick random unselected embedding
		var idx int
		for {
			idx = rand.Intn(n)
			if !selected[idx] {
				selected[idx] = true
				break
			}
		}

		// Copy embedding as centroid
		centroids[i] = make([]float32, dims)
		start := idx * dims
		copy(centroids[i], ci.cpuVectors[start:start+dims])
	}

	return centroids, nil
}

// initCentroidsKMeansPlusPlus initializes centroids using k-means++ algorithm.
// This produces better initial centroids than random selection.
func (ci *ClusterIndex) initCentroidsKMeansPlusPlus(k int) ([][]float32, error) {
	n := len(ci.nodeIDs)
	dims := ci.dimensions

	centroids := make([][]float32, k)

	// Step 1: Choose first centroid randomly
	firstIdx := rand.Intn(n)
	centroids[0] = make([]float32, dims)
	start := firstIdx * dims
	copy(centroids[0], ci.cpuVectors[start:start+dims])

	// Distance to nearest chosen centroid (cached)
	minDistances := make([]float64, n)

	// Initialize distances to first centroid
	for i := 0; i < n; i++ {
		embStart := i * dims
		emb := ci.cpuVectors[embStart : embStart+dims]
		minDistances[i] = squaredEuclidean(emb, centroids[0])
	}

	// Step 2: Choose remaining centroids proportional to D(x)^2
	for c := 1; c < k; c++ {
		// Compute total weight and sample next centroid
		totalWeight := 0.0
		for i := 0; i < n; i++ {
			totalWeight += minDistances[i]
		}

		// Sample next centroid weighted by distance^2
		target := rand.Float64() * totalWeight
		cumWeight := 0.0
		selectedIdx := n - 1

		for i := 0; i < n; i++ {
			cumWeight += minDistances[i]
			if cumWeight >= target {
				selectedIdx = i
				break
			}
		}

		// Copy selected embedding as centroid
		centroids[c] = make([]float32, dims)
		start := selectedIdx * dims
		copy(centroids[c], ci.cpuVectors[start:start+dims])

		// Update minDistances: only compare with new centroid
		// (if new centroid is closer, update cached distance)
		newCentroid := centroids[c]
		for i := 0; i < n; i++ {
			embStart := i * dims
			emb := ci.cpuVectors[embStart : embStart+dims]
			distToNew := squaredEuclidean(emb, newCentroid)
			if distToNew < minDistances[i] {
				minDistances[i] = distToNew
			}
		}
	}

	return centroids, nil
}

// squaredEuclidean computes squared Euclidean distance.
// Uses 4-way loop unrolling for better instruction-level parallelism.
func squaredEuclidean(a []float32, b []float32) float64 {
	n := len(a)
	var sum0, sum1, sum2, sum3 float64

	// Process 4 elements at a time
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float64(a[i] - b[i])
		d1 := float64(a[i+1] - b[i+1])
		d2 := float64(a[i+2] - b[i+2])
		d3 := float64(a[i+3] - b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}

	// Handle remaining elements
	for ; i < n; i++ {
		diff := float64(a[i] - b[i])
		sum0 += diff * diff
	}

	return sum0 + sum1 + sum2 + sum3
}

// Clear removes all embeddings and cluster state from the index.
// This overrides EmbeddingIndex.Clear() to also reset clustering state.
func (ci *ClusterIndex) Clear() {
	ci.clusterMu.Lock()
	defer ci.clusterMu.Unlock()

	// Clear the embedded EmbeddingIndex
	ci.EmbeddingIndex.Clear()

	// Reset cluster state
	ci.routing = nil
	clear(ci.pendingUpdates)
	ci.pendingUpdates = ci.pendingUpdates[:0]
	ci.updatesSinceCluster = 0
	ci.iterations = 0
	ci.clusterIterations = 0
	ci.centroidDrift = 0
}

// RestoreClusteringState sets clustering state from persisted centroids and id->cluster map
// so that k-means can be skipped on load. Call after AddBatch has populated the index.
// A node ID not in idToCluster (or mapped to no centroid) joins its nearest centroid.
func (ci *ClusterIndex) RestoreClusteringState(centroids [][]float32, idToCluster map[string]int) error {
	if idToCluster == nil {
		return errors.New("centroids and idToCluster required")
	}
	ci.mu.RLock()
	err := ci.checkCentroidsLocked(centroids)
	var clustering *Clustering
	if err == nil {
		clustering = &Clustering{centroids: centroids, membership: newClusterMembership(len(idToCluster))}
		for _, id := range ci.nodeIDs {
			if cid, ok := idToCluster[id]; ok && cid >= 0 && cid < len(centroids) {
				clustering.membership.set(id, cid)
			}
		}
	}
	ci.mu.RUnlock()
	if err != nil {
		return err
	}
	ci.PublishClustering(clustering)
	return nil
}

// AssignedClustering places every embedding in the index at the centroid
// nearest its current coordinates, as an unpublished generation (for example
// one restored from persisted centroids). Like ComputeClustering it copies
// the embeddings and assigns them without holding the index's locks.
func (ci *ClusterIndex) AssignedClustering(ctx context.Context, centroids [][]float32) (*Clustering, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ci.mu.RLock()
	if err := ci.checkCentroidsLocked(centroids); err != nil {
		ci.mu.RUnlock()
		return nil, err
	}
	n, dims := len(ci.nodeIDs), ci.dimensions
	vectors := append([]float32(nil), ci.cpuVectors[:n*dims]...)
	nodeIDs := append([]string(nil), ci.nodeIDs...)
	ci.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	assignments := make([]int, n)
	assignToCentroidsFromVectors(vectors, centroids, assignments, n, dims, len(centroids))
	return newClustering(centroids, nodeIDs, assignments), nil
}

// checkCentroidsLocked validates restored centroids against the index;
// callers hold ci.mu.
func (ci *ClusterIndex) checkCentroidsLocked(centroids [][]float32) error {
	if len(centroids) == 0 {
		return errors.New("centroids and idToCluster required")
	}
	if len(ci.nodeIDs) == 0 {
		return errors.New("index has no embeddings; call AddBatch before RestoreClusteringState")
	}
	for _, c := range centroids {
		if len(c) != ci.dimensions {
			return fmt.Errorf("centroid dimensions %d != index dimensions %d", len(c), ci.dimensions)
		}
	}
	return nil
}

// IsClustered returns true if clustering has been performed.
func (ci *ClusterIndex) IsClustered() bool {
	return ci.PublishedClustering() != nil
}

// NumClusters returns the number of clusters.
func (ci *ClusterIndex) NumClusters() int {
	if c := ci.PublishedClustering(); c != nil {
		return c.NumClusters()
	}
	return 0
}

// GetCentroids returns a copy of the centroid vectors for persistence.
// Returns nil if not clustered.
func (ci *ClusterIndex) GetCentroids() [][]float32 {
	c := ci.PublishedClustering()
	if c == nil || len(c.centroids) == 0 {
		return nil
	}
	out := make([][]float32, len(c.centroids))
	for i, vec := range c.centroids {
		out[i] = make([]float32, len(vec))
		copy(out[i], vec)
	}
	return out
}

// ClusterStats returns clustering statistics.
func (ci *ClusterIndex) ClusterStats() ClusterStats {
	ci.clusterMu.RLock()
	defer ci.clusterMu.RUnlock()

	ci.mu.RLock()
	embeddingCount := len(ci.nodeIDs)
	ci.mu.RUnlock()

	stats := ClusterStats{
		EmbeddingCount:  embeddingCount,
		Iterations:      ci.iterations,
		LastClusterTime: ci.lastClusterDuration,
		CentroidDrift:   ci.centroidDrift,
		Clustered:       ci.routing != nil,
	}
	if ci.routing == nil {
		return stats
	}
	stats.NumClusters = ci.routing.NumClusters()
	ci.routing.mu.RLock()
	defer ci.routing.mu.RUnlock()

	populated := 0
	for _, members := range ci.routing.membership.members {
		if len(members) > 0 {
			populated++
		}
	}
	if populated > 0 {
		totalSize := 0
		stats.MinClusterSize = math.MaxInt32
		stats.MaxClusterSize = 0

		for _, members := range ci.routing.membership.members {
			size := len(members)
			if size == 0 {
				continue
			}
			totalSize += size
			if size < stats.MinClusterSize {
				stats.MinClusterSize = size
			}
			if size > stats.MaxClusterSize {
				stats.MaxClusterSize = size
			}
		}

		stats.AvgClusterSize = float64(totalSize) / float64(populated)
	}

	return stats
}

// FindNearestCentroid finds the cluster ID nearest to the given embedding.
func (ci *ClusterIndex) FindNearestCentroid(embedding []float32) int {
	c := ci.PublishedClustering()
	if c == nil || len(c.centroids) == 0 {
		return -1
	}
	return nearestCentroidFromList(embedding, c.centroids)
}

// FindNearestClusters finds the k nearest cluster IDs to the given embedding.
func (ci *ClusterIndex) FindNearestClusters(embedding []float32, k int) []int {
	if c := ci.PublishedClustering(); c != nil {
		return c.NearestClusters(embedding, k)
	}
	return nil
}

// GetClusterMembers returns the embedding indices belonging to the given clusters.
func (ci *ClusterIndex) GetClusterMembers(clusterIDs []int) []int {
	c := ci.PublishedClustering()
	if c == nil {
		return nil
	}
	ids := c.MemberIDsOf(clusterIDs)
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	members := make([]int, 0, len(ids))
	for _, id := range ids {
		if idx, ok := ci.idToIndex[id]; ok {
			members = append(members, idx)
		}
	}
	return members
}

// GetClusterMemberIDs returns node IDs belonging to the given clusters.
//
// This is the stable, package-level API for consuming cluster membership outside
// the gpu package. It intentionally copies the IDs to avoid exposing internal
// slices that may be mutated during re-clustering.
func (ci *ClusterIndex) GetClusterMemberIDs(clusterIDs []int) []string {
	if c := ci.PublishedClustering(); c != nil {
		return c.MemberIDsOf(clusterIDs)
	}
	return nil
}

// GetClusterMemberIDsForCluster returns node IDs for a single cluster ID.
func (ci *ClusterIndex) GetClusterMemberIDsForCluster(clusterID int) []string {
	return ci.GetClusterMemberIDs([]int{clusterID})
}

// SearchWithClusters performs cluster-accelerated similarity search.
//
// This method:
//  1. Finds the k nearest clusters to the query
//  2. Gets all embeddings from those clusters as candidates
//  3. Performs exact similarity search on candidates only
//
// Parameters:
//   - query: Query embedding vector
//   - topK: Number of results to return
//   - numClusters: Number of clusters to search (expansion factor)
//
// Returns: SearchResult slice sorted by similarity (descending)
//
// Example:
//
//	// Search 3 nearest clusters for top 10 results
//	results, err := index.SearchWithClusters(query, 10, 3)
func (ci *ClusterIndex) SearchWithClusters(query []float32, topK, numClusters int) ([]SearchResult, error) {
	if !ci.IsClustered() {
		// Fall back to brute-force search
		return ci.Search(query, topK)
	}

	// Find nearest clusters
	clusterIDs := ci.FindNearestClusters(query, numClusters)
	if len(clusterIDs) == 0 {
		return nil, nil
	}

	// Get candidate embedding indices
	candidates := ci.GetClusterMembers(clusterIDs)
	if len(candidates) == 0 {
		return nil, nil
	}

	// Search among candidates only
	return ci.SearchCandidates(context.Background(), query, candidates, topK)
}

// SearchCandidates performs similarity search on a subset of embeddings.
func (ci *ClusterIndex) SearchCandidates(ctx context.Context, query []float32, candidateIndices []int, topK int) ([]SearchResult, error) {
	if len(query) != ci.dimensions {
		return nil, ErrInvalidDimensions
	}

	ci.mu.RLock()
	defer ci.mu.RUnlock()

	if len(candidateIndices) == 0 {
		return nil, nil
	}

	if topK > len(candidateIndices) {
		topK = len(candidateIndices)
	}

	// Compute similarities for candidates only
	type scoreIdx struct {
		score float32
		idx   int
	}

	scores := make([]scoreIdx, len(candidateIndices))
	for i, embIdx := range candidateIndices {
		start := embIdx * ci.dimensions
		end := start + ci.dimensions
		emb := ci.cpuVectors[start:end]
		scores[i] = scoreIdx{
			score: cosineSimilarityFlat(query, emb),
			idx:   embIdx,
		}
	}

	// Partial sort for top-k
	for i := 0; i < topK; i++ {
		maxIdx := i
		for j := i + 1; j < len(scores); j++ {
			if scores[j].score > scores[maxIdx].score {
				maxIdx = j
			}
		}
		scores[i], scores[maxIdx] = scores[maxIdx], scores[i]
	}

	// Build results
	results := make([]SearchResult, topK)
	for i := 0; i < topK; i++ {
		embIdx := scores[i].idx
		results[i] = SearchResult{
			ID:       ci.nodeIDs[embIdx],
			Score:    scores[i].score,
			Distance: 1 - scores[i].score,
		}
	}

	return results, nil
}

// OnNodeUpdate handles real-time embedding changes (Tier 1).
//
// This method:
//  1. Adds/updates the embedding in the index
//  2. If clustered, reassigns to nearest centroid
//  3. Tracks update for potential batch centroid recalculation
//
// Example:
//
//	// Called when a node's embedding changes
//	if err := index.OnNodeUpdate("node-123", newEmbedding); err != nil {
//	    log.Printf("Update failed: %v", err)
//	}
func (ci *ClusterIndex) OnNodeUpdate(nodeID string, embedding []float32) error {
	// Add/update in base index
	if err := ci.Add(nodeID, embedding); err != nil {
		return err
	}

	ci.clusterMu.Lock()
	defer ci.clusterMu.Unlock()
	if ci.routing == nil {
		return nil
	}

	oldCluster, wasMember, newCluster, ok := ci.AssignToClustering(ci.routing, nodeID)
	if !ok {
		return nil
	}

	// Track a changed assignment for the batch centroid update.
	if wasMember && oldCluster != newCluster {
		ci.pendingUpdates = append(ci.pendingUpdates, nodeUpdate{
			id:         nodeID,
			oldCluster: oldCluster,
			newCluster: newCluster,
		})
	}

	atomic.AddInt64(&ci.updatesSinceCluster, 1)

	return nil
}

// ShouldRecluster checks if re-clustering is needed based on thresholds.
func (ci *ClusterIndex) ShouldRecluster() bool {
	ci.clusterMu.RLock()
	defer ci.clusterMu.RUnlock()

	if ci.routing == nil {
		return false
	}

	// Trigger if too many updates (>10% of dataset)
	updateRatio := float64(atomic.LoadInt64(&ci.updatesSinceCluster)) / float64(len(ci.nodeIDs))
	if updateRatio > 0.1 {
		return true
	}

	// Trigger if centroid drift is high
	if ci.centroidDrift > ci.config.DriftThreshold {
		return true
	}

	// Trigger if too much time has passed (1 hour)
	if time.Since(ci.lastClusterTime) > time.Hour {
		return true
	}

	return false
}

// UpdateCentroidsBatch recomputes centroids for affected clusters (Tier 2).
// Call periodically to keep centroids accurate after node updates.
func (ci *ClusterIndex) UpdateCentroidsBatch() {
	ci.clusterMu.Lock()
	updates := ci.pendingUpdates
	ci.pendingUpdates = make([]nodeUpdate, 0, 1000)
	ci.clusterMu.Unlock()

	if len(updates) == 0 {
		return
	}

	// Collect affected clusters
	affectedClusters := make(map[int]bool)
	for _, u := range updates {
		affectedClusters[u.oldCluster] = true
		affectedClusters[u.newCluster] = true
	}

	ci.mu.RLock()
	ci.clusterMu.Lock()
	defer ci.mu.RUnlock()
	defer ci.clusterMu.Unlock()
	routing := ci.routing
	if routing == nil {
		return
	}
	routing.mu.Lock()
	defer routing.mu.Unlock()

	// Recompute centroids for affected clusters
	dims := ci.dimensions
	for cluster := range affectedClusters {
		members := routing.membership.members[cluster]
		if len(members) == 0 {
			continue
		}

		// Compute new centroid as mean
		newCentroid := make([]float64, dims)
		for _, id := range members {
			idx, ok := ci.idToIndex[id]
			if !ok {
				continue
			}
			start := idx * dims
			for d := 0; d < dims; d++ {
				newCentroid[d] += float64(ci.cpuVectors[start+d])
			}
		}

		for d := 0; d < dims; d++ {
			routing.centroids[cluster][d] = float32(newCentroid[d] / float64(len(members)))
		}
	}
}

// Dimensions returns the embedding dimensions.
func (ci *ClusterIndex) Dimensions() int {
	return ci.dimensions
}

// GetConfig returns the k-means configuration.
func (ci *ClusterIndex) GetConfig() *KMeansConfig {
	return ci.config
}
