// Package gpu tests for GPU-accelerated k-means clustering.
package gpu

import (
	"math"
	"sync"
	"testing"
)

// Helper function to generate synthetic embeddings
func generateTestEmbeddings(count, dims int) ([]string, [][]float32) {
	nodeIDs := make([]string, count)
	embeddings := make([][]float32, count)

	for i := 0; i < count; i++ {
		nodeIDs[i] = "node-" + string(rune('0'+i%10)) + string(rune('A'+i/10%26))
		embeddings[i] = make([]float32, dims)
		for d := 0; d < dims; d++ {
			// Create synthetic embeddings with some structure
			embeddings[i][d] = float32((i*d)%100) / 100.0
		}
	}

	return nodeIDs, embeddings
}

// Helper function to generate clustered embeddings (for testing cluster quality)
func generateClusteredEmbeddings(clustersCount, pointsPerCluster, dims int) ([]string, [][]float32, []int) {
	total := clustersCount * pointsPerCluster
	nodeIDs := make([]string, total)
	embeddings := make([][]float32, total)
	trueLabels := make([]int, total)

	for c := 0; c < clustersCount; c++ {
		// Generate a random centroid for each cluster
		centroid := make([]float32, dims)
		for d := 0; d < dims; d++ {
			centroid[d] = float32(c) / float32(clustersCount)
		}

		// Generate points around this centroid
		for p := 0; p < pointsPerCluster; p++ {
			idx := c*pointsPerCluster + p
			nodeIDs[idx] = "node-" + string(rune('0'+c)) + "-" + string(rune('A'+p))
			embeddings[idx] = make([]float32, dims)
			trueLabels[idx] = c

			for d := 0; d < dims; d++ {
				// Small noise around centroid
				noise := float32(p%5) * 0.01
				embeddings[idx][d] = centroid[d] + noise
			}
		}
	}

	return nodeIDs, embeddings, trueLabels
}

// Test constants - use same dimensions as embedding config will auto-detect
const testDims = 64

// embConfig creates a test embedding config with specified dimensions
func embConfig(dims int) *EmbeddingIndexConfig {
	return DefaultEmbeddingIndexConfig(dims)
}

// testManager creates a disabled GPU manager for testing
func testManager() *Manager {
	m, _ := NewManager(nil)
	return m
}

func TestDefaultKMeansConfig(t *testing.T) {
	config := DefaultKMeansConfig()

	if config.NumClusters != 0 {
		t.Errorf("expected NumClusters=0 for auto-detect, got %d", config.NumClusters)
	}
	if config.MaxIterations != 100 {
		t.Errorf("expected MaxIterations=100, got %d", config.MaxIterations)
	}
	if config.Tolerance != 0.0001 {
		t.Errorf("expected Tolerance=0.0001, got %f", config.Tolerance)
	}
	if config.InitMethod != "kmeans++" {
		t.Errorf("expected InitMethod=kmeans++, got %s", config.InitMethod)
	}
	if !config.AutoK {
		t.Error("expected AutoK=true")
	}
	if config.DriftThreshold != 0.1 {
		t.Errorf("expected DriftThreshold=0.1, got %f", config.DriftThreshold)
	}
	if config.MinClusterSize != 10 {
		t.Errorf("expected MinClusterSize=10, got %d", config.MinClusterSize)
	}
}

func TestNewClusterIndex(t *testing.T) {
	t.Run("with nil configs", func(t *testing.T) {
		ci := NewClusterIndex(testManager(), nil, nil)
		if ci == nil {
			t.Fatal("NewClusterIndex returned nil")
		}
		if ci.EmbeddingIndex == nil {
			t.Error("EmbeddingIndex should not be nil")
		}
		if ci.config == nil {
			t.Error("config should not be nil")
		}
		if ci.clusterMap == nil {
			t.Error("clusterMap should not be nil")
		}
	})

	t.Run("with custom config", func(t *testing.T) {
		kconfig := &KMeansConfig{
			NumClusters:   50,
			MaxIterations: 25,
			InitMethod:    "random",
		}
		ci := NewClusterIndex(testManager(), embConfig(testDims), kconfig)
		if ci.config.NumClusters != 50 {
			t.Errorf("expected NumClusters=50, got %d", ci.config.NumClusters)
		}
		if ci.config.MaxIterations != 25 {
			t.Errorf("expected MaxIterations=25, got %d", ci.config.MaxIterations)
		}
	})
}

func TestClusterIndex_AddAndCluster(t *testing.T) {
	t.Run("basic clustering", func(t *testing.T) {
		ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
			NumClusters:   3,
			MaxIterations: 50,
			InitMethod:    "random",
		})

		// Add some embeddings
		nodeIDs, embeddings := generateTestEmbeddings(100, testDims)
		for i, emb := range embeddings {
			if err := ci.Add(nodeIDs[i], emb); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
		}

		// Cluster
		if err := ci.Cluster(); err != nil {
			t.Fatalf("Cluster() error = %v", err)
		}

		// Verify state
		if !ci.IsClustered() {
			t.Error("IsClustered() should be true after Cluster()")
		}

		if ci.NumClusters() != 3 {
			t.Errorf("expected 3 clusters, got %d", ci.NumClusters())
		}

		stats := ci.ClusterStats()
		if stats.EmbeddingCount != 100 {
			t.Errorf("expected 100 embeddings, got %d", stats.EmbeddingCount)
		}
		if stats.NumClusters != 3 {
			t.Errorf("expected 3 clusters in stats, got %d", stats.NumClusters)
		}
		if stats.Iterations == 0 {
			t.Error("expected iterations > 0")
		}
	})

	t.Run("clustering empty index", func(t *testing.T) {
		ci := NewClusterIndex(testManager(), embConfig(testDims), nil)
		if err := ci.Cluster(); err != nil {
			t.Errorf("Cluster() on empty index should not error, got %v", err)
		}
	})

	t.Run("auto-K clustering", func(t *testing.T) {
		ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
			AutoK:         true,
			MaxIterations: 50,
		})

		// Add embeddings
		nodeIDs, embeddings := generateTestEmbeddings(200, testDims)
		for i, emb := range embeddings {
			if err := ci.Add(nodeIDs[i], emb); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
		}

		// Cluster with auto-K
		if err := ci.Cluster(); err != nil {
			t.Fatalf("Cluster() error = %v", err)
		}

		// Auto-K should select reasonable cluster count
		k := ci.NumClusters()
		if k < 10 || k > 50 {
			t.Errorf("auto-K selected %d clusters, expected 10-50 for 200 points", k)
		}
	})

	t.Run("kmeans++ initialization", func(t *testing.T) {
		ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
			NumClusters:   5,
			MaxIterations: 50,
			InitMethod:    "kmeans++",
		})

		// Add embeddings
		nodeIDs, embeddings := generateTestEmbeddings(100, testDims)
		for i, emb := range embeddings {
			if err := ci.Add(nodeIDs[i], emb); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
		}

		// Cluster
		if err := ci.Cluster(); err != nil {
			t.Fatalf("Cluster() error = %v", err)
		}

		if !ci.IsClustered() {
			t.Error("should be clustered")
		}
	})
}

func TestClusterIndex_FindNearestCentroid(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   3,
		MaxIterations: 50,
	})

	// Add embeddings in 3 distinct clusters

	// Cluster 1: vectors near [1, 0, 0, ...]
	for i := 0; i < 10; i++ {
		emb := make([]float32, testDims)
		emb[0] = 1.0 + float32(i)*0.01
		ci.Add("cluster1-"+string(rune('A'+i)), emb)
	}

	// Cluster 2: vectors near [0, 1, 0, ...]
	for i := 0; i < 10; i++ {
		emb := make([]float32, testDims)
		emb[1] = 1.0 + float32(i)*0.01
		ci.Add("cluster2-"+string(rune('A'+i)), emb)
	}

	// Cluster 3: vectors near [0, 0, 1, ...]
	for i := 0; i < 10; i++ {
		emb := make([]float32, testDims)
		emb[2] = 1.0 + float32(i)*0.01
		ci.Add("cluster3-"+string(rune('A'+i)), emb)
	}

	// Before clustering, FindNearestCentroid should return -1
	queryNear1 := make([]float32, testDims)
	queryNear1[0] = 1.0
	if ci.FindNearestCentroid(queryNear1) != -1 {
		t.Error("FindNearestCentroid should return -1 before clustering")
	}

	// Cluster
	if err := ci.Cluster(); err != nil {
		t.Fatalf("Cluster() error = %v", err)
	}

	// Now FindNearestCentroid should work
	clusterID := ci.FindNearestCentroid(queryNear1)
	if clusterID < 0 || clusterID >= 3 {
		t.Errorf("FindNearestCentroid returned invalid cluster %d", clusterID)
	}
}

func TestClusterIndex_FindNearestClusters(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   5,
		MaxIterations: 50,
	})

	// Add embeddings
	nodeIDs, embeddings := generateTestEmbeddings(100, testDims)
	for i, emb := range embeddings {
		ci.Add(nodeIDs[i], emb)
	}

	// Cluster
	if err := ci.Cluster(); err != nil {
		t.Fatalf("Cluster() error = %v", err)
	}

	// Find 3 nearest clusters
	query := make([]float32, testDims)
	query[0] = 0.5
	clusters := ci.FindNearestClusters(query, 3)

	if len(clusters) != 3 {
		t.Errorf("expected 3 clusters, got %d", len(clusters))
	}

	// Each cluster ID should be valid
	for _, cid := range clusters {
		if cid < 0 || cid >= 5 {
			t.Errorf("invalid cluster ID %d", cid)
		}
	}

	// Should not have duplicates
	seen := make(map[int]bool)
	for _, cid := range clusters {
		if seen[cid] {
			t.Errorf("duplicate cluster ID %d", cid)
		}
		seen[cid] = true
	}
}

func TestClusterIndex_GetClusterMembers(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   3,
		MaxIterations: 50,
	})

	// Add embeddings
	nodeIDs, embeddings := generateTestEmbeddings(30, testDims)
	for i, emb := range embeddings {
		ci.Add(nodeIDs[i], emb)
	}

	// Before clustering
	if members := ci.GetClusterMembers([]int{0, 1}); members != nil {
		t.Error("GetClusterMembers should return nil before clustering")
	}

	// Cluster
	if err := ci.Cluster(); err != nil {
		t.Fatalf("Cluster() error = %v", err)
	}

	// Get members of all clusters
	allMembers := ci.GetClusterMembers([]int{0, 1, 2})
	if len(allMembers) != 30 {
		t.Errorf("expected 30 members total, got %d", len(allMembers))
	}
}

func TestClusterIndex_SearchWithClusters(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   5,
		MaxIterations: 50,
	})

	// Add embeddings
	nodeIDs, embeddings := generateTestEmbeddings(100, testDims)
	for i, emb := range embeddings {
		if err := ci.Add(nodeIDs[i], emb); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	t.Run("before clustering falls back to brute force", func(t *testing.T) {
		query := make([]float32, testDims)
		results, err := ci.SearchWithClusters(query, 5, 2)
		if err != nil {
			t.Fatalf("SearchWithClusters() error = %v", err)
		}
		if len(results) != 5 {
			t.Errorf("expected 5 results, got %d", len(results))
		}
	})

	// Cluster
	if err := ci.Cluster(); err != nil {
		t.Fatalf("Cluster() error = %v", err)
	}

	t.Run("after clustering uses clusters", func(t *testing.T) {
		query := make([]float32, testDims)
		results, err := ci.SearchWithClusters(query, 5, 2)
		if err != nil {
			t.Fatalf("SearchWithClusters() error = %v", err)
		}
		if len(results) != 5 {
			t.Errorf("expected 5 results, got %d", len(results))
		}

		// Results should be sorted by score (descending)
		for i := 1; i < len(results); i++ {
			if results[i].Score > results[i-1].Score {
				t.Error("results should be sorted by score descending")
			}
		}
	})

	t.Run("search more clusters for better recall", func(t *testing.T) {
		query := make([]float32, testDims)

		// Search 1 cluster
		results1, _ := ci.SearchWithClusters(query, 10, 1)

		// Search all clusters
		resultsAll, _ := ci.SearchWithClusters(query, 10, 5)

		// Searching more clusters should give same or better top result
		if len(results1) > 0 && len(resultsAll) > 0 {
			if resultsAll[0].Score < results1[0].Score {
				// This is possible but unlikely with good clustering
				t.Log("Note: searching more clusters gave worse top result")
			}
		}
	})
}

func TestClusterIndex_OnNodeUpdate(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   3,
		MaxIterations: 50,
	})

	// Add initial embeddings
	for i := 0; i < 30; i++ {
		emb := make([]float32, testDims)
		emb[0] = float32(i) / 30.0
		ci.Add("node-"+string(rune('A'+i)), emb)
	}

	// Cluster
	if err := ci.Cluster(); err != nil {
		t.Fatalf("Cluster() error = %v", err)
	}

	t.Run("update existing node", func(t *testing.T) {
		// Update a node with new embedding
		newEmb := make([]float32, testDims)
		newEmb[1] = 1.0 // Move to different region

		if err := ci.OnNodeUpdate("node-A", newEmb); err != nil {
			t.Fatalf("OnNodeUpdate() error = %v", err)
		}
	})

	t.Run("add new node via update", func(t *testing.T) {
		newEmb := make([]float32, testDims)
		newEmb[2] = 1.0

		if err := ci.OnNodeUpdate("new-node", newEmb); err != nil {
			t.Fatalf("OnNodeUpdate() error = %v", err)
		}

		// Should be searchable
		results, _ := ci.Search(newEmb, 1)
		if len(results) == 0 || results[0].ID != "new-node" {
			t.Error("new node should be searchable after OnNodeUpdate")
		}
	})
}

func TestClusterIndex_ShouldRecluster(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   3,
		MaxIterations: 50,
	})

	// Before clustering
	if ci.ShouldRecluster() {
		t.Error("ShouldRecluster should be false before initial clustering")
	}

	// Add embeddings and cluster
	for i := 0; i < 30; i++ {
		emb := make([]float32, testDims)
		emb[0] = float32(i) / 30.0
		ci.Add("node-"+string(rune('A'+i)), emb)
	}
	ci.Cluster()

	// Immediately after clustering
	if ci.ShouldRecluster() {
		t.Error("ShouldRecluster should be false immediately after clustering")
	}

	// Add many updates (>10% of dataset)
	for i := 0; i < 10; i++ {
		newEmb := make([]float32, testDims)
		newEmb[1] = float32(i)
		ci.OnNodeUpdate("update-"+string(rune('A'+i)), newEmb)
	}

	// Now should recommend reclustering
	if !ci.ShouldRecluster() {
		t.Error("ShouldRecluster should be true after many updates")
	}
}

func TestClusterIndex_UpdateCentroidsBatch(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   3,
		MaxIterations: 50,
	})

	// Add embeddings and cluster
	for i := 0; i < 30; i++ {
		emb := make([]float32, testDims)
		emb[0] = float32(i) / 30.0
		ci.Add("node-"+string(rune('A'+i)), emb)
	}
	ci.Cluster()

	// Get original centroids
	stats1 := ci.ClusterStats()

	// Make updates
	for i := 0; i < 5; i++ {
		newEmb := make([]float32, testDims)
		newEmb[1] = float32(i)
		ci.OnNodeUpdate("node-"+string(rune('A'+i)), newEmb)
	}

	// Batch update centroids
	ci.UpdateCentroidsBatch()

	// Verify no error and state is valid
	stats2 := ci.ClusterStats()
	if stats2.NumClusters != stats1.NumClusters {
		t.Error("cluster count should not change after batch update")
	}
}

func TestClusterIndex_ClusterStats(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   4,
		MaxIterations: 50,
	})

	// Add enough embeddings to ensure measurable clustering time
	const numEmbeddings = 500
	for i := 0; i < numEmbeddings; i++ {
		emb := make([]float32, testDims)
		emb[i%testDims] = 1.0
		ci.Add("node-"+string(rune('A'+i)), emb)
	}

	// Before clustering
	stats := ci.ClusterStats()
	if stats.Clustered {
		t.Error("Clustered should be false before clustering")
	}
	if stats.EmbeddingCount != numEmbeddings {
		t.Errorf("expected %d embeddings, got %d", numEmbeddings, stats.EmbeddingCount)
	}

	// Cluster
	ci.Cluster()

	// After clustering
	stats = ci.ClusterStats()
	if !stats.Clustered {
		t.Error("Clustered should be true after clustering")
	}
	if stats.NumClusters != 4 {
		t.Errorf("expected 4 clusters, got %d", stats.NumClusters)
	}
	if stats.Iterations == 0 {
		t.Error("Iterations should be > 0")
	}
	// With 500 embeddings, clustering should take measurable time
	if stats.LastClusterTime == 0 {
		t.Log("Warning: LastClusterTime is 0, clustering was extremely fast")
	}
	t.Logf("LastClusterTime: %v, Iterations: %d", stats.LastClusterTime, stats.Iterations)
	if stats.AvgClusterSize == 0 {
		t.Error("AvgClusterSize should be > 0")
	}
	if stats.MinClusterSize <= 0 {
		t.Error("MinClusterSize should be > 0")
	}
	if stats.MaxClusterSize < stats.MinClusterSize {
		t.Error("MaxClusterSize should be >= MinClusterSize")
	}
}

func TestClusterIndex_Dimensions(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), nil)

	// After creating with explicit dimensions
	if ci.Dimensions() != testDims {
		t.Errorf("expected dimensions=%d from config, got %d", testDims, ci.Dimensions())
	}

	// Add embedding should work
	ci.Add("node-1", make([]float32, testDims))

	if ci.Dimensions() != testDims {
		t.Errorf("expected dimensions=%d, got %d", testDims, ci.Dimensions())
	}
}

func TestClusterIndex_GetConfig(t *testing.T) {
	config := &KMeansConfig{
		NumClusters:   42,
		MaxIterations: 99,
	}
	ci := NewClusterIndex(testManager(), embConfig(testDims), config)

	if ci.GetConfig().NumClusters != 42 {
		t.Error("GetConfig should return the same config")
	}
}

func TestClusterIndex_ThreadSafety(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   10,
		MaxIterations: 20,
	})

	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			emb := make([]float32, testDims)
			emb[idx%testDims] = float32(idx)
			ci.Add("node-"+string(rune('A'+idx%26))+string(rune('0'+idx/26)), emb)
		}(i)
	}
	wg.Wait()

	// Cluster
	if err := ci.Cluster(); err != nil {
		t.Fatalf("Cluster() error = %v", err)
	}

	// Concurrent reads and updates
	for i := 0; i < 50; i++ {
		wg.Add(3)

		// Concurrent search
		go func() {
			defer wg.Done()
			query := make([]float32, testDims)
			ci.SearchWithClusters(query, 5, 3)
		}()

		// Concurrent stats
		go func() {
			defer wg.Done()
			ci.ClusterStats()
		}()

		// Concurrent updates
		go func(idx int) {
			defer wg.Done()
			emb := make([]float32, testDims)
			emb[0] = float32(idx)
			ci.OnNodeUpdate("update-"+string(rune('A'+idx)), emb)
		}(i)
	}
	wg.Wait()

	// Should not panic and state should be consistent
	stats := ci.ClusterStats()
	if stats.NumClusters == 0 {
		t.Error("should have clusters after concurrent operations")
	}
}

func TestOptimalK(t *testing.T) {
	tests := []struct {
		n       int
		wantMin int
		wantMax int
	}{
		{100, 10, 10},         // sqrt(100/2) = 7, min=10
		{200, 10, 10},         // sqrt(200/2) = 10
		{800, 10, 30},         // sqrt(800/2) = 20
		{2000, 20, 50},        // sqrt(2000/2) = 31
		{10000, 50, 100},      // sqrt(10000/2) = 70
		{2000000, 1000, 1000}, // Capped at 1000
	}

	for _, tt := range tests {
		k := optimalK(tt.n)
		if k < tt.wantMin || k > tt.wantMax {
			t.Errorf("optimalK(%d) = %d, want between %d and %d",
				tt.n, k, tt.wantMin, tt.wantMax)
		}
	}
}

func TestSquaredEuclidean(t *testing.T) {
	tests := []struct {
		a, b []float32
		want float64
	}{
		{[]float32{0, 0, 0}, []float32{0, 0, 0}, 0},
		{[]float32{1, 0, 0}, []float32{0, 0, 0}, 1},
		{[]float32{1, 1, 1}, []float32{0, 0, 0}, 3},
		{[]float32{3, 4, 0}, []float32{0, 0, 0}, 25},
		{[]float32{1, 2, 3}, []float32{4, 5, 6}, 27},
	}

	for _, tt := range tests {
		got := squaredEuclidean(tt.a, tt.b)
		if math.Abs(got-tt.want) > 0.0001 {
			t.Errorf("squaredEuclidean(%v, %v) = %f, want %f",
				tt.a, tt.b, got, tt.want)
		}
	}
}

func TestClusterIndex_Errors(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), &KMeansConfig{
		NumClusters:   10, // More clusters than points
		MaxIterations: 50,
	})

	// Add only 5 points
	for i := 0; i < 5; i++ {
		emb := make([]float32, testDims)
		emb[0] = float32(i)
		ci.Add("node-"+string(rune('A'+i)), emb)
	}

	// Clustering should adjust K to n
	err := ci.Cluster()
	if err != nil {
		t.Fatalf("Cluster() should adjust K, got error: %v", err)
	}

	// Should have 5 clusters (one per point)
	if ci.NumClusters() != 5 {
		t.Errorf("expected 5 clusters (adjusted to n), got %d", ci.NumClusters())
	}
}

func TestClusterIndex_SearchCandidates(t *testing.T) {
	ci := NewClusterIndex(testManager(), embConfig(testDims), nil)

	// Add embeddings
	for i := 0; i < 20; i++ {
		emb := make([]float32, testDims)
		emb[i%testDims] = float32(i)
		ci.Add("node-"+string(rune('A'+i)), emb)
	}

	t.Run("search specific candidates", func(t *testing.T) {
		query := make([]float32, testDims)
		query[0] = 1.0

		// Search only first 5 candidates
		candidates := []int{0, 1, 2, 3, 4}
		results, err := ci.SearchCandidates(nil, query, candidates, 3)
		if err != nil {
			t.Fatalf("SearchCandidates() error = %v", err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}
	})

	t.Run("empty candidates", func(t *testing.T) {
		query := make([]float32, testDims)
		results, err := ci.SearchCandidates(nil, query, []int{}, 3)
		if err != nil {
			t.Fatalf("SearchCandidates() error = %v", err)
		}
		if len(results) != 0 {
			t.Error("expected 0 results for empty candidates")
		}
	})

	t.Run("wrong dimensions", func(t *testing.T) {
		query := make([]float32, testDims+1) // Wrong dimensions
		_, err := ci.SearchCandidates(nil, query, []int{0, 1}, 1)
		if err != ErrInvalidDimensions {
			t.Errorf("expected ErrInvalidDimensions, got %v", err)
		}
	})

	t.Run("topK larger than candidates", func(t *testing.T) {
		query := make([]float32, testDims)
		results, err := ci.SearchCandidates(nil, query, []int{0, 1, 2}, 100)
		if err != nil {
			t.Fatalf("SearchCandidates() error = %v", err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results (capped), got %d", len(results))
		}
	})
}

// Benchmarks

func BenchmarkClusterIndex_Cluster_100(b *testing.B) {
	benchmarkCluster(b, 100, 64, 10)
}

func BenchmarkClusterIndex_Cluster_1000(b *testing.B) {
	benchmarkCluster(b, 1000, 64, 30)
}

func BenchmarkClusterIndex_Cluster_10000(b *testing.B) {
	benchmarkCluster(b, 10000, 64, 100)
}

func benchmarkCluster(b *testing.B, n, dims, k int) {
	// Pre-generate embeddings
	embeddings := make([][]float32, n)
	nodeIDs := make([]string, n)
	for i := 0; i < n; i++ {
		nodeIDs[i] = "node-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		embeddings[i] = make([]float32, dims)
		for d := 0; d < dims; d++ {
			embeddings[i][d] = float32((i*d)%100) / 100.0
		}
	}

	embConfig := &EmbeddingIndexConfig{
		Dimensions: dims,
		InitialCap: n,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create fresh index each iteration to actually measure clustering
		ci := NewClusterIndex(testManager(), embConfig, &KMeansConfig{
			NumClusters:   k,
			MaxIterations: 50,
			InitMethod:    "kmeans++",
		})

		for j := 0; j < n; j++ {
			if err := ci.Add(nodeIDs[j], embeddings[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := ci.Cluster(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterIndex_SearchWithClusters(b *testing.B) {
	dims := 64
	n := 10000

	embConfig := &EmbeddingIndexConfig{
		Dimensions: dims,
		InitialCap: n,
	}
	ci := NewClusterIndex(testManager(), embConfig, &KMeansConfig{
		NumClusters:   100,
		MaxIterations: 50,
	})

	// Add embeddings
	for i := 0; i < n; i++ {
		emb := make([]float32, dims)
		for d := 0; d < dims; d++ {
			emb[d] = float32((i*d)%100) / 100.0
		}
		if err := ci.Add("node-"+string(rune('A'+i%26))+string(rune('0'+i/26)), emb); err != nil {
			b.Fatal(err)
		}
	}
	if err := ci.Cluster(); err != nil {
		b.Fatal(err)
	}

	query := make([]float32, dims)
	for d := 0; d < dims; d++ {
		query[d] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ci.SearchWithClusters(query, 10, 3)
	}
}

func BenchmarkClusterIndex_Search_BruteForce(b *testing.B) {
	dims := 64
	n := 10000

	embConfig := &EmbeddingIndexConfig{
		Dimensions: dims,
		InitialCap: n,
	}
	ci := NewClusterIndex(testManager(), embConfig, nil)

	// Add embeddings
	for i := 0; i < n; i++ {
		emb := make([]float32, dims)
		for d := 0; d < dims; d++ {
			emb[d] = float32((i*d)%100) / 100.0
		}
		if err := ci.Add("node-"+string(rune('A'+i%26))+string(rune('0'+i/26)), emb); err != nil {
			b.Fatal(err)
		}
	}

	query := make([]float32, dims)
	for d := 0; d < dims; d++ {
		query[d] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ci.Search(query, 10)
	}
}

func BenchmarkClusterIndex_OnNodeUpdate(b *testing.B) {
	dims := 64
	n := 5000

	embConfig := &EmbeddingIndexConfig{
		Dimensions: dims,
		InitialCap: n,
	}
	ci := NewClusterIndex(testManager(), embConfig, &KMeansConfig{
		NumClusters:   50,
		MaxIterations: 30,
	})

	// Add embeddings and cluster
	for i := 0; i < n; i++ {
		emb := make([]float32, dims)
		for d := 0; d < dims; d++ {
			emb[d] = float32((i*d)%100) / 100.0
		}
		if err := ci.Add("node-"+string(rune('A'+i%26))+string(rune('0'+i/26)), emb); err != nil {
			b.Fatal(err)
		}
	}
	if err := ci.Cluster(); err != nil {
		b.Fatal(err)
	}

	updateEmb := make([]float32, dims)
	for d := 0; d < dims; d++ {
		updateEmb[d] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ci.OnNodeUpdate("node-A0", updateEmb)
	}
}
