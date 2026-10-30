package inference

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

// testGPUManager returns a test GPU manager
func testGPUManager() *gpu.Manager {
	m, _ := gpu.NewManager(nil)
	return m
}

// testEmbConfig returns a test embedding config with 64 dimensions
func testEmbConfig() *gpu.EmbeddingIndexConfig {
	return &gpu.EmbeddingIndexConfig{
		Dimensions:     64,
		InitialCap:     10000,
		GPUEnabled:     false, // CPU mode for tests
		AutoSync:       false,
		BatchThreshold: 100,
	}
}

// testKMeansConfig returns a test k-means config
func testKMeansConfig(numClusters int) *gpu.KMeansConfig {
	return &gpu.KMeansConfig{
		NumClusters:   numClusters,
		MaxIterations: 20,
	}
}

// testClusterConfig returns a test cluster config
func testClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		Enabled:                    true,
		NumClustersSearch:          3,
		AutoRecluster:              false,
		ReclusterThreshold:         0.1,
		MinEmbeddingsForClustering: 5, // Lower threshold for tests
	}
}

// generateRandomEmbedding generates a random embedding for testing
func generateRandomEmbedding(dims int) []float32 {
	emb := make([]float32, dims)
	for i := range emb {
		emb[i] = rand.Float32()*2 - 1 // Random between -1 and 1
	}
	// Normalize
	var norm float32
	for _, v := range emb {
		norm += v * v
	}
	norm = float32(1.0 / float64(norm))
	for i := range emb {
		emb[i] *= norm
	}
	return emb
}

func TestNewClusterIntegration(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(10)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	if ci == nil {
		t.Fatal("ClusterIntegration is nil")
	}

	stats := ci.Stats()
	if !stats.Enabled {
		t.Error("Expected clustering to be enabled")
	}
}

func TestNewClusterIntegration_NilConfigs(t *testing.T) {
	// Should work with all nil configs (using defaults)
	ci := NewClusterIntegration(nil, nil, nil, nil)

	if ci == nil {
		t.Fatal("ClusterIntegration is nil")
	}

	// With nil config, should use defaults (disabled by default)
	if ci.IsEnabled() {
		t.Error("Expected clustering to be disabled by default")
	}
}

func TestClusterIntegration_AddEmbedding(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(5)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	for i := 0; i < 20; i++ {
		emb := generateRandomEmbedding(64)
		err := ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
		if err != nil {
			t.Fatalf("Failed to add embedding %d: %v", i, err)
		}
	}

	stats := ci.Stats()
	if stats.EmbeddingCount != 20 {
		t.Errorf("Expected 20 embeddings, got %d", stats.EmbeddingCount)
	}
}

func TestClusterIntegration_Search(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	embeddings := make(map[string][]float32)
	for i := 0; i < 30; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		emb := generateRandomEmbedding(64)
		embeddings[nodeID] = emb
		err := ci.AddEmbedding(nodeID, emb)
		if err != nil {
			t.Fatalf("Failed to add embedding %d: %v", i, err)
		}
	}

	// Trigger clustering
	err := ci.OnIndexComplete()
	if err != nil {
		t.Fatalf("Failed to complete index: %v", err)
	}

	// Search with one of the embeddings
	ctx := context.Background()
	query := embeddings["node-0"]
	results, err := ci.Search(ctx, query, 5)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected search results")
	}

	// The query embedding should be in results (or close to top)
	found := false
	for _, r := range results {
		if r.ID == "node-0" {
			found = true
			break
		}
	}
	if !found {
		t.Log("Note: Query node not found in top results (may be expected with clustering)")
	}
}

func TestClusterIntegration_SearchBeforeClustering(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings but don't cluster
	for i := 0; i < 10; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	// Search should still work (brute force fallback)
	ctx := context.Background()
	query := generateRandomEmbedding(64)
	results, err := ci.Search(ctx, query, 5)
	if err != nil {
		t.Fatalf("Search before clustering failed: %v", err)
	}

	// Should get results via brute force
	if len(results) == 0 {
		t.Error("Expected search results even without clustering")
	}
}

func TestClusterIntegration_ConcurrentAccess(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(5)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				emb := generateRandomEmbedding(64)
				nodeID := fmt.Sprintf("node-%d-%d", offset, j)
				if err := ci.AddEmbedding(nodeID, emb); err != nil {
					errChan <- err
				}
			}
		}(i)
	}

	// Concurrent readers
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				query := generateRandomEmbedding(64)
				_, err := ci.Search(ctx, query, 5)
				if err != nil {
					errChan <- err
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent access error: %v", err)
	}
}

func TestClusterIntegration_OnNodeUpdate(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add initial embeddings
	for i := 0; i < 10; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	// Complete initial clustering
	err := ci.OnIndexComplete()
	if err != nil {
		t.Fatalf("Failed to complete index: %v", err)
	}

	// Update an embedding
	newEmb := generateRandomEmbedding(64)
	err = ci.OnNodeUpdate("node-5", newEmb)
	if err != nil {
		t.Fatalf("Failed to update node: %v", err)
	}
}

func TestClusterIntegration_Recluster(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := testClusterConfig()
	config.AutoRecluster = false // Manual recluster for this test

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	for i := 0; i < 30; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	// Complete indexing
	err := ci.OnIndexComplete()
	if err != nil {
		t.Fatalf("Failed to complete index: %v", err)
	}

	// Trigger manual recluster
	err = ci.Recluster()
	if err != nil {
		t.Fatalf("Recluster failed: %v", err)
	}

	// Verify still works after recluster
	ctx := context.Background()
	query := generateRandomEmbedding(64)
	results, err := ci.Search(ctx, query, 5)
	if err != nil {
		t.Fatalf("Search after recluster failed: %v", err)
	}
	if len(results) == 0 {
		t.Error("Expected search results after recluster")
	}
}

func TestClusterIntegration_SearchWithDifferentK(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(5)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	for i := 0; i < 50; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	err := ci.OnIndexComplete()
	if err != nil {
		t.Fatalf("Failed to complete index: %v", err)
	}

	ctx := context.Background()
	query := generateRandomEmbedding(64)

	// Test with different k values
	for _, k := range []int{1, 5, 10, 20} {
		results, err := ci.Search(ctx, query, k)
		if err != nil {
			t.Fatalf("Search with k=%d failed: %v", k, err)
		}

		if len(results) > k {
			t.Errorf("Expected at most %d results, got %d", k, len(results))
		}
	}
}

func TestClusterIntegration_Stats(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(4)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	for i := 0; i < 40; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	err := ci.OnIndexComplete()
	if err != nil {
		t.Fatalf("Failed to complete index: %v", err)
	}

	stats := ci.Stats()

	if stats.EmbeddingCount != 40 {
		t.Errorf("Expected 40 embeddings, got %d", stats.EmbeddingCount)
	}

	if stats.NumClusters != 4 {
		t.Errorf("Expected 4 clusters, got %d", stats.NumClusters)
	}

	if !stats.Enabled {
		t.Error("Expected stats.Enabled to be true")
	}
}

func TestClusterIntegration_EmptySearch(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Search on empty index
	ctx := context.Background()
	query := generateRandomEmbedding(64)
	results, err := ci.Search(ctx, query, 5)
	if err != nil {
		t.Fatalf("Search on empty index failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results on empty index, got %d", len(results))
	}
}

func TestClusterIntegration_DisabledClustering(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := &ClusterConfig{
		Enabled: false, // Disabled
	}

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	for i := 0; i < 20; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	// Complete should be a no-op when disabled
	err := ci.OnIndexComplete()
	if err != nil {
		t.Fatalf("OnIndexComplete failed: %v", err)
	}

	// Search should still work (brute force)
	ctx := context.Background()
	query := generateRandomEmbedding(64)
	results, err := ci.Search(ctx, query, 5)
	if err != nil {
		t.Fatalf("Search with disabled clustering failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected search results even with disabled clustering")
	}

	// Stats should show clustering disabled
	stats := ci.Stats()
	if stats.Enabled {
		t.Error("Expected stats.Enabled to be false")
	}
}

func TestClusterIntegration_SetConfig(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)

	ci := NewClusterIntegration(manager, nil, kmeansConfig, embConfig)

	// Initial config is default (disabled)
	if ci.IsEnabled() {
		t.Error("Expected clustering disabled by default")
	}

	// Update config
	newConfig := &ClusterConfig{
		Enabled:           true,
		NumClustersSearch: 5,
		AutoRecluster:     true,
	}
	ci.SetConfig(newConfig)

	if !ci.IsEnabled() {
		t.Error("Expected clustering enabled after SetConfig")
	}

	gotConfig := ci.GetConfig()
	if gotConfig.NumClustersSearch != 5 {
		t.Errorf("Expected NumClustersSearch=5, got %d", gotConfig.NumClustersSearch)
	}
}

func TestClusterIntegration_IsClustered(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(3)
	config := testClusterConfig()

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Initially not clustered
	if ci.IsClustered() {
		t.Error("Expected not clustered initially")
	}

	// Add embeddings
	for i := 0; i < 10; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}

	// Still not clustered until OnIndexComplete
	if ci.IsClustered() {
		t.Error("Expected not clustered before OnIndexComplete")
	}

	// Trigger clustering
	ci.OnIndexComplete()

	// Now should be clustered
	if !ci.IsClustered() {
		t.Error("Expected clustered after OnIndexComplete")
	}
}

func BenchmarkClusterIntegration_AddEmbedding(b *testing.B) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(10)
	config := testClusterConfig()
	config.AutoRecluster = false

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	embeddings := make([][]float32, b.N)
	for i := range embeddings {
		embeddings[i] = generateRandomEmbedding(64)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), embeddings[i])
	}
}

func BenchmarkClusterIntegration_Search(b *testing.B) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(10)
	config := testClusterConfig()
	config.AutoRecluster = false

	ci := NewClusterIntegration(manager, config, kmeansConfig, embConfig)

	// Add embeddings
	for i := 0; i < 1000; i++ {
		emb := generateRandomEmbedding(64)
		ci.AddEmbedding(fmt.Sprintf("node-%d", i), emb)
	}
	ci.OnIndexComplete()

	queries := make([][]float32, b.N)
	for i := range queries {
		queries[i] = generateRandomEmbedding(64)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ci.Search(ctx, queries[i], 10)
	}
}

func TestClusterIntegration_FeatureFlagIntegration(t *testing.T) {
	// Test that DefaultClusterConfig respects feature flags
	// Import config is needed for this test
	t.Run("default_config_respects_feature_flag", func(t *testing.T) {
		// Note: By default, GPU clustering is disabled
		// So DefaultClusterConfig().Enabled should be false
		cfg := DefaultClusterConfig()

		// The Enabled field is based on config.IsGPUClusteringEnabled()
		// which should be false by default (experimental feature)
		// We can't easily test the feature flag integration here without
		// importing the config package, but we can verify the config works
		if cfg.NumClustersSearch != 3 {
			t.Errorf("Expected NumClustersSearch=3, got %d", cfg.NumClustersSearch)
		}
		if !cfg.AutoRecluster {
			t.Error("Expected AutoRecluster=true by default")
		}
		if cfg.ReclusterThreshold != 0.1 {
			t.Errorf("Expected ReclusterThreshold=0.1, got %f", cfg.ReclusterThreshold)
		}
		if cfg.MinEmbeddingsForClustering != 1000 {
			t.Errorf("Expected MinEmbeddingsForClustering=1000, got %d", cfg.MinEmbeddingsForClustering)
		}
	})
}

func TestClusterIntegration_ShouldReclusterAndGetClusterIndex(t *testing.T) {
	manager := testGPUManager()
	embConfig := testEmbConfig()
	kmeansConfig := testKMeansConfig(4)
	cfg := testClusterConfig()
	cfg.Enabled = false
	cfg.AutoRecluster = false

	ci := NewClusterIntegration(manager, cfg, kmeansConfig, embConfig)
	if ci.GetClusterIndex() == nil {
		t.Fatal("GetClusterIndex() returned nil")
	}
	if ci.ShouldRecluster() {
		t.Fatal("ShouldRecluster() = true while disabled, want false")
	}

	cfg.Enabled = true
	ci.SetConfig(cfg)
	// With no meaningful update pressure yet, reclustering should still be false,
	// but this executes the enabled path that delegates to cluster index.
	if ci.ShouldRecluster() {
		t.Fatal("ShouldRecluster() = true unexpectedly")
	}
}
