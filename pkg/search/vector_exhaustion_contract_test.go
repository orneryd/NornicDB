package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

// Exhaustion is each candidate generator's own evidence that it evaluated its
// whole population; the pipeline never infers it from a short page.

// exhaustionFixtureVectors is two tight groups of 30 vectors, along x and
// along y; a query along x scores the x group near 1 and the y group near 0.
func exhaustionFixtureVectors() map[string][]float32 {
	vectors := make(map[string][]float32, 60)
	for i := 0; i < 30; i++ {
		vectors[fmt.Sprintf("x-%02d", i)] = []float32{1, float32(i) / 1000}
		vectors[fmt.Sprintf("y-%02d", i)] = []float32{float32(i) / 1000, 1}
	}
	return vectors
}

var exhaustionQuery = []float32{1, 0}

type exhaustionGenerator interface {
	searchCandidatesWithExhaustion(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, bool, error)
}

func requireExhaustion(t *testing.T, gen exhaustionGenerator, k int, minSimilarity float64, wantCount int, wantExhausted bool) {
	t.Helper()
	candidates, exhausted, err := gen.searchCandidatesWithExhaustion(context.Background(), exhaustionQuery, k, minSimilarity)
	require.NoError(t, err)
	if wantCount >= 0 {
		require.Len(t, candidates, wantCount)
	}
	require.Equal(t, wantExhausted, exhausted, "k=%d minSimilarity=%v returned %d", k, minSimilarity, len(candidates))
}

func TestExactCandidateGeneratorsReportTheirOwnExhaustion(t *testing.T) {
	vectors := exhaustionFixtureVectors()
	index := NewVectorIndex(2)
	store, err := NewVectorFileStore(t.TempDir()+"/vectors", 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	embCfg := gpu.DefaultEmbeddingIndexConfig(2)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	embeddings := gpu.NewEmbeddingIndex(nil, embCfg)
	for id, vec := range vectors {
		require.NoError(t, index.Add(id, vec))
		require.NoError(t, store.Add(id, vec))
		require.NoError(t, embeddings.Add(id, vec))
	}
	for name, gen := range map[string]exhaustionGenerator{
		"brute force":            NewBruteForceCandidateGen(index),
		"file store brute force": NewFileStoreBruteForceCandidateGen(store),
		"GPU brute force (CPU)":  NewGPUBruteForceCandidateGen(embeddings),
	} {
		t.Run(name, func(t *testing.T) {
			requireExhaustion(t, gen, 5, -1, 5, false)     // the limit cut the rest
			requireExhaustion(t, gen, 1000, -1, 60, true)  // everything returned
			requireExhaustion(t, gen, 1000, 0.5, 30, true) // a threshold-cut short page is complete
			require.Equal(t, 60, gen.(candidatePopulationReporter).candidatePopulation(context.Background()))
		})
	}
}

// twoClusterIndex clusters the fixture into its x and y groups.
func twoClusterIndex(t *testing.T) *gpu.ClusterIndex {
	t.Helper()
	embCfg := gpu.DefaultEmbeddingIndexConfig(2)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	clusterIndex := gpu.NewClusterIndex(nil, embCfg, &gpu.KMeansConfig{NumClusters: 2, MaxIterations: 10, Tolerance: 0.001, InitMethod: "kmeans++"})
	for id, vec := range exhaustionFixtureVectors() {
		require.NoError(t, clusterIndex.Add(id, vec))
	}
	require.NoError(t, clusterIndex.Cluster())
	require.Len(t, clusterIndex.PublishedClustering().PopulatedClusters(), 2)
	return clusterIndex
}

func TestClusteredCandidateGeneratorsAreExhaustedOnlyAfterEveryCluster(t *testing.T) {
	clusterIndex := twoClusterIndex(t)
	vectorIndex := NewVectorIndex(2)
	for id, vec := range exhaustionFixtureVectors() {
		require.NoError(t, vectorIndex.Add(id, vec))
	}
	routing := clusterIndex.PublishedClustering()
	graphs := map[int]*HNSWIndex{}
	for _, cluster := range routing.PopulatedClusters() {
		graphs[cluster] = NewHNSWIndex(2, DefaultHNSWConfig())
		for _, id := range routing.MemberIDs(cluster) {
			require.NoError(t, graphs[cluster].Add(id, exhaustionFixtureVectors()[id]))
		}
	}
	for name, gen := range map[string]exhaustionGenerator{
		"k-means":     NewKMeansCandidateGen(clusterIndex, vectorIndex, 1),
		"GPU k-means": NewGPUKMeansCandidateGen(clusterIndex, 1),
		"IVF-HNSW":    NewIVFHNSWCandidateGen(clusterIndex, func(c int) *HNSWIndex { return graphs[c] }, 1),
	} {
		t.Run(name, func(t *testing.T) {
			// One probed cluster: whatever it returns, the other cluster
			// wasn't searched, so the page is no evidence of the end.
			requireExhaustion(t, gen, 5, 0.5, -1, false)
			requireExhaustion(t, gen, 40, 0.5, -1, false)
			// A request as wide as the population searches every cluster
			// completely.
			requireExhaustion(t, gen, 60, -1, 60, true)
			requireExhaustion(t, gen, 1000, 0.5, 30, true)
			require.Equal(t, 60, gen.(candidatePopulationReporter).candidatePopulation(context.Background()))
		})
	}
}

func TestIVFPQCandidateGeneratorIsExhaustedOnlyAfterEveryList(t *testing.T) {
	store, err := NewVectorFileStore(t.TempDir()+"/vectors", 8)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	for i := 0; i < 300; i++ {
		vec := make([]float32, 8)
		vec[i%4] = 1
		vec[4+i%4] = float32(i) / 1000
		require.NoError(t, store.Add(fmt.Sprintf("d-%03d", i), vec))
	}
	index, _, err := BuildIVFPQFromVectorStore(context.Background(), store, IVFPQProfile{
		Dimensions: 8, IVFLists: 4, PQSegments: 4, PQBits: 4, NProbe: 1, RerankTopK: 50,
		TrainingSampleMax: 300, KMeansMaxIterations: 6,
	}, nil)
	require.NoError(t, err)
	gen := NewIVFPQCandidateGen(index, 1)
	population := gen.candidatePopulation(context.Background())
	require.Equal(t, 300, population)
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	narrow, exhausted, err := gen.searchCandidatesWithExhaustion(context.Background(), query, 20, -1)
	require.NoError(t, err)
	require.NotEmpty(t, narrow)
	require.False(t, exhausted, "one probed list of four")
	all, exhausted, err := gen.searchCandidatesWithExhaustion(context.Background(), query, population, -1)
	require.NoError(t, err)
	require.Len(t, all, population)
	require.True(t, exhausted, "every list probed and returned whole")
}

// shortPageGenerator returns one candidate whatever the request and reports
// nothing about exhaustion.
type shortPageGenerator struct{}

func (shortPageGenerator) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	return []Candidate{{ID: "only", Score: 1}}, nil
}

// reportingGenerator returns its candidates and claims exhaustion.
type reportingGenerator struct{ candidates []Candidate }

func (g reportingGenerator) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	return g.candidates, nil
}

func (g reportingGenerator) searchCandidatesWithExhaustion(context.Context, []float32, int, float64) ([]Candidate, bool, error) {
	return g.candidates, true, nil
}

func TestVectorPipelineNeverInfersExhaustion(t *testing.T) {
	t.Run("a generator that doesn't report is not exhausted by a short page", func(t *testing.T) {
		pipeline := NewVectorSearchPipeline(shortPageGenerator{}, &IdentityExactScorer{})
		results, exhausted, err := pipeline.searchWithExhaustion(context.Background(), exhaustionQuery, 10, -1)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.False(t, exhausted)
	})
	t.Run("a reported exhaustion doesn't survive the cut to k", func(t *testing.T) {
		pipeline := NewVectorSearchPipeline(reportingGenerator{candidates: []Candidate{{ID: "a", Score: 0.9}, {ID: "b", Score: 0.8}, {ID: "c", Score: 0.7}}}, &IdentityExactScorer{})
		results, exhausted, err := pipeline.searchWithExhaustion(context.Background(), exhaustionQuery, 2, -1)
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.False(t, exhausted, "c was cut")
		results, exhausted, err = pipeline.searchWithExhaustion(context.Background(), exhaustionQuery, 2, 0.85)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.True(t, exhausted, "nothing above the threshold was cut")
	})
}
