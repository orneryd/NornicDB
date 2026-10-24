package search

import (
	"context"
	"fmt"

	"math"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The exact stage alone decides a vector result's node, score, threshold and
// cut: generators only select which nodes to score, whatever their scores,
// routing or compression.

const exactStageDims = 4

// exactStageChunkA and exactStageChunkB are the two chunks of node "multi".
// Every compressed code puts them close or tied for the queries below, while
// exactly chunk B is clearly the better one, so a node collapsed to its first
// approximate hit carries chunk A's score.
var (
	exactStageChunkA = []float32{0.6, 0.8, 0, 0}
	exactStageChunkB = []float32{0.9, 0.4358899, 0, 0}
	exactStageSingle = []float32{0.8, 0.6, 0, 0}
)

func exactStageCosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	return dot / math.Sqrt(na*nb)
}

// exactStageService indexes node "multi" (chunks A and B), node "single" and
// 300 fillers pointing away from every query, then serves vector search with
// the named generator. Generators the Service picks itself (HNSW, IVF-HNSW,
// CPU k-means, CPU brute force) are its own pipelines; the others are built
// over the same vectors and installed, as their strategies would be.
func exactStageService(t *testing.T, generator, quantization string) *Service {
	t.Helper()
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", quantization)
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", fmt.Sprint(generator == "ivf_hnsw"))
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "1")
	t.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	if generator == "brute" {
		t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
	}
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	svc := NewServiceWithDimensions(engine, exactStageDims)
	t.Cleanup(func() { closeAfterBuilds(t, svc) })
	if generator == "kmeans" || generator == "ivf_hnsw" {
		svc.EnableClustering(nil, 4)
		svc.SetMinEmbeddingsForClustering(1)
	}
	nodes := []*storage.Node{
		{ID: "multi", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{exactStageChunkA, exactStageChunkB}},
		{ID: "single", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{exactStageSingle}},
	}
	for i := 0; i < 300; i++ {
		angle := (100 + 160*float64(i)/299) * math.Pi / 180
		nodes = append(nodes, &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("filler-%03d", i)),
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{float32(math.Cos(angle)), float32(math.Sin(angle)), 0, 0}},
		})
	}
	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	ctx := context.Background()

	// Every indexed vector, by its vector ID.
	vectors := map[string][]float32{}
	svc.indexMu.RLock()
	svc.mu.RLock()
	for _, ids := range svc.nodeChunkVectors {
		for _, id := range ids {
			vec, ok := svc.vectorIndex.GetVector(id)
			require.True(t, ok, id)
			vectors[id] = vec
		}
	}
	svc.mu.RUnlock()
	svc.indexMu.RUnlock()
	require.Len(t, vectors, 303)

	var pipeline *VectorSearchPipeline
	switch generator {
	case "hnsw", "brute":
		if generator == "hnsw" {
			requireHNSWReady(t, svc)
		}
		p, err := svc.getOrCreateVectorPipeline(ctx)
		require.NoError(t, err)
		if generator == "hnsw" {
			h, ok := p.candidateGen.(*HNSWCandidateGen)
			require.True(t, ok, "%T", p.candidateGen)
			require.Equal(t, quantization != "none", h.quantizationActive(ctx), "the cell must search %s codes", quantization)
		} else {
			require.IsType(t, &BruteForceCandidateGen{}, p.candidateGen)
		}
		return svc
	case "kmeans", "ivf_hnsw":
		require.NoError(t, svc.TriggerClustering(ctx))
		p, err := svc.getOrCreateVectorPipeline(ctx)
		require.NoError(t, err)
		if generator == "kmeans" {
			require.IsType(t, &KMeansCandidateGen{}, p.candidateGen)
		} else {
			require.IsType(t, &IVFHNSWCandidateGen{}, p.candidateGen)
		}
		return svc
	case "gpu_kmeans":
		cfg := gpu.DefaultEmbeddingIndexConfig(exactStageDims)
		cfg.GPUEnabled, cfg.AutoSync = false, false
		clusters := gpu.NewClusterIndex(nil, cfg, &gpu.KMeansConfig{NumClusters: 4, MaxIterations: 10, Tolerance: 0.001, InitMethod: "kmeans++"})
		for id, vec := range vectors {
			require.NoError(t, clusters.Add(id, vec))
		}
		require.NoError(t, clusters.Cluster())
		pipeline = NewVectorSearchPipeline(NewGPUKMeansCandidateGen(clusters, 1), svc.resolveVectorExactScorer(svc.vectorIndex, nil))
	case "gpu_brute":
		cfg := gpu.DefaultEmbeddingIndexConfig(exactStageDims)
		cfg.GPUEnabled, cfg.AutoSync = false, false
		embeddings := gpu.NewEmbeddingIndex(nil, cfg)
		for id, vec := range vectors {
			require.NoError(t, embeddings.Add(id, vec))
		}
		pipeline = NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(embeddings), svc.resolveVectorExactScorer(svc.vectorIndex, nil))
	case "filestore", "ivfpq":
		store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), exactStageDims)
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close() })
		for id, vec := range vectors {
			require.NoError(t, store.Add(id, vec))
		}
		if generator == "filestore" {
			pipeline = NewVectorSearchPipeline(NewFileStoreBruteForceCandidateGen(store), NewCPUExactScorer(store))
			break
		}
		index, _, err := BuildIVFPQFromVectorStore(ctx, store, IVFPQProfile{
			Dimensions: exactStageDims, IVFLists: 4, PQSegments: 2, PQBits: 4, NProbe: 1, RerankTopK: 40,
			TrainingSampleMax: len(vectors), KMeansMaxIterations: 10,
		}, nil)
		require.NoError(t, err)
		pipeline = NewVectorSearchPipeline(NewIVFPQCandidateGen(index, 1), svc.resolveVectorExactScorer(svc.vectorIndex, nil))
	default:
		t.Fatalf("unknown generator %s", generator)
	}
	svc.pipelineMu.Lock()
	svc.vectorPipeline = pipeline
	svc.pipelineMu.Unlock()
	return svc
}

type exactStageCell struct {
	generator    string
	quantization string
}

func exactStageCells() []exactStageCell {
	var cells []exactStageCell
	for _, generator := range []string{"hnsw", "ivf_hnsw"} {
		for _, mode := range []string{"none", "scalar", "binary", "pq"} {
			cells = append(cells, exactStageCell{generator, mode})
		}
	}
	for _, generator := range []string{"kmeans", "gpu_kmeans", "ivfpq", "brute", "filestore", "gpu_brute"} {
		cells = append(cells, exactStageCell{generator, "none"})
	}
	return cells
}

// TestExactStageDecidesEveryGeneratorsResults runs each generator under each
// quantization mode its graphs support: a multi-chunk node must come back with
// its best chunk's exact score, a single-vector node beside it, through
// vector search, hybrid search and ranked continuation, with and without a
// threshold; exhaustion only when the whole eligible population was returned.
func TestExactStageDecidesEveryGeneratorsResults(t *testing.T) {
	queries := [][]float32{{1, 0.1, 0, 0}, {1, 0, 0, 0}}
	for _, cell := range exactStageCells() {
		t.Run(cell.generator+"/"+cell.quantization, func(t *testing.T) {
			svc := exactStageService(t, cell.generator, cell.quantization)
			for _, query := range queries {
				best := exactStageCosine(query, exactStageChunkB)
				single := exactStageCosine(query, exactStageSingle)
				require.Greater(t, best, exactStageCosine(query, exactStageChunkA))
				for _, text := range []string{"", "unmatchedword"} {
					name := fmt.Sprintf("query=%v/text=%q", query, text)
					fallback := false

					// Threshold between chunk A and both qualifying vectors:
					// exactly two nodes qualify, and that is the whole page.
					selective := DefaultSearchOptions()
					selective.Limit = 7
					threshold := 0.75
					selective.MinSimilarity = &threshold
					selective.FallbackEnabled = &fallback
					response, err := svc.Search(context.Background(), text, query, selective)
					require.NoError(t, err, name)
					require.Equal(t, []string{"multi", "single"}, exactStageIDs(response.Results), name)
					require.True(t, response.RetrievalExhausted, "%s: the whole eligible population was returned", name)
					if text == "" {
						require.InDelta(t, best, response.Results[0].Similarity, 1e-5, "%s: multi carries chunk B's exact score", name)
						require.InDelta(t, single, response.Results[1].Similarity, 1e-5, name)
					}
					embed := func(context.Context, string) ([]float32, error) { return query, nil }
					page, err := svc.SearchTextContinuation(context.Background(), text, selective, SearchContinuationRequest{Owner: "exact-stage", N: 7}, nil, embed, svc.Search, ChunkedSearchErrorPolicy{})
					require.NoError(t, err, name)
					require.Equal(t, []string{"multi", "single"}, exactStageIDs(page.Results), name)
					require.Equal(t, SearchContinuationCollectionComplete, page.Completion, name)

					// No threshold: a page cut at the limit, so not exhausted.
					// Which nodes an approximate pass reaches is its recall;
					// every score on the page is its node's exact best.
					open := DefaultSearchOptions()
					open.Limit = 7
					none := -1.0
					open.MinSimilarity = &none
					open.FallbackEnabled = &fallback
					response, err = svc.Search(context.Background(), text, query, open)
					require.NoError(t, err, name)
					require.Len(t, response.Results, 7, name)
					require.False(t, response.RetrievalExhausted, "%s: the limit cut the fillers", name)
					if text == "" {
						exact := exactStageBestScores(t, svc, query)
						for i, result := range response.Results {
							require.InDelta(t, exact[result.ID], result.Similarity, 1e-5, "%s: %s", name, result.ID)
							if i > 0 {
								require.LessOrEqual(t, result.Similarity, response.Results[i-1].Similarity, name)
							}
						}
					}
				}
			}
		})
	}
}

// exactStageBestScores is each node's best cosine over all its vectors.
func exactStageBestScores(t *testing.T, svc *Service, query []float32) map[string]float64 {
	t.Helper()
	best := map[string]float64{}
	svc.indexMu.RLock()
	defer svc.indexMu.RUnlock()
	svc.mu.RLock()
	defer svc.mu.RUnlock()
	for nodeID, ids := range svc.nodeChunkVectors {
		best[nodeID] = math.Inf(-1)
		for _, id := range ids {
			vec, ok := svc.vectorIndex.GetVector(id)
			require.True(t, ok, id)
			best[nodeID] = math.Max(best[nodeID], exactStageCosine(query, vec))
		}
	}
	return best
}

func exactStageIDs(results []SearchResult) []string {
	ids := make([]string, len(results))
	for i, result := range results {
		ids[i] = result.ID
	}
	return ids
}

// TestMultiChunkNodeKeepsItsBestChunkUnderCompression is review r4's F4 case
// (i20261018202022-70216-6834): the two chunks tie under binary codes and the
// worse one comes first; collapsing before exact scoring lost the node while
// still claiming the population was exhausted.
func TestMultiChunkNodeKeepsItsBestChunkUnderCompression(t *testing.T) {
	for _, mode := range []string{"none", "scalar", "binary"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(EnvSearchResultCacheEnabled, "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", mode)
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { closeAfterBuilds(t, svc) })
			node := &storage.Node{ID: "multi", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.6, 0.8}, {0.9, 0.4358899}}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))
			requireHNSWReady(t, svc)
			for _, query := range [][]float32{{1, 0}, {1, 0.1}} {
				opts := DefaultSearchOptions()
				opts.Limit = 7
				threshold := 0.8
				opts.MinSimilarity = &threshold
				fallback := false
				opts.FallbackEnabled = &fallback
				want := exactStageCosine(query, []float32{0.9, 0.4358899})
				for _, text := range []string{"", "unmatchedword"} {
					response, err := svc.Search(context.Background(), text, query, opts)
					require.NoError(t, err)
					require.Equal(t, []string{"multi"}, exactStageIDs(response.Results), "query %v text %q", query, text)
					require.True(t, response.RetrievalExhausted)
					if text == "" {
						require.InDelta(t, want, response.Results[0].Similarity, 1e-5)
					}
					embed := func(context.Context, string) ([]float32, error) { return query, nil }
					page, err := svc.SearchTextContinuation(context.Background(), text, opts, SearchContinuationRequest{Owner: "f4", N: 7}, nil, embed, svc.Search, ChunkedSearchErrorPolicy{})
					require.NoError(t, err)
					require.Equal(t, []string{"multi"}, exactStageIDs(page.Results))
					require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
				}
			}
		})
	}
}

// splitNodeFixture is the exhaustion fixture's x and y groups clustered in
// two, plus node "split": its weak chunk lies in the y cluster, its strong
// chunk in the x cluster. Routing a query along x to the y cluster only
// surfaces the weak chunk.
func splitNodeFixture(t *testing.T) (*gpu.ClusterIndex, *VectorIndex, int, map[int]*HNSWIndex) {
	t.Helper()
	vectors := exhaustionFixtureVectors()
	vectors["split"] = []float32{0.2, 1}
	vectors[nodeChunkVectorKey("split", 1)] = []float32{1, 0.01}
	cfg := gpu.DefaultEmbeddingIndexConfig(2)
	cfg.GPUEnabled, cfg.AutoSync = false, false
	clusters := gpu.NewClusterIndex(nil, cfg, &gpu.KMeansConfig{NumClusters: 2, MaxIterations: 10, Tolerance: 0.001, InitMethod: "kmeans++"})
	index := NewVectorIndex(2)
	for id, vec := range vectors {
		require.NoError(t, clusters.Add(id, vec))
		require.NoError(t, index.Add(id, vec))
	}
	require.NoError(t, clusters.Cluster())
	routing := clusters.PublishedClustering()
	weakCluster, strongCluster := -1, -1
	graphs := map[int]*HNSWIndex{}
	for _, cluster := range routing.PopulatedClusters() {
		graphs[cluster] = NewHNSWIndex(2, DefaultHNSWConfig())
		for _, id := range routing.MemberIDs(cluster) {
			require.NoError(t, graphs[cluster].Add(id, vectors[id]))
			switch id {
			case "split":
				weakCluster = cluster
			case nodeChunkVectorKey("split", 1):
				strongCluster = cluster
			}
		}
	}
	require.NotEqual(t, -1, weakCluster)
	require.NotEqual(t, weakCluster, strongCluster, "the chunks must lie in different clusters")
	return clusters, index, weakCluster, graphs
}

// TestRestrictedPassScoresANodesUnroutedChunks: a node's score is its best
// vector's even when routing surfaced only a weaker one.
func TestRestrictedPassScoresANodesUnroutedChunks(t *testing.T) {
	clusters, index, weakCluster, graphs := splitNodeFixture(t)
	members := func(nodeIDs []string) map[string][]string {
		out := map[string][]string{}
		for _, nodeID := range nodeIDs {
			if nodeID == "split" {
				out[nodeID] = []string{"split", nodeChunkVectorKey("split", 1)}
			} else {
				out[nodeID] = []string{nodeID}
			}
		}
		return out
	}
	toWeak := func(context.Context, []float32, int) []int { return []int{weakCluster} }
	for name, generator := range map[string]CandidateGenerator{
		"k-means":     NewKMeansCandidateGen(clusters, index, 1).SetClusterSelector(toWeak),
		"GPU k-means": NewGPUKMeansCandidateGen(clusters, 1).SetClusterSelector(toWeak),
		"IVF-HNSW":    NewIVFHNSWCandidateGen(clusters, func(c int) *HNSWIndex { return graphs[c] }, 1).SetClusterSelector(toWeak),
	} {
		t.Run(name, func(t *testing.T) {
			pipeline := NewVectorSearchPipeline(generator, NewCPUExactScorer(index))
			ctx := withVectorMembers(context.Background(), members)
			strong := exactStageCosine(exhaustionQuery, []float32{1, 0.01})
			results, evidence, err := pipeline.searchWithEvidence(ctx, exhaustionQuery, 5, -1, nil)
			require.NoError(t, err)
			require.False(t, evidence.exhausted, "one cluster of two")
			require.NotEmpty(t, results)
			require.Equal(t, nodeChunkVectorKey("split", 1), results[0].ID, "%+v", results)
			require.InDelta(t, strong, results[0].Score, 1e-5)

			// Under a threshold the weak chunk is provably below it, so the
			// pass may leave the node out; it never scores it by that chunk,
			// and the pass isn't exhausted.
			results, evidence, err = pipeline.searchWithEvidence(ctx, exhaustionQuery, 5, 0.5, nil)
			require.NoError(t, err)
			require.False(t, evidence.exhausted)
			for _, result := range results {
				if mustVectorEntryOwner(t, result.ID) == "split" {
					require.InDelta(t, strong, result.Score, 1e-5)
				}
			}
		})
	}
}

// thresholdRecordingGenerator returns one vector and records the threshold it
// was asked for.
type thresholdRecordingGenerator struct{ thresholds []float64 }

func (g *thresholdRecordingGenerator) SearchCandidates(_ context.Context, _ []float32, _ int, minSimilarity float64) ([]Candidate, error) {
	g.thresholds = append(g.thresholds, minSimilarity)
	return []Candidate{{ID: "a", Score: 1}}, nil
}

// TestGeneratorThresholdIsAPrefilterTheStageDecides: generators get the
// threshold widened by the float32 bound, and only the exact score decides.
func TestGeneratorThresholdIsAPrefilterTheStageDecides(t *testing.T) {
	require.Equal(t, -1.0, candidatePrefilter(-1, 768))
	require.Equal(t, 0.8-float64(768+4)*math.Ldexp(1, -22), candidatePrefilter(0.8, 768))

	index := NewVectorIndex(2)
	require.NoError(t, index.Add("a", []float32{0.6, 0.8}))
	exact := exactStageCosine([]float32{1, 0}, []float32{0.6, 0.8})
	generator := &thresholdRecordingGenerator{}
	pipeline := NewVectorSearchPipeline(generator, NewCPUExactScorer(index))
	kept, _, err := pipeline.searchWithEvidence(context.Background(), []float32{1, 0}, 5, exact-1e-6, nil)
	require.NoError(t, err)
	require.Len(t, kept, 1)
	dropped, _, err := pipeline.searchWithEvidence(context.Background(), []float32{1, 0}, 5, exact+1e-6, nil)
	require.NoError(t, err)
	require.Empty(t, dropped, "the generator's score of 1 doesn't qualify the vector")
	require.Equal(t, []float64{candidatePrefilter(exact-1e-6, 2), candidatePrefilter(exact+1e-6, 2)}, generator.thresholds)
}

// passRecordingGenerator wraps a clustered generator and records each pass.
type passRecordingGenerator struct {
	inner interface {
		CandidateGenerator
		candidateGeneratorWithRouting
		candidatePopulationReporter
	}
	limits   []int
	evidence []candidateEvidence
}

func (g *passRecordingGenerator) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := g.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	return candidates, err
}

func (g *passRecordingGenerator) searchCandidatesWithEvidence(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, candidateEvidence, error) {
	candidates, evidence, err := g.inner.searchCandidatesWithEvidence(ctx, query, k, minSimilarity)
	g.limits = append(g.limits, k)
	g.evidence = append(g.evidence, evidence)
	return candidates, evidence, err
}

func (g *passRecordingGenerator) candidatePopulation(ctx context.Context) int {
	return g.inner.candidatePopulation(ctx)
}

// TestSelectiveClusteredSearchJumpsToTheAllClusterPass: once a pass is bound
// by its routing, a wider limit on the same clusters repeats it; the next
// request is the population-wide one, the only one that routes everywhere.
func TestSelectiveClusteredSearchJumpsToTheAllClusterPass(t *testing.T) {
	for _, generator := range []string{"kmeans", "gpu_kmeans", "ivf_hnsw", "ivfpq"} {
		t.Run(generator, func(t *testing.T) {
			svc := exactStageService(t, generator, "none")
			pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
			require.NoError(t, err)
			inner, ok := pipeline.candidateGen.(interface {
				CandidateGenerator
				candidateGeneratorWithRouting
				candidatePopulationReporter
			})
			require.True(t, ok, "%T", pipeline.candidateGen)
			recording := &passRecordingGenerator{inner: inner}
			svc.pipelineMu.Lock()
			svc.vectorPipeline = NewVectorSearchPipeline(recording, pipeline.exactScorer)
			svc.pipelineMu.Unlock()

			opts := DefaultSearchOptions()
			opts.Limit = 20
			threshold := 0.75
			opts.MinSimilarity = &threshold
			opts.MaxOverfetchRatio = math.MaxFloat64
			fallback := false
			opts.FallbackEnabled = &fallback
			response, err := svc.Search(context.Background(), "", []float32{1, 0.1, 0, 0}, opts)
			require.NoError(t, err)
			require.Equal(t, []string{"multi", "single"}, exactStageIDs(response.Results))
			require.True(t, response.RetrievalExhausted)

			population := inner.candidatePopulation(context.Background())
			t.Logf("passes %v, population %d", recording.limits, population)
			require.GreaterOrEqual(t, len(recording.limits), 2, "a restricted pass, then the all-cluster one: %v", recording.limits)
			// The last pass routes every cluster; a routing-bound restricted
			// pass is followed directly by it. (Ordinary widening may also
			// reach the population first, when no restricted pass searched
			// its clusters whole.)
			last := len(recording.limits) - 1
			require.GreaterOrEqual(t, recording.limits[last], population, "passes %v", recording.limits)
			require.True(t, recording.evidence[last].exhausted)
			for i := 0; i < last; i++ {
				require.Less(t, recording.limits[i], population, "passes %v", recording.limits)
				if recording.evidence[i].routedComplete {
					require.Equal(t, last, i+1, "pass %d was routing-bound, yet another restricted pass followed: %v", i, recording.limits)
					require.Equal(t, population, recording.limits[last], "passes %v", recording.limits)
				}
			}
		})
	}
}

func randomUnitVectors(rng *rand.Rand, n, dims int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, dims)
		for d := range v {
			v[d] = float32(rng.NormFloat64())
		}
		out[i] = vector.Normalize(v)
	}
	return out
}

// TestCompressedScoresStayWithinTheirBound: for every stored vector and
// query, a scalar or PQ graph's exact cosine is at most its approximate score
// plus the codes' bound (and float32 slack), also after a save and load; a
// binary graph has no bound.
func TestCompressedScoresStayWithinTheirBound(t *testing.T) {
	const n, dims = 400, 16
	rng := rand.New(rand.NewSource(7))
	vectors := randomUnitVectors(rng, n, dims)
	queries := randomUnitVectors(rng, 25, dims)
	for _, mode := range []HNSWQuantization{HNSWQuantizationScalar, HNSWQuantizationProduct, HNSWQuantizationBinary} {
		t.Run(string(mode), func(t *testing.T) {
			index := NewHNSWIndex(dims, DefaultHNSWConfig())
			byID := map[string][]float32{}
			for i, v := range vectors {
				id := fmt.Sprintf("v-%03d", i)
				byID[id] = v
				require.NoError(t, index.Add(id, v))
			}
			require.NoError(t, index.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: mode}))
			path := filepath.Join(t.TempDir(), "hnsw")
			require.NoError(t, index.Save(path))
			loaded, err := LoadHNSWIndex(path, func(id string) ([]float32, bool) { v, ok := byID[id]; return v, ok })
			require.NoError(t, err)
			for name, graph := range map[string]*HNSWIndex{"built": index, "loaded": loaded} {
				graph.mu.RLock()
				require.NotNil(t, graph.quantized, name)
				for _, q := range queries {
					scorer := graph.quantized.query(q)
					bound := scorer.scoreBound()
					if mode == HNSWQuantizationBinary {
						require.True(t, math.IsInf(bound, 1), "binary codes have no bound")
						scorer.release()
						continue
					}
					require.False(t, math.IsInf(bound, 1), "%s %s", name, mode)
					for id, internal := range graph.idToInternal {
						exact := float64(vector.DotProduct(q, byID[id]))
						approx := float64(scorer.dot(internal))
						require.LessOrEqual(t, exact, approx+bound+exactScoreSlack(dims), "%s %s %s", name, mode, id)
					}
					scorer.release()
				}
				graph.mu.RUnlock()
			}
		})
	}
}

// TestIVFPQScoresStayWithinTheirBound: an exact cosine is at most a list
// member's PQ score plus the index's residual bound.
func TestIVFPQScoresStayWithinTheirBound(t *testing.T) {
	const n, dims = 600, 16
	rng := rand.New(rand.NewSource(11))
	store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), dims)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	byID := map[string][]float32{}
	for i, v := range randomUnitVectors(rng, n, dims) {
		id := fmt.Sprintf("v-%03d", i)
		byID[id] = v
		require.NoError(t, store.Add(id, v))
	}
	index, _, err := BuildIVFPQFromVectorStore(context.Background(), store, IVFPQProfile{
		Dimensions: dims, IVFLists: 8, PQSegments: 4, PQBits: 4, NProbe: 8, RerankTopK: 40,
		TrainingSampleMax: n, KMeansMaxIterations: 8,
	}, nil)
	require.NoError(t, err)
	bound := index.scoreBound()
	require.False(t, math.IsInf(bound, 1))
	queries := randomUnitVectors(rng, 20, dims)
	for d := 0; d < dims; d++ {
		q := make([]float32, dims)
		q[d] = 1
		queries = append(queries, q)
	}
	tightened := false
	for _, q := range queries {
		bound := index.queryScoreBound(q)
		tightened = tightened || bound < index.scoreBound()
		require.LessOrEqual(t, bound, index.scoreBound())
		candidates, err := index.SearchApprox(context.Background(), q, n, -1, 8)
		require.NoError(t, err)
		require.Len(t, candidates, n)
		for _, c := range candidates {
			exact := float64(vector.DotProduct(q, byID[c.ID]))
			require.LessOrEqual(t, exact, c.Score+bound+exactScoreSlack(dims), c.ID)
		}
	}
	require.True(t, tightened, "coordinate projection must tighten a nonzero residual bound")
}

// boundedNoiseGenerator returns every vector of a corpus with its exact
// score lowered by up to noise: a generator whose scores are within noise.
type boundedNoiseGenerator struct {
	candidates []Candidate
}

func (g *boundedNoiseGenerator) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	return g.candidates, nil
}

// TestBoundedStageMatchesFullStage: scoring nodes best bound first and
// stopping once none can qualify or reach the top k returns exactly what
// scoring every node returns when every vector was surfaced (an exhaustive
// pass), for single- and multi-chunk nodes, thresholds and page sizes, and
// reports a cut whenever the full stage does. With some chunks unsurfaced (a
// restricted pass), every returned node still carries its best chunk's exact
// score, ranked and above the threshold.
func TestBoundedStageMatchesFullStage(t *testing.T) {
	const dims, noise = 8, 0.05
	rng := rand.New(rand.NewSource(3))
	index := NewVectorIndex(dims)
	members := map[string][]string{}
	best := map[string]float64{}
	var all, some []Candidate
	query := randomUnitVectors(rng, 1, dims)[0]
	for n := 0; n < 300; n++ {
		node := fmt.Sprintf("doc-%03d", n)
		best[node] = math.Inf(-1)
		for c, v := range randomUnitVectors(rng, 1+n%4, dims) {
			id := node
			if c > 0 {
				id = nodeChunkVectorKey(node, c)
			}
			require.NoError(t, index.Add(id, v))
			members[node] = append(members[node], id)
			exact := float64(vector.DotProduct(query, v))
			best[node] = math.Max(best[node], exact)
			candidate := Candidate{ID: id, Score: exact - noise*rng.Float64()}
			all = append(all, candidate)
			if c == 0 || rng.Intn(2) == 0 {
				some = append(some, candidate)
			}
		}
	}
	resolve := func(ids []string) map[string][]string {
		out := map[string][]string{}
		for _, id := range ids {
			out[id] = members[id]
		}
		return out
	}
	pipeline := NewVectorSearchPipeline(&boundedNoiseGenerator{candidates: all}, NewCPUExactScorer(index))
	ctx := withVectorMembers(context.Background(), resolve)
	bound := noise + exactScoreSlack(dims)
	for _, k := range []int{1, 5, 20, 0} {
		for _, threshold := range []float64{-1, 0, 0.3, 0.6, 0.9} {
			full, fullCut, err := pipeline.scoreNodes(ctx, query, all, k, threshold, math.Inf(1))
			require.NoError(t, err)
			bounded, boundedCut, err := pipeline.scoreNodes(ctx, query, all, k, threshold, bound)
			require.NoError(t, err)
			require.Equal(t, full, bounded, "k=%d threshold=%v", k, threshold)
			if fullCut {
				require.True(t, boundedCut, "k=%d threshold=%v", k, threshold) // conservative: may also cut where full doesn't
			}

			partial, _, err := pipeline.scoreNodes(ctx, query, some, k, threshold, bound)
			require.NoError(t, err)
			partialFull, _, err := pipeline.scoreNodes(ctx, query, some, k, threshold, math.Inf(1))
			require.NoError(t, err)
			require.Equal(t, partialFull, partial, "partial members: k=%d threshold=%v", k, threshold)
			for i, result := range partial {
				node := mustVectorEntryOwner(t, result.ID)
				require.InDelta(t, best[node], result.Score, 1e-6, "k=%d threshold=%v %s", k, threshold, node)
				require.GreaterOrEqual(t, result.Score, threshold)
				if i > 0 {
					require.LessOrEqual(t, result.Score, partial[i-1].Score)
				}
			}
		}
	}
}
