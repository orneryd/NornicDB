package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// closeAfterBuilds closes svc once no background HNSW build of it is in
// flight: a build outliving its test would run the package's build hooks
// inside another test.
func closeAfterBuilds(tb testing.TB, svc *Service) {
	for deadline := time.Now().Add(30 * time.Second); svc.hnswRebuildInFlight.Load() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	_ = svc.Close()
}

// selectiveClusteredService is review r4's Service fixture: 2,000 documents
// in 16 dimensions, 8 clusters or lists probed one at a time, and 11 documents
// above the 0.95 threshold, all in the nearest cluster. Old and new code
// return the same rows; only the work to prove the page complete differs.
func selectiveClusteredService(tb testing.TB, strategy string) (*Service, []float32) {
	tb.Helper()
	tb.Setenv(EnvSearchResultCacheEnabled, "false")
	tb.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	tb.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", fmt.Sprint(strategy == "ivf_hnsw"))
	tb.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "1")
	tb.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	const count, dims = 2000, 16
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	svc := NewServiceWithDimensions(engine, dims)
	tb.Cleanup(func() { closeAfterBuilds(tb, svc) })
	if strategy != "ivfpq" {
		svc.EnableClustering(nil, 8)
		svc.SetMinEmbeddingsForClustering(1)
	}
	cfg := gpu.DefaultEmbeddingIndexConfig(dims)
	cfg.GPUEnabled, cfg.AutoSync = false, false
	clusters := gpu.NewClusterIndex(nil, cfg, &gpu.KMeansConfig{NumClusters: 8, MaxIterations: 10, Tolerance: 0.001, InitMethod: "kmeans++"})
	store, err := NewVectorFileStore(tb.TempDir()+"/vectors", dims)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	for i := 0; i < count; i++ {
		v := make([]float32, dims)
		v[0], v[1+i%8] = 0.6, 0.8
		if i < 11 {
			v = make([]float32, dims)
			v[0] = 1
		}
		id := fmt.Sprintf("doc-%04d", i)
		node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{v}}
		if _, err := engine.CreateNode(node); err != nil {
			tb.Fatal(err)
		}
		if err := svc.IndexNode(node); err != nil {
			tb.Fatal(err)
		}
		if err := clusters.Add(id, v); err != nil {
			tb.Fatal(err)
		}
		if err := store.Add(id, v); err != nil {
			tb.Fatal(err)
		}
	}
	var pipeline *VectorSearchPipeline
	switch strategy {
	case "kmeans", "ivf_hnsw":
		if err := svc.TriggerClustering(context.Background()); err != nil {
			tb.Fatal(err)
		}
		if pipeline, err = svc.getOrCreateVectorPipeline(context.Background()); err != nil {
			tb.Fatal(err)
		}
	case "gpu_kmeans_cpu":
		if err := clusters.Cluster(); err != nil {
			tb.Fatal(err)
		}
		pipeline = NewVectorSearchPipeline(NewGPUKMeansCandidateGen(clusters, 1), NewCPUExactScorer(svc.vectorIndex))
	case "ivfpq":
		index, _, err := BuildIVFPQFromVectorStore(context.Background(), store, IVFPQProfile{Dimensions: dims, IVFLists: 8, PQSegments: 4, PQBits: 4, NProbe: 1, RerankTopK: 40, TrainingSampleMax: count, KMeansMaxIterations: 10}, nil)
		if err != nil {
			tb.Fatal(err)
		}
		pipeline = NewVectorSearchPipeline(NewIVFPQCandidateGen(index, 1), NewCPUExactScorer(svc.vectorIndex))
	}
	svc.pipelineMu.Lock()
	svc.vectorPipeline = pipeline
	svc.pipelineMu.Unlock()
	query := make([]float32, dims)
	query[0] = 1
	return svc, query
}

// BenchmarkSelectiveClusteredServiceSearch is a selective vector search that
// must reach the all-cluster pass to prove its page complete.
func BenchmarkSelectiveClusteredServiceSearch(b *testing.B) {
	for _, strategy := range []string{"kmeans", "gpu_kmeans_cpu", "ivf_hnsw", "ivfpq"} {
		b.Run(strategy, func(b *testing.B) {
			svc, query := selectiveClusteredService(b, strategy)
			opts := DefaultSearchOptions()
			threshold := 0.95
			opts.MinSimilarity = &threshold
			opts.Limit = 20
			opts.MaxOverfetchRatio = math.MaxFloat64
			fallback := false
			opts.FallbackEnabled = &fallback
			response, err := svc.Search(context.Background(), "", query, opts)
			if err != nil {
				b.Fatal(err)
			}
			rows := len(response.Results)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err = svc.Search(context.Background(), "", query, opts)
				if err != nil || len(response.Results) != rows {
					b.Fatalf("rows=%d error=%v", len(response.Results), err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(rows), "docs/op")
			if response.Metrics != nil {
				b.ReportMetric(float64(response.Metrics.VectorOverfetchRetries), "retries/op")
			}
		})
	}
}

// BenchmarkChunkedCorpusVectorSearch is vector search over 2,000 documents of
// 8 chunks each through the global HNSW graph: an open page of 10 and a
// selective one.
func BenchmarkChunkedCorpusVectorSearch(b *testing.B) {
	const nodes, chunks, dims = 2000, 8, 32
	for _, mode := range []string{"none"} {
		b.Run("quantization="+mode, func(b *testing.B) {
			b.Setenv(EnvSearchResultCacheEnabled, "false")
			b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			svc := NewServiceWithDimensions(engine, dims)
			b.Cleanup(func() { closeAfterBuilds(b, svc) })
			rng := rand.New(rand.NewSource(1))
			next := func() []float32 {
				v := make([]float32, dims)
				for i := range v {
					v[i] = rng.Float32()*2 - 1
				}
				return v
			}
			for n := 0; n < nodes; n++ {
				embeddings := make([][]float32, chunks)
				for c := range embeddings {
					embeddings[c] = next()
				}
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%04d", n)), Labels: []string{"Doc"}, ChunkEmbeddings: embeddings}
				if _, err := engine.CreateNode(node); err != nil {
					b.Fatal(err)
				}
				if err := svc.IndexNode(node); err != nil {
					b.Fatal(err)
				}
			}
			// Exact brute force serves while the graph builds.
			for deadline := time.Now().Add(time.Minute); ; {
				p, err := svc.getOrCreateVectorPipeline(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if _, ok := p.candidateGen.(*HNSWCandidateGen); ok {
					break
				}
				if time.Now().After(deadline) {
					b.Fatal("HNSW build did not finish")
				}
				time.Sleep(time.Millisecond)
			}
			query := next()
			for _, page := range []struct {
				name      string
				threshold float64
			}{{"open", -1}, {"selective", 0.6}} {
				b.Run(page.name, func(b *testing.B) {
					opts := DefaultSearchOptions()
					opts.Limit = 10
					threshold := page.threshold
					opts.MinSimilarity = &threshold
					fallback := false
					opts.FallbackEnabled = &fallback
					b.ReportAllocs()
					b.ResetTimer()
					rows := 0
					for i := 0; i < b.N; i++ {
						response, err := svc.Search(context.Background(), "", query, opts)
						if err != nil {
							b.Fatal(err)
						}
						rows = len(response.Results)
					}
					b.StopTimer()
					b.ReportMetric(float64(rows), "docs/op")
				})
			}
		})
	}
}
