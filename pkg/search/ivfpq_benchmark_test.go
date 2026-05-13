package search

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type benchmarkVectorCorpus struct {
	IDs  []string
	Vecs [][]float32
	Dims int
}

func (c benchmarkVectorCorpus) prefix(n int) benchmarkVectorCorpus {
	if n <= 0 || n > len(c.IDs) {
		n = len(c.IDs)
	}
	return benchmarkVectorCorpus{
		IDs:  c.IDs[:n],
		Vecs: c.Vecs[:n],
		Dims: c.Dims,
	}
}

func loadBenchmarkCorpus(tb testing.TB, syntheticCount, syntheticDims int) benchmarkVectorCorpus {
	tb.Helper()

	dataDir := os.Getenv("NORNICDB_IVFPQ_BENCH_DATA_DIR")
	if dataDir == "" {
		dataDir = "~/src/NornicDB/data/test-small"
	}
	dbName := os.Getenv("NORNICDB_IVFPQ_BENCH_DB")
	if dbName == "" {
		dbName = "nornic"
	}
	maxVectors := clampInt(envInt("NORNICDB_IVFPQ_BENCH_MAX_VECTORS", 50000), 500, 500000)

	if _, err := os.Stat(dataDir); err == nil {
		baseEngine, err := storage.NewBadgerEngine(dataDir)
		if err == nil {
			tb.Cleanup(func() { _ = baseEngine.Close() })
			engine := storage.NewNamespacedEngine(baseEngine, dbName)
			nodes, err := engine.AllNodes()
			if err == nil && len(nodes) > 0 {
				corpus := benchmarkVectorCorpus{
					IDs:  make([]string, 0, maxVectors),
					Vecs: make([][]float32, 0, maxVectors),
				}
				for _, node := range nodes {
					if len(corpus.Vecs) >= maxVectors {
						break
					}
					for name, vec := range node.NamedEmbeddings {
						if len(vec) == 0 {
							continue
						}
						if corpus.Dims == 0 {
							corpus.Dims = len(vec)
						}
						if len(vec) != corpus.Dims {
							continue
						}
						corpus.IDs = append(corpus.IDs, fmt.Sprintf("%s#named:%s", node.ID, name))
						corpus.Vecs = append(corpus.Vecs, append([]float32(nil), vec...))
						if len(corpus.Vecs) >= maxVectors {
							break
						}
					}
					for i, vec := range node.ChunkEmbeddings {
						if len(corpus.Vecs) >= maxVectors {
							break
						}
						if len(vec) == 0 {
							continue
						}
						if corpus.Dims == 0 {
							corpus.Dims = len(vec)
						}
						if len(vec) != corpus.Dims {
							continue
						}
						corpus.IDs = append(corpus.IDs, fmt.Sprintf("%s#chunk:%d", node.ID, i))
						corpus.Vecs = append(corpus.Vecs, append([]float32(nil), vec...))
					}
				}
				if len(corpus.Vecs) >= 500 && corpus.Dims > 0 {
					tb.Logf("using disk benchmark corpus: dir=%s db=%s vectors=%d dims=%d", dataDir, dbName, len(corpus.Vecs), corpus.Dims)
					return corpus
				}
			}
		}
	}

	return buildSyntheticBenchmarkCorpus(syntheticCount, syntheticDims)
}

func buildSyntheticBenchmarkCorpus(count, dims int) benchmarkVectorCorpus {
	ids := make([]string, 0, count)
	vecs := make([][]float32, 0, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		vec[i%dims] = 1
		ids = append(ids, fmt.Sprintf("doc-%d", i))
		vecs = append(vecs, vec)
	}
	return benchmarkVectorCorpus{
		IDs:  ids,
		Vecs: vecs,
		Dims: dims,
	}
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	var v int
	if _, err := fmt.Sscanf(raw, "%d", &v); err != nil {
		return fallback
	}
	return v
}

func profileForCorpus(c benchmarkVectorCorpus) IVFPQProfile {
	ivfLists := clampInt(len(c.Vecs)/8, 16, 256)
	training := clampInt(len(c.Vecs)/2, 2000, 60000)
	segments := 8
	if c.Dims%8 != 0 {
		segments = 4
	}
	if c.Dims%segments != 0 {
		segments = 1
	}
	return IVFPQProfile{
		Dimensions:          c.Dims,
		IVFLists:            ivfLists,
		PQSegments:          segments,
		PQBits:              4,
		NProbe:              clampInt(ivfLists/16, 4, 32),
		RerankTopK:          200,
		TrainingSampleMax:   training,
		KMeansMaxIterations: 6,
	}
}

type benchBuildMetrics struct {
	buildDuration time.Duration
	heapBuild     uint64
	heapLive      uint64
}

type queryMemoryPressure struct {
	heapDelta  uint64
	totalAlloc uint64
	numGC      uint32
	duration   time.Duration
}

func measureBuildMemory(buildFn func() error) (benchBuildMetrics, error) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	if err := buildFn(); err != nil {
		return benchBuildMetrics{}, err
	}
	buildElapsed := time.Since(start)
	runtime.GC()
	runtime.ReadMemStats(&after)
	heapBuild := uint64(0)
	if after.HeapAlloc > before.HeapAlloc {
		heapBuild = after.HeapAlloc - before.HeapAlloc
	}
	return benchBuildMetrics{
		buildDuration: buildElapsed,
		heapBuild:     heapBuild,
		heapLive:      after.HeapAlloc,
	}, nil
}

func measureQueryPressure(iterations int, runQuery func(i int) error) (queryMemoryPressure, error) {
	if iterations <= 0 {
		iterations = 256
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		if err := runQuery(i); err != nil {
			return queryMemoryPressure{}, err
		}
	}
	runtime.ReadMemStats(&after)
	heapDelta := uint64(0)
	if after.HeapAlloc > before.HeapAlloc {
		heapDelta = after.HeapAlloc - before.HeapAlloc
	}
	totalAlloc := uint64(0)
	if after.TotalAlloc > before.TotalAlloc {
		totalAlloc = after.TotalAlloc - before.TotalAlloc
	}
	gcDelta := uint32(0)
	if after.NumGC > before.NumGC {
		gcDelta = after.NumGC - before.NumGC
	}
	return queryMemoryPressure{
		heapDelta:  heapDelta,
		totalAlloc: totalAlloc,
		numGC:      gcDelta,
		duration:   time.Since(start),
	}, nil
}

func BenchmarkIVFPQCandidateGen(b *testing.B) {
	dir := b.TempDir()
	corpus := loadBenchmarkCorpus(b, 10000, 32)
	vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), corpus.Dims)
	if err != nil {
		b.Fatal(err)
	}
	defer vfs.Close()

	for i := range corpus.IDs {
		if err := vfs.Add(corpus.IDs[i], corpus.Vecs[i]); err != nil {
			b.Fatal(err)
		}
	}

	profile := profileForCorpus(corpus)
	idx, _, err := BuildIVFPQFromVectorStore(context.Background(), vfs, profile, nil)
	if err != nil {
		b.Fatal(err)
	}
	gen := NewIVFPQCandidateGen(idx, profile.NProbe)
	query := make([]float32, corpus.Dims)
	query[0] = 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := gen.SearchCandidates(context.Background(), query, 20, -1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkANNQualityMatrix(b *testing.B) {
	dir := b.TempDir()
	corpus := loadBenchmarkCorpus(b, 12000, 32)
	vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), corpus.Dims)
	if err != nil {
		b.Fatal(err)
	}
	defer vfs.Close()

	for i := range corpus.IDs {
		if err := vfs.Add(corpus.IDs[i], corpus.Vecs[i]); err != nil {
			b.Fatal(err)
		}
	}
	query := make([]float32, corpus.Dims)
	query[0] = 1
	profile := profileForCorpus(corpus)

	b.Run("quality=balanced_hnsw", func(b *testing.B) {
		hidx := NewHNSWIndex(corpus.Dims, HNSWConfig{M: 16, EfConstruction: 100, EfSearch: 100})
		if err := vfs.IterateChunked(4096, func(ids []string, vecs [][]float32) error {
			for i := range ids {
				if err := hidx.Add(ids[i], vecs[i]); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		gen := NewHNSWCandidateGen(hidx)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := gen.SearchCandidates(context.Background(), query, 20, -1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("quality=compressed_ivfpq", func(b *testing.B) {
		idx, _, err := BuildIVFPQFromVectorStore(context.Background(), vfs, profile, nil)
		if err != nil {
			b.Fatal(err)
		}
		gen := NewIVFPQCandidateGen(idx, profile.NProbe)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := gen.SearchCandidates(context.Background(), query, 20, -1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkANNQualityMatrixChunked(b *testing.B) {
	baseCorpus := loadBenchmarkCorpus(b, 12000, 32)
	query := make([]float32, baseCorpus.Dims)
	query[0] = 1

	total := len(baseCorpus.IDs)
	if total < 1000 {
		b.Fatalf("need at least 1000 vectors for chunked matrix; got %d", total)
	}
	chunks := []struct {
		name string
		size int
	}{
		{name: "full", size: total},
		{name: "half", size: total / 2},
		{name: "quarter", size: total / 4},
		{name: "eighth", size: total / 8},
	}

	for _, chunk := range chunks {
		if chunk.size < 1000 {
			continue
		}
		corpus := baseCorpus.prefix(chunk.size)
		b.Run(fmt.Sprintf("%s_n=%d", chunk.name, chunk.size), func(b *testing.B) {
			dir := b.TempDir()
			vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), corpus.Dims)
			if err != nil {
				b.Fatal(err)
			}
			defer vfs.Close()
			for i := range corpus.IDs {
				if err := vfs.Add(corpus.IDs[i], corpus.Vecs[i]); err != nil {
					b.Fatal(err)
				}
			}
			profile := profileForCorpus(corpus)

			b.Run("quality=balanced_hnsw", func(b *testing.B) {
				var hidx *HNSWIndex
				mem, err := measureBuildMemory(func() error {
					hidx = NewHNSWIndex(corpus.Dims, HNSWConfig{M: 16, EfConstruction: 100, EfSearch: 100})
					return vfs.IterateChunked(4096, func(ids []string, vecs [][]float32) error {
						for i := range ids {
							if err := hidx.Add(ids[i], vecs[i]); err != nil {
								return err
							}
						}
						return nil
					})
				})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(mem.heapBuild)/(1024.0*1024.0), "heap_mib_build")
				b.ReportMetric(float64(mem.heapLive)/(1024.0*1024.0), "heap_mib_live")
				b.ReportMetric(float64(mem.buildDuration.Milliseconds()), "build_ms")
				b.Logf("memory profile hnsw n=%d dims=%d heap_build_mib=%.2f heap_live_mib=%.2f build_ms=%d",
					len(corpus.IDs), corpus.Dims, float64(mem.heapBuild)/(1024.0*1024.0), float64(mem.heapLive)/(1024.0*1024.0), mem.buildDuration.Milliseconds())
				gen := NewHNSWCandidateGen(hidx)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := gen.SearchCandidates(context.Background(), query, 20, -1)
					if err != nil {
						b.Fatal(err)
					}
				}
				runtime.KeepAlive(hidx)
			})

			b.Run("quality=compressed_ivfpq", func(b *testing.B) {
				var idx *IVFPQIndex
				mem, err := measureBuildMemory(func() error {
					var buildErr error
					idx, _, buildErr = BuildIVFPQFromVectorStore(context.Background(), vfs, profile, nil)
					return buildErr
				})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(mem.heapBuild)/(1024.0*1024.0), "heap_mib_build")
				b.ReportMetric(float64(mem.heapLive)/(1024.0*1024.0), "heap_mib_live")
				b.ReportMetric(float64(mem.buildDuration.Milliseconds()), "build_ms")
				b.Logf("memory profile ivfpq n=%d dims=%d heap_build_mib=%.2f heap_live_mib=%.2f build_ms=%d",
					len(corpus.IDs), corpus.Dims, float64(mem.heapBuild)/(1024.0*1024.0), float64(mem.heapLive)/(1024.0*1024.0), mem.buildDuration.Milliseconds())
				gen := NewIVFPQCandidateGen(idx, profile.NProbe)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := gen.SearchCandidates(context.Background(), query, 20, -1)
					if err != nil {
						b.Fatal(err)
					}
				}
				runtime.KeepAlive(idx)
			})
		})
	}
}

func sampleQueryVectors(c benchmarkVectorCorpus, maxQueries int) [][]float32 {
	if len(c.Vecs) == 0 {
		return nil
	}
	if maxQueries <= 0 {
		maxQueries = 32
	}
	if maxQueries > len(c.Vecs) {
		maxQueries = len(c.Vecs)
	}
	step := len(c.Vecs) / maxQueries
	if step < 1 {
		step = 1
	}
	out := make([][]float32, 0, maxQueries)
	for i := 0; i < len(c.Vecs) && len(out) < maxQueries; i += step {
		q := make([]float32, len(c.Vecs[i]))
		copy(q, c.Vecs[i])
		out = append(out, q)
	}
	if len(out) == 0 {
		q := make([]float32, len(c.Vecs[0]))
		copy(q, c.Vecs[0])
		out = append(out, q)
	}
	return out
}

func BenchmarkANNQueryPipelineChunked(b *testing.B) {
	baseCorpus := loadBenchmarkCorpus(b, 12000, 32)
	total := len(baseCorpus.IDs)
	if total < 1000 {
		b.Fatalf("need at least 1000 vectors for query pipeline matrix; got %d", total)
	}
	chunks := []struct {
		name string
		size int
	}{
		{name: "full", size: total},
		{name: "half", size: total / 2},
		{name: "quarter", size: total / 4},
		{name: "eighth", size: total / 8},
	}

	for _, chunk := range chunks {
		if chunk.size < 1000 {
			continue
		}
		corpus := baseCorpus.prefix(chunk.size)
		queries := sampleQueryVectors(corpus, envInt("NORNICDB_IVFPQ_BENCH_QUERY_COUNT", 64))
		probeQueries := envInt("NORNICDB_IVFPQ_BENCH_QUERY_PRESSURE_ITERS", 512)
		profile := profileForCorpus(corpus)

		b.Run(fmt.Sprintf("%s_n=%d", chunk.name, chunk.size), func(b *testing.B) {
			dir := b.TempDir()
			vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), corpus.Dims)
			if err != nil {
				b.Fatal(err)
			}
			defer vfs.Close()
			for i := range corpus.IDs {
				if err := vfs.Add(corpus.IDs[i], corpus.Vecs[i]); err != nil {
					b.Fatal(err)
				}
			}

			b.Run("quality=balanced_hnsw_query", func(b *testing.B) {
				hidx := NewHNSWIndex(corpus.Dims, HNSWConfig{M: 16, EfConstruction: 100, EfSearch: 100})
				if err := vfs.IterateChunked(4096, func(ids []string, vecs [][]float32) error {
					for i := range ids {
						if err := hidx.Add(ids[i], vecs[i]); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				pipeline := NewVectorSearchPipeline(NewHNSWCandidateGen(hidx), NewCPUExactScorer(vfs))
				mem, err := measureQueryPressure(probeQueries, func(i int) error {
					q := queries[i%len(queries)]
					_, runErr := pipeline.Search(context.Background(), q, 20, -1)
					return runErr
				})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(mem.heapDelta)/(1024.0*1024.0), "query_heap_delta_mib")
				b.ReportMetric(float64(mem.totalAlloc)/(1024.0*1024.0), "query_total_alloc_mib")
				b.ReportMetric(float64(mem.numGC), "query_gc_cycles")
				b.Logf("query pressure hnsw n=%d dims=%d iters=%d heap_delta_mib=%.2f total_alloc_mib=%.2f gc_cycles=%d probe_ms=%d",
					len(corpus.IDs), corpus.Dims, probeQueries, float64(mem.heapDelta)/(1024.0*1024.0), float64(mem.totalAlloc)/(1024.0*1024.0), mem.numGC, mem.duration.Milliseconds())
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					q := queries[i%len(queries)]
					if _, err := pipeline.Search(context.Background(), q, 20, -1); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("quality=compressed_ivfpq_query", func(b *testing.B) {
				idx, _, err := BuildIVFPQFromVectorStore(context.Background(), vfs, profile, nil)
				if err != nil {
					b.Fatal(err)
				}
				pipeline := NewVectorSearchPipeline(NewIVFPQCandidateGen(idx, profile.NProbe), NewCPUExactScorer(vfs))
				mem, err := measureQueryPressure(probeQueries, func(i int) error {
					q := queries[i%len(queries)]
					_, runErr := pipeline.Search(context.Background(), q, 20, -1)
					return runErr
				})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(mem.heapDelta)/(1024.0*1024.0), "query_heap_delta_mib")
				b.ReportMetric(float64(mem.totalAlloc)/(1024.0*1024.0), "query_total_alloc_mib")
				b.ReportMetric(float64(mem.numGC), "query_gc_cycles")
				b.Logf("query pressure ivfpq n=%d dims=%d iters=%d heap_delta_mib=%.2f total_alloc_mib=%.2f gc_cycles=%d probe_ms=%d",
					len(corpus.IDs), corpus.Dims, probeQueries, float64(mem.heapDelta)/(1024.0*1024.0), float64(mem.totalAlloc)/(1024.0*1024.0), mem.numGC, mem.duration.Milliseconds())
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					q := queries[i%len(queries)]
					if _, err := pipeline.Search(context.Background(), q, 20, -1); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
