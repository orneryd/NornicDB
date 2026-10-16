package search

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

// exactGraphBuildAccelerator runs the shared GPU graph build orchestration
// (candidateSearchGraphBatched) with an exact CPU distance kernel in place of
// a device's, so the orchestration's search quality is tested on any platform.
type exactGraphBuildAccelerator struct{}

func (exactGraphBuildAccelerator) Prepare(int, int) error { return nil }
func (exactGraphBuildAccelerator) Close() error           { return nil }

func (a exactGraphBuildAccelerator) CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) ([][]int, [][]float32, error) {
	return a.candidateSearch(ctx, queries, frontier, topK, true)
}

func (exactGraphBuildAccelerator) candidateSearch(_ context.Context, queries [][]float32, frontier [][]float32, topK int, _ bool) ([][]int, [][]float32, error) {
	idx := make([][]int, len(queries))
	dist := make([][]float32, len(queries))
	for qi, q := range queries {
		order := make([]int, len(frontier))
		scores := make([]float32, len(frontier))
		for fi, f := range frontier {
			order[fi] = fi
			scores[fi] = vector.DotProductSIMD(q, f)
		}
		sort.Slice(order, func(i, j int) bool { return scores[order[i]] > scores[order[j]] })
		if len(order) > topK {
			order = order[:topK]
		}
		idx[qi] = order
		dist[qi] = make([]float32, len(order))
		for i, o := range order {
			dist[qi][i] = 1 - scores[o]
		}
	}
	return idx, dist, nil
}

func (a exactGraphBuildAccelerator) CandidateSearchGraph(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
	return candidateSearchGraphBatched(ctx, a, queries, graph, topK)
}

// exactResidentBuildAccelerator is the resident build path
// (HNSWResidentBuildAccelerator) with the inserted vectors held in host memory
// and scored exactly, so the build's use of it is tested on any platform.
type exactResidentBuildAccelerator struct {
	exactGraphBuildAccelerator
	inserted [][]float32
	appended []int // vectors per AppendInserted call
}

func (*exactResidentBuildAccelerator) ResidentReady() bool { return true }

func (a *exactResidentBuildAccelerator) AppendInserted(vecs [][]float32) error {
	a.inserted = append(a.inserted, vecs...)
	a.appended = append(a.appended, len(vecs))
	return nil
}

func (a *exactResidentBuildAccelerator) CandidateSearchInserted(ctx context.Context, queries [][]float32, topK int) ([][]uint32, [][]float32, error) {
	normalized := make([][]float32, len(queries))
	for i, q := range queries {
		normalized[i] = vector.Normalize(q)
	}
	idx, dist, err := a.candidateSearch(ctx, normalized, a.inserted, topK, true)
	out := make([][]uint32, len(idx))
	for qi, row := range idx {
		for _, i := range row {
			out[qi] = append(out[qi], uint32(i))
		}
	}
	return out, dist, err
}

func clusteredUnitVectors(rng *rand.Rand, n, dims, clusters int) [][]float32 {
	centers := make([][]float32, clusters)
	for i := range centers {
		centers[i] = make([]float32, dims)
		for d := range centers[i] {
			centers[i][d] = float32(rng.NormFloat64())
		}
	}
	out := make([][]float32, n)
	for i := range out {
		c := centers[rng.Intn(clusters)]
		v := make([]float32, dims)
		for d := range v {
			v[d] = c[d] + 0.6*float32(rng.NormFloat64())
		}
		out[i] = vector.Normalize(v)
	}
	return out
}

// buildHNSWSequential is the reference build: every vector inserted one at a
// time through Add, as live inserts are, with its lexical hint registered first.
func buildHNSWSequential(dims int, config HNSWConfig, pairs []hnswBuildPair) (*HNSWIndex, hnswBuildStats, error) {
	idx := NewHNSWIndex(dims, config)
	for _, p := range pairs {
		if p.hasHint {
			idx.setBuildLexicalHint(p.hint)
		}
		if err := idx.Add(p.id, p.vec); err != nil {
			return nil, hnswBuildStats{}, err
		}
	}
	return idx, hnswBuildStats{Strategy: "sequential"}, nil
}

func hnswRecallAt10(t *testing.T, idx *HNSWIndex, data, queries [][]float32, ef int) float64 {
	t.Helper()
	hits := 0
	for _, q := range queries {
		scores := make([]float32, len(data))
		order := make([]int, len(data))
		for i, v := range data {
			order[i], scores[i] = i, vector.DotProductSIMD(q, v)
		}
		sort.Slice(order, func(i, j int) bool { return scores[order[i]] > scores[order[j]] })
		truth := map[string]bool{}
		for _, i := range order[:10] {
			truth[fmt.Sprintf("v%d", i)] = true
		}
		got, err := idx.SearchWithEf(context.Background(), q, 10, -1, ef)
		require.NoError(t, err)
		for _, r := range got {
			if truth[r.ID] {
				hits++
			}
		}
	}
	return float64(hits) / float64(len(queries)*10)
}

// The GPU-assisted build links each vector at level 0 to neighbours found by
// the accelerator's graph search. On a graph much larger than one build batch,
// the index it builds must find nearest neighbours about as well as the CPU
// build's: those neighbours have to come from near each vector, not from
// around the entry point.
func TestHNSWGraphAcceleratedBuildRecallMatchesCPU(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two 20k-vector indexes")
	}
	rng := rand.New(rand.NewSource(7))
	const dims = 64
	data := clusteredUnitVectors(rng, 20000, dims, 200)
	queries := clusteredUnitVectors(rng, 100, dims, 200)
	pairs := make([]hnswBuildPair, len(data))
	for i, v := range data {
		pairs[i] = hnswBuildPair{id: fmt.Sprintf("v%d", i), vec: v}
	}
	config := DefaultHNSWConfig()
	config.UseGPUBuild = false

	cpu, _, err := buildHNSWSequential(dims, config, pairs)
	require.NoError(t, err)
	gpuConfig := config
	gpuConfig.UseGPUBuild = true
	accelerated, stats, err := buildHNSWWithOptionalGPU(context.Background(), dims, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), exactGraphBuildAccelerator{})
	require.NoError(t, err)
	require.False(t, stats.Fallback, "the accelerated build ran")

	// GPU build off: the same batch build on the CPU kernel and CPU searches.
	parallel, stats, err := buildHNSWWithOptionalGPU(context.Background(), dims, config, nil, len(pairs), hnswPairSliceIterator(pairs), nil)
	require.NoError(t, err)
	require.Equal(t, "batch_parallel", stats.Strategy)

	cpuRecall := hnswRecallAt10(t, cpu, data, queries, 64)
	acceleratedRecall := hnswRecallAt10(t, accelerated, data, queries, 64)
	parallelRecall := hnswRecallAt10(t, parallel, data, queries, 64)
	t.Logf("recall@10 at ef=64: one-at-a-time %.3f, GPU graph search %.3f, CPU batch-parallel %.3f", cpuRecall, acceleratedRecall, parallelRecall)
	require.Greater(t, cpuRecall, 0.85)
	require.GreaterOrEqual(t, acceleratedRecall, cpuRecall-0.03)
	require.GreaterOrEqual(t, parallelRecall, cpuRecall-0.03)
}

// A device holding the inserted vectors gives each vector its exact nearest
// inserted vectors as level-0 candidates; the index matches the one-at-a-time
// build's recall, and the build appends each batch once, in internal-ID order.
func TestHNSWResidentAcceleratedBuildRecallMatchesCPU(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two 20k-vector indexes")
	}
	rng := rand.New(rand.NewSource(7))
	const dims = 64
	data := clusteredUnitVectors(rng, 20000, dims, 200)
	queries := clusteredUnitVectors(rng, 100, dims, 200)
	pairs := make([]hnswBuildPair, len(data))
	for i, v := range data {
		pairs[i] = hnswBuildPair{id: fmt.Sprintf("v%d", i), vec: v}
	}
	config := DefaultHNSWConfig()
	cpu, _, err := buildHNSWSequential(dims, config, pairs)
	require.NoError(t, err)

	config.UseGPUBuild = true
	accel := &exactResidentBuildAccelerator{}
	resident, stats, err := buildHNSWWithOptionalGPU(context.Background(), dims, config, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
	require.NoError(t, err)
	require.False(t, stats.Fallback)
	require.Len(t, accel.inserted, len(data))
	require.Len(t, accel.appended, stats.Batches, "one append per batch")
	for i := range data {
		id, ok := resident.internalID(fmt.Sprintf("v%d", i))
		require.True(t, ok)
		require.InDeltaSlice(t, data[i], accel.inserted[id], 1e-5, "resident vector %d is internal ID %d's", i, id)
	}

	cpuRecall := hnswRecallAt10(t, cpu, data, queries, 64)
	residentRecall := hnswRecallAt10(t, resident, data, queries, 64)
	t.Logf("recall@10 at ef=64: one-at-a-time %.3f, resident exact candidates %.3f", cpuRecall, residentRecall)
	require.GreaterOrEqual(t, residentRecall, cpuRecall-0.03)
}

// With BM25 lexical hints, each GPU build query also starts from inserted
// vectors sharing a signature band with it; the index still matches the CPU
// build's recall.
func TestHNSWGraphAcceleratedBuildWithLexicalHintsRecallMatchesCPU(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two 20k-vector indexes")
	}
	rng := rand.New(rand.NewSource(11))
	const dims, clusters = 64, 200
	data := clusteredUnitVectors(rng, 20000, dims, clusters)
	queries := clusteredUnitVectors(rng, 100, dims, clusters)
	// A vector's "terms" are its nearest cluster centroids' ids, so its
	// signature is shared with lexically and spatially close vectors, as BM25
	// signatures of similar text are.
	pairs := make([]hnswBuildPair, len(data))
	for i, v := range data {
		var sig uint64
		for b := 0; b < 4; b++ {
			sig |= uint64(stableLexicalHash(fmt.Sprintf("t%d", int(v[b]*8+8)))) & 0xffff << (16 * uint(b))
		}
		pairs[i] = hnswBuildPair{id: fmt.Sprintf("v%d", i), vec: v, hasHint: true,
			hint: LexicalSeedHint{ID: fmt.Sprintf("v%d", i), Rank: uint32(i), Signature: sig}}
	}
	config := DefaultHNSWConfig()
	cpu, _, err := buildHNSWSequential(dims, config, pairs)
	require.NoError(t, err)
	gpuConfig := config
	gpuConfig.UseGPUBuild = true
	accelerated, stats, err := buildHNSWWithOptionalGPU(context.Background(), dims, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), exactGraphBuildAccelerator{})
	require.NoError(t, err)
	require.False(t, stats.Fallback)

	cpuRecall := hnswRecallAt10(t, cpu, data, queries, 64)
	acceleratedRecall := hnswRecallAt10(t, accelerated, data, queries, 64)
	t.Logf("with lexical hints, recall@10 at ef=64: CPU build %.3f, graph-accelerated build %.3f", cpuRecall, acceleratedRecall)
	require.GreaterOrEqual(t, acceleratedRecall, cpuRecall-0.03)
}

// The band index returns inserted vectors sharing any 16-bit band with a
// signature, without duplicates, up to the limit; a zero signature (no hint)
// has none.
func TestLexicalBandIndexSeeds(t *testing.T) {
	bands := newLexicalBandIndex()
	bands.add(0x1111_2222_3333_4444, "a")
	bands.add(0x1111_9999_9999_9999, "b") // shares the top band
	bands.add(0x5555_6666_7777_8888, "c") // shares none
	bands.add(0, "d")                     // no hint: never indexed

	require.ElementsMatch(t, []string{"a", "b"}, bands.seeds(0x1111_0000_0000_4444, 8))
	require.Equal(t, []string{"a"}, bands.seeds(0x0000_0000_0000_4444, 8))
	require.Len(t, bands.seeds(0x1111_2222_3333_4444, 1), 1)
	require.Empty(t, bands.seeds(0, 8))
}

// Equal-distance candidates are ordered by lexical closeness to the query,
// candidates without a signature last; other distances keep their order.
func TestOrderLexicalTies(t *testing.T) {
	signatures := map[uint32]uint64{0: 0b0000, 1: 0b1110, 3: 0b1111, 4: 0b1100}
	candidates := []uint32{4, 0, 2, 1, 3}
	dists := []float32{0.1, 0.2, 0.2, 0.2, 0.3}
	orderLexicalTies(0b1111, candidates, dists, func(id uint32) uint64 { return signatures[id] })
	require.Equal(t, []uint32{4, 1, 0, 2, 3}, candidates, "0.2 ties: 1 (1 bit off), 0 (4 bits), 2 (no signature)")
}

// On a Metal device, the resident search returns each query's exact nearest
// inserted vectors, also after the build outgrows the room Prepare reserved.
func TestMetalResidentCandidateSearchIsExact(t *testing.T) {
	accel, err := NewMetalHNSWBuildAccelerator()
	if err != nil {
		t.Skipf("Metal unavailable: %v", err)
	}
	defer accel.Close()
	rng := rand.New(rand.NewSource(3))
	const dims = 32
	data := clusteredUnitVectors(rng, 3000, dims, 30)
	queries := clusteredUnitVectors(rng, 50, dims, 30)
	require.NoError(t, accel.Prepare(dims, 1000))
	require.True(t, accel.ResidentReady())
	for start := 0; start < len(data); start += 700 {
		require.NoError(t, accel.AppendInserted(data[start:min(start+700, len(data))]))
	}

	got, dists, err := accel.CandidateSearchInserted(context.Background(), queries, 20)
	require.NoError(t, err)
	want, wantDists, err := exactGraphBuildAccelerator{}.candidateSearch(context.Background(), queries, data, 20, true)
	require.NoError(t, err)
	for qi := range queries {
		require.Len(t, got[qi], 20)
		for i := range got[qi] {
			require.InDelta(t, wantDists[qi][i], dists[qi][i], 1e-4, "query %d rank %d", qi, i)
			require.InDelta(t, wantDists[qi][i], 1-vector.DotProductSIMD(queries[qi], data[got[qi][i]]), 1e-4, "query %d rank %d: id %d (exact %d)", qi, i, got[qi][i], want[qi][i])
		}
	}
}

// A score row's top k come highest first, equal scores lower index first, as
// distances 1-score; k beyond the row returns the whole row.
func TestTopKScoreRow(t *testing.T) {
	ids, dists := topKScoreRow([]float32{0.1, 0.9, 0.5, 0.9, -0.2, 0.7}, 4)
	require.Equal(t, []uint32{1, 3, 5, 2}, ids)
	require.InDeltaSlice(t, []float32{0.1, 0.1, 0.3, 0.5}, dists, 1e-6)
	ids, _ = topKScoreRow([]float32{0.2, 0.4}, 5)
	require.Equal(t, []uint32{1, 0}, ids)
}

// Metal's frontier search (the graph search's and the batch self-scoring's
// kernel) returns each query's exact nearest frontier vectors, also across
// frontier shards.
func TestMetalCandidateSearchIsExact(t *testing.T) {
	accel, err := NewMetalHNSWBuildAccelerator()
	if err != nil {
		t.Skipf("Metal unavailable: %v", err)
	}
	defer accel.Close()
	accel.maxFrontierPerShard = 700
	rng := rand.New(rand.NewSource(5))
	const dims = 32
	frontier := clusteredUnitVectors(rng, 2500, dims, 25)
	queries := clusteredUnitVectors(rng, 40, dims, 25)
	require.NoError(t, accel.Prepare(dims, 0))

	got, dists, err := accel.CandidateSearch(context.Background(), queries, frontier, 30)
	require.NoError(t, err)
	_, wantDists, err := exactGraphBuildAccelerator{}.candidateSearch(context.Background(), queries, frontier, 30, true)
	require.NoError(t, err)
	for qi := range queries {
		require.Len(t, got[qi], 30)
		require.InDeltaSlice(t, wantDists[qi], dists[qi], 1e-4, "query %d", qi)
		for i, id := range got[qi] {
			require.InDelta(t, dists[qi][i], 1-vector.DotProductSIMD(queries[qi], frontier[id]), 1e-4)
		}
	}
}
