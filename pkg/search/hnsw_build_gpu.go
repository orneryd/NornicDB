package search

import (
	"context"
	"fmt"
	"math/bits"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu/metal"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/util"
)

const hnswProgressInterval = 50000

// HNSWBuildAccelerator scores a build batch against itself for HNSW
// construction (CandidateSearch with the batch as both queries and frontier): a
// dense all-pairs product, which suits a GPU. The graph search for neighbours
// already in the index runs on the CPU, and the CPU performs all graph mutation
// and reciprocal linking, so persisted artifacts are identical in format to
// CPU-built indexes.
type HNSWBuildAccelerator interface {
	Prepare(dim int, maxNodes int) error
	CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) (indices [][]int, distances [][]float32, err error)
	Close() error
}

type HNSWGraphBuildAccelerator interface {
	CandidateSearchGraph(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) (indices [][]uint32, distances [][]float32, err error)
}

// HNSWResidentBuildAccelerator keeps every vector the build has inserted on the
// device, by internal ID, so a batch's level-0 candidates are its exact nearest
// inserted vectors from one scoring pass, with nothing re-uploaded. A graph
// search (HNSWGraphBuildAccelerator) uploads the vectors of each round's
// candidates instead, which dominates a large build. ResidentReady reports
// whether the device is prepared to retain inserted vectors; otherwise the build uses the
// accelerator's graph search.
type HNSWResidentBuildAccelerator interface {
	ResidentReady() bool
	// AppendInserted stores normalized vectors as the next internal IDs.
	AppendInserted(vecs [][]float32) error
	// CandidateSearchInserted returns each query's topK closest inserted
	// vectors by internal ID, closest first, with cosine distances.
	CandidateSearchInserted(ctx context.Context, queries [][]float32, topK int) (indices [][]uint32, distances [][]float32, err error)
}

// gpuBuildCandidateSearcher is the backend-local candidate-search contract the
// beam-orchestration kernel needs from each GPU accelerator (CUDA, Metal,
// Vulkan). The frontier-based method already shares one signature across the
// three accelerators; this interface just names it for the generic kernel.
type gpuBuildCandidateSearcher interface {
	candidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int, wantDistances bool) ([][]int, [][]float32, error)
}

// candidateSearchGraphBatched finds each query's level-0 neighbour candidates
// with a beam search over the build graph, in groups of
// NORNICDB_HNSW_BUILD_GPU_BEAM_QUERY_GROUP queries. Query i starts from
// graph.querySeeds[i] (its own level-0 entry point and lexical seeds), and the
// caller orders the queries so each group lies in one region of the graph
// (buildHNSWBatchParallel sorts them by entry point), which keeps the
// candidate set a group shares compact. Each accelerator's
// CandidateSearchGraph (CUDA, Metal, Vulkan) keeps only its backend-specific
// precondition and device checks and delegates here.
func candidateSearchGraphBatched[A gpuBuildCandidateSearcher](ctx context.Context, a A, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
	groupSize := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_QUERY_GROUP", 32)
	if groupSize <= 0 {
		groupSize = 32
	}
	outIdx := make([][]uint32, len(queries))
	outDist := make([][]float32, len(queries))
	for start := 0; start < len(queries); start += groupSize {
		end := min(start+groupSize, len(queries))
		idx, dist, err := candidateSearchGraphGroup(ctx, a, queries[start:end], graph, start, topK)
		if err != nil {
			return nil, nil, err
		}
		copy(outIdx[start:end], idx)
		copy(outDist[start:end], dist)
	}
	return outIdx, outDist, nil
}

// candidateSearchGraphGroup runs one group's beam search over the build graph.
// Each round expands the beam nodes not expanded yet (their level-0
// neighbours join the candidates), scores every query against the group's
// candidates and keeps each query's closest beamWidth until all retained nodes
// have been expanded. A fixed round cap can truncate a still-improving search. Each query's result is its
// beam cut to topK, equal distances ordered by lexical closeness to the query.
// offset is the group's first query within graph.querySeeds.
func candidateSearchGraphGroup[A gpuBuildCandidateSearcher](ctx context.Context, a A, queries [][]float32, graph *hnswBuildGraphSnapshot, offset int, topK int) ([][]uint32, [][]float32, error) {
	beamWidth := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_WIDTH", topK)
	if beamWidth <= 0 {
		beamWidth = topK
	}
	beamWidth = min(beamWidth, 256)
	unionMax := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_UNION_MAX", 16384)
	if unionMax <= 0 {
		unionMax = 16384
	}

	maxDegree := 0
	for _, neighbors := range graph.neighbors {
		maxDegree = max(maxDegree, len(neighbors))
	}
	unionMax = max(unionMax, len(queries)*beamWidth+maxDegree)

	beams := make([][]uint32, len(queries))
	dists := make([][]float32, len(queries))
	expanded := make([]map[uint32]struct{}, len(queries))
	for qi := range queries {
		beams[qi] = graph.seedsForQuery(offset+qi, beamWidth)
		expanded[qi] = make(map[uint32]struct{}, beamWidth*2)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		unionIDs, grew := graph.expandBeams(beams, expanded, unionMax)
		if !grew || len(unionIDs) == 0 {
			break
		}
		localIdx, localDist, err := a.candidateSearch(ctx, queries, graph.vectorsForInternalIDs(unionIDs), beamWidth, true)
		if err != nil {
			return nil, nil, err
		}
		for qi := range localIdx {
			next := make([]uint32, 0, len(localIdx[qi]))
			nextDist := make([]float32, 0, len(localIdx[qi]))
			for i, idx := range localIdx[qi] {
				if idx < 0 || idx >= len(unionIDs) {
					continue
				}
				next = append(next, unionIDs[idx])
				if qi < len(localDist) && i < len(localDist[qi]) {
					nextDist = append(nextDist, localDist[qi][i])
				}
			}
			beams[qi], dists[qi] = next, nextDist
		}
	}
	for qi := range beams {
		query := uint64(0)
		if offset+qi < len(graph.querySignatures) {
			query = graph.querySignatures[offset+qi]
		}
		orderLexicalTies(query, beams[qi], dists[qi], graph.signatureOf)
		if topK < len(beams[qi]) {
			beams[qi] = beams[qi][:topK]
			if len(dists[qi]) > topK {
				dists[qi] = dists[qi][:topK]
			}
		}
	}
	return beams, dists, nil
}

// seedsForQuery is query qi's starting beam: its own seeds (level-0 entry
// point, lexical seeds) followed by the entry point's level-0 neighbours, or
// the global entry point's neighbourhood when the query has no seeds.
func (g *hnswBuildGraphSnapshot) seedsForQuery(qi int, limit int) []uint32 {
	if qi >= len(g.querySeeds) || len(g.querySeeds[qi]) == 0 {
		return g.seedCandidates(limit)
	}
	out := make([]uint32, 0, util.SafePreallocCap(limit, len(g.vectors)))
	seen := make(map[uint32]struct{}, limit)
	add := func(id uint32) {
		if len(out) >= limit || int(id) >= len(g.vectors) {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	seeds := g.querySeeds[qi]
	for _, id := range seeds {
		add(id)
	}
	if entry := seeds[0]; int(entry) < len(g.neighbors) {
		for _, n := range g.neighbors[entry] {
			add(n)
		}
	}
	return out
}

// expandBeams is one round's candidate set for a group of beams: every beam
// node (already candidates), then the level-0 neighbours of beam nodes not
// expanded yet, closest-first across all beams so a capped set is shared
// fairly, up to limit nodes. It marks the nodes it expands and reports whether
// it expanded any; a node left unexpanded by the cap is expanded next round.
func (g *hnswBuildGraphSnapshot) expandBeams(beams [][]uint32, expanded []map[uint32]struct{}, limit int) ([]uint32, bool) {
	if g == nil || limit <= 0 {
		return nil, false
	}
	g.seenGen++
	if g.seenGen == 0 {
		clear(g.seen)
		g.seenGen = 1
	}
	out := g.unionBuf[:0]
	add := func(id uint32) {
		if len(out) >= limit || int(id) >= len(g.vectors) || g.seen[id] == g.seenGen {
			return
		}
		g.seen[id] = g.seenGen
		out = append(out, id)
	}
	widest := 0
	for _, beam := range beams {
		widest = max(widest, len(beam))
		for _, id := range beam {
			add(id)
		}
	}
	grew := false
	for rank := 0; rank < widest && len(out) < limit; rank++ {
		for qi, beam := range beams {
			if rank >= len(beam) || len(out) >= limit {
				continue
			}
			id := beam[rank]
			if _, done := expanded[qi][id]; done {
				continue
			}
			// Leave this node for the next round if its complete adjacency list
			// cannot fit. Never mark a partly explored node as expanded.
			if int(id) < len(g.neighbors) && len(out)+len(g.neighbors[id]) > limit {
				continue
			}
			expanded[qi][id] = struct{}{}
			grew = true
			if int(id) < len(g.neighbors) {
				for _, n := range g.neighbors[id] {
					add(n)
				}
			}
		}
	}
	g.unionBuf = out
	return out, grew
}

// signatureOf is the lexical signature of an inserted vector (0: none).
func (g *hnswBuildGraphSnapshot) signatureOf(id uint32) uint64 {
	if int(id) < len(g.signatures) {
		return g.signatures[id]
	}
	return 0
}

type hnswBuildPair struct {
	id      string
	vec     []float32
	hint    LexicalSeedHint
	hasHint bool
}

type hnswBuildStats struct {
	Strategy     string
	Backend      string
	Batches      int
	KernelErrors int
	Fallback     bool
	Duration     time.Duration
}

type hnswBuildIterator func(batchSize int, fn func([]hnswBuildPair) error) error

type hnswBuildGraphSnapshot struct {
	dim           int
	entryPoint    uint32
	hasEntryPoint bool
	vectors       [][]float32
	neighbors     [][]uint32
	seen          []uint32
	seenGen       uint32
	unionBuf      []uint32
	vecBuf        [][]float32

	// Per query of the batch being searched, in the order passed to
	// CandidateSearchGraph: where its beam starts (its level-0 entry point,
	// then lexical seeds) and its lexical signature (0: none).
	querySeeds      [][]uint32
	querySignatures []uint64
	// signatures holds each inserted vector's lexical signature by internal ID
	// (0: none), for ordering equal-distance candidates.
	signatures []uint64
}

// CPUHNSWBuildAccelerator is the CPU build kernel: exact all-pairs scoring of
// a build batch, parallel across queries. The HNSW build uses it when no GPU is
// enabled or available.
type CPUHNSWBuildAccelerator struct {
	dim int
}

// NewCPUHNSWBuildAccelerator creates a CPU implementation of the build
// accelerator interface.
func NewCPUHNSWBuildAccelerator() *CPUHNSWBuildAccelerator {
	return &CPUHNSWBuildAccelerator{}
}

func (a *CPUHNSWBuildAccelerator) Prepare(dim int, _ int) error {
	if dim <= 0 {
		return localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(dim), nil)
	}
	a.dim = dim
	return nil
}

func (a *CPUHNSWBuildAccelerator) CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) ([][]int, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || len(frontier) == 0 {
		return make([][]int, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	outIdx := make([][]int, len(queries))
	outDist := make([][]float32, len(queries))
	err := parallelOver(ctx, len(queries), func(qi int) {
		q := queries[qi]
		normalized := append([]float32(nil), q...)
		vector.NormalizeInPlace(normalized)
		cands := make([]hnswCandidateDistance, 0, len(frontier))
		for i, f := range frontier {
			if len(f) != a.dim {
				continue
			}
			cands = append(cands, hnswCandidateDistance{
				index: i,
				dist:  float32(1.0) - vector.DotProductSIMD(normalized, f),
			})
		}
		sortHNSWBuildCandidates(cands)
		if topK < len(cands) {
			cands = cands[:topK]
		}
		outIdx[qi] = make([]int, len(cands))
		outDist[qi] = make([]float32, len(cands))
		for i, c := range cands {
			outIdx[qi][i] = c.index
			outDist[qi][i] = c.dist
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return outIdx, outDist, nil
}

func (a *CPUHNSWBuildAccelerator) Close() error {
	return nil
}

// MetalHNSWBuildAccelerator uses the existing Metal cosine/top-k kernels for
// construction candidate search.
type MetalHNSWBuildAccelerator struct {
	device              *metal.Device
	dim                 int
	maxFrontierPerShard int

	// resident holds the inserted vectors (residentN of them, room for
	// residentCap) for CandidateSearchInserted.
	resident    *metal.Buffer
	residentN   int
	residentCap int
	// scores is scoreTopK's reusable score matrix (scoresCap floats).
	scores    *metal.Buffer
	scoresCap int
}

// hnswBuildScoreFloats bounds each backend's score matrix for one scoring call
// (queries × vectors), 256 MB; queries are split to fit.
const hnswBuildScoreFloats = 64 << 20

// NewMetalHNSWBuildAccelerator creates a Metal-backed HNSW build accelerator.
func NewMetalHNSWBuildAccelerator() (*MetalHNSWBuildAccelerator, error) {
	device, err := metal.NewDevice()
	if err != nil {
		return nil, err
	}
	return &MetalHNSWBuildAccelerator{
		device:              device,
		maxFrontierPerShard: 65536,
	}, nil
}

// Prepare sets the dimension and reserves the resident buffer for maxNodes
// vectors. Without the room, the build uses the graph search.
func (a *MetalHNSWBuildAccelerator) Prepare(dim int, maxNodes int) error {
	if dim <= 0 {
		return localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(dim), nil)
	}
	a.dim = dim
	a.releaseResident()
	if a.device != nil && maxNodes > 0 {
		if buf, err := a.device.NewEmptyBuffer(uint64(maxNodes)*uint64(dim)*4, metal.StorageShared); err == nil {
			a.resident, a.residentCap = buf, maxNodes
		} else {
			logSearchPrintf("[HNSW] Metal build: no room for %d resident vectors, using graph search: %v", maxNodes, err)
		}
	}
	return nil
}

func (a *MetalHNSWBuildAccelerator) ResidentReady() bool {
	return a.device != nil && a.resident != nil
}

// AppendInserted copies vecs (normalized) into the resident buffer after the
// vectors already there, doubling the buffer when the build outgrows maxNodes.
func (a *MetalHNSWBuildAccelerator) AppendInserted(vecs [][]float32) error {
	if !a.ResidentReady() {
		return metal.ErrMetalNotAvailable
	}
	if need := a.residentN + len(vecs); need > a.residentCap {
		newCap := max(need, a.residentCap*2)
		buf, err := a.device.NewEmptyBuffer(uint64(newCap)*uint64(a.dim)*4, metal.StorageShared)
		if err != nil {
			return err
		}
		if err := buf.WriteFloat32(a.resident.ReadFloat32(a.residentN*a.dim), 0); err != nil {
			buf.Release()
			return err
		}
		a.resident.Release()
		a.resident, a.residentCap = buf, newCap
	}
	flat := flattenHNSWBuildQueriesInto(nil, vecs, a.dim)
	if len(flat) != len(vecs)*a.dim {
		return localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(a.dim), nil)
	}
	if err := a.resident.WriteFloat32(flat, a.residentN*a.dim); err != nil {
		return err
	}
	a.residentN += len(vecs)
	return nil
}

// CandidateSearchInserted scores the queries against every resident vector
// (scoreTopK) and keeps each query's topK, at most 256.
func (a *MetalHNSWBuildAccelerator) CandidateSearchInserted(ctx context.Context, queries [][]float32, topK int) ([][]uint32, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || a.residentN == 0 {
		return make([][]uint32, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if !a.ResidentReady() {
		return nil, nil, metal.ErrMetalNotAvailable
	}
	return a.scoreTopK(ctx, a.resident, a.residentN, queries, min(topK, 256))
}

func (a *MetalHNSWBuildAccelerator) releaseResident() {
	if a.resident != nil {
		a.resident.Release()
	}
	a.resident, a.residentN, a.residentCap = nil, 0, 0
	if a.scores != nil {
		a.scores.Release()
	}
	a.scores, a.scoresCap = nil, 0
}

func (a *MetalHNSWBuildAccelerator) CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) ([][]int, [][]float32, error) {
	return a.candidateSearch(ctx, queries, frontier, topK, true)
}

func (a *MetalHNSWBuildAccelerator) candidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int, wantDistances bool) ([][]int, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || len(frontier) == 0 {
		return make([][]int, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if a.device == nil {
		return nil, nil, metal.ErrMetalNotAvailable
	}
	if topK > 256 {
		topK = 256
	}
	shardSize := a.maxFrontierPerShard
	if shardSize <= 0 {
		shardSize = 65536
	}
	merged := make([][]hnswCandidateDistance, len(queries))
	flat := make([]float32, 0, min(len(frontier), shardSize)*a.dim)
	for start := 0; start < len(frontier); start += shardSize {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		end := min(start+shardSize, len(frontier))
		flat = flattenHNSWBuildQueriesInto(flat[:0], frontier[start:end], a.dim)
		if len(flat) != (end-start)*a.dim {
			return nil, nil, localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(a.dim), nil)
		}
		frontierBuf, err := a.device.NewBuffer(flat, metal.StorageShared)
		if err != nil {
			return nil, nil, err
		}
		ids, dists, err := a.scoreTopK(ctx, frontierBuf, end-start, queries, topK)
		frontierBuf.Release()
		if err != nil {
			return nil, nil, err
		}
		for qi := range ids {
			for i, id := range ids[qi] {
				merged[qi] = append(merged[qi], hnswCandidateDistance{index: start + int(id), dist: dists[qi][i]})
			}
		}
	}

	outIdx := make([][]int, len(queries))
	var outDist [][]float32
	if wantDistances {
		outDist = make([][]float32, len(queries))
	}
	for qi := range queries {
		sortHNSWBuildCandidates(merged[qi])
		if topK < len(merged[qi]) {
			merged[qi] = merged[qi][:topK]
		}
		outIdx[qi] = make([]int, len(merged[qi]))
		if wantDistances {
			outDist[qi] = make([]float32, len(merged[qi]))
		}
		for i, c := range merged[qi] {
			outIdx[qi][i] = c.index
			if wantDistances {
				outDist[qi][i] = c.dist
			}
		}
	}
	return outIdx, outDist, nil
}

// scoreTopK scores queries against the first frontierN normalized vectors of
// frontier: one Metal Performance Shaders matrix product per chunk of queries
// (the score matrix bounded by hnswBuildScoreFloats), then each query's
// topK picked on the CPU from the score matrix in shared memory, in parallel.
// Results are closest first with cosine distances; equal scores keep the lower
// index.
func (a *MetalHNSWBuildAccelerator) scoreTopK(ctx context.Context, frontier *metal.Buffer, frontierN int, queries [][]float32, topK int) ([][]uint32, [][]float32, error) {
	outIdx := make([][]uint32, len(queries))
	outDist := make([][]float32, len(queries))
	topK = min(topK, frontierN)
	if topK <= 0 {
		return outIdx, outDist, ctx.Err()
	}
	chunk := max(1, min(len(queries), hnswBuildScoreFloats/frontierN))
	var flat []float32
	for start := 0; start < len(queries); start += chunk {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		end := min(start+chunk, len(queries))
		rows := end - start
		flat = flattenHNSWBuildQueriesInto(flat[:0], queries[start:end], a.dim)
		if len(flat) != rows*a.dim {
			return nil, nil, localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(a.dim), nil)
		}
		queryBuf, err := a.device.NewBuffer(flat, metal.StorageShared)
		if err != nil {
			return nil, nil, err
		}
		scores, err := a.scoreScratch(rows * frontierN)
		if err == nil {
			err = a.device.MPSMatrixMultiplyTransposeB(queryBuf, frontier, scores, uint32(rows), uint32(frontierN), uint32(a.dim))
		}
		queryBuf.Release()
		if err != nil {
			return nil, nil, err
		}
		matrix := scores.Float32View(rows * frontierN)
		if err := parallelOver(ctx, rows, func(q int) {
			outIdx[start+q], outDist[start+q] = topKScoreRow(matrix[q*frontierN:(q+1)*frontierN], topK)
		}); err != nil {
			return nil, nil, err
		}
	}
	return outIdx, outDist, nil
}

// scoreScratch is a reusable shared buffer of at least floats float32s.
func (a *MetalHNSWBuildAccelerator) scoreScratch(floats int) (*metal.Buffer, error) {
	if a.scores != nil && a.scoresCap >= floats {
		return a.scores, nil
	}
	if a.scores != nil {
		a.scores.Release()
		a.scores, a.scoresCap = nil, 0
	}
	buf, err := a.device.NewEmptyBuffer(uint64(floats)*4, metal.StorageShared)
	if err != nil {
		return nil, err
	}
	a.scores, a.scoresCap = buf, floats
	return buf, nil
}

// topKScoreRow returns the indices of row's k highest scores, highest first
// (equal scores: lower index first), with distances 1-score.
func topKScoreRow(row []float32, k int) ([]uint32, []float32) {
	// heap is a min-heap of the best k seen: its root is the worst of them.
	heap := make([]hnswCandidateDistance, 0, k)
	worse := func(x, y hnswCandidateDistance) bool { // x ranks below y
		return x.dist < y.dist || (x.dist == y.dist && x.index > y.index)
	}
	down := func(i int) {
		for {
			l, r, m := 2*i+1, 2*i+2, i
			if l < len(heap) && worse(heap[l], heap[m]) {
				m = l
			}
			if r < len(heap) && worse(heap[r], heap[m]) {
				m = r
			}
			if m == i {
				return
			}
			heap[i], heap[m] = heap[m], heap[i]
			i = m
		}
	}
	for i, score := range row {
		c := hnswCandidateDistance{index: i, dist: score} // dist holds the score here
		if len(heap) < k {
			heap = append(heap, c)
			for j := len(heap) - 1; j > 0; {
				p := (j - 1) / 2
				if !worse(heap[j], heap[p]) {
					break
				}
				heap[j], heap[p] = heap[p], heap[j]
				j = p
			}
			continue
		}
		if score > heap[0].dist {
			heap[0] = c
			down(0)
		}
	}
	ids := make([]uint32, len(heap))
	dists := make([]float32, len(heap))
	for n := len(heap) - 1; n >= 0; n-- {
		ids[n], dists[n] = uint32(heap[0].index), 1-heap[0].dist
		heap[0] = heap[len(heap)-1]
		heap = heap[:len(heap)-1]
		down(0)
	}
	return ids, dists
}

func (a *MetalHNSWBuildAccelerator) CandidateSearchGraph(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || graph == nil || !graph.hasEntryPoint || len(graph.vectors) == 0 {
		return make([][]uint32, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if a.device == nil {
		return nil, nil, metal.ErrMetalNotAvailable
	}
	if topK > 256 {
		topK = 256
	}
	return candidateSearchGraphBatched(ctx, a, queries, graph, topK)
}

func (a *MetalHNSWBuildAccelerator) Close() error {
	a.releaseResident()
	if a.device != nil {
		a.device.Release()
		a.device = nil
	}
	return nil
}

type hnswCandidateDistance struct {
	index int
	dist  float32
}

func sortHNSWBuildCandidates(cands []hnswCandidateDistance) {
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].dist == cands[j].dist {
			return cands[i].index < cands[j].index
		}
		return cands[i].dist < cands[j].dist
	})
}

func flattenHNSWBuildFrontierInto(flat []float32, frontier [][]float32, dim int) []float32 {
	for _, vec := range frontier {
		if len(vec) != dim {
			continue
		}
		flat = append(flat, vec...)
	}
	return flat
}

func flattenHNSWBuildQueriesInto(flat []float32, queries [][]float32, dim int) []float32 {
	if cap(flat) < len(queries)*dim {
		flat = make([]float32, 0, len(queries)*dim)
	}
	for _, vec := range queries {
		if len(vec) != dim {
			continue
		}
		start := len(flat)
		flat = append(flat, vec...)
		vector.NormalizeInPlace(flat[start : start+dim])
	}
	return flat
}

func buildHNSWGraphSnapshot(idx *HNSWIndex, frontierVecs [][]float32, reuse *hnswBuildGraphSnapshot) *hnswBuildGraphSnapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if reuse == nil {
		reuse = &hnswBuildGraphSnapshot{}
	}
	if cap(reuse.neighbors) < len(idx.nodeLevel) {
		reuse.neighbors = make([][]uint32, len(idx.nodeLevel))
	} else {
		reuse.neighbors = reuse.neighbors[:len(idx.nodeLevel)]
		clear(reuse.neighbors)
	}
	for i := range idx.nodeLevel {
		internalID := uint32(i)
		if int(internalID) < len(idx.deleted) && idx.deleted[internalID] {
			continue
		}
		ns, ok := idx.neighborsAtLevelLocked(internalID, 0)
		if !ok {
			continue
		}
		reuse.neighbors[i] = ns
	}
	reuse.dim = idx.dimensions
	reuse.entryPoint = idx.entryPoint
	reuse.hasEntryPoint = idx.hasEntryPoint
	reuse.vectors = frontierVecs
	if cap(reuse.seen) < len(frontierVecs) {
		reuse.seen = make([]uint32, len(frontierVecs))
	} else {
		reuse.seen = reuse.seen[:len(frontierVecs)]
	}
	return reuse
}

func (g *hnswBuildGraphSnapshot) seedCandidates(limit int) []uint32 {
	if g == nil || !g.hasEntryPoint || int(g.entryPoint) >= len(g.vectors) {
		return nil
	}
	out := make([]uint32, 0, util.SafePreallocCap(limit, len(g.vectors)))
	out = append(out, g.entryPoint)
	if int(g.entryPoint) < len(g.neighbors) {
		for _, n := range g.neighbors[g.entryPoint] {
			if len(out) >= limit {
				break
			}
			if int(n) >= len(g.vectors) {
				continue
			}
			out = append(out, n)
		}
	}
	return out
}

func (g *hnswBuildGraphSnapshot) vectorsForInternalIDs(ids []uint32) [][]float32 {
	if cap(g.vecBuf) < len(ids) {
		g.vecBuf = make([][]float32, 0, len(ids))
	}
	vecs := g.vecBuf[:0]
	for _, id := range ids {
		if int(id) >= len(g.vectors) {
			continue
		}
		vecs = append(vecs, g.vectors[id])
	}
	g.vecBuf = vecs
	return vecs
}

// newBestHNSWBuildAccelerator tries each available GPU backend in order of
// preference and returns the first one that initializes successfully.
// Falls back to CPU if no GPU backend is available.
func newBestHNSWBuildAccelerator() (HNSWBuildAccelerator, error) {
	// Explicit selection is also used by the cross-backend benchmark. An
	// unavailable selected device reaches the existing CPU fallback, rather
	// than silently substituting another GPU backend.
	switch strings.ToLower(envutil.Get("NORNICDB_GPU_BACKEND", "")) {
	case "cpu":
		return NewCPUHNSWBuildAccelerator(), nil
	case "cuda":
		return NewCudaHNSWBuildAccelerator()
	case "vulkan":
		return NewVulkanHNSWBuildAccelerator()
	case "metal":
		return NewMetalHNSWBuildAccelerator()
	}
	// Try CUDA first (NVIDIA GPUs)
	if accel, err := NewCudaHNSWBuildAccelerator(); err == nil {
		logSearchPrintf("[HNSW] Using CUDA GPU accelerator")
		return accel, nil
	}
	// Try Vulkan next (cross-platform)
	if accel, err := NewVulkanHNSWBuildAccelerator(); err == nil {
		logSearchPrintf("[HNSW] Using Vulkan GPU accelerator")
		return accel, nil
	}
	// Try Metal last (Apple Silicon)
	if accel, err := NewMetalHNSWBuildAccelerator(); err == nil {
		logSearchPrintf("[HNSW] Using Metal GPU accelerator")
		return accel, nil
	}
	return nil, localizedError(localization.SearchGPUAcceleratorUnavailable(), nil)
}

// buildHNSWWithOptionalGPU builds an HNSW index with buildHNSWBatchParallel:
// on a GPU when config.UseGPUBuild is set and one is available (accel, or the
// best available when nil), otherwise, or when the GPU fails, with the CPU
// kernel.
func buildHNSWWithOptionalGPU(ctx context.Context, dimensions int, config HNSWConfig, lookup VectorLookup, total int, iter hnswBuildIterator, accel HNSWBuildAccelerator) (*HNSWIndex, hnswBuildStats, error) {
	cpuBuild := func(reason string, cause error, gpuStats hnswBuildStats) (*HNSWIndex, hnswBuildStats, error) {
		if reason != "" {
			logSearchPrintf("[HNSW] %s, building on the CPU: %v", reason, cause)
		}
		cpu := NewCPUHNSWBuildAccelerator()
		if err := cpu.Prepare(dimensions, total); err != nil {
			return nil, gpuStats, err
		}
		built, stats, err := buildHNSWBatchParallel(ctx, dimensions, config, lookup, total, iter, cpu)
		stats.Fallback = reason != ""
		stats.KernelErrors += gpuStats.KernelErrors
		return built, stats, err
	}
	if !config.UseGPUBuild {
		return cpuBuild("", nil, hnswBuildStats{})
	}
	if accel == nil {
		var err error
		if accel, err = newBestHNSWBuildAccelerator(); err != nil {
			return cpuBuild("GPU build unavailable", err, hnswBuildStats{})
		}
	}
	if err := accel.Prepare(dimensions, total); err != nil {
		_ = accel.Close()
		return cpuBuild("GPU build prepare failed", err, hnswBuildStats{})
	}
	defer accel.Close()

	built, stats, err := buildHNSWBatchParallel(ctx, dimensions, config, lookup, total, iter, accel)
	if err == nil {
		return built, stats, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, stats, ctxErr
	}
	stats.KernelErrors++
	return cpuBuild(fmt.Sprintf("GPU build failed after %d batches", stats.Batches), err, stats)
}

// buildHNSWBatchParallel builds an HNSW index batch by batch. For each batch:
//
//  1. Every vector's level-0 candidates among the vectors already in the index
//     come from a beam search of width efConstruction that starts at the
//     vector's own level-0 entry point (the upper-layer descent an insert
//     makes) and at inserted vectors sharing a lexical signature band with it
//     when the build has BM25 hints. A GPU accelerator holding the inserted
//     vectors on the device (HNSWResidentBuildAccelerator: Metal) instead
//     returns each vector's exact nearest inserted vectors, which a beam search
//     approximates; one with a graph search (HNSWGraphBuildAccelerator: CUDA,
//     Metal, Vulkan) runs the beam search on the device
//     (CandidateSearchGraph); otherwise each vector runs the insert's own
//     search (buildLevelZeroCandidates), in parallel, since it only reads.
//  2. accel scores the batch against itself, so each vector also gets the
//     closest members of its batch inserted before it, which step 1 cannot see.
//  3. The vectors are inserted in order, each linked at level 0 from those
//     candidates (equal distances ordered by lexical closeness) and at upper
//     levels by the usual insert.
//
// The result matches a one-at-a-time insert's quality; only the order in which
// a batch's vectors see each other differs.
func buildHNSWBatchParallel(ctx context.Context, dimensions int, config HNSWConfig, lookup VectorLookup, total int, iter hnswBuildIterator, accel HNSWBuildAccelerator) (*HNSWIndex, hnswBuildStats, error) {
	started := time.Now()
	built := NewHNSWIndex(dimensions, config)
	if lookup != nil {
		built.SetVectorLookup(lookup)
	}
	stats := hnswBuildStats{Strategy: "batch_parallel", Backend: fmt.Sprintf("%T", accel)}
	ef := max(config.GPUBuildCandidateK, config.EfConstruction, config.M)
	bands := newLexicalBandIndex()
	signatures := make(map[uint32]uint64)
	resident, onDevice := accel.(HNSWResidentBuildAccelerator)
	onDevice = onDevice && resident.ResidentReady()
	graphAccel, onGPU := accel.(HNSWGraphBuildAccelerator)
	onGPU = onGPU && !onDevice
	var residentVecs [][]float32 // the batch's vectors to append, by internal ID
	var graphSnapshot *hnswBuildGraphSnapshot
	var frontierVecs [][]float32 // by internal ID: what a GPU graph search scores
	var signaturesByID []uint64
	added := 0

	err := iter(config.GPUBuildBatchSize, func(batch []hnswBuildPair) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.Batches++
		batchStart := time.Now()
		queries := make([][]float32, len(batch))
		for i := range batch {
			queries[i] = batch[i].vec
			if batch[i].hasHint {
				built.setBuildLexicalHint(batch[i].hint)
			}
		}

		graphIDs := make([][]uint32, len(batch))
		graphDists := make([][]float32, len(batch))
		var searchErr error
		var searchErrOnce sync.Once
		if onDevice {
			if added > 0 {
				found, dists, err := resident.CandidateSearchInserted(ctx, queries, ef)
				if err != nil {
					stats.KernelErrors++
					return err
				}
				copy(graphIDs, found)
				copy(graphDists, dists)
			}
		} else if onGPU && len(frontierVecs) > 0 {
			graphSnapshot = buildHNSWGraphSnapshot(built, frontierVecs, graphSnapshot)
			graphSnapshot.signatures = signaturesByID
			order := seedGraphQueries(ctx, built, graphSnapshot, batch, bands)
			ordered := make([][]float32, len(order))
			for i, qi := range order {
				ordered[i] = queries[qi]
			}
			found, dists, err := graphAccel.CandidateSearchGraph(ctx, ordered, graphSnapshot, ef)
			if err != nil {
				stats.KernelErrors++
				return err
			}
			for i, qi := range order {
				if i < len(found) {
					graphIDs[qi] = found[i]
				}
				if i < len(dists) {
					graphDists[qi] = dists[i]
				}
			}
		} else if err := parallelOver(ctx, len(batch), func(i int) {
			normalized := append([]float32(nil), batch[i].vec...)
			vector.NormalizeInPlace(normalized)
			var entries []string
			if batch[i].hasHint {
				entries = bands.seeds(batch[i].hint.Signature, lexicalSeedsPerQuery)
			}
			ids, dists, err := built.buildLevelZeroCandidates(ctx, normalized, ef, entries)
			if err != nil {
				searchErrOnce.Do(func() { searchErr = err })
				return
			}
			graphIDs[i], graphDists[i] = ids, dists
		}); err != nil {
			return err
		}
		if searchErr != nil {
			return searchErr
		}

		var batchNeighbors [][]int
		var batchDists [][]float32
		if len(batch) > 1 {
			var err error
			if batchNeighbors, batchDists, err = accel.CandidateSearch(ctx, queries, queries, ef); err != nil {
				stats.KernelErrors++
				return err
			}
		}

		searched := time.Since(batchStart)
		batchInternalIDs := make([]uint32, len(batch))
		for i, p := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			candidates := graphIDs[i]
			dists := graphDists[i]
			if i < len(batchNeighbors) {
				for k, j := range batchNeighbors[i] {
					if j >= 0 && j < i {
						candidates = append(candidates, batchInternalIDs[j])
						if i < len(batchDists) && k < len(batchDists[i]) {
							dists = append(dists, batchDists[i][k])
						}
					}
				}
			}
			if len(dists) == len(candidates) {
				sortCandidatesByDistance(candidates, dists)
				if p.hasHint {
					orderLexicalTies(p.hint.Signature, candidates, dists, func(id uint32) uint64 { return signatures[id] })
				}
			}
			if err := built.addWithLevel0Candidates(p.id, p.vec, candidates); err != nil {
				return fmt.Errorf("failed to add vector to HNSW: %w", err)
			}
			internalID, ok := built.internalID(p.id)
			if !ok {
				return fmt.Errorf("failed to resolve HNSW internal ID for %q", p.id)
			}
			batchInternalIDs[i] = internalID
			if onDevice {
				if int(internalID) != added {
					return fmt.Errorf("HNSW build: internal ID %d of %q is not the next resident vector %d", internalID, p.id, added)
				}
				residentVecs = append(residentVecs, hnswBuildVectorReference(built, internalID, p.vec))
			}
			if onGPU {
				frontierVecs = append(frontierVecs, hnswBuildVectorReference(built, internalID, p.vec))
				for len(signaturesByID) <= int(internalID) {
					signaturesByID = append(signaturesByID, 0)
				}
				if p.hasHint {
					signaturesByID[internalID] = p.hint.Signature
				}
			}
			if p.hasHint && p.hint.Signature != 0 {
				signatures[internalID] = p.hint.Signature
				bands.add(p.hint.Signature, p.id)
			}
			added++
			if added%hnswProgressInterval == 0 || added == total {
				logSearchPrintf("[HNSW] 🔨 Progress: %d / %d vectors", added, total)
			}
		}
		if onDevice {
			if err := resident.AppendInserted(residentVecs); err != nil {
				stats.KernelErrors++
				return err
			}
			residentVecs = residentVecs[:0]
		}
		elapsed := time.Since(batchStart)
		logSearchPrintf("[HNSW] build batch=%d vectors=%d indexed=%d duration=%s search=%s link=%s", stats.Batches, len(batch), added, elapsed, searched, elapsed-searched)
		return nil
	})
	stats.Duration = time.Since(started)
	return built, stats, err
}

// seedGraphQueries sets graph.querySeeds and graph.querySignatures for a GPU
// graph search of a batch and returns the order to search it in. Each vector's
// beam starts at its own level-0 entry point (the upper-layer descent an insert
// makes), then at inserted vectors sharing a lexical signature band with it when
// it has BM25 hints. The order sorts the batch by entry point, so a group of
// queries searched together starts in one region of the graph.
func seedGraphQueries(ctx context.Context, built *HNSWIndex, graph *hnswBuildGraphSnapshot, batch []hnswBuildPair, bands *lexicalBandIndex) []int {
	entries := make([]uint32, len(batch))
	seeds := make([][]uint32, len(batch))
	signatures := make([]uint64, len(batch))
	normalized := make([]float32, graph.dim)
	for i, pair := range batch {
		copy(normalized, pair.vec)
		vector.NormalizeInPlace(normalized)
		if entry, ok := built.levelZeroEntry(ctx, normalized); ok {
			entries[i] = entry
			seeds[i] = append(seeds[i], entry)
		}
		if pair.hasHint {
			signatures[i] = pair.hint.Signature
			for _, id := range bands.seeds(pair.hint.Signature, lexicalSeedsPerQuery) {
				if internalID, ok := built.internalID(id); ok && !slices.Contains(seeds[i], internalID) {
					seeds[i] = append(seeds[i], internalID)
				}
			}
		}
	}
	order := make([]int, len(batch))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return entries[order[a]] < entries[order[b]] })
	graph.querySeeds = make([][]uint32, len(batch))
	graph.querySignatures = make([]uint64, len(batch))
	for i, qi := range order {
		graph.querySeeds[i], graph.querySignatures[i] = seeds[qi], signatures[qi]
	}
	return order
}

// parallelOver runs fn(i) for i in [0, n) on GOMAXPROCS workers and returns
// ctx's error if it is canceled before every index ran.
func parallelOver(ctx context.Context, n int, fn func(i int)) error {
	workers := min(runtime.GOMAXPROCS(0), n)
	if workers <= 1 {
		for i := 0; i < n; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			fn(i)
		}
		return nil
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n || ctx.Err() != nil {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// sortCandidatesByDistance orders candidates and their distances closest
// first, both in place; a candidate listed twice keeps its closest entry.
func sortCandidatesByDistance(candidates []uint32, dists []float32) {
	order := make([]int, len(candidates))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return dists[order[a]] < dists[order[b]] })
	ids := make([]uint32, len(order))
	ds := make([]float32, len(order))
	for i, o := range order {
		ids[i], ds[i] = candidates[o], dists[o]
	}
	copy(candidates, ids)
	copy(dists, ds)
}

// orderLexicalTies orders runs of equal distance in candidates (closest first,
// dists alongside) by how close each candidate's lexical signature is to the
// query's, fewer differing bits first and candidates without one last, as the
// CPU insert breaks equal-distance choices with the BM25 signature. Both slices
// are reordered in place.
func orderLexicalTies(query uint64, candidates []uint32, dists []float32, signatureOf func(uint32) uint64) {
	if query == 0 || len(dists) != len(candidates) {
		return
	}
	closeness := func(id uint32) int {
		if sig := signatureOf(id); sig != 0 {
			return bits.OnesCount64(query ^ sig)
		}
		return 65
	}
	for start := 0; start < len(candidates); {
		end := start + 1
		for end < len(candidates) && dists[end] == dists[start] {
			end++
		}
		if end-start > 1 {
			run := candidates[start:end]
			sort.SliceStable(run, func(i, j int) bool { return closeness(run[i]) < closeness(run[j]) })
		}
		start = end
	}
}

// lexicalSeedsPerQuery bounds the lexical entry points of a build search.
const lexicalSeedsPerQuery = 8

// lexicalBandIndex finds already-inserted vectors whose BM25 lexical signature
// (LexicalSeedHint.Signature, a SimHash of high-IDF terms) shares one of four
// 16-bit bands with a query's: they share weighted terms, so they are likely
// near it. Each bucket keeps its first lexicalBandBucketMax members: the build
// inserts high-IDF seeded vectors first.
type lexicalBandIndex struct {
	buckets map[uint64][]string
}

const (
	lexicalSignatureBands = 4
	lexicalBandBucketMax  = 32
)

func newLexicalBandIndex() *lexicalBandIndex {
	return &lexicalBandIndex{buckets: make(map[uint64][]string)}
}

func lexicalBandKey(band int, signature uint64) uint64 {
	return uint64(band)<<16 | (signature>>(16*uint(band)))&0xffff
}

func (b *lexicalBandIndex) add(signature uint64, id string) {
	if signature == 0 {
		return
	}
	for band := 0; band < lexicalSignatureBands; band++ {
		key := lexicalBandKey(band, signature)
		if len(b.buckets[key]) < lexicalBandBucketMax {
			b.buckets[key] = append(b.buckets[key], id)
		}
	}
}

// seeds returns up to limit inserted vectors sharing a band with signature,
// without duplicates; none for a zero signature (no hint).
func (b *lexicalBandIndex) seeds(signature uint64, limit int) []string {
	if signature == 0 || limit <= 0 {
		return nil
	}
	var out []string
	for band := 0; band < lexicalSignatureBands && len(out) < limit; band++ {
		for _, id := range b.buckets[lexicalBandKey(band, signature)] {
			if len(out) >= limit {
				break
			}
			if !slices.Contains(out, id) {
				out = append(out, id)
			}
		}
	}
	return out
}

func hnswBuildVectorReference(idx *HNSWIndex, internalID uint32, fallback []float32) []float32 {
	idx.mu.RLock()
	if idx.vectorLookup == nil && int(internalID) < len(idx.vecOff) {
		off := int(idx.vecOff[internalID])
		if off >= 0 && off+idx.dimensions <= len(idx.vectors) {
			vec := idx.vectors[off : off+idx.dimensions]
			idx.mu.RUnlock()
			return vec
		}
	}
	dim := idx.dimensions
	idx.mu.RUnlock()

	normalized := make([]float32, dim)
	copy(normalized, fallback)
	vector.NormalizeInPlace(normalized)
	return normalized
}
