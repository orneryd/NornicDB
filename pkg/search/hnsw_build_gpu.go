package search

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu/metal"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/util"
)

const hnswProgressInterval = 50000

// HNSWBuildAccelerator supplies nearest-neighbor candidate search for HNSW
// construction. The CPU still performs all graph mutation and reciprocal
// linking, so persisted artifacts remain compatible with CPU-built indexes.
type HNSWBuildAccelerator interface {
	Prepare(dim int, maxNodes int) error
	CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) (indices [][]int, distances [][]float32, err error)
	Close() error
}

type HNSWGraphBuildAccelerator interface {
	CandidateSearchGraph(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) (indices [][]uint32, distances [][]float32, err error)
}

type hnswBuildPair struct {
	id  string
	vec []float32
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
}

// CPUHNSWBuildAccelerator is a deterministic test/development shim for the
// build accelerator interface. Production CPU fallback uses the existing HNSW
// insertion path instead of this O(N^2) candidate search.
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
	for qi, q := range queries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
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
	maxQueriesPerShard  int
}

// NewMetalHNSWBuildAccelerator creates a Metal-backed HNSW build accelerator.
func NewMetalHNSWBuildAccelerator() (*MetalHNSWBuildAccelerator, error) {
	device, err := metal.NewDevice()
	if err != nil {
		return nil, err
	}
	return &MetalHNSWBuildAccelerator{
		device:              device,
		maxFrontierPerShard: 65536,
		maxQueriesPerShard:  512,
	}, nil
}

func (a *MetalHNSWBuildAccelerator) Prepare(dim int, _ int) error {
	if dim <= 0 {
		return localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(dim), nil)
	}
	a.dim = dim
	return nil
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
	queryShardSize := a.maxQueriesPerShard
	if queryShardSize <= 0 {
		queryShardSize = 512
	}
	merged := make([][]hnswCandidateDistance, len(queries))
	flat := make([]float32, 0, min(len(frontier), shardSize)*a.dim)
	flatQueries := make([]float32, 0, min(len(queries), queryShardSize)*a.dim)
	for start := 0; start < len(frontier); start += shardSize {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		end := start + shardSize
		if end > len(frontier) {
			end = len(frontier)
		}
		flat = flattenHNSWBuildFrontierInto(flat[:0], frontier[start:end], a.dim)
		if len(flat) == 0 {
			continue
		}
		frontierBuf, err := a.device.NewBuffer(flat, metal.StorageShared)
		if err != nil {
			return nil, nil, err
		}
		for qStart := 0; qStart < len(queries); qStart += queryShardSize {
			if err := ctx.Err(); err != nil {
				frontierBuf.Release()
				return nil, nil, err
			}
			qEnd := qStart + queryShardSize
			if qEnd > len(queries) {
				qEnd = len(queries)
			}
			flatQueries = flattenHNSWBuildQueriesInto(flatQueries[:0], queries[qStart:qEnd], a.dim)
			queryBuf, err := a.device.NewBuffer(flatQueries, metal.StorageShared)
			if err != nil {
				frontierBuf.Release()
				return nil, nil, err
			}
			indices, scores, err := a.device.HNSWBuildTopK(frontierBuf, queryBuf, uint32(end-start), uint32(qEnd-qStart), uint32(a.dim), topK)
			queryBuf.Release()
			if err != nil {
				frontierBuf.Release()
				return nil, nil, err
			}
			for localQ := 0; localQ < qEnd-qStart; localQ++ {
				globalQ := qStart + localQ
				row := localQ * topK
				for i := 0; i < topK && row+i < len(indices) && row+i < len(scores); i++ {
					localIdx := int(indices[row+i])
					idx := start + localIdx
					if localIdx < 0 || idx < start || idx >= end {
						continue
					}
					merged[globalQ] = append(merged[globalQ], hnswCandidateDistance{
						index: idx,
						dist:  float32(1.0) - scores[row+i],
					})
				}
			}
		}
		frontierBuf.Release()
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
	groupSize := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_QUERY_GROUP", 512)
	if groupSize <= 0 {
		groupSize = 512
	}
	outIdx := make([][]uint32, len(queries))
	outDist := make([][]float32, len(queries))
	for start := 0; start < len(queries); start += groupSize {
		end := start + groupSize
		if end > len(queries) {
			end = len(queries)
		}
		idx, dist, err := a.candidateSearchGraphGroup(ctx, queries[start:end], graph, topK)
		if err != nil {
			return nil, nil, err
		}
		copy(outIdx[start:end], idx)
		copy(outDist[start:end], dist)
	}
	return outIdx, outDist, nil
}

func (a *MetalHNSWBuildAccelerator) candidateSearchGraphGroup(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
	defaultBeamWidth := topK
	if defaultBeamWidth > 64 {
		defaultBeamWidth = 64
	}
	beamWidth := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_WIDTH", defaultBeamWidth)
	if beamWidth <= 0 {
		beamWidth = topK
	}
	if beamWidth > 256 {
		beamWidth = 256
	}
	iterations := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_ITERS", 2)
	if iterations <= 0 {
		iterations = 2
	}
	unionMax := envutil.GetInt("NORNICDB_HNSW_BUILD_GPU_BEAM_UNION_MAX", 4096)
	if unionMax <= 0 {
		unionMax = 4096
	}

	beamStorage := make([]uint32, 0, len(queries)*beamWidth)
	beams := make([][]uint32, len(queries))
	seed := graph.seedCandidates(beamWidth)
	for i := range beams {
		start := len(beamStorage)
		beamStorage = append(beamStorage, seed...)
		beams[i] = beamStorage[start:]
	}
	for iter := 0; iter < iterations; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		unionIDs := graph.expandBeamUnion(beams, unionMax)
		if len(unionIDs) == 0 {
			break
		}
		unionVectors := graph.vectorsForInternalIDs(unionIDs)
		localIdx, _, err := a.candidateSearch(ctx, queries, unionVectors, beamWidth, false)
		if err != nil {
			return nil, nil, err
		}
		nextStorage := make([]uint32, 0, len(localIdx)*beamWidth)
		next := make([][]uint32, len(queries))
		for qi := range localIdx {
			start := len(nextStorage)
			for _, idx := range localIdx[qi] {
				if idx < 0 || idx >= len(unionIDs) {
					continue
				}
				nextStorage = append(nextStorage, unionIDs[idx])
			}
			next[qi] = nextStorage[start:]
		}
		beams = next
	}
	for qi := range beams {
		if topK < len(beams[qi]) {
			beams[qi] = beams[qi][:topK]
		}
	}
	return beams, nil, nil
}

func (a *MetalHNSWBuildAccelerator) Close() error {
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

func flattenHNSWBuildFrontier(frontier [][]float32, dim int) []float32 {
	return flattenHNSWBuildFrontierInto(make([]float32, 0, len(frontier)*dim), frontier, dim)
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

func flattenHNSWBuildQueries(queries [][]float32, dim int) []float32 {
	return flattenHNSWBuildQueriesInto(make([]float32, 0, len(queries)*dim), queries, dim)
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

func (g *hnswBuildGraphSnapshot) expandBeamUnion(beams [][]uint32, limit int) []uint32 {
	if g == nil || limit <= 0 {
		return nil
	}
	g.seenGen++
	if g.seenGen == 0 {
		clear(g.seen)
		g.seenGen = 1
	}
	if cap(g.unionBuf) < min(limit, len(g.vectors)) {
		g.unionBuf = make([]uint32, 0, min(limit, len(g.vectors)))
	}
	out := g.unionBuf[:0]
	add := func(id uint32) bool {
		if int(id) >= len(g.vectors) {
			return len(out) < limit
		}
		if g.seen[id] == g.seenGen {
			return len(out) < limit
		}
		g.seen[id] = g.seenGen
		out = append(out, id)
		return len(out) < limit
	}
	for _, beam := range beams {
		for _, id := range beam {
			if !add(id) {
				return out
			}
			if int(id) >= len(g.neighbors) {
				continue
			}
			for _, n := range g.neighbors[id] {
				if !add(n) {
					return out
				}
			}
		}
	}
	g.unionBuf = out
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

func buildHNSWCPU(ctx context.Context, dimensions int, config HNSWConfig, lookup VectorLookup, total int, iter hnswBuildIterator) (*HNSWIndex, hnswBuildStats, error) {
	started := time.Now()
	built := NewHNSWIndex(dimensions, config)
	if lookup != nil {
		built.SetVectorLookup(lookup)
	}
	added := 0
	stats := hnswBuildStats{Strategy: "cpu", Backend: "cpu"}
	err := iter(10000, func(batch []hnswBuildPair) error {
		for _, p := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := built.Add(p.id, p.vec); err != nil {
				return fmt.Errorf("failed to add vector to HNSW: %w", err)
			}
			added++
			if added%hnswProgressInterval == 0 || added == total {
				log.Printf("[HNSW] 🔨 Progress: %d / %d vectors", added, total)
			}
		}
		return nil
	})
	stats.Duration = time.Since(started)
	return built, stats, err
}

// newBestHNSWBuildAccelerator tries each available GPU backend in order of
// preference and returns the first one that initializes successfully.
// Falls back to CPU if no GPU backend is available.
func newBestHNSWBuildAccelerator() (HNSWBuildAccelerator, error) {
	// Try CUDA first (NVIDIA GPUs)
	if accel, err := NewCudaHNSWBuildAccelerator(); err == nil {
		log.Printf("[HNSW] Using CUDA GPU accelerator")
		return accel, nil
	}
	// Try Vulkan next (cross-platform)
	if accel, err := NewVulkanHNSWBuildAccelerator(); err == nil {
		log.Printf("[HNSW] Using Vulkan GPU accelerator")
		return accel, nil
	}
	// Try Metal last (Apple Silicon)
	if accel, err := NewMetalHNSWBuildAccelerator(); err == nil {
		log.Printf("[HNSW] Using Metal GPU accelerator")
		return accel, nil
	}
	return nil, localizedError(localization.SearchGPUAcceleratorUnavailable(), nil)
}

func buildHNSWWithOptionalGPU(ctx context.Context, dimensions int, config HNSWConfig, lookup VectorLookup, total int, iter hnswBuildIterator, accel HNSWBuildAccelerator) (*HNSWIndex, hnswBuildStats, error) {
	if !config.UseGPUBuild {
		return buildHNSWCPU(ctx, dimensions, config, lookup, total, iter)
	}
	if accel == nil {
		var err error
		accel, err = newBestHNSWBuildAccelerator()
		if err != nil {
			log.Printf("[HNSW] GPU build unavailable, falling back to CPU: %v", err)
			built, stats, cpuErr := buildHNSWCPU(ctx, dimensions, config, lookup, total, iter)
			stats.Fallback = true
			return built, stats, cpuErr
		}
	}
	if err := accel.Prepare(dimensions, total); err != nil {
		_ = accel.Close()
		log.Printf("[HNSW] GPU build prepare failed, falling back to CPU: %v", err)
		built, stats, cpuErr := buildHNSWCPU(ctx, dimensions, config, lookup, total, iter)
		stats.Fallback = true
		return built, stats, cpuErr
	}
	defer accel.Close()

	built, stats, err := buildHNSWAccelerated(ctx, dimensions, config, lookup, total, iter, accel)
	if err == nil {
		return built, stats, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, stats, ctxErr
	}
	log.Printf("[HNSW] GPU build failed after %d batches, falling back to CPU: %v", stats.Batches, err)
	cpuBuilt, cpuStats, cpuErr := buildHNSWCPU(ctx, dimensions, config, lookup, total, iter)
	cpuStats.Fallback = true
	cpuStats.KernelErrors = stats.KernelErrors + 1
	return cpuBuilt, cpuStats, cpuErr
}

func buildHNSWAccelerated(ctx context.Context, dimensions int, config HNSWConfig, lookup VectorLookup, total int, iter hnswBuildIterator, accel HNSWBuildAccelerator) (*HNSWIndex, hnswBuildStats, error) {
	started := time.Now()
	built := NewHNSWIndex(dimensions, config)
	if lookup != nil {
		built.SetVectorLookup(lookup)
	}
	stats := hnswBuildStats{Strategy: "gpu_assisted", Backend: fmt.Sprintf("%T", accel)}
	frontierVecs := make([][]float32, 0, min(total, config.GPUBuildBatchSize))
	frontierInternalIDs := make([]uint32, 0, min(total, config.GPUBuildBatchSize))
	var graphSnapshot *hnswBuildGraphSnapshot
	added := 0

	err := iter(config.GPUBuildBatchSize, func(batch []hnswBuildPair) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.Batches++
		queries := make([][]float32, len(batch))
		for i := range batch {
			queries[i] = batch[i].vec
		}
		var candidateIdx [][]int
		var candidateInternalIDs [][]uint32
		if len(frontierVecs) > 0 {
			var err error
			batchStart := time.Now()
			if graphAccel, ok := accel.(HNSWGraphBuildAccelerator); ok {
				graphSnapshot = buildHNSWGraphSnapshot(built, frontierVecs, graphSnapshot)
				candidateInternalIDs, _, err = graphAccel.CandidateSearchGraph(ctx, queries, graphSnapshot, config.GPUBuildCandidateK)
			} else {
				candidateIdx, _, err = accel.CandidateSearch(ctx, queries, frontierVecs, config.GPUBuildCandidateK)
			}
			if err != nil {
				stats.KernelErrors++
				return err
			}
			log.Printf("[HNSW] GPU build batch=%d vectors=%d frontier=%d duration=%s", stats.Batches, len(batch), len(frontierVecs), time.Since(batchStart))
		}
		for i, p := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			var candidates []uint32
			if i < len(candidateInternalIDs) {
				candidates = candidateInternalIDs[i]
			} else if i < len(candidateIdx) {
				candidates = make([]uint32, 0, len(candidateIdx[i]))
				for _, idx := range candidateIdx[i] {
					if idx < 0 || idx >= len(frontierInternalIDs) {
						continue
					}
					candidates = append(candidates, frontierInternalIDs[idx])
				}
			}
			if err := built.addWithLevel0Candidates(p.id, p.vec, candidates); err != nil {
				return fmt.Errorf("failed to add vector to HNSW: %w", err)
			}
			internalID, ok := built.internalID(p.id)
			if !ok {
				return fmt.Errorf("failed to resolve HNSW internal ID for %q", p.id)
			}
			normalized := hnswBuildVectorReference(built, internalID, p.vec)
			frontierVecs = append(frontierVecs, normalized)
			frontierInternalIDs = append(frontierInternalIDs, internalID)
			added++
			if added%hnswProgressInterval == 0 || added == total {
				log.Printf("[HNSW] 🔨 Progress: %d / %d vectors", added, total)
			}
		}
		return nil
	})
	stats.Duration = time.Since(started)
	return built, stats, err
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
