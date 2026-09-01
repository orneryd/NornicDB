//go:build cuda && (linux || windows)

package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/gpu/cuda"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// CudaHNSWBuildAccelerator uses CUDA cosine/top-k kernels for HNSW
// construction candidate search. The CPU still performs all graph mutation
// and reciprocal linking, so persisted artifacts remain compatible with
// CPU-built indexes.
type CudaHNSWBuildAccelerator struct {
	device              *cuda.Device
	dim                 int
	maxFrontierPerShard int
	maxQueriesPerShard  int
}

// NewCudaHNSWBuildAccelerator creates a CUDA-backed HNSW build accelerator.
func NewCudaHNSWBuildAccelerator() (*CudaHNSWBuildAccelerator, error) {
	if !cuda.IsAvailable() {
		return nil, localizedError(localization.SearchCUDANotAvailable(), cuda.ErrCUDANotAvailable)
	}
	device, err := cuda.NewDevice(0)
	if err != nil {
		return nil, err
	}
	return &CudaHNSWBuildAccelerator{
		device:              device,
		maxFrontierPerShard: 65536,
		maxQueriesPerShard:  512,
	}, nil
}

func (a *CudaHNSWBuildAccelerator) Prepare(dim int, _ int) error {
	if dim <= 0 {
		return localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(dim), nil)
	}
	a.dim = dim
	return nil
}

func (a *CudaHNSWBuildAccelerator) CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) ([][]int, [][]float32, error) {
	return a.candidateSearch(ctx, queries, frontier, topK, true)
}

func (a *CudaHNSWBuildAccelerator) candidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int, wantDistances bool) ([][]int, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || len(frontier) == 0 {
		return make([][]int, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if a.device == nil {
		return nil, nil, cuda.ErrCUDANotAvailable
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
		frontierBuf, err := a.device.NewBuffer(flat, cuda.MemoryDevice)
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
			queryBuf, err := a.device.NewBuffer(flatQueries, cuda.MemoryDevice)
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

func (a *CudaHNSWBuildAccelerator) CandidateSearchGraph(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || graph == nil || !graph.hasEntryPoint || len(graph.vectors) == 0 {
		return make([][]uint32, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if a.device == nil {
		return nil, nil, cuda.ErrCUDANotAvailable
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

func (a *CudaHNSWBuildAccelerator) candidateSearchGraphGroup(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
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

func (a *CudaHNSWBuildAccelerator) Close() error {
	if a.device != nil {
		a.device.Release()
		a.device = nil
	}
	return nil
}
