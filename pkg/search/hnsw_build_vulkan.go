//go:build !cgovulkan

package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/gpu/vulkan"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// VulkanHNSWBuildAccelerator uses Vulkan compute shaders for HNSW
// construction candidate search. The CPU still performs all graph mutation
// and reciprocal linking, so persisted artifacts remain compatible with
// CPU-built indexes.
type VulkanHNSWBuildAccelerator struct {
	device              *vulkan.Device
	compute             *vulkan.ComputeContext
	dim                 int
	maxFrontierPerShard int
	maxQueriesPerShard  int
	resident            residentBuildVectors[*vulkan.Buffer]
}

// NewVulkanHNSWBuildAccelerator creates a Vulkan-backed HNSW build accelerator.
func NewVulkanHNSWBuildAccelerator() (*VulkanHNSWBuildAccelerator, error) {
	if !vulkan.IsAvailable() {
		return nil, localizedError(localization.SearchVulkanNotAvailable(), vulkan.ErrVulkanNotAvailable)
	}
	device, err := vulkan.NewDevice(0)
	if err != nil {
		return nil, err
	}
	compute, err := device.NewComputeContext()
	if err != nil {
		device.Release()
		return nil, err
	}
	return &VulkanHNSWBuildAccelerator{
		device:              device,
		compute:             compute,
		maxFrontierPerShard: 65536,
		maxQueriesPerShard:  512,
	}, nil
}

func (a *VulkanHNSWBuildAccelerator) Prepare(dim int, _ int) error {
	if dim <= 0 {
		return localizedError(localization.SearchHNSWGPUBuildDimensionInvalid(dim), nil)
	}
	a.resident.close()
	a.dim = dim
	a.resident = residentBuildVectors[*vulkan.Buffer]{
		dimensions: dim,
		upload:     func(flat []float32) (*vulkan.Buffer, error) { return a.device.NewBuffer(flat) },
		score:      a.scoreResidentBuffer,
	}
	return nil
}

func (a *VulkanHNSWBuildAccelerator) CandidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int) ([][]int, [][]float32, error) {
	return a.candidateSearch(ctx, queries, frontier, topK, true)
}

func (a *VulkanHNSWBuildAccelerator) candidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int, wantDistances bool) ([][]int, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || len(frontier) == 0 {
		return make([][]int, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if a.device == nil || a.compute == nil {
		return nil, nil, localizedError(localization.SearchVulkanDeviceNotInitialized(), nil)
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
		end := start + shardSize
		if end > len(frontier) {
			end = len(frontier)
		}
		flat = flattenHNSWBuildFrontierInto(flat[:0], frontier[start:end], a.dim)
		if len(flat) == 0 {
			continue
		}
		frontierBuf, err := a.device.NewBuffer(flat)
		if err != nil {
			return nil, nil, err
		}
		indices, distances, err := a.scoreResidentBuffer(ctx, frontierBuf, end-start, queries, min(topK, end-start))
		if err != nil {
			frontierBuf.Release()
			return nil, nil, err
		}
		for qi, row := range indices {
			for j, id := range row {
				merged[qi] = append(merged[qi], hnswCandidateDistance{index: start + int(id), dist: distances[qi][j]})
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

func (a *VulkanHNSWBuildAccelerator) CandidateSearchGraph(ctx context.Context, queries [][]float32, graph *hnswBuildGraphSnapshot, topK int) ([][]uint32, [][]float32, error) {
	if topK <= 0 || len(queries) == 0 || graph == nil || !graph.hasEntryPoint || len(graph.vectors) == 0 {
		return make([][]uint32, len(queries)), make([][]float32, len(queries)), ctx.Err()
	}
	if a.device == nil || a.compute == nil {
		return nil, nil, localizedError(localization.SearchVulkanDeviceNotInitialized(), nil)
	}
	if topK > 256 {
		topK = 256
	}
	return candidateSearchGraphBatched(ctx, a, queries, graph, topK)
}

func (a *VulkanHNSWBuildAccelerator) Close() error {
	a.resident.close()
	if a.compute != nil {
		a.compute.Release()
		a.compute = nil
	}
	if a.device != nil {
		a.device.Release()
		a.device = nil
	}
	return nil
}

// ResidentReady reports whether device buffers can be retained for this build.
func (a *VulkanHNSWBuildAccelerator) ResidentReady() bool { return a.device != nil && a.dim > 0 }

// AppendInserted uploads only the newly inserted normalized vectors.
func (a *VulkanHNSWBuildAccelerator) AppendInserted(vecs [][]float32) error {
	return a.resident.append(vecs)
}

// CandidateSearchInserted scores resident vectors without re-uploading the corpus.
func (a *VulkanHNSWBuildAccelerator) CandidateSearchInserted(ctx context.Context, queries [][]float32, topK int) ([][]uint32, [][]float32, error) {
	return a.resident.search(ctx, queries, topK)
}

func (a *VulkanHNSWBuildAccelerator) scoreResidentBuffer(ctx context.Context, frontierBuf *vulkan.Buffer, frontierN int, queries [][]float32, topK int) ([][]uint32, [][]float32, error) {
	ids := make([][]uint32, len(queries))
	distances := make([][]float32, len(queries))
	// Bound the score matrix as well as the number of submitted queries.
	chunk := max(1, min(512, hnswBuildScoreFloats/frontierN))
	for qStart := 0; qStart < len(queries); qStart += chunk {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		qEnd := min(qStart+chunk, len(queries))
		flatQueries := flattenHNSWBuildQueriesInto(nil, queries[qStart:qEnd], a.dim)
		queryBuf, err := a.device.NewBuffer(flatQueries)
		if err != nil {

			return nil, nil, err
		}

		vectorCount := uint32(frontierN)
		queryN := uint32(qEnd - qStart)
		outCount := uint64(queryN) * uint64(topK)

		scoresBuf, err := a.device.NewEmptyBuffer(uint64(vectorCount) * uint64(queryN))
		if err != nil {
			queryBuf.Release()

			return nil, nil, err
		}
		indicesBuf, err := a.device.NewEmptyBuffer(outCount)
		if err != nil {
			scoresBuf.Release()
			queryBuf.Release()

			return nil, nil, err
		}
		topkScoresBuf, err := a.device.NewEmptyBuffer(outCount)
		if err != nil {
			indicesBuf.Release()
			scoresBuf.Release()
			queryBuf.Release()

			return nil, nil, err
		}

		err = a.compute.HNSWBuildTopK(frontierBuf, queryBuf, scoresBuf, indicesBuf, topkScoresBuf, vectorCount, queryN, uint32(a.dim), topK)
		queryBuf.Release()
		if err != nil {
			topkScoresBuf.Release()
			indicesBuf.Release()
			scoresBuf.Release()

			return nil, nil, err
		}

		indices := indicesBuf.ReadUint32(int(outCount))
		scores := topkScoresBuf.ReadFloat32(int(outCount))

		topkScoresBuf.Release()
		indicesBuf.Release()
		scoresBuf.Release()

		for qi := qStart; qi < qEnd; qi++ {
			for j := 0; j < topK; j++ {
				at := (qi-qStart)*topK + j
				if at < len(indices) && at < len(scores) && uint64(indices[at]) < uint64(frontierN) {
					ids[qi] = append(ids[qi], uint32(indices[at]))
					distances[qi] = append(distances[qi], 1-scores[at])
				}
			}
		}
	}
	return ids, distances, nil
}
