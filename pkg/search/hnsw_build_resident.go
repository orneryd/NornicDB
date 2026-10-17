package search

import "context"

// residentBuildVectors owns immutable device shards in insertion order. Each
// vector is uploaded once; device-specific kernels score existing buffers.
// Keeping this ordering and top-K merge shared makes CUDA/Vulkan return the
// same internal IDs and cosine-distance contract as the Metal resident path.
type residentBuildVectors[B interface{ Release() }] struct {
	dimensions int
	count      int
	shards     []residentBuildShard[B]
	upload     func([]float32) (B, error)
	score      func(context.Context, B, int, [][]float32, int) ([][]uint32, [][]float32, error)
}

type residentBuildShard[B interface{ Release() }] struct {
	buffer B
	count  int
	offset int
}

func (r *residentBuildVectors[B]) append(vecs [][]float32) error {
	if len(vecs) == 0 {
		return nil
	}
	for _, v := range vecs {
		if len(v) != r.dimensions {
			return ErrDimensionMismatch
		}
	}
	flat := flattenHNSWBuildQueriesInto(nil, vecs, r.dimensions)
	buffer, err := r.upload(flat)
	if err != nil {
		return err
	}
	r.shards = append(r.shards, residentBuildShard[B]{buffer: buffer, count: len(vecs), offset: r.count})
	r.count += len(vecs)
	return nil
}

func (r *residentBuildVectors[B]) search(ctx context.Context, queries [][]float32, topK int) ([][]uint32, [][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	topK = hnswBuildTopKLimit(topK)
	ids := make([][]uint32, len(queries))
	distances := make([][]float32, len(queries))
	if topK <= 0 || r.count == 0 {
		return ids, distances, nil
	}
	merged := make([][]hnswCandidateDistance, len(queries))
	for _, shard := range r.shards {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		indices, dists, err := r.score(ctx, shard.buffer, shard.count, queries, min(topK, shard.count))
		if err != nil {
			return nil, nil, err
		}
		for qi, row := range indices {
			for j, id := range row {
				if int(id) < shard.count && j < len(dists[qi]) {
					merged[qi] = append(merged[qi], hnswCandidateDistance{index: shard.offset + int(id), dist: dists[qi][j]})
				}
			}
			sortHNSWBuildCandidates(merged[qi])
			if len(merged[qi]) > topK {
				merged[qi] = merged[qi][:topK]
			}
		}
	}
	for qi, row := range merged {
		for _, item := range row {
			ids[qi] = append(ids[qi], uint32(item.index))
			distances[qi] = append(distances[qi], item.dist)
		}
	}
	return ids, distances, nil
}

func (r *residentBuildVectors[B]) close() {
	for _, shard := range r.shards {
		shard.buffer.Release()
	}
	r.shards = nil
	r.count = 0
}
