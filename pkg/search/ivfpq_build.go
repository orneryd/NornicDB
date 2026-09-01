package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/util"
)

const ivfpqTrainSeed = 42

// BuildIVFPQFromVectorStore builds a full IVF/PQ index from VectorFileStore.
func BuildIVFPQFromVectorStore(ctx context.Context, vfs *VectorFileStore, profile IVFPQProfile, seedDocIDs []string) (*IVFPQIndex, *IVFPQBuildStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if vfs == nil {
		return nil, nil, localizedError(localization.SearchIVFPQVectorStoreRequired(), nil)
	}
	if profile.Dimensions <= 0 {
		return nil, nil, localizedError(localization.SearchIVFPQDimensionsInvalid(), nil)
	}
	if profile.PQSegments <= 0 || profile.Dimensions%profile.PQSegments != 0 {
		return nil, nil, localizedError(localization.SearchIVFPQSegmentsInvalid(profile.Dimensions, profile.PQSegments), nil)
	}
	start := time.Now()
	rng := rand.New(rand.NewSource(ivfpqTrainSeed))

	training, err := ivfpqCollectTrainingSample(ctx, vfs, profile.TrainingSampleMax, seedDocIDs, rng)
	if err != nil {
		return nil, nil, err
	}
	if len(training) < profile.IVFLists {
		return nil, nil, localizedError(localization.SearchIVFPQTrainingVectorsInsufficient(len(training), profile.IVFLists), nil)
	}

	centroids, err := ivfpqTrainKMeans(ctx, training, profile.IVFLists, profile.KMeansMaxIterations, rng)
	if err != nil {
		return nil, nil, localizedError(localization.SearchIVFCoarseTrainingFailed(err), err)
	}
	centroidNorm := normalizeCentroids(centroids)
	codebooks, err := ivfpqTrainPQCodebooks(ctx, training, centroids, profile, rng)
	if err != nil {
		return nil, nil, localizedError(localization.SearchPQCodebookTrainingFailed(err), err)
	}

	lists := make([]ivfpqList, profile.IVFLists)
	for i := range lists {
		lists[i].CodeSize = profile.PQSegments
	}
	vectorCount := 0
	if err := vfs.IterateChunked(4096, func(ids []string, vecs [][]float32) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range ids {
			listID := nearestCentroidIndexNormalized(vecs[i], centroidNorm)
			code := encodePQResidual(vecs[i], centroids[listID], codebooks)
			lists[listID].IDs = append(lists[listID].IDs, ids[i])
			lists[listID].appendCode(code)
			vectorCount++
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}

	idx := &IVFPQIndex{
		profile:         profile,
		centroids:       centroids,
		centroidNorm:    centroidNorm,
		codebooks:       codebooks,
		lists:           lists,
		formatVersion:   ivfpqBundleFormatVersion,
		builtAtUnixNano: time.Now().UnixNano(),
	}
	idx.initScratchPool()
	stats := &IVFPQBuildStats{
		VectorCount:         vectorCount,
		TrainingSampleCount: len(training),
		ListCount:           len(lists),
		AvgListSize:         ivfpqAvgListSize(lists),
		MaxListSize:         ivfpqMaxListSize(lists),
		BytesPerVector:      ivfpqBytesPerVector(profile),
		BuildDuration:       time.Since(start),
	}
	return idx, stats, nil
}

func ivfpqCollectTrainingSample(ctx context.Context, vfs *VectorFileStore, maxSample int, seedDocIDs []string, rng *rand.Rand) ([][]float32, error) {
	if maxSample <= 0 {
		maxSample = 1000
	}
	sample := make([][]float32, 0, maxSample)
	seenSeed := make(map[string]struct{}, len(seedDocIDs))
	for _, id := range seedDocIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if id == "" {
			continue
		}
		vec, ok := vfs.GetVector(id)
		if !ok || len(vec) == 0 {
			continue
		}
		seenSeed[id] = struct{}{}
		sample = append(sample, append([]float32(nil), vec...))
		if len(sample) >= maxSample {
			return sample[:maxSample], nil
		}
	}

	seen := 0
	err := vfs.IterateChunked(4096, func(ids []string, vecs [][]float32) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range ids {
			if _, ok := seenSeed[ids[i]]; ok {
				continue
			}
			seen++
			vecCopy := append([]float32(nil), vecs[i]...)
			if len(sample) < maxSample {
				sample = append(sample, vecCopy)
				continue
			}
			j := rng.Intn(seen)
			if j < maxSample {
				sample[j] = vecCopy
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sample, nil
}

func ivfpqTrainPQCodebooks(ctx context.Context, training [][]float32, centroids [][]float32, profile IVFPQProfile, rng *rand.Rand) ([]ivfpqCodebook, error) {
	subDim := profile.Dimensions / profile.PQSegments
	codewords := 1 << profile.PQBits
	centroidNorm := normalizeCentroids(centroids)
	segments := make([][]float32, profile.PQSegments)
	for seg := range segments {
		segments[seg] = make([]float32, 0, util.SafePreallocProduct(len(training), subDim))
	}
	for _, vec := range training {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		coarse := nearestCentroidIndexNormalized(vec, centroidNorm)
		for seg := 0; seg < profile.PQSegments; seg++ {
			start := seg * subDim
			for d := 0; d < subDim; d++ {
				segments[seg] = append(segments[seg], vec[start+d]-centroids[coarse][start+d])
			}
		}
	}

	out := make([]ivfpqCodebook, profile.PQSegments)
	for seg := 0; seg < profile.PQSegments; seg++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vectors := flatToVectors(segments[seg], subDim)
		if len(vectors) == 0 {
			return nil, fmt.Errorf("empty segment training set for segment %d", seg)
		}
		if len(vectors) < codewords {
			for len(vectors) < codewords {
				vectors = append(vectors, append([]float32(nil), vectors[rng.Intn(len(vectors))]...))
			}
		}
		centers, err := ivfpqTrainKMeans(ctx, vectors, codewords, profile.KMeansMaxIterations, rng)
		if err != nil {
			return nil, err
		}
		out[seg] = ivfpqCodebook{SubDim: subDim, Codeword: centers}
	}
	return out, nil
}

func ivfpqTrainKMeans(ctx context.Context, vectors [][]float32, k, maxIter int, rng *rand.Rand) ([][]float32, error) {
	if len(vectors) == 0 {
		return nil, fmt.Errorf("no vectors for kmeans")
	}
	if k <= 0 {
		return nil, fmt.Errorf("invalid k")
	}
	if k > len(vectors) {
		k = len(vectors)
	}
	if maxIter <= 0 {
		maxIter = 10
	}
	dim := len(vectors[0])
	centroids := make([][]float32, k)
	chosen := make(map[int]struct{}, k)
	for i := 0; i < k; i++ {
		for {
			idx := rng.Intn(len(vectors))
			if _, ok := chosen[idx]; ok {
				continue
			}
			chosen[idx] = struct{}{}
			centroids[i] = append([]float32(nil), vectors[idx]...)
			break
		}
	}

	assign := make([]int, len(vectors))
	vectorNorm := make([][]float32, len(vectors))
	for i := range vectors {
		vectorNorm[i] = vector.Normalize(vectors[i])
	}
	centroidNorm := normalizeCentroids(centroids)
	for iter := 0; iter < maxIter; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changed := false
		for i := range vectors {
			best := nearestCentroidIndexFromNormalized(vectorNorm[i], centroidNorm)
			if assign[i] != best {
				assign[i] = best
				changed = true
			}
		}
		if !changed && iter > 0 {
			break
		}
		sums := make([][]float64, k)
		counts := make([]int, k)
		for i := 0; i < k; i++ {
			sums[i] = make([]float64, dim)
		}
		for i := range vectors {
			c := assign[i]
			counts[c]++
			for d := 0; d < dim; d++ {
				sums[c][d] += float64(vectors[i][d])
			}
		}
		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				centroids[c] = append([]float32(nil), vectors[rng.Intn(len(vectors))]...)
				continue
			}
			for d := 0; d < dim; d++ {
				centroids[c][d] = float32(sums[c][d] / float64(counts[c]))
			}
		}
		centroidNorm = normalizeCentroids(centroids)
	}
	return centroids, nil
}

func encodePQResidual(vec []float32, centroid []float32, codebooks []ivfpqCodebook) []byte {
	out := make([]byte, len(codebooks))
	offset := 0
	for seg := 0; seg < len(codebooks); seg++ {
		subDim := codebooks[seg].SubDim
		best := 0
		bestDist := math.MaxFloat64
		for cIdx, codeword := range codebooks[seg].Codeword {
			dist := 0.0
			for d := 0; d < subDim; d++ {
				resid := float64(vec[offset+d] - centroid[offset+d] - codeword[d])
				dist += resid * resid
			}
			if dist < bestDist {
				bestDist = dist
				best = cIdx
			}
		}
		out[seg] = byte(best)
		offset += subDim
	}
	return out
}

func nearestCentroidIndexNormalized(vec []float32, centroids [][]float32) int {
	return nearestCentroidIndexFromNormalized(vector.Normalize(vec), centroids)
}

func nearestCentroidIndexFromNormalized(vecNorm []float32, centroidNorm [][]float32) int {
	bestIdx := 0
	bestScore := math.Inf(-1)
	for i := range centroidNorm {
		score := float64(vector.DotProduct(vecNorm, centroidNorm[i]))
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}
	return bestIdx
}

func nearestCentroidIndexCosine(vec []float32, centroids [][]float32) int {
	bestIdx := 0
	bestScore := math.Inf(-1)
	norm := vector.Normalize(vec)
	for i := range centroids {
		score := float64(vector.DotProduct(norm, vector.Normalize(centroids[i])))
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}
	return bestIdx
}

func normalizeCentroids(centroids [][]float32) [][]float32 {
	out := make([][]float32, len(centroids))
	for i := range centroids {
		out[i] = vector.Normalize(centroids[i])
	}
	return out
}

func flatToVectors(flat []float32, dim int) [][]float32 {
	if dim <= 0 || len(flat) < dim {
		return nil
	}
	n := len(flat) / dim
	out := make([][]float32, 0, n)
	for i := 0; i < n; i++ {
		start := i * dim
		out = append(out, append([]float32(nil), flat[start:start+dim]...))
	}
	return out
}

func ivfpqAvgListSize(lists []ivfpqList) float64 {
	if len(lists) == 0 {
		return 0
	}
	total := 0
	for i := range lists {
		total += len(lists[i].IDs)
	}
	return float64(total) / float64(len(lists))
}

func ivfpqMaxListSize(lists []ivfpqList) int {
	maxSize := 0
	for i := range lists {
		if len(lists[i].IDs) > maxSize {
			maxSize = len(lists[i].IDs)
		}
	}
	return maxSize
}

func ivfpqBytesPerVector(profile IVFPQProfile) float64 {
	if profile.PQSegments <= 0 {
		return 0
	}
	// PQ code bytes + coarse list id (uint16-ish accounting).
	return float64(profile.PQSegments + 2)
}

// ivfpqTopCentroidsByQuery returns closest IVF lists using an allocation-light top-k insert.
func ivfpqTopCentroidsByQuery(queryNorm []float32, centroidNorm [][]float32, nprobe int) []int {
	if nprobe <= 0 {
		nprobe = 1
	}
	if nprobe > len(centroidNorm) {
		nprobe = len(centroidNorm)
	}
	if nprobe == 0 {
		return nil
	}
	bestIdx := make([]int, nprobe)
	bestScore := make([]float64, nprobe)
	for i := 0; i < nprobe; i++ {
		bestIdx[i] = -1
		bestScore[i] = math.Inf(-1)
	}
	for i := range centroidNorm {
		score := vector.DotProduct(queryNorm, centroidNorm[i])
		if score <= bestScore[nprobe-1] {
			continue
		}
		pos := nprobe - 1
		for pos > 0 && score > bestScore[pos-1] {
			bestScore[pos] = bestScore[pos-1]
			bestIdx[pos] = bestIdx[pos-1]
			pos--
		}
		bestScore[pos] = score
		bestIdx[pos] = i
	}
	out := make([]int, 0, nprobe)
	for i := 0; i < nprobe; i++ {
		if bestIdx[i] >= 0 {
			out = append(out, bestIdx[i])
		}
	}
	return out
}
