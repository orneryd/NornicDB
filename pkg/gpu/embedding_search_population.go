package gpu

import "sync/atomic"

// SearchWithPopulation returns Search's exact top-k results and the number of
// vectors in the same read-locked snapshot. All vectors qualify for this
// unfiltered search. The receipt is meaningful only when err is nil.
func (ei *EmbeddingIndex) SearchWithPopulation(query []float32, k int) ([]SearchResult, int, error) {
	return ei.searchWithPopulation(query, k, nil)
}

// SearchWithThresholdPopulation returns exact top-k threshold-qualified results
// and their population in the same read-locked scan. The population is capped
// at k+1: that many qualifying matches already prove that k cannot exhaust the
// query. A count <= k is exact. GPU backends supply one extra result; the CPU
// fallback ranks only qualified indices from its existing score array. Neither
// needs another vector scan or a download of the whole GPU result population.
func (ei *EmbeddingIndex) SearchWithThresholdPopulation(query []float32, k int, minSimilarity float64) ([]SearchResult, int, error) {
	return ei.searchWithPopulation(query, k, &minSimilarity)
}

func (ei *EmbeddingIndex) searchWithPopulation(query []float32, k int, minSimilarity *float64) ([]SearchResult, int, error) {
	if len(query) != ei.dimensions {
		return nil, 0, ErrInvalidDimensions
	}
	ei.mu.RLock()
	defer ei.mu.RUnlock()
	population := len(ei.nodeIDs)
	if population == 0 {
		return nil, 0, nil
	}
	useGPU := ei.manager != nil && ei.manager.IsEnabled() && ei.gpuSynced
	var results []SearchResult
	var err error
	if useGPU {
		depth := k
		if minSimilarity != nil && depth < population {
			depth++
		}
		results, err = ei.searchGPU(query, depth)
	} else {
		results, population, err = ei.searchCPUWithPopulation(query, k, minSimilarity)
	}
	if err != nil {
		return nil, 0, err
	}
	if minSimilarity != nil && useGPU {
		qualified := results[:0]
		for _, r := range results {
			if float64(r.Score) >= *minSimilarity {
				qualified = append(qualified, r)
			}
		}
		population = len(qualified)
		results = qualified[:min(k, len(qualified))]
	}
	return results, population, nil
}

// searchCPU performs similarity search on CPU (fallback).
func (ei *EmbeddingIndex) searchCPU(query []float32, k int) ([]SearchResult, error) {
	results, _, err := ei.searchCPUWithPopulation(query, k, nil)
	return results, err
}

// searchCPUWithPopulation owns the fallback scan and its coverage receipt.
// Its caller holds mu. Threshold qualification uses the computed score array,
// keeping the cosine loop unchanged and excluding ineligible indices before
// selection. A receipt counts qualified indices before the result window cut.
func (ei *EmbeddingIndex) searchCPUWithPopulation(query []float32, k int, minSimilarity *float64) ([]SearchResult, int, error) {
	atomic.AddInt64(&ei.searchesCPU, 1)

	n := len(ei.nodeIDs)
	if k > n {
		k = n
	}

	// Compute all similarities
	scores := make([]float32, n)
	for i := 0; i < n; i++ {
		start := i * ei.dimensions
		end := start + ei.dimensions
		scores[i] = cosineSimilarityFlat(query, ei.cpuVectors[start:end])
	}

	// Build the selection population once. Below-threshold vectors cannot
	// enter the returned window, so they need no ranking comparisons.
	indices := make([]int, 0, n)
	if minSimilarity == nil {
		indices = indices[:n]
		for i := range indices {
			indices[i] = i
		}
	} else {
		minimum := *minSimilarity
		for i, score := range scores {
			if float64(score) >= minimum {
				indices = append(indices, i)
			}
		}
	}
	population := len(indices)
	if minSimilarity != nil {
		population = min(population, k+1)
	}
	k = min(k, len(indices))

	// Select the exact top-k of the eligible population.
	partialSort(indices, scores, k)

	// Build results
	results := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		idx := indices[i]
		results[i] = SearchResult{
			ID:       ei.nodeIDs[idx],
			Score:    scores[idx],
			Distance: 1 - scores[idx],
		}
	}

	return results, population, nil
}

// partialSort performs partial quicksort to get top-k elements.
func partialSort(indices []int, scores []float32, k int) {
	if k >= len(indices) {
		// Full sort needed
		for i := 0; i < len(indices)-1; i++ {
			for j := i + 1; j < len(indices); j++ {
				if scores[indices[j]] > scores[indices[i]] {
					indices[i], indices[j] = indices[j], indices[i]
				}
			}
		}
		return
	}

	// Simple partial sort: just get top-k
	for i := 0; i < k; i++ {
		maxIdx := i
		for j := i + 1; j < len(indices); j++ {
			if scores[indices[j]] > scores[indices[maxIdx]] {
				maxIdx = j
			}
		}
		indices[i], indices[maxIdx] = indices[maxIdx], indices[i]
	}
}
