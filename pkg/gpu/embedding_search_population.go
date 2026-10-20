package gpu

// SearchWithPopulation returns Search's exact top-k results and the number of
// vectors in the same read-locked snapshot. All vectors qualify for this
// unfiltered search. The receipt is meaningful only when err is nil.
func (ei *EmbeddingIndex) SearchWithPopulation(query []float32, k int) ([]SearchResult, int, error) {
	return ei.searchWithPopulation(query, k, nil)
}

// SearchWithThresholdPopulation returns exact top-k threshold-qualified results
// and their population in the same read-locked scan. The population is capped
// at k+1: that many qualifying matches already prove that k cannot exhaust the
// query. A count <= k is exact. One extra backend result supplies this evidence
// without a second CPU scan or downloading the whole GPU result population.
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
	depth := k
	if minSimilarity != nil && depth < population {
		depth++
	}
	var results []SearchResult
	var err error
	if ei.manager != nil && ei.manager.IsEnabled() && ei.gpuSynced {
		results, err = ei.searchGPU(query, depth)
	} else {
		results, err = ei.searchCPU(query, depth)
	}
	if err != nil {
		return nil, 0, err
	}
	if minSimilarity != nil {
		qualified := results[:0]
		for _, r := range results {
			if float64(r.Score) >= *minSimilarity {
				qualified = append(qualified, r)
			}
		}
		population = len(qualified)
		results = qualified[:min(k, population)]
	}
	return results, population, nil
}
