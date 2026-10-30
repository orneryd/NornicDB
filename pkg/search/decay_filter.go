package search

// NodeDecayFilterFunc returns true if the given node ID should be suppressed
// (filtered out) due to knowledge-layer decay scoring. When nil, no decay
// filtering is applied to search results.
type NodeDecayFilterFunc func(nodeID string) bool

// SetNodeDecayFilter installs a decay visibility filter for search results.
// Candidates that the filter reports as suppressed are removed from vector
// and BM25 candidate pools before RRF fusion.
func (s *Service) SetNodeDecayFilter(fn NodeDecayFilterFunc) {
	s.mu.Lock()
	s.nodeDecayFilter = fn
	s.mu.Unlock()
}

// filterDecayedCandidates removes suppressed candidates from the result pool.
func (s *Service) filterDecayedCandidates(results []indexResult) []indexResult {
	s.mu.RLock()
	filter := s.nodeDecayFilter
	s.mu.RUnlock()

	if filter == nil {
		return results
	}

	filtered := results[:0]
	for _, r := range results {
		if filter(r.ID) {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}
