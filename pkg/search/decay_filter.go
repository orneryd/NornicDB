package search

import (
	"context"

	"github.com/orneryd/nornicdb/internal/queryprofile"
)

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

// nodeDecaySuppresses reports the actual invocation of the installed decay
// filter and returns its suppression decision. The filter is the installer's
// own function, so whatever database state it reads while deciding is not part
// of the reads observed around this call: one report immediately before the
// call is the complete record of that invocation. A nil filter is never
// invoked and reports nothing, and this reporting owner makes no decision of
// its own.
func nodeDecaySuppresses(ctx context.Context, filter NodeDecayFilterFunc, nodeID string) bool {
	if filter == nil {
		return false
	}
	queryprofile.Callback(ctx, "node decay filter", "")
	return filter(nodeID)
}

// filterDecayedCandidates removes suppressed candidates from the result pool.
func (s *Service) filterDecayedCandidates(ctx context.Context, results []indexResult) []indexResult {
	s.mu.RLock()
	filter := s.nodeDecayFilter
	s.mu.RUnlock()

	if filter == nil {
		return results
	}

	filtered := results[:0]
	for _, r := range results {
		if nodeDecaySuppresses(ctx, filter, r.ID) {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}
