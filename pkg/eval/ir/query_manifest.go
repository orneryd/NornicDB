package ir

import (
	"fmt"
	"math/rand/v2"
	"sort"
)

// SelectQueryIDs chooses at most limit unique query IDs without replacement.
// It sorts the input before sampling so output is independent of ingestion order.
func SelectQueryIDs(queryIDs []string, limit int, seed uint64) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("query limit must be positive")
	}
	unique := make(map[string]struct{}, len(queryIDs))
	for _, queryID := range queryIDs {
		if queryID == "" {
			return nil, fmt.Errorf("query ID must not be empty")
		}
		if _, exists := unique[queryID]; exists {
			return nil, fmt.Errorf("duplicate query ID %q", queryID)
		}
		unique[queryID] = struct{}{}
	}
	selected := append([]string(nil), queryIDs...)
	sort.Strings(selected)
	if len(selected) <= limit {
		return selected, nil
	}
	rng := rand.New(rand.NewPCG(seed, seed^0xd1b54a32d192ed03))
	for index := len(selected) - 1; index > 0; index-- {
		swap := rng.IntN(index + 1)
		selected[index], selected[swap] = selected[swap], selected[index]
	}
	selected = selected[:limit]
	sort.Strings(selected)
	return selected, nil
}
