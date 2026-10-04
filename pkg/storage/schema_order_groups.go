package storage

import (
	"sort"
)

// VisitPropertyIndexGroups visits non-null property-index entries in key order.
// Each callback receives the complete group of IDs whose keys compare equally,
// including differently typed numeric keys. Returning false stops the visit.
// IDs within a group are ordered by node identity. The callback runs without
// schema/index locks so it can read nodes or perform index operations safely.
// The return value reports whether the index exists, even when it is empty.
//
// Query planners use this to finish a primary-key tie before selecting a page
// by secondary ORDER BY keys. For example, a title index must visit every node
// with the boundary title before choosing the smallest timestamp/ID tuple.
func (sm *SchemaManager) VisitPropertyIndexGroups(label, property string, descending bool, visit func([]NodeID) bool) bool {
	sm.mu.RLock()
	idx, exists := sm.seekablePropertyIndexLocked(label, property)
	sm.mu.RUnlock()
	if !exists {
		return false
	}
	// Snapshot group membership before invoking callbacks so index mutations
	// cannot move an ID into a group that has yet to be visited.
	idx.mu.Lock()
	keys := idx.sortedKeysLocked()
	if descending {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	totalIDs := 0
	for _, key := range keys {
		totalIDs += len(idx.values[key])
	}
	ids := make([]NodeID, 0, totalIDs)
	groupEnds := make([]int, 0, len(keys))
	for start := 0; start < len(keys); {
		end := start + 1
		for end < len(keys) && compareSchemaIndexValues(keys[start], keys[end]) == 0 {
			end++
		}
		groupStart := len(ids)
		for _, key := range keys[start:end] {
			ids = append(ids, idx.values[key]...)
		}
		if len(ids) > groupStart {
			groupEnds = append(groupEnds, len(ids))
		}
		start = end
	}
	idx.mu.Unlock()

	groupStart := 0
	for _, groupEnd := range groupEnds {
		group := ids[groupStart:groupEnd]
		sort.Slice(group, func(i, j int) bool { return group[i] < group[j] })
		if !visit(group) {
			break
		}
		groupStart = groupEnd
	}
	return true
}
