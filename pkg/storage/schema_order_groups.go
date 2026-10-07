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
	return sm.VisitPropertyIndexGroupsInRange(label, property, descending, PropertyIndexBounds{}, visit)
}

// VisitPropertyIndexGroupsInRange is VisitPropertyIndexGroups starting and
// ending at bounds: groups whose keys fall outside them are skipped without
// being read, found by binary search where the keys allow it
// (sortedKeyRangeLocked). Bounds only skip groups that can't match; the
// caller still tests every visited node, and gets every group when the keys
// can't be searched. ORDER BY … LIMIT under a range or keyset predicate
// (WHERE n.t > $last …) starts at the bound instead of reading the whole
// index before it (#939).
func (sm *SchemaManager) VisitPropertyIndexGroupsInRange(label, property string, descending bool, bounds PropertyIndexBounds, visit func([]NodeID) bool) bool {
	sm.mu.RLock()
	idx, exists := sm.seekablePropertyIndexLocked(label, property)
	sm.mu.RUnlock()
	if !exists {
		return false
	}
	// Snapshot group membership before invoking callbacks so index mutations
	// cannot move an ID into a group that has yet to be visited.
	idx.mu.Lock()
	keys, _ := idx.sortedKeyRangeLocked(bounds)
	totalIDs := 0
	for _, key := range keys {
		totalIDs += len(idx.values[key])
	}
	ids := make([]NodeID, 0, totalIDs)
	groupEnds := make([]int, 0, len(keys))
	appendGroup := func(group []interface{}) {
		groupStart := len(ids)
		for _, key := range group {
			ids = append(ids, idx.values[key]...)
		}
		if len(ids) > groupStart {
			groupEnds = append(groupEnds, len(ids))
		}
	}
	if descending {
		for end := len(keys); end > 0; {
			start := end - 1
			for start > 0 && compareSchemaIndexValues(keys[start-1], keys[end-1]) == 0 {
				start--
			}
			appendGroup(keys[start:end])
			end = start
		}
	} else {
		for start := 0; start < len(keys); {
			end := start + 1
			for end < len(keys) && compareSchemaIndexValues(keys[start], keys[end]) == 0 {
				end++
			}
			appendGroup(keys[start:end])
			start = end
		}
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
